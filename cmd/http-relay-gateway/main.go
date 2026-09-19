// Command http-relay-gateway is a general-purpose HTTP relay manager: one
// internal-network endpoint that forwards relay-spec requests
// (X-Relay-Target / X-Relay-Path) through a health-aware, round-robin pool
// of edge relays (Vercel / Deno Deploy / Cloudflare Worker apps), so any
// client can hide its egress IP without managing a relay list itself.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/gateway"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"

	"github.com/rs/zerolog"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "0.1.0-dev"

// fatalLog reports startup failures before any configuration is available.
// It is deliberately ungated (no global level is set yet) and writes to the
// same stdout stream as the runtime logger so all structured output stays on
// one stream for the json-file log driver.
var fatalLog = logging.New(os.Stdout)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version":
			fmt.Println(version)
			return
		case "healthcheck":
			os.Exit(healthcheck())
		}
	}
	if err := run(); err != nil {
		fatalLog.Error().Str("err", sanitize.ErrorString(err)).Msg("fatal")
		os.Exit(1)
	}
}

// healthcheck probes the bootstrap-configured listener. It deliberately does
// not read the runtime YAML or the database: a bad reload must not make a
// healthy, already running process fail Docker's health probe. The scratch
// image has no shell, so this subcommand is what the Docker HEALTHCHECK runs.
func healthcheck() int {
	bootstrap, err := config.LoadBootstrap()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bootstrap config:", sanitize.ErrorString(err))
		return 1
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(healthcheckURL(bootstrap.ListenAddr))
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", sanitize.ErrorString(err))
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		fmt.Fprintf(os.Stderr, "healthcheck: status %d, body %q\n", resp.StatusCode, body)
		return 1
	}
	return 0
}

func healthcheckURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Sprintf("healthcheckURL requires a validated host:port address: %q", addr))
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/healthz"}).String()
}

// generation is one immutable serving snapshot derived from the database:
// the runtime values the data plane reads plus the pool built from them.
type generation struct {
	cfg  *config.RuntimeConfig
	pool *pool.Pool
}

func run() error {
	bootstrap, err := config.LoadBootstrap()
	if err != nil {
		return err
	}
	db, err := store.Open(bootstrap.DataFile)
	if err != nil {
		return fmt.Errorf("open data store: %w", err)
	}
	defer func() { _ = db.Close() }()

	log := setupDynamicLogger(initialLogLevel(db))

	// Legacy config bridge (transitional): on first boot an empty database
	// adopts the YAML wholesale; on later boots the YAML resyncs its own
	// relays and settings. A missing file is fine — the database stands
	// alone; a broken file never touches the database, so the last-known
	// good state keeps serving.
	empty, err := db.IsEmpty()
	if err != nil {
		return fmt.Errorf("inspect data store: %w", err)
	}
	if _, err := os.Stat(bootstrap.ConfigFile); err == nil {
		imported, err := syncLegacy(bootstrap.ConfigFile, db)
		switch {
		case err != nil && empty:
			// Nothing to serve from and nothing adoptable: fail fast, exactly
			// like a bad startup config did before the database existed.
			return fmt.Errorf("adopt legacy config: %w", err)
		case err != nil:
			log.Warn().Str("error", sanitize.ErrorString(err)).Msg("legacy config invalid; serving from database")
		case imported > 0:
			log.Info().Int("relays", imported).Msg("imported legacy config into database")
		}
	} else if empty {
		return fmt.Errorf("no legacy config at %q and empty database at %q: provide either to define relays", bootstrap.ConfigFile, bootstrap.DataFile)
	} else {
		log.Info().Str("path", bootstrap.ConfigFile).Msg("legacy config absent; serving from database")
	}

	// The gateway publishes one immutable generation (validated config + pool
	// snapshot). Handlers load it once per request; any change swaps the
	// whole generation atomically and in-flight requests finish on their own.
	gen, err := buildGeneration(db)
	if err != nil {
		return err
	}
	if len(gen.cfg.Relays) == 0 {
		return errors.New("no serving relays in the database")
	}
	g := gateway.New(&gateway.State{Cfg: gen.cfg, Pool: gen.pool}, version, log)

	ln, err := net.Listen("tcp", bootstrap.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", bootstrap.ListenAddr, err)
	}
	srv := &http.Server{
		Handler:           g,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info().Str("addr", ln.Addr().String()).Str("version", version).
			Int("relays", len(gen.cfg.Relays)).Msg("relay-gateway listening")
		// Serve returns nil once shutdown closes the listener.
		if err := srv.Serve(ln); err != nil {
			errCh <- fmt.Errorf("listener: %w", err)
		}
	}()

	poller := config.NewPoller(bootstrap.ConfigFile, config.DefaultPollInterval, log)
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()
	go poller.Run(pollCtx, log)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Every database mutation — the YAML bridge here, the admin plane later —
	// converges on this one apply path: rebuild the generation from the
	// database and swap it atomically.
	apply := func(source string) {
		next, err := buildGeneration(db)
		if err != nil {
			log.Warn().Str("source", source).Str("error", sanitize.ErrorString(err)).
				Msg("generation build failed; keeping previous configuration")
			return
		}
		g.Swap(&gateway.State{Cfg: next.cfg, Pool: next.pool})
		// Only this goroutine writes the global level, so the package-global
		// atomic swap is race-free and every future event picks it up.
		zerolog.SetGlobalLevel(parseZerologLevel(next.cfg.LogLevel))
		log.Info().Str("source", source).Int("relays", len(next.cfg.Relays)).Msg("configuration reloaded")
	}

	// The poller hash-gates on applied content, so one signal means one
	// distinct file change; the sync either lands in the database (which
	// notifies the apply loop) or leaves the old generation untouched.
	reload := func(source string) {
		if _, err := syncLegacy(bootstrap.ConfigFile, db); err != nil {
			log.Warn().Str("source", source).Str("error", sanitize.ErrorString(err)).
				Msg("reload failed; keeping previous configuration")
		}
	}

	// The startup sync already flowed into the initial generation; drop its
	// pending wakeup so the loop does not rebuild redundantly.
	select {
	case <-db.Changes():
	default:
	}

	for {
		select {
		case err := <-errCh:
			shutdownServer(srv, bootstrap.ShutdownGrace)
			return err
		case sig := <-sigCh:
			log.Info().Str("signal", sig.String()).Str("grace", bootstrap.ShutdownGrace.String()).Msg("shutting down")
			shutdownServer(srv, bootstrap.ShutdownGrace)
			return nil
		case <-poller.Changes():
			reload("poll")
		case <-db.Changes():
			apply("database")
		}
	}
}

// syncLegacy parses the legacy YAML and writes it into the database in one
// transaction. Parse or validation failures return an error before the
// database is touched, so a broken edit can never reach the pool.
func syncLegacy(path string, db *store.Store) (int, error) {
	cfg, err := config.LoadRuntime(path)
	if err != nil {
		return 0, err
	}
	providers := make([]store.ProviderRow, 0, len(cfg.Providers))
	for name, spec := range cfg.Providers {
		providers = append(providers, store.ProviderRow{Name: name, MaxBody: spec.MaxBody})
	}
	relays := make([]store.LegacyRelay, 0, len(cfg.Relays))
	for _, relay := range cfg.Relays {
		relays = append(relays, store.LegacyRelay{
			Name:     relay.Name,
			Provider: relay.Provider,
			URL:      relay.URL.String(),
			Active:   relay.Active,
		})
	}
	return db.SyncLegacy(store.RuntimeValues{
		LogLevel:         cfg.LogLevel,
		MaxRetries:       cfg.MaxRetries,
		FailureThreshold: cfg.FailureThreshold,
		CooldownMs:       cfg.Cooldown.Milliseconds(),
	}, providers, relays)
}

// buildGeneration reads the database and derives the next immutable serving
// generation — the single place pool state is ever built from.
func buildGeneration(db *store.Store) (generation, error) {
	settings, err := db.Settings()
	if err != nil {
		return generation{}, fmt.Errorf("read settings: %w", err)
	}
	providers, err := db.Providers()
	if err != nil {
		return generation{}, fmt.Errorf("read providers: %w", err)
	}
	rows, err := db.Relays()
	if err != nil {
		return generation{}, fmt.Errorf("read relays: %w", err)
	}

	cfg := &config.RuntimeConfig{
		LogLevel:         settings.LogLevel,
		MaxRetries:       settings.MaxRetries,
		FailureThreshold: settings.FailureThreshold,
		Cooldown:         time.Duration(settings.CooldownMs) * time.Millisecond,
		Providers:        make(map[string]config.ProviderSpec, len(providers)),
		Relays:           make([]config.RelaySpec, 0, len(rows)),
	}
	for _, provider := range providers {
		cfg.Providers[provider.Name] = config.ProviderSpec{MaxBody: provider.MaxBody}
	}
	for _, row := range rows {
		if spec, ok := relaySpec(row); ok {
			cfg.Relays = append(cfg.Relays, spec)
		}
	}
	p, err := pool.New(cfg)
	if err != nil {
		return generation{}, err
	}
	return generation{cfg: cfg, pool: p}, nil
}

// relaySpec maps a database relay row into the pool's relay spec. Legacy
// relays always appear (an inactive one shows in /stats as inactive); a
// managed relay joins only through its verified active deployment, whose URL
// is the stable platform domain rather than the placeholder relay row.
func relaySpec(row store.RelayRow) (config.RelaySpec, bool) {
	switch row.Origin {
	case store.OriginLegacy:
		// active flag passes through verbatim.
	case store.OriginManaged:
		// Until Phase 5 wires auth tokens into the engine, managed relays
		// stay out of the pool entirely: there is no admin plane to create
		// them yet, so this branch exists to keep the mapping honest.
		if row.Deployment == nil {
			return config.RelaySpec{}, false
		}
		row.URL = row.Deployment.URL
		row.Active = true
	default:
		return config.RelaySpec{}, false
	}
	u, err := url.Parse(row.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return config.RelaySpec{}, false
	}
	return config.RelaySpec{
		Name:     row.Name,
		Provider: row.Provider,
		URL:      u,
		Active:   row.Active,
	}, true
}

// initialLogLevel reads just the log level so startup messages before the
// first generation honor the persisted setting.
func initialLogLevel(db *store.Store) string {
	settings, err := db.Settings()
	if err != nil {
		return config.DefaultLogLevel
	}
	return settings.LogLevel
}

// shutdownServer drains in-flight requests against one whole-process grace
// budget; expiry force-closes whatever is left.
func shutdownServer(server *http.Server, grace time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func parseZerologLevel(level string) zerolog.Level {
	switch level {
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}

// setupDynamicLogger installs the initial global level (the apply loop is
// the only other writer) and returns the process logger.
func setupDynamicLogger(level string) zerolog.Logger {
	zerolog.SetGlobalLevel(parseZerologLevel(level))
	return logging.New(os.Stdout)
}
