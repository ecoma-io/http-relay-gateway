// Command http-relay-gateway is a general-purpose HTTP relay manager: one
// internal-network endpoint that forwards relay-spec requests
// (X-Relay-Target / X-Relay-Path) through a health-aware, round-robin pool
// of edge relays (Vercel / Deno Deploy / Cloudflare Worker apps), so any
// client can hide its egress IP without managing a relay list itself.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"http-relay-gateway/internal/admin"
	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/gateway"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/readiness"
	"http-relay-gateway/internal/reconcile"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"
	"http-relay-gateway/internal/web"

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

// healthcheck probes the bootstrap-configured data-plane listener. It
// deliberately does not read the database: a broken database or a pending
// first-run setup must not fail Docker's health probe for a serving process.
// The scratch image has no shell, so this subcommand is what the Docker
// HEALTHCHECK runs.
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
// the fully resolved gateway state (pool, client, knobs) plus the values the
// apply loop reads outside the hot path.
type generation struct {
	state      *gateway.State
	logLevel   string
	relays     int
	configured int // database relay rows, ready or not — drives the startup warning
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

	// The readiness registry is the admissions gate for the data plane: a
	// relay joins the serving pool only after the reconciler has verified it
	// end to end. Seeded from the rows already in the database, then fed by
	// the reconciler's verify pass.
	reg := readiness.New(readiness.Config{
		BackoffBase: envDuration("RELAY_VERIFY_BACKOFF_BASE", 5*time.Second),
		BackoffMax:  envDuration("RELAY_VERIFY_BACKOFF_MAX", 5*time.Minute),
		RecoverMax:  envDuration("RELAY_VERIFY_RECOVER_MAX", 15*time.Second),
		DemoteAfter: envInt("RELAY_VERIFY_DEMOTE_AFTER", 3),
	})
	// An empty database is a legal first boot: the admin plane serves the
	// setup flow while the data plane answers 503 until the first relay
	// exists. Only an unreadable database is fatal.
	gen, err := buildGeneration(db, reg)
	if err != nil {
		return err
	}
	if gen.configured == 0 {
		log.Warn().Msg("no relays configured; data plane returns 503 until a relay is created")
	} else if gen.relays == 0 {
		log.Warn().Msg("no relays ready; data plane returns 503 until a relay verifies end to end")
	}
	g := gateway.New(gen.state, version, deploy.RelayVersion, log)

	// The fleet worker owns every deploy: startup probes, the periodic
	// reconcile (interval read fresh each cycle so a settings change lands
	// without a restart), and the admin plane's queue-and-return actions.
	factory := deploy.NewFactory(nil)
	rec := reconcile.Start(db, factory, log, func() int64 {
		settings, err := db.Settings()
		if err != nil {
			return store.DefaultReconcileIntervalSeconds
		}
		return settings.ReconcileIntervalSeconds
	}, reg)
	defer rec.Stop()
	spa, err := web.Handler()
	if err != nil {
		return fmt.Errorf("load management UI: %w", err)
	}
	var adminOpts []admin.Option
	if bootstrap.AdminCookieSecure {
		adminOpts = append(adminOpts, admin.WithSecureCookie())
	}
	adminOpts = append(adminOpts, admin.WithFleet(rec, factory), admin.WithReadiness(reg))
	adminHandler := admin.New(db, version, log, spa, adminOpts...)

	ln, err := net.Listen("tcp", bootstrap.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", bootstrap.ListenAddr, err)
	}
	srv := &http.Server{
		Handler:           g,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	adminLn, err := net.Listen("tcp", bootstrap.AdminAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", bootstrap.AdminAddr, err)
	}
	adminSrv := &http.Server{
		Handler:           adminHandler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info().Str("addr", ln.Addr().String()).Str("version", version).
			Int("relays", gen.relays).Msg("relay-gateway listening")
		// Serve returns nil once shutdown closes the listener.
		if err := srv.Serve(ln); err != nil {
			errCh <- fmt.Errorf("listener: %w", err)
		}
	}()
	go func() {
		log.Info().Str("addr", adminLn.Addr().String()).Msg("admin plane listening")
		if err := adminSrv.Serve(adminLn); err != nil {
			errCh <- fmt.Errorf("admin listener: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Every database mutation — the admin API here, the reconciler later —
	// converges on this one apply path: rebuild the generation from the
	// database and swap it atomically.
	apply := func(source string) {
		next, err := buildGeneration(db, reg)
		if err != nil {
			log.Warn().Str("source", source).Str("error", sanitize.ErrorString(err)).
				Msg("generation build failed; keeping previous configuration")
			return
		}
		g.Swap(next.state)
		// Only this goroutine writes the global level, so the package-global
		// atomic swap is race-free and every future event picks it up.
		zerolog.SetGlobalLevel(parseZerologLevel(next.logLevel))
		log.Info().Str("source", source).Int("relays", next.relays).Msg("configuration reloaded")
	}

	// The startup generation already reflects current state; drop its pending
	// wakeup so the loop does not rebuild redundantly.
	select {
	case <-db.Changes():
	default:
	}

	// shutdown cancels fleet work first: a deploy mid-flight must not eat the
	// whole-process drain budget, and the reconciler's database writes should
	// be over before the final generation is whatever the DB says.
	shutdown := func() {
		rec.Stop()
		shutdownAll([]*http.Server{srv, adminSrv}, bootstrap.ShutdownGrace)
	}

	for {
		select {
		case err := <-errCh:
			shutdown()
			return err
		case sig := <-sigCh:
			log.Info().Str("signal", sig.String()).Str("grace", bootstrap.ShutdownGrace.String()).Msg("shutting down")
			shutdown()
			return nil
		case <-db.Changes():
			apply("database")
		case <-reg.Changes():
			apply("readiness")
		}
	}
}

// buildGeneration reads the database and derives the next immutable serving
// generation — the single place pool state is ever built from. Everything
// the hot path would otherwise re-derive is resolved here: body limits,
// inherited header policies, tokens, transport timeouts.
func buildGeneration(db *store.Store, reg *readiness.Registry) (generation, error) {
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

	// Announce the current relay set to the registry: rows that appear here
	// (fresh database, deleted relay re-added, first boot) enter the mirror
	// as configured-but-unverified; rows that disappeared are dropped. This
	// never notifies — rebuilds only happen on real state transitions.
	present := make([]readiness.Key, 0, len(rows))
	for _, row := range rows {
		present = append(present, readiness.Key{Provider: row.Provider, Name: row.Name})
	}
	reg.Sync(present)

	providerLimits := make(map[string]int64, len(providers))
	providerPolicies := make(map[string]*pool.HeaderPolicy, len(providers))
	for _, provider := range providers {
		providerLimits[provider.Name] = provider.MaxBody
		policy, err := pool.ParseHeaderPolicy(provider.HeaderPolicy)
		if err != nil {
			return generation{}, fmt.Errorf("provider %s: %w", provider.Name, err)
		}
		providerPolicies[provider.Name] = policy
	}

	in := pool.Input{
		FailureThreshold: settings.FailureThreshold,
		Cooldown:         time.Duration(settings.CooldownMs) * time.Millisecond,
		Relays:           make([]pool.RelayInput, 0, len(rows)),
	}
	maxBuffer := int64(0)
	for _, row := range rows {
		ready := reg.IsReady(readiness.Key{Provider: row.Provider, Name: row.Name})
		relay, ok, err := relayInput(row, providerLimits, providerPolicies, ready)
		if err != nil {
			return generation{}, err
		}
		if !ok {
			continue
		}
		if relay.MaxBody > maxBuffer {
			maxBuffer = relay.MaxBody
		}
		in.Relays = append(in.Relays, relay)
	}
	p, err := pool.New(in)
	if err != nil {
		return generation{}, err
	}

	// The lifecycle snapshot rides every generation so /stats renders the
	// readiness state machine without reading the registry on the hot path.
	snapshot := reg.Snapshot()
	lifecycle := make([]gateway.LifecycleRow, 0, len(snapshot))
	for _, record := range snapshot {
		lifecycle = append(lifecycle, gateway.LifecycleRow{
			Name:     record.Key.Name,
			Provider: record.Key.Provider,
			State:    string(record.State),
			Reason:   record.Reason,
		})
	}
	state := &gateway.State{
		Pool: p,
		Client: gateway.NewClient(gateway.NewTransport(
			time.Duration(settings.DialTimeoutMs)*time.Millisecond,
			time.Duration(settings.ResponseHeaderTimeoutMs)*time.Millisecond)),
		MaxRetries:           settings.MaxRetries,
		MaxBufferBytes:       maxBuffer,
		StreamThresholdBytes: settings.StreamThresholdBytes,
		Lifecycle:            lifecycle,
	}
	return generation{state: state, logLevel: settings.LogLevel, relays: len(in.Relays), configured: len(rows)}, nil
}

// relayInput maps a database relay row into a resolved pool input. Legacy
// relays always appear (an inactive one shows in /stats as inactive). A
// managed relay serves from its own URL until a deployment exists — that is
// what an admin-created relay is before its first deploy — and once its
// verified active deployment joins, that deployment's stable URL and token
// take over and the relay is unconditionally active. Once a deployment
// exists but is not active — stale, unreachable, paused by its platform,
// failed — the deployment owns the relay's serving and it is not serving, so
// the row contributes nothing: neither an unverified deployment URL nor the
// tokenless relay-row placeholder may answer clients. The bool is false when
// the row contributes no pool entry.
func relayInput(row store.RelayRow, providerLimits map[string]int64, providerPolicies map[string]*pool.HeaderPolicy, ready bool) (pool.RelayInput, bool, error) {
	// The readiness gate is the admission authority: a relay serves only
	// after the reconciler verified it end to end. Everything below assumes
	// an admitted relay, so an unverified one contributes no pool entry —
	// neither a stale deployment URL nor the tokenless relay-row placeholder
	// may answer clients.
	if !ready {
		return pool.RelayInput{}, false, nil
	}
	switch row.Origin {
	case store.OriginLegacy:
		// active flag passes through verbatim; the registry only marks a
		// legacy relay ready after a forward probe proved it serves.
	case store.OriginManaged:
		if row.InactiveDeployment {
			return pool.RelayInput{}, false, nil
		}
		if row.Deployment != nil {
			row.URL = row.Deployment.URL
			row.Active = true
		}
	default:
		return pool.RelayInput{}, false, nil
	}
	u, err := url.Parse(row.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return pool.RelayInput{}, false, nil
	}
	relayPolicy, err := pool.ParseHeaderPolicy(row.HeaderPolicy)
	if err != nil {
		return pool.RelayInput{}, false, fmt.Errorf("relay %s: %w", row.Name, err)
	}
	token := ""
	if row.Deployment != nil {
		token = row.Deployment.AuthToken
	}
	maxBody, ok := providerLimits[row.Provider]
	if !ok {
		maxBody = store.DefaultProviderMaxBody
	}
	return pool.RelayInput{
		ID:       row.ID,
		Name:     row.Name,
		Provider: row.Provider,
		URL:      u,
		Active:   row.Active,
		Origin:   pool.Origin(row.Origin),
		Token:    token,
		Policy:   pool.InheritHeaderPolicy(relayPolicy, providerPolicies[row.Provider]),
		MaxBody:  maxBody,
	}, true, nil
}

// initialLogLevel reads just the log level so startup messages before the
// first generation honor the persisted setting.
func initialLogLevel(db *store.Store) string {
	settings, err := db.Settings()
	if err != nil {
		return store.DefaultLogLevel
	}
	return settings.LogLevel
}

// shutdownAll drains both planes' in-flight requests against one
// whole-process grace budget; expiry force-closes whatever is left.
func shutdownAll(servers []*http.Server, grace time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			_ = s.Shutdown(ctx)
		}(srv)
	}
	wg.Wait()
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

// envDuration and envInt are test-only knobs for the readiness registry:
// env vars that tune verification cadence without touching the database or
// the API. Invalid values silently fall back to the default.
func envDuration(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			return d
		}
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
