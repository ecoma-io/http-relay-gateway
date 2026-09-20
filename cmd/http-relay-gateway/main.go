// Command http-relay-gateway is a general-purpose HTTP relay manager: one
// internal-network endpoint that forwards relay-spec requests
// (X-Relay-Target / X-Relay-Path) through a health-aware, round-robin pool
// of edge relays (Vercel / Deno Deploy / Cloudflare Workers), so any client
// can hide its egress IP without managing a relay list itself.
//
// Desired state is one YAML file; everything dynamic is derived from it. The
// watcher re-reads the file, the reconciler drives the remote fleet toward
// it, the readiness registry gates admission, and the applier rebuilds the
// serving pool from the registry's verified snapshot alone. There is no
// database, no admin plane, no second source of truth.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/gateway"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/readiness"
	"http-relay-gateway/internal/reconcile"
	"http-relay-gateway/internal/sanitize"

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
			os.Exit(healthcheck("/healthz", "ok\n"))
		case "readinesscheck":
			os.Exit(healthcheck("/readyz", ""))
		}
	}
	if err := run(); err != nil {
		fatalLog.Error().Str("error", sanitize.ErrorString(err)).Msg("fatal")
		os.Exit(1)
	}
}

// healthcheck probes the data-plane listener. path selects the probe:
// /healthz answers once the process serves (Docker's HEALTHCHECK — it must
// not fail for an unverified fleet), /readyz only when at least one relay is
// verified (orchestrators that gate traffic on it). Both are deliberately
// lenient about bootstrap configuration: a probe reports what the process is
// doing, it does not re-run config validation. The scratch image has no
// shell, so these subcommands are what the container runs. wantBody pins the
// expected body when non-empty (/healthz answers "ok\n"); /readyz matches on
// status alone.
func healthcheck(path, wantBody string) int {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = config.DefaultListenAddr
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(probeURL(addr, path))
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", sanitize.ErrorString(err))
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	if resp.StatusCode != http.StatusOK || (wantBody != "" && string(body) != wantBody) {
		fmt.Fprintf(os.Stderr, "healthcheck: status %d, body %q\n", resp.StatusCode, body)
		return 1
	}
	return 0
}

// probeURL turns a listen address into the loopback URL a same-container
// probe reaches it on.
func probeURL(addr, path string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Sprintf("probeURL requires a host:port address: %q", addr))
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: path}).String()
}

func run() error {
	bootstrap, err := config.LoadBootstrap()
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout)

	// The desired-state store holds the last successfully loaded
	// configuration. A missing file at boot is a legal start (an empty fleet
	// that self-heals when the file appears); an invalid reload keeps the
	// last-known-good state serving and only logs.
	store := newDesiredStore()
	store.poll(bootstrap.ConfigFile, log)

	// Boot-time tuning comes from the file when it loaded, from the defaults
	// otherwise. The registry's backoff knobs are read here once (they shape
	// in-flight retry state; changing them mid-run is a restart); cadences
	// stay live through the store.
	settings := config.DefaultSettings()
	if cfg := store.get(); cfg != nil {
		settings = cfg.Settings
	}
	zerolog.SetGlobalLevel(parseZerologLevel(settings.LogLevel))
	reg := readiness.New(readiness.Config{
		BackoffBase: settings.VerifyBackoffBase,
		BackoffMax:  settings.VerifyBackoffMax,
		RecoverMax:  settings.VerifyRecoverMax,
		DemoteAfter: settings.VerifyDemoteAfter,
		PauseRetry:  settings.ReviveScanInterval,
	})

	g := gateway.New(buildState(reg, settings), version, deploy.RelayVersion, log)

	// The apply loop is the only writer of the serving generation: registry
	// notification (admission changed) or desired-state reload (settings
	// changed) rebuild one immutable State from the registry's verified
	// snapshot and swap it atomically. A rejected reload simply never fires
	// this path — the last-known-good pool keeps serving.
	//
	// applied/cond implement the settle barrier the reconciler's Strategy A
	// rollout waits on: after a demote it must know the pool swap that
	// revokes the relay has actually landed before draining in-flight
	// requests. applied only ever records a seq whose changes the snapshot
	// already included, so the barrier errs toward waiting — never toward
	// deploying under traffic that has not been drained.
	var (
		applyMu  sync.Mutex
		applied  uint64
		cond     = sync.NewCond(&applyMu)
		shutting bool
	)
	apply := func(source string) {
		seq := reg.NotifySeq() // captured BEFORE the snapshot: any later notify stays pending
		cfg := store.get()
		s := config.DefaultSettings()
		if cfg != nil {
			s = cfg.Settings
		}
		g.Swap(buildState(reg, s))
		zerolog.SetGlobalLevel(parseZerologLevel(s.LogLevel))
		applyMu.Lock()
		if seq > applied {
			applied = seq
		}
		cond.Broadcast()
		applyMu.Unlock()
		log.Debug().Str("source", source).Int("relays", reg.ReadyCount()).
			Msg("serving generation rebuilt")
	}
	settle := func() bool {
		target := reg.NotifySeq()
		applyMu.Lock()
		for applied < target && !shutting {
			cond.Wait()
		}
		settled := applied >= target
		applyMu.Unlock()
		return settled
	}

	// The fleet worker turns desired state into verified deployments; the
	// watcher turns file changes into desired state. The reconciler wakes on
	// every reload; the apply loop rebuilds on every registry change and
	// every (successful) reload.
	rec := reconcile.Start(reconcile.Config{
		Desired:  store.get,
		RelayKey: bootstrap.ResolveRelayKey,
		Factory:  deploy.NewFactory(nil),
		Registry: reg,
		Drainer:  g,
		Settle:   settle,
		Log:      log,
	})
	defer rec.Stop()

	reloaded := make(chan struct{}, 1)
	go store.watch(bootstrap.ConfigFile, log, func() {
		rec.Wake()
		select {
		case reloaded <- struct{}{}:
		default:
		}
	})

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
			Int("readyRelays", reg.ReadyCount()).Msg("relay-gateway listening")
		// Serve returns nil once shutdown closes the listener.
		if err := srv.Serve(ln); err != nil {
			errCh <- fmt.Errorf("listener: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Shutdown releases a rollout parked on the settle barrier first — the
	// applier loop is about to leave its select, so without this broadcast
	// a mid-replacement Stop() would wait for a barrier nobody will ever
	// satisfy. Then it cancels fleet work: a deploy mid-flight must not eat
	// the whole-process drain budget. Then the data plane drains its
	// in-flight requests against one whole-process grace budget.
	shutdown := func() {
		applyMu.Lock()
		shutting = true
		cond.Broadcast()
		applyMu.Unlock()
		rec.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), bootstrap.ShutdownGrace)
		defer cancel()
		_ = srv.Shutdown(ctx)
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
		case <-reg.Changes():
			apply("readiness")
		case <-reloaded:
			apply("config")
		}
	}
}

// buildState derives one immutable serving snapshot: the pool is built
// exclusively from the registry's verified serving snapshot — membership
// here means verified for the current incarnation, and the URL+key pair is
// the exact one that passed verification. Everything the hot path would
// otherwise re-derive is resolved here: body limits, transport timeouts.
func buildState(reg *readiness.Registry, settings config.Settings) *gateway.State {
	serving := reg.Serving()
	in := pool.Input{
		FailureThreshold: settings.FailureThreshold,
		Cooldown:         settings.Cooldown,
		Relays:           make([]pool.RelayInput, 0, len(serving)),
	}
	maxBuffer := int64(0)
	for _, s := range serving {
		u, err := url.Parse(s.URL)
		if err != nil || u.Host == "" {
			// A verified snapshot URL is always absolute; this cannot fire
			// today. Skip rather than serve a broken entry if it ever does.
			continue
		}
		maxBody := pool.ProviderMaxBody(s.Key.Provider)
		in.Relays = append(in.Relays, pool.RelayInput{
			Name:     s.Key.Name,
			Provider: s.Key.Provider,
			URL:      u,
			Token:    s.Token,
			MaxBody:  maxBody,
		})
		if maxBody > maxBuffer {
			maxBuffer = maxBody
		}
	}
	p, err := pool.New(in)
	if err != nil {
		// pool.New is infallible today; an empty pool keeps the process
		// alive (zero-ready answers 503) instead of panicking the data plane.
		p, _ = pool.New(pool.Input{})
	}

	snapshot := reg.Snapshot()
	lifecycle := make([]gateway.LifecycleRow, 0, len(snapshot))
	for _, record := range snapshot {
		lifecycle = append(lifecycle, gateway.LifecycleRow{
			Name:       record.Key.Name,
			Provider:   record.Key.Provider,
			State:      string(record.State),
			Reason:     record.Reason,
			Generation: record.Generation,
		})
	}
	return &gateway.State{
		Pool: p,
		Client: gateway.NewClient(gateway.NewTransport(
			settings.DialTimeout, settings.ResponseHeaderTimeout)),
		MaxRetries:           settings.MaxRetries,
		MaxBufferBytes:       maxBuffer,
		StreamThresholdBytes: settings.StreamThresholdBytes,
		Lifecycle:            lifecycle,
	}
}

// desiredStore holds the last successfully loaded configuration behind an
// atomic pointer, re-reading the file on a short poll. Only a successful
// load ever replaces the stored state: a missing or invalid file logs once
// per distinct condition and leaves the last-known-good serving.
type desiredStore struct {
	p atomic.Pointer[config.Config]
	// lastKey deduplicates poll observations so a persistent problem logs
	// once, not once per tick: the file's content hash when valid, a
	// sanitized description of the problem otherwise.
	lastKey string
}

func newDesiredStore() *desiredStore { return &desiredStore{} }

func (s *desiredStore) get() *config.Config { return s.p.Load() }

// poll reads the file once; watch calls it on the reload interval and fires
// onChange whenever a new valid configuration replaced the stored one.
func (s *desiredStore) poll(path string, log zerolog.Logger) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		key := "unreadable: " + sanitize.ErrorString(err)
		if key != s.lastKey {
			s.lastKey = key
			log.Warn().Str("path", path).Str("error", sanitize.ErrorString(err)).
				Msg("desired state file unreadable; serving the last known configuration")
		}
		return false
	}
	sum := sha256.Sum256(raw)
	cfg, parseErr := config.Load(path)
	if parseErr != nil {
		key := "invalid: " + sanitize.ErrorString(parseErr)
		if key != s.lastKey {
			s.lastKey = key
			log.Error().Str("path", path).Str("error", sanitize.ErrorString(parseErr)).
				Msg("desired state file invalid; keeping the last known configuration")
		}
		return false
	}
	s.lastKey = "hash: " + hex.EncodeToString(sum[:])
	s.p.Store(cfg)
	return true
}

// watch polls the file until the context ends, invoking onChange for every
// newly applied configuration.
func (s *desiredStore) watch(path string, log zerolog.Logger, onChange func()) {
	ticker := time.NewTicker(config.ReloadPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		if s.poll(path, log) {
			onChange()
		}
	}
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
