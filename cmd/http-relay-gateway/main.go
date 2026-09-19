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
	"syscall"
	"time"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/gateway"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
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
			os.Exit(healthcheck())
		}
	}
	if err := run(); err != nil {
		fatalLog.Error().Str("err", sanitize.ErrorString(err)).Msg("fatal")
		os.Exit(1)
	}
}

// healthcheck probes the bootstrap-configured listener. It deliberately does
// not parse runtime YAML: a bad reload must not make a healthy, already
// running process fail Docker's health probe. The scratch image has no shell,
// so this subcommand is what the Docker HEALTHCHECK runs.
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

func run() error {
	bootstrap, err := config.LoadBootstrap()
	if err != nil {
		return err
	}
	runtimeCfg, err := config.LoadRuntime(bootstrap.ConfigFile)
	if err != nil {
		return err
	}

	log := setupDynamicLogger(runtimeCfg.LogLevel)

	// The gateway publishes one immutable generation (validated config + pool
	// snapshot). Handlers load it once per request; reload swaps the whole
	// generation atomically and in-flight requests finish on their own one.
	store, err := pool.New(runtimeCfg)
	if err != nil {
		return err
	}
	g := gateway.New(&gateway.State{Cfg: runtimeCfg, Pool: store}, version, log)

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
			Int("relays", len(runtimeCfg.Relays)).Msg("relay-gateway listening")
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

	// Reloads from the poller arrive on one channel and are handled by this
	// serialized loop; the source label only records how it was reached.
	reload := func(source string) {
		next, err := config.LoadRuntime(bootstrap.ConfigFile)
		if err != nil {
			log.Warn().Str("source", source).Str("error", sanitize.ErrorString(err)).Msg("reload failed; keeping previous configuration")
			return
		}
		// LoadRuntime fully validates before the next generation is built and
		// swapped. Invalid input keeps the previous generation (and its relay
		// health) serving untouched.
		nextPool, err := pool.New(next)
		if err != nil {
			log.Warn().Str("source", source).Str("error", sanitize.ErrorString(err)).Msg("reload failed; keeping previous configuration")
			return
		}
		g.Swap(&gateway.State{Cfg: next, Pool: nextPool})
		// Only this goroutine writes the global level, so the package-global
		// atomic swap is race-free and every future event picks it up.
		zerolog.SetGlobalLevel(parseZerologLevel(next.LogLevel))
		log.Info().Str("source", source).Int("relays", len(next.Relays)).Msg("configuration reloaded")
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
			// The poller hash-gates on applied content, so one signal means one
			// distinct configuration; no debounce is needed.
			reload("poll")
		}
	}
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

// setupDynamicLogger installs the initial global level (the reload loop is
// the only other writer) and returns the process logger.
func setupDynamicLogger(level string) zerolog.Logger {
	zerolog.SetGlobalLevel(parseZerologLevel(level))
	return logging.New(os.Stdout)
}
