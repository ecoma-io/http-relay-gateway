package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/gateway"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/readiness"
)

// freshState is buildState over throwaway runtime state and client cache —
// the shape run() wires once per process. Tests that do not assert
// cross-rebuild persistence use it so generations stay independent.
func freshState(reg *readiness.Registry, settings config.Settings, log zerolog.Logger) *gateway.State {
	return buildState(reg, settings, log, pool.NewRuntimeState(), gateway.NewClientCache())
}

// fixtureURL is the shared prefix of every test relay URL, so a leak in a
// snapshot would be recognizable.
func fixtureURL(provider, name string) (string, error) {
	if provider == "" || name == "" {
		return "", fmt.Errorf("provider and name are required")
	}
	return "http://" + name + "." + provider + ".internal", nil
}

// admit verifies the given keys end to end: the registry then reports them
// as serving, exactly what the applier's buildState consumes.
func admit(t *testing.T, reg *readiness.Registry, keys ...readiness.Key) {
	t.Helper()
	for _, k := range keys {
		gen, ok := reg.GenerationOf(k)
		if !ok {
			t.Fatalf("key %v not in registry", k)
		}
		u, err := fixtureURL(k.Provider, k.Name)
		if err != nil {
			t.Fatal(err)
		}
		reg.Ready(k, gen, u, "relay-key-"+k.Name, time.Millisecond)
	}
	for {
		select {
		case <-reg.Changes():
		default:
			return
		}
	}
}

func TestBuildStateServesOnlyVerified(t *testing.T) {
	reg := readiness.New(readiness.Config{})
	a := readiness.Key{Provider: deploy.PlatformVercel, Name: "bravo"}
	b := readiness.Key{Provider: deploy.PlatformCloudflare, Name: "alpha"}
	c := readiness.Key{Provider: deploy.PlatformDeno, Name: "charlie"} // stays unverified
	reg.Sync([]readiness.Key{a, b, c})
	admit(t, reg, a, b)

	st := freshState(reg, config.DefaultSettings(), zerolog.Nop())
	if st.Pool.ReadyCount() != 2 {
		t.Fatalf("pool serving %d relays, want exactly the 2 verified", st.Pool.ReadyCount())
	}
	// Registry.Serving sorts by provider/name and buildState preserves that
	// order, so the pool rotation is the sorted one, never config-file order.
	first, second := st.Pool.Pick(pool.KeyAll), st.Pool.Pick(pool.KeyAll)
	if first == nil || second == nil {
		t.Fatal("verified relays missing from the built pool")
	}
	if first.Provider != deploy.PlatformCloudflare || first.Name != "alpha" {
		t.Fatalf("first pick = %s/%s, want cloudflare/alpha (sorted order)", first.Provider, first.Name)
	}
	if second.Provider != deploy.PlatformVercel || second.Name != "bravo" {
		t.Fatalf("second pick = %s/%s, want vercel/bravo (sorted order)", second.Provider, second.Name)
	}
	if !reg.IsReady(a) || !reg.IsReady(b) || reg.IsReady(c) {
		t.Fatal("buildState must mirror registry admission")
	}
	// Admission carries the exact URL+key pair verification passed.
	if first.Token != "relay-key-alpha" || first.URL.Host != "alpha.cloudflare.internal" {
		t.Fatalf("admitted relay = %s token=%q url=%s", first.Name, first.Token, first.URL)
	}

	// Lifecycle renders every known relay — the unverified one included —
	// with name, provider, state, reason and generation, never a URL or token.
	type row struct {
		provider   string
		state      string
		generation uint64
	}
	byName := map[string]row{}
	for _, lr := range st.Lifecycle {
		rec, ok := reg.StateOf(readiness.Key{Provider: lr.Provider, Name: lr.Name})
		if !ok {
			t.Fatalf("lifecycle row %v matches no registry entry", lr)
		}
		if lr.State != string(rec.State) || lr.Generation != rec.Generation || lr.Reason != rec.Reason {
			t.Fatalf("lifecycle row %v diverges from registry record %+v", lr, rec)
		}
		byName[lr.Name] = row{lr.Provider, lr.State, lr.Generation}
	}
	if len(byName) != 3 {
		t.Fatalf("lifecycle rows = %d, want one per configured relay", len(byName))
	}
	if r := byName["charlie"]; r.state != string(readiness.StateConfigured) || r.generation != 1 {
		t.Fatalf("unverified relay lifecycle = %+v", r)
	}
	raw, err := json.Marshal(st.Lifecycle)
	if err != nil {
		t.Fatalf("marshal lifecycle: %v", err)
	}
	if s := string(raw); strings.Contains(s, "http://") || strings.Contains(s, "relay-key-") {
		t.Fatalf("lifecycle leaks a URL or token: %s", s)
	}
}

func TestBuildStateBodyLimitsAndSettings(t *testing.T) {
	reg := readiness.New(readiness.Config{})
	v := readiness.Key{Provider: deploy.PlatformVercel, Name: "v"}
	c := readiness.Key{Provider: deploy.PlatformCloudflare, Name: "c"}
	reg.Sync([]readiness.Key{v, c})
	admit(t, reg, v, c)

	settings := config.DefaultSettings()
	settings.MaxRetries = 4
	settings.StreamThresholdBytes = 1024
	st := freshState(reg, settings, zerolog.Nop())

	// MaxBufferBytes is the largest serving provider limit: vercel's ~4.5MB
	// loses to cloudflare's 100MB.
	if st.MaxBufferBytes != pool.CloudflareMaxBody {
		t.Fatalf("MaxBufferBytes = %d, want the largest provider limit %d", st.MaxBufferBytes, pool.CloudflareMaxBody)
	}
	if st.MaxRetries != 4 || st.StreamThresholdBytes != 1024 {
		t.Fatalf("settings did not flow: retries=%d stream=%d", st.MaxRetries, st.StreamThresholdBytes)
	}
	if st.Client == nil {
		t.Fatal("buildState must attach a shared client")
	}

	t.Run("single small provider keeps its own cap", func(t *testing.T) {
		solo := readiness.New(readiness.Config{})
		k := readiness.Key{Provider: deploy.PlatformVercel, Name: "solo"}
		solo.Sync([]readiness.Key{k})
		gen, _ := solo.GenerationOf(k)
		u, err := fixtureURL(k.Provider, k.Name)
		if err != nil {
			t.Fatal(err)
		}
		solo.Ready(k, gen, u, "key", time.Millisecond)
		if got := freshState(solo, config.DefaultSettings(), zerolog.Nop()).MaxBufferBytes; got != pool.VercelMaxBody {
			t.Fatalf("MaxBufferBytes = %d, want the vercel cap %d", got, pool.VercelMaxBody)
		}
	})

	t.Run("zero serving leaves a live empty pool", func(t *testing.T) {
		blank := readiness.New(readiness.Config{})
		st := freshState(blank, config.DefaultSettings(), zerolog.Nop())
		if st.Pool.ReadyCount() != 0 {
			t.Fatalf("ReadyCount = %d, want 0", st.Pool.ReadyCount())
		}
		if st.MaxBufferBytes != 0 {
			t.Fatalf("MaxBufferBytes = %d with nothing serving, want 0", st.MaxBufferBytes)
		}
		if st.Pool.Pick(pool.KeyAll) != nil {
			t.Fatal("an empty pool must not serve")
		}
	})

	t.Run("invalid verified urls are skipped not served", func(t *testing.T) {
		bad := readiness.New(readiness.Config{})
		k := readiness.Key{Provider: deploy.PlatformVercel, Name: "bad"}
		bad.Sync([]readiness.Key{k})
		gen, _ := bad.GenerationOf(k)
		bad.Ready(k, gen, "://not a url", "key", time.Millisecond)
		var logs bytes.Buffer
		st := freshState(bad, config.DefaultSettings(), zerolog.New(&logs))
		if st.Pool == nil || st.Pool.ReadyCount() != 0 {
			t.Fatalf("an unparseable verified URL must be skipped: %+v", st.Pool)
		}
		// The skip is loud — but names the relay, never its URL.
		line := logs.String()
		if !strings.Contains(line, "malformed URL") || !strings.Contains(line, `"relay":"bad"`) {
			t.Fatalf("skip without a warning identifying the relay: %s", line)
		}
		if strings.Contains(line, "not a url") {
			t.Fatalf("the malformed URL itself must never reach the log: %s", line)
		}
	})
}

// --- desiredStore ---

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const validConfig = `relays:
  - name: alpha
    provider: vercel
    token: literal-secret
`

func takeLog(buf *bytes.Buffer) string {
	out := buf.String()
	buf.Reset()
	return out
}

func TestDesiredStorePollLifecycle(t *testing.T) {
	var buf bytes.Buffer
	log := logging.New(&buf)
	store := newDesiredStore()
	if store.get() != nil {
		t.Fatal("a fresh store must hold nothing")
	}

	t.Run("missing file warns once per condition", func(t *testing.T) {
		missing := t.TempDir() + "/absent.yaml"
		if store.poll(missing, log) {
			t.Fatal("poll on a missing file must return false")
		}
		if out := takeLog(&buf); !strings.Contains(out, "desired state file unreadable") {
			t.Fatalf("missing file must warn: %q", out)
		}
		if store.get() != nil {
			t.Fatal("a missing file must not populate the store")
		}
		if store.poll(missing, log) || takeLog(&buf) != "" {
			t.Fatal("a persistent condition must log once, not once per tick")
		}
	})

	t.Run("valid file stores and updates", func(t *testing.T) {
		path := writeConfig(t, validConfig)
		if !store.poll(path, log) {
			t.Fatalf("poll must succeed: %q", takeLog(&buf))
		}
		cfg := store.get()
		if cfg == nil || len(cfg.Relays) != 1 || cfg.Relays[0].Name != "alpha" {
			t.Fatalf("stored config = %+v", cfg)
		}
		if cfg.Settings.MaxRetries != config.DefaultMaxRetries {
			t.Fatalf("settings defaults not applied: %+v", cfg.Settings)
		}
		if out := takeLog(&buf); out != "" {
			t.Fatalf("a successful load must be silent: %q", out)
		}

		// Unchanged content is not a change: the published pointer stays,
		// nothing signals, nothing logs — an idle tick must not re-apply.
		applied := store.get()
		if store.poll(path, log) {
			t.Fatal("an unchanged poll must not signal a change")
		}
		if store.get() != applied {
			t.Fatal("an unchanged poll must not re-publish the config")
		}
		if takeLog(&buf) != "" {
			t.Fatal("a quiet re-poll must not log")
		}

		// A byte-identical rewrite (fresh mtime) is still not a change: the
		// gate is content, never file metadata.
		if err := os.WriteFile(path, []byte(validConfig), 0o600); err != nil {
			t.Fatalf("rewrite identical config: %v", err)
		}
		if store.poll(path, log) {
			t.Fatal("a byte-identical rewrite must not signal a change")
		}

		// Edited content is a change and lands atomically.
		if err := os.WriteFile(path, []byte(validConfig+"settings:\n  max_retries: 5\n"), 0o600); err != nil {
			t.Fatalf("rewrite config: %v", err)
		}
		if !store.poll(path, log) {
			t.Fatal("rewritten config must load")
		}
		if got := store.get().Settings.MaxRetries; got != 5 {
			t.Fatalf("MaxRetries = %d after rewrite, want 5", got)
		}
	})

	t.Run("invalid file keeps last known good", func(t *testing.T) {
		path := writeConfig(t, validConfig)
		if !store.poll(path, log) {
			t.Fatalf("setup poll failed: %q", takeLog(&buf))
		}
		takeLog(&buf)
		if err := os.WriteFile(path, []byte("relays: [broken\n"), 0o600); err != nil {
			t.Fatalf("break config: %v", err)
		}
		if store.poll(path, log) {
			t.Fatal("poll on an invalid file must return false")
		}
		if out := takeLog(&buf); !strings.Contains(out, "desired state file invalid") {
			t.Fatalf("invalid file must error: %q", out)
		}
		if got := store.get(); got == nil || len(got.Relays) != 1 {
			t.Fatalf("invalid reload must keep last-known-good: %+v", got)
		}
		if store.poll(path, log) || takeLog(&buf) != "" {
			t.Fatal("a persistent invalid file must log once per distinct condition")
		}

		// A different error re-arms the log: deduplication is per condition
		// (the sanitized error text), not global. A bad provider name fails
		// validation with a message unlike the YAML syntax error above.
		if err := os.WriteFile(path, []byte("relays:\n  - name: x\n    provider: nope\n    token: t\n"), 0o600); err != nil {
			t.Fatalf("break config differently: %v", err)
		}
		if store.poll(path, log) {
			t.Fatal("poll must fail again")
		}
		if out := takeLog(&buf); !strings.Contains(out, "desired state file invalid") {
			t.Fatalf("a new condition must re-log: %q", out)
		}
	})

	t.Run("good invalid good is one logical load", func(t *testing.T) {
		// Distinct content from every earlier subtest, so this store's
		// first observation of it is a genuine apply.
		roundTrip := "relays:\n  - name: delta\n    provider: vercel\n    token: literal-secret\n"
		path := writeConfig(t, roundTrip)
		if !store.poll(path, log) {
			t.Fatalf("setup poll failed: %q", takeLog(&buf))
		}
		applied := store.get()
		takeLog(&buf)

		if err := os.WriteFile(path, []byte("relays: [broken\n"), 0o600); err != nil {
			t.Fatalf("break config: %v", err)
		}
		if store.poll(path, log) {
			t.Fatal("an invalid file must not signal a change")
		}
		takeLog(&buf)

		// Back to the byte-identical good content: the desired state never
		// actually moved, so the return must not re-apply it either — the
		// change gate only moves on a successful load.
		if err := os.WriteFile(path, []byte(roundTrip), 0o600); err != nil {
			t.Fatalf("restore config: %v", err)
		}
		if store.poll(path, log) {
			t.Fatal("returning to the applied content must not signal a change")
		}
		if store.get() != applied {
			t.Fatal("the applied configuration must be the original load")
		}
		if takeLog(&buf) != "" {
			t.Fatal("an unchanged return must not log")
		}
	})
}

// --- probeURL ---

func TestProbeURL(t *testing.T) {
	cases := []struct {
		addr string
		want string
	}{
		{":8080", "http://127.0.0.1:8080/x"},
		{"0.0.0.0:8080", "http://127.0.0.1:8080/x"},
		{"127.0.0.1:8080", "http://127.0.0.1:8080/x"},
		{"[::]:8080", "http://[::1]:8080/x"},
		{"[::1]:8080", "http://[::1]:8080/x"},
	}
	for _, tc := range cases {
		got, err := probeURL(tc.addr, "/x")
		if err != nil {
			t.Fatalf("probeURL(%q) error = %v", tc.addr, err)
		}
		if got != tc.want {
			t.Fatalf("probeURL(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

// A malformed LISTEN_ADDR must surface as an error the subcommand reports,
// never a panic — the probe path is what a Docker HEALTHCHECK runs.
func TestProbeURLRejectsMalformedAddresses(t *testing.T) {
	for _, addr := range []string{"8080", "127.0.0.1", "http://127.0.0.1:8080", "1:2:3:4"} {
		got, err := probeURL(addr, "/x")
		if err == nil {
			t.Fatalf("probeURL(%q) = %q, want an error for a non-host:port address", addr, got)
		}
		if !strings.Contains(err.Error(), "LISTEN_ADDR") {
			t.Fatalf("probeURL(%q) error = %v, want it to name LISTEN_ADDR", addr, err)
		}
		if got != "" {
			t.Fatalf("probeURL(%q) returned %q alongside the error", addr, got)
		}
	}
}

// --- dispatch ---

// dispatch is the gate between subcommands and the server: anything it does
// not recognize must never fall through to run().
func TestDispatch(t *testing.T) {
	var out, errOut bytes.Buffer
	if code, handled := dispatch(&out, &errOut, nil); handled || code != 0 {
		t.Fatalf("dispatch(nil) = %d, %v; no arguments must fall through to run()", code, handled)
	}
	if code, handled := dispatch(&out, &errOut, []string{"version"}); !handled || code != 0 {
		t.Fatalf("dispatch(version) = %d, %v; want handled with exit 0", code, handled)
	}
	if out.String() != version+"\n" {
		t.Fatalf("dispatch(version) wrote %q, want the build version", out.String())
	}
	if errOut.String() != "" {
		t.Fatalf("dispatch(version) wrote %q to stderr, want it silent", errOut.String())
	}
}

// A typo or an unrecognized flag must print usage to stderr and report exit
// code 2 — never start the gateway.
func TestDispatchUnknownArgumentIsRejected(t *testing.T) {
	for _, arg := range []string{"healthchek", "--help-typo", "serve"} {
		var out, errOut bytes.Buffer
		code, handled := dispatch(&out, &errOut, []string{arg})
		if !handled {
			t.Fatalf("dispatch(%q) not handled; unknown arguments must never reach run()", arg)
		}
		if code != 2 {
			t.Fatalf("dispatch(%q) = %d, want exit code 2", arg, code)
		}
		if out.String() != "" {
			t.Fatalf("dispatch(%q) wrote to stdout %q, want usage on stderr only", arg, out.String())
		}
		diagnostic := errOut.String()
		for _, want := range []string{"unknown argument: " + arg, "usage:", "version", "healthcheck", "readinesscheck"} {
			if !strings.Contains(diagnostic, want) {
				t.Fatalf("dispatch(%q) stderr %q must mention %q", arg, diagnostic, want)
			}
		}
	}
}

// Surplus arguments after a recognized subcommand must not be silently
// ignored: `version extra` exiting 0 or `healthcheck extra` probing anyway
// hides an operator mistake the same way an unknown subcommand does. The
// healthcheck case in particular must fail before any probe runs — the
// wrong-address mistake the surplus hides is exactly what the probe would
// misreport on.
func TestDispatchRejectsSurplusArguments(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"version with extra", []string{"version", "extra"}},
		{"healthcheck with extra", []string{"healthcheck", "extra"}},
		{"readinesscheck with two extras", []string{"readinesscheck", "one", "two"}},
		{"help flag with extra", []string{"--help", "extra"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			code, handled := dispatch(&out, &errOut, tc.args)
			if !handled {
				t.Fatalf("dispatch(%v) not handled; surplus arguments must never reach run()", tc.args)
			}
			if code != 2 {
				t.Fatalf("dispatch(%v) = %d, want exit code 2", tc.args, code)
			}
			if out.String() != "" {
				t.Fatalf("dispatch(%v) wrote to stdout %q, want usage on stderr only", tc.args, out.String())
			}
			diagnostic := errOut.String()
			if !strings.Contains(diagnostic, "unexpected arguments after") {
				t.Fatalf("dispatch(%v) stderr %q must say the arguments are unexpected", tc.args, diagnostic)
			}
			if !strings.Contains(diagnostic, "usage:") {
				t.Fatalf("dispatch(%v) stderr %q must include usage", tc.args, diagnostic)
			}
		})
	}
}

func TestDispatchHelpPrintsUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	code, handled := dispatch(&out, &errOut, []string{"--help"})
	if !handled || code != 0 {
		t.Fatalf("dispatch(--help) = %d, %v; want handled with exit 0", code, handled)
	}
	if errOut.String() != "" {
		t.Fatalf("dispatch(--help) wrote %q to stderr, want it silent", errOut.String())
	}
	for _, want := range []string{"usage:", "version", "healthcheck", "readinesscheck"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("--help output %q must mention %q", out.String(), want)
		}
	}
}

// --- parseZerologLevel ---

func TestParseZerologLevel(t *testing.T) {
	cases := []struct {
		in   string
		want zerolog.Level
	}{
		{"debug", zerolog.DebugLevel},
		{"warn", zerolog.WarnLevel},
		{"error", zerolog.ErrorLevel},
		{"info", zerolog.InfoLevel},
		{"", zerolog.InfoLevel},
		{"TRACE", zerolog.InfoLevel},
		{"banana", zerolog.InfoLevel},
	}
	for _, tc := range cases {
		if got := parseZerologLevel(tc.in); got != tc.want {
			t.Fatalf("parseZerologLevel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// --- healthcheck shape ---

// silenceStderr keeps failing-probe diagnostics out of the test output.
func silenceStderr(t *testing.T) {
	t.Helper()
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open devnull: %v", err)
	}
	saved := os.Stderr
	os.Stderr = devnull
	t.Cleanup(func() {
		os.Stderr = saved
		_ = devnull.Close()
	})
}

func TestHealthcheckAgainstStub(t *testing.T) {
	silenceStderr(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"ready":false,"readyRelays":0}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("LISTEN_ADDR", strings.TrimPrefix(srv.URL, "http://"))

	if got := healthcheck("/healthz", "ok\n"); got != 0 {
		t.Fatalf("healthcheck /healthz = %d, want 0", got)
	}
	if got := healthcheck("/readyz", ""); got != 1 {
		t.Fatalf("healthcheck /readyz = %d, want 1 for a 503 body", got)
	}
	if got := healthcheck("/healthz", "wrong-body"); got != 1 {
		t.Fatalf("healthcheck with a body mismatch = %d, want 1", got)
	}
}

func TestHealthcheckUnreachable(t *testing.T) {
	silenceStderr(t)
	// A port nothing listens on: the probe must fail and report 1.
	t.Setenv("LISTEN_ADDR", "127.0.0.1:1")
	if got := healthcheck("/healthz", "ok\n"); got != 1 {
		t.Fatalf("healthcheck on a dead port = %d, want 1", got)
	}
}

// A malformed LISTEN_ADDR must fail the probe with a single line on stderr —
// the subcommand is what a Docker HEALTHCHECK runs, and a panic trace there
// is operator noise (and a stack trace where the address may appear).
func TestHealthcheckMalformedListenAddr(t *testing.T) {
	var stderr bytes.Buffer
	saved := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = saved })

	done := make(chan string, 1)
	go func() {
		raw, _ := io.ReadAll(r)
		done <- string(raw)
	}()

	t.Setenv("LISTEN_ADDR", "8080") // missing the host:port colon
	code := healthcheck("/healthz", "ok\n")

	_ = w.Close()
	os.Stderr = saved
	if code != 1 {
		t.Fatalf("healthcheck on a malformed LISTEN_ADDR = %d, want 1", code)
	}
	stderr.WriteString(<-done)
	if !strings.Contains(stderr.String(), "healthcheck:") ||
		!strings.Contains(stderr.String(), "LISTEN_ADDR") {
		t.Fatalf("healthcheck stderr = %q, want a single line naming LISTEN_ADDR", stderr.String())
	}
	if strings.Contains(stderr.String(), "panic") {
		t.Fatalf("healthcheck stderr = %q, want no panic trace", stderr.String())
	}
}

// --- runtime health across rebuilds (issue #15) ---

// shortFuseSettings make one transport failure a cooldown, so a test can
// arm a relay's passive health without driving it through the hot path.
func shortFuseSettings() config.Settings {
	s := config.DefaultSettings()
	s.FailureThreshold = 1
	s.Cooldown = time.Hour
	return s
}

// TestBuildStateReusesRuntimeHealthAcrossSwaps pins the applier contract:
// two consecutive buildState calls over the same runtime state — the shape
// of a registry notification for an unrelated relay — carry the counters,
// the cooldown and the rotation position into the second pool, and reuse
// the cached client instead of building a fresh transport.
func TestBuildStateReusesRuntimeHealthAcrossSwaps(t *testing.T) {
	reg := readiness.New(readiness.Config{})
	a := readiness.Key{Provider: deploy.PlatformCloudflare, Name: "alpha"}
	b := readiness.Key{Provider: deploy.PlatformVercel, Name: "bravo"}
	reg.Sync([]readiness.Key{a, b})
	admit(t, reg, a, b)

	settings := shortFuseSettings()
	rt := pool.NewRuntimeState()
	cc := gateway.NewClientCache()
	first := buildState(reg, settings, zerolog.Nop(), rt, cc)

	// Three picks pin the sorted order and leave the shared cursor at 3
	// (alpha, bravo, alpha — the cooled alpha is still the best-effort
	// answer); then a failure cools alpha down and bumps its counters.
	alpha := first.Pool.Pick(pool.KeyAll)
	first.Pool.Pick(pool.KeyAll)
	first.Pool.Pick(pool.KeyAll)
	if alpha.Name != "alpha" || alpha.Provider != deploy.PlatformCloudflare {
		t.Fatalf("first pick = %s/%s, want cloudflare/alpha (sorted order)", alpha.Provider, alpha.Name)
	}
	alpha.Requests.Add(3)
	boom := errors.New("dial relay: connection refused")
	first.Pool.RecordFailure(alpha, boom)

	// The unrelated change: a third relay joins and verifies. Nothing about
	// alpha or bravo changed — the rebuild must not touch their runtime
	// state.
	c := readiness.Key{Provider: deploy.PlatformDeno, Name: "charlie"}
	reg.Sync([]readiness.Key{a, b, c})
	admit(t, reg, c)

	second := buildState(reg, settings, zerolog.Nop(), rt, cc)

	rows := map[string]pool.StatsRow{}
	for _, row := range second.Pool.Stats() {
		rows[row.Name] = row
	}
	if row := rows["alpha"]; row.Healthy || row.Failures != 1 || row.Requests != 3 || row.LastError != boom.Error() {
		t.Fatalf("alpha's runtime state did not survive the rebuild: %+v", row)
	}
	if row := rows["charlie"]; !row.Healthy || row.Failures != 0 || row.Requests != 0 {
		t.Fatalf("the newly admitted relay must start clean: %+v", row)
	}

	// Rotation continues where the previous generation left it. Sorted pool
	// order is [alpha, charlie, bravo]; alpha is cooled, so the healthy
	// pair is [charlie, bravo] and the carried cursor (3, from alpha,
	// bravo, alpha) wraps to index 3 mod 2 = 1 — bravo. A rotation
	// restarted by the rebuild would have served charlie instead.
	if got := second.Pool.Pick(pool.KeyAll); got == nil || got.Name != "bravo" {
		t.Fatalf("first pick after the rebuild = %v, want bravo (rotation carried, cooldown respected)", got)
	}

	// And the outbound client is the cached one, keep-alive conns included.
	if second.Client != first.Client {
		t.Fatal("an unrelated rebuild must reuse the cached client")
	}
}

// TestBuildStatePrunesRemovedRelays: a relay that left the desired
// configuration loses its runtime state — a re-added identity starts clean
// — while a relay that stayed desired keeps its cooldown through the same
// rebuild. Prune is the applier's call, so the test drives the exact
// Prune-then-buildState interplay run() performs.
func TestBuildStatePrunesRemovedRelays(t *testing.T) {
	if got := desiredIdentities(nil); len(got) != 0 {
		t.Fatalf("desiredIdentities(nil) = %v, want an empty set (the empty-fleet boot)", got)
	}

	reg := readiness.New(readiness.Config{})
	a := readiness.Key{Provider: deploy.PlatformVercel, Name: "alpha"}
	b := readiness.Key{Provider: deploy.PlatformCloudflare, Name: "bravo"}
	reg.Sync([]readiness.Key{a, b})
	admit(t, reg, a, b)

	settings := shortFuseSettings()
	rt := pool.NewRuntimeState()
	cc := gateway.NewClientCache()
	first := buildState(reg, settings, zerolog.Nop(), rt, cc)
	if got := first.Pool.Pick(pool.KeyAll); got.Name != "bravo" {
		t.Fatalf("first pick = %s, want bravo (sorted order)", got.Name)
	}
	alpha := first.Pool.Pick(pool.KeyAll)
	if alpha.Name != "alpha" {
		t.Fatalf("second pick = %s, want alpha (sorted order)", alpha.Name)
	}
	first.Pool.RecordFailure(alpha, errors.New("dial relay: connection refused"))

	// bravo leaves the desired state: the registry revokes its admission,
	// the applier prunes the identities the last-known-good file no longer
	// names, and the rebuild serves only alpha.
	reg.Sync([]readiness.Key{a})
	rt.Prune(desiredIdentities(&config.Config{Relays: []config.Relay{
		{Name: "alpha", Provider: deploy.PlatformVercel},
	}}))
	second := buildState(reg, settings, zerolog.Nop(), rt, cc)
	if second.Pool.ReadyCount() != 1 {
		t.Fatalf("pool serves %d relays after the removal, want 1", second.Pool.ReadyCount())
	}

	// alpha is still desired: its cooldown survived the removal rebuild.
	row, ok := relayStatsRow(second.Pool, "alpha")
	if !ok || row.Healthy || row.Failures != 1 {
		t.Fatalf("alpha after the prune = %+v (ok=%v), want its cooldown intact", row, ok)
	}

	// bravo comes back as a new incarnation: its pruned state must not
	// resurface — the re-added relay starts clean while alpha stays cooled.
	reg.Sync([]readiness.Key{a, b})
	admit(t, reg, b)
	third := buildState(reg, settings, zerolog.Nop(), rt, cc)
	row, ok = relayStatsRow(third.Pool, "bravo")
	if !ok || !row.Healthy || row.Failures != 0 {
		t.Fatalf("re-added bravo = %+v (ok=%v), want a clean slate after the prune", row, ok)
	}
	if row, _ := relayStatsRow(third.Pool, "alpha"); row.Healthy || row.Failures != 1 {
		t.Fatalf("alpha drifted while bravo cycled: %+v", row)
	}
}

// relayStatsRow fetches one relay's row from a pool snapshot.
func relayStatsRow(p *pool.Pool, name string) (pool.StatsRow, bool) {
	for _, row := range p.Stats() {
		if row.Name == name {
			return row, true
		}
	}
	return pool.StatsRow{}, false
}
