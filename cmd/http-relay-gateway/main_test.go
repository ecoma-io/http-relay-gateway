package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/readiness"
)

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

	st := buildState(reg, config.DefaultSettings(), zerolog.Nop())
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
	st := buildState(reg, settings, zerolog.Nop())

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
		if got := buildState(solo, config.DefaultSettings(), zerolog.Nop()).MaxBufferBytes; got != pool.VercelMaxBody {
			t.Fatalf("MaxBufferBytes = %d, want the vercel cap %d", got, pool.VercelMaxBody)
		}
	})

	t.Run("zero serving leaves a live empty pool", func(t *testing.T) {
		blank := readiness.New(readiness.Config{})
		st := buildState(blank, config.DefaultSettings(), zerolog.Nop())
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
		st := buildState(bad, config.DefaultSettings(), zerolog.New(&logs))
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
		if got := probeURL(tc.addr, "/x"); got != tc.want {
			t.Fatalf("probeURL(%q) = %q, want %q", tc.addr, got, tc.want)
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
