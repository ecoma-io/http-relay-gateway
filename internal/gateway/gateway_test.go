package gateway

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
)

// deadRelayURL returns a URL that refuses connections (a closed listener).
func deadRelayURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + l.Addr().String()
	_ = l.Close()
	return url
}

// capture records the last request seen by the fake upstream edge relay.
type capture struct {
	mu      sync.Mutex
	headers http.Header
	body    []byte
}

func (c *capture) header(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.headers == nil {
		return ""
	}
	return c.headers.Get(name)
}

type fixture struct {
	gateway *Gateway
	cfg     *config.RuntimeConfig
	pool    *pool.Pool
	saw     *capture
}

// newFixture starts one live upstream edge relay (echoing what it received)
// and builds a gateway whose "vercel" pool is [dead, upstream].
func newFixture(t *testing.T, mutate func(*config.RuntimeConfig)) *fixture {
	t.Helper()

	saw := &capture{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw.mu.Lock()
		saw.headers = r.Header.Clone()
		saw.body, _ = io.ReadAll(r.Body)
		saw.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hi\n\n"))
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.RuntimeConfig{
		LogLevel:         "debug",
		MaxRetries:       2,
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Providers: map[string]config.ProviderSpec{
			"vercel":     {MaxBody: 4_500_000},
			"cloudflare": {MaxBody: 100_000_000},
		},
		Relays: []config.RelaySpec{
			{Name: "dead", Provider: "vercel", URL: mustURL(t, deadRelayURL(t)), Active: true},
			{Name: "good", Provider: "vercel", URL: mustURL(t, upstream.URL), Active: true},
			{Name: "good-cf", Provider: "cloudflare", URL: mustURL(t, upstream.URL), Active: true},
		},
	}
	if mutate != nil {
		mutate(cfg)
	}
	p, err := pool.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{Cfg: cfg, Pool: p}, "test", logging.Nop())
	return &fixture{gateway: g, cfg: cfg, pool: p, saw: saw}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// relayRequest sends a spec-conformant relay request. The target's host part
// is a label ("http://gateway/vercel"); only its path
// is used, pointed at the test server.
func relayRequest(t *testing.T, g *Gateway, target string, body string, headers map[string]string) *http.Response {
	t.Helper()
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)

	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+u.Path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

func TestRelayForwardsAndStrips(t *testing.T) {
	f := newFixture(t, func(c *config.RuntimeConfig) {
		// Remove the dead relay: this test is about the forwarding contract,
		// not failover.
		c.Relays = c.Relays[1:]
	})

	res := relayRequest(t, f.gateway, "http://gateway/vercel", `{"model":"x"}`, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages?beta=true",
		"Authorization":  "Bearer provider-secret",
	})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if string(body) != "data: hi\n\n" {
		t.Fatalf("streamed body = %q", body)
	}

	if got := f.saw.header("X-Relay-Target"); got != "https://api.example.com" {
		t.Fatalf("X-Relay-Target = %q", got)
	}
	if got := f.saw.header("X-Relay-Path"); got != "/v1/messages?beta=true" {
		t.Fatalf("X-Relay-Path = %q", got)
	}
	if got := f.saw.header("Authorization"); got != "Bearer provider-secret" {
		t.Fatalf("Authorization = %q (provider auth must flow through)", got)
	}
	if got := f.saw.header(HeaderProvider); got != "" {
		t.Fatalf("provider pin leaked upstream: %q", got)
	}
	if string(f.saw.body) != `{"model":"x"}` {
		t.Fatalf("upstream body = %q", f.saw.body)
	}
}

func TestFailoverToNextRelay(t *testing.T) {
	f := newFixture(t, nil)

	res := relayRequest(t, f.gateway, "http://gateway/vercel", `{}`, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
	})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (failover to live relay expected)", res.StatusCode)
	}
	var deadFails int64
	for _, row := range f.pool.Stats() {
		if row.Name == "dead" {
			deadFails = row.Failures
		}
	}
	if deadFails == 0 {
		t.Fatal("dead relay recorded no failures; failover did not happen")
	}
}

func TestHeaderPinBeatsPathPrefix(t *testing.T) {
	f := newFixture(t, nil)

	// Path pins vercel (whose pool holds the dead relay), header pins
	// cloudflare — header wins; the dead vercel relay must never be contacted.
	res := relayRequest(t, f.gateway, "http://gateway/vercel", `{}`, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
		HeaderProvider:   "cloudflare",
	})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	for _, row := range f.pool.Stats() {
		if row.Name == "dead" && row.Failures != 0 {
			t.Fatal("dead vercel relay was contacted despite cloudflare pin")
		}
	}
}

func TestUnknownProviderRejected(t *testing.T) {
	f := newFixture(t, nil)

	res := relayRequest(t, f.gateway, "http://gateway/", `{}`, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
		HeaderProvider:   "deno",
	})
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown provider", res.StatusCode)
	}
}

func TestBodySkipsToProviderThatAccepts(t *testing.T) {
	f := newFixture(t, func(c *config.RuntimeConfig) {
		// vercel's real limit is 4.5MB; shrink it so the test body qualifies.
		c.Providers["vercel"] = config.ProviderSpec{MaxBody: 8}
	})

	// 12 bytes: over vercel's 8-byte limit, under cloudflare's. Pinned
	// vercel must 413; auto must skip to cloudflare and succeed.
	big := "123456789012"

	res := relayRequest(t, f.gateway, "http://gateway/vercel", big, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
	})
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("pinned vercel status = %d, want 413", res.StatusCode)
	}

	res = relayRequest(t, f.gateway, "http://gateway/", big, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
	})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("auto status = %d, want 200 via cloudflare", res.StatusCode)
	}
	if string(f.saw.body) != big {
		t.Fatalf("upstream body = %q, want %q", f.saw.body, big)
	}
}

func TestBodyOverEveryLimitRejected(t *testing.T) {
	f := newFixture(t, func(c *config.RuntimeConfig) {
		// Shrink every provider under the 10-byte test body.
		c.Providers["vercel"] = config.ProviderSpec{MaxBody: 4}
		c.Providers["cloudflare"] = config.ProviderSpec{MaxBody: 4}
	})

	res := relayRequest(t, f.gateway, "http://gateway/", "1234567890", map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
	})
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 over every provider limit", res.StatusCode)
	}
}

func TestAllAttemptsExhaustedReturns502(t *testing.T) {
	cfg := &config.RuntimeConfig{
		MaxRetries:       1,
		FailureThreshold: 100,
		Cooldown:         time.Second,
		Providers:        map[string]config.ProviderSpec{},
		Relays: []config.RelaySpec{
			{Name: "dead1", Provider: "vercel", URL: mustURL(t, deadRelayURL(t)), Active: true},
			{Name: "dead2", Provider: "vercel", URL: mustURL(t, deadRelayURL(t)), Active: true},
		},
	}
	p, err := pool.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{Cfg: cfg, Pool: p}, "test", logging.Nop())

	res := relayRequest(t, g, "http://gateway/", `{}`, map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
	})
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when every attempt fails", res.StatusCode)
	}
}

func TestStatsEndpointListsRelays(t *testing.T) {
	f := newFixture(t, nil)
	ts := httptest.NewServer(f.gateway)
	t.Cleanup(ts.Close)

	res, err := ts.Client().Get(ts.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(raw), `"provider":"vercel"`) || !strings.Contains(string(raw), `"version":"test"`) {
		t.Fatalf("/stats payload wrong: %s", raw)
	}
}

func TestHealthzBody(t *testing.T) {
	f := newFixture(t, nil)
	ts := httptest.NewServer(f.gateway)
	t.Cleanup(ts.Close)

	res, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("healthz = %d %q, want 200 %q", res.StatusCode, body, "ok\n")
	}
}
