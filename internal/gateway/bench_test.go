package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
)

// BenchmarkRelayForward is the data-plane hot path: a buffered relay
// request through pinning, header filtering, pool pick and one upstream
// round trip, against a single verified relay.
func BenchmarkRelayForward(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	relayURL, err := url.Parse(upstream.URL)
	if err != nil {
		b.Fatal(err)
	}
	in := pool.Input{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "good", Provider: "vercel", URL: relayURL, Active: true, Origin: pool.OriginLegacy, MaxBody: 4_500_000},
		},
	}
	p, err := pool.New(in)
	if err != nil {
		b.Fatal(err)
	}
	state := &State{
		Pool:           p,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
		MaxRetries:     2,
		MaxBufferBytes: 100_000_000,
	}
	g := New(state, "test", "1", logging.Nop())
	ts := httptest.NewServer(g)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/vercel", strings.NewReader(`{"m":1}`))
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set(HeaderTarget, "https://api.example.com")
	req.Header.Set(HeaderPath, "/v1/messages")

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		resp, err := ts.Client().Do(req)
		if err != nil {
			b.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `{"ok":true}`) {
			b.Fatalf("status %d body %s", resp.StatusCode, body)
		}
	}
}

// BenchmarkHandleReadyz is the admission answer on an empty pool — the
// zero-ready fast path every request and probe hits until a relay verifies.
func BenchmarkHandleReadyz(b *testing.B) {
	p, err := pool.New(pool.Input{Relays: nil})
	if err != nil {
		b.Fatal(err)
	}
	state := &State{Pool: p, Client: NewClient(NewTransport(2*time.Second, 0))}
	g := New(state, "test", "1", logging.Nop())
	ts := httptest.NewServer(g)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/readyz", nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		resp, err := ts.Client().Do(req)
		if err != nil {
			b.Fatal(err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			b.Fatalf("status %d, want 503", resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}
