package gateway

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
)

// BenchmarkGatewayRelay measures the buffered data-plane forward path: spec
// parsing, pick, header filtering, a real local round trip and the response
// copy.
func BenchmarkGatewayRelay(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	u, err := url.Parse(upstream.URL)
	if err != nil {
		b.Fatalf("parse upstream url: %v", err)
	}
	p, err := pool.New(pool.Input{
		FailureThreshold: 3,
		Cooldown:         time.Second,
		Relays: []pool.RelayInput{{
			Name:     "bench",
			Provider: "vercel",
			URL:      u,
			Token:    "bench-token",
			MaxBody:  pool.VercelMaxBody,
		}},
	})
	if err != nil {
		b.Fatalf("pool.New: %v", err)
	}
	g := New(&State{
		Pool:           p,
		Client:         NewClient(NewTransport(time.Second, time.Second)),
		MaxRetries:     1,
		MaxBufferBytes: pool.VercelMaxBody,
	}, "bench", "worker", logging.Nop())

	body := `{"ping":1}`
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set(HeaderTarget, "https://api.example.com")
		req.Header.Set(HeaderPath, "/v1/ping")
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}
