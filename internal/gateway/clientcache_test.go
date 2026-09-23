package gateway

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"http-relay-gateway/internal/pool"
)

// closeIdlerTransport counts CloseIdleConnections: the one observable the
// shared-client contract turns on (Swap must not close a client another
// generation is still using).
type closeIdlerTransport struct {
	*http.Transport
	closes atomic.Int64
}

func (t *closeIdlerTransport) CloseIdleConnections() {
	t.closes.Add(1)
	t.Transport.CloseIdleConnections()
}

// TestSwapKeepsIdleConnsOfASharedClient pins the Swap half of the
// keep-alive contract (issue #15): a rebuild that reuses the cached client
// must leave its pooled connections alone, and only a state that actually
// swaps in a different client (a timeout-settings change) closes the old
// one's idle conns.
func TestSwapKeepsIdleConnsOfASharedClient(t *testing.T) {
	tr := &closeIdlerTransport{Transport: NewTransport(time.Second, time.Second)}
	// NewClient takes the concrete *http.Transport, so the counting wrapper
	// wraps it here: same shape, observable CloseIdleConnections.
	shared := &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	poolA := buildPool(t, []relaySpec{{name: "alpha", provider: "vercel", rawURL: "http://alpha.example.internal"}})
	poolB := buildPool(t, []relaySpec{{name: "alpha", provider: "vercel", rawURL: "http://alpha.example.internal"}})
	state := func(p *pool.Pool, c *http.Client) *State {
		return &State{Pool: p, Client: c, MaxRetries: 1, MaxBufferBytes: bufferBytes()}
	}

	g := newGateway(t, state(poolA, shared))

	// The unrelated rebuild: new pool, same cached client.
	g.Swap(state(poolB, shared))
	if tr.closes.Load() != 0 {
		t.Fatalf("a shared client had its idle conns closed on an unrelated rebuild (%d closes)", tr.closes.Load())
	}

	// A genuine client change (the timeouts moved): the old client is
	// retired exactly once.
	g.Swap(state(poolB, NewClient(NewTransport(2*time.Second, time.Second))))
	if tr.closes.Load() != 1 {
		t.Fatalf("a swapped-out client was closed %d times, want exactly 1", tr.closes.Load())
	}
}

// TestClientCacheSharesOneClientPerTimeoutPair: generations that agree on
// the transport timeouts share one client; a changed pair builds its own.
func TestClientCacheSharesOneClientPerTimeoutPair(t *testing.T) {
	cc := NewClientCache()
	a := cc.Client(time.Second, time.Second)
	if got := cc.Client(time.Second, time.Second); got != a {
		t.Fatal("the same timeout pair must reuse the cached client")
	}
	if got := cc.Client(2*time.Second, time.Second); got == a {
		t.Fatal("a changed dial timeout must build a different transport")
	}
	if got := cc.Client(time.Second, 2*time.Second); got == a {
		t.Fatal("a changed response-header timeout must build a different transport")
	}
}

// TestClientCacheCloseAllKeepsClientsUsable: closing the pooled idle conns
// at shutdown must not poison the cache — a closed client dials again.
func TestClientCacheCloseAllKeepsClientsUsable(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)

	cc := NewClientCache()
	c := cc.Client(time.Second, time.Second)
	resp, err := c.Get(up.URL)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	_ = resp.Body.Close()

	cc.CloseAll()

	resp, err = c.Get(up.URL)
	if err != nil {
		t.Fatalf("request after CloseAll: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request after CloseAll = %d, want 200", resp.StatusCode)
	}
}
