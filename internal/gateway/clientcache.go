// The outbound HTTP clients are process-lifetime objects, not per-generation
// ones. Every serving build used to construct a fresh Transport/Client and
// Swap closed the old one's idle connections, so the keep-alive pool never
// survived a rebuild — an unrelated relay's registry notification cost the
// data plane its pooled connections to every relay. The cache keys a client
// by the settings pair the transport is built from, so generations that
// agree on the timeouts share one client and its conns, and only a real
// settings change produces (and retires) a transport.
package gateway

import (
	"net/http"
	"sync"
	"time"
)

// transportKey is the settings pair a transport is built from: two states
// that agree on it can share one client and its keep-alive pool.
type transportKey struct {
	dialTimeout           time.Duration
	responseHeaderTimeout time.Duration
}

// ClientCache builds and shares the outbound clients across serving
// generations. The clients are exactly the NewTransport/NewClient pair every
// generation used to build fresh — same no-proxy transport, same
// compression and redirect policy — so sharing them changes nothing about
// the relay leg, only how long the conns live.
type ClientCache struct {
	mu sync.Mutex
	m  map[transportKey]*http.Client
}

// NewClientCache builds the empty cache a process carries for its lifetime.
func NewClientCache() *ClientCache {
	return &ClientCache{m: map[transportKey]*http.Client{}}
}

// Client returns the shared client for the timeout pair, building it on
// first use. Safe for concurrent use; the build path holds the lock so a
// burst of rebuilds produces one client, not one per caller.
func (cc *ClientCache) Client(dialTimeout, headerTimeout time.Duration) *http.Client {
	key := transportKey{dialTimeout: dialTimeout, responseHeaderTimeout: headerTimeout}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if c, ok := cc.m[key]; ok {
		return c
	}
	c := NewClient(NewTransport(dialTimeout, headerTimeout))
	cc.m[key] = c
	return c
}

// CloseAll closes the cached clients' idle connections. Shutdown calls it
// after the HTTP drain, so whatever keep-alive sockets are still pooled
// close on the way out instead of being dropped by process exit. The cache
// itself stays usable — a client whose idle conns were closed simply dials
// again.
func (cc *ClientCache) CloseAll() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, c := range cc.m {
		c.CloseIdleConnections()
	}
}
