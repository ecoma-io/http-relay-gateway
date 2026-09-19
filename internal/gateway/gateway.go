// Package gateway implements the HTTP data plane: provider selection,
// bounded failover and streaming pass-through to edge relays.
//
// Wire contract (the relay spec): the caller sends the request to the
// gateway root (optionally /{provider}) with the real origin in
// X-Relay-Target and the real path+query in X-Relay-Path. The gateway picks a
// relay and forwards everything verbatim; the edge relay then forwards to the
// target. The gateway never adds X-Forwarded-For — hiding the client IP is
// the whole point. There is no gateway authentication: the process is an
// internal-network sidecar and must not be exposed beyond its compose network.
package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/sanitize"

	"github.com/rs/zerolog"
)

// HeaderProvider pins a provider ("vercel" | "cloudflare" | "deno" | ...).
// Empty, "none", "auto" or "all" means round-robin across every provider.
const HeaderProvider = "X-Relay-Provider"

// hopByHop are dropped on both legs.
var hopByHop = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

func unpinned(v string) bool { return v == "" || v == "none" || v == "auto" || v == "all" }

// State is one coherent config+pool snapshot, swappable on reload.
type State struct {
	Cfg  *config.RuntimeConfig
	Pool *pool.Pool
}

// Gateway is the root http.Handler.
type Gateway struct {
	st      atomic.Pointer[State]
	client  *http.Client
	version string
	log     zerolog.Logger
}

// New builds the Gateway and its shared transport: keep-alive pooled (TLS
// handshakes amortize to zero), HTTP/2 where available, and Proxy: nil so
// relay traffic can never leak through HTTP_PROXY/HTTPS_PROXY env vars.
func New(st *State, version string, log zerolog.Logger) *Gateway {
	g := &Gateway{
		log: log,
		client: &http.Client{
			Transport: &http.Transport{
				Proxy:                 nil,
				DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   64,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: 0, // edge relays may cold-start; no cap here
				ExpectContinueTimeout: time.Second,
			},
			// Treat redirects as responses to pass through; the gateway is a dumb pipe.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		version: version,
	}
	g.st.Store(st)
	return g
}

// Swap atomically replaces the active state (config reload).
func (g *Gateway) Swap(st *State) { g.st.Store(st) }

// ServeHTTP routes the control endpoints; everything else is the relay path.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	case "/stats":
		g.handleStats(w)
	default:
		g.handleRelay(w, r, g.log)
	}
}

func (g *Gateway) handleStats(w http.ResponseWriter) {
	st := g.st.Load()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"version": g.version,
		"relays":  st.Pool.Stats(),
	})
}

func (g *Gateway) handleRelay(w http.ResponseWriter, r *http.Request, log zerolog.Logger) {
	st := g.st.Load()

	provider, ok := parseProvider(r, st.Pool)
	if !ok {
		jsonError(w, http.StatusNotFound,
			"unknown provider: use /{provider} prefix or "+HeaderProvider+" header (root / = round-robin all)")
		return
	}

	// Buffer the body (capped at the largest per-provider limit) so failover
	// can replay it on the next relay. Per-relay limits are enforced after
	// selection: a body too large for the picked relay's provider skips to
	// another provider that accepts it instead of failing at the edge.
	body, err := io.ReadAll(io.LimitReader(r.Body, st.Cfg.MaxBufferBytes()+1))
	if err != nil {
		jsonError(w, http.StatusBadRequest, "read request body: "+sanitize.ErrorString(err))
		return
	}
	if int64(len(body)) > st.Cfg.MaxBufferBytes() {
		jsonError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("body exceeds every configured provider limit (%d bytes)", st.Cfg.MaxBufferBytes()))
		return
	}

	key := provider
	if key == "" {
		key = pool.KeyAll
	}

	attempts := st.Cfg.MaxRetries + 1
	var lastErr error
	skippedBySize := 0
	for attempt := range attempts {
		relay := st.Pool.Pick(key)
		if relay == nil {
			jsonError(w, http.StatusServiceUnavailable, "no relay configured for "+key)
			return
		}
		if int64(len(body)) > relay.MaxBody {
			skippedBySize++
			log.Debug().Str("relay", relay.Name).Str("provider", relay.Provider).
				Int64("body", int64(len(body))).Int64("maxBody", relay.MaxBody).
				Msg("body exceeds provider limit; skipping relay")
			continue
		}
		start := time.Now()
		resp, err := g.roundTrip(r, relay, body)
		if err != nil {
			// The request never produced a response — safe to fail over.
			// Once response bytes have reached the client, never retry.
			st.Pool.RecordFailure(relay, err)
			lastErr = err
			log.Warn().Str("relay", relay.Name).Str("provider", relay.Provider).
				Int("attempt", attempt+1).Str("err", sanitize.ErrorString(err)).
				Msg("relay attempt failed")
			continue
		}
		relay.Requests.Add(1)
		log.Info().Str("method", r.Method).Str("provider", providerLabel(provider)).
			Str("relay", relay.Name).Int("status", resp.StatusCode).
			Int64("body", int64(len(body))).
			Str("duration", time.Since(start).Round(time.Millisecond).String()).
			Msg("relayed")
		g.copyResponse(w, resp)
		return
	}
	if skippedBySize == attempts && skippedBySize > 0 {
		jsonError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("body of %d bytes exceeds the %s provider limit", len(body), key))
		return
	}
	jsonError(w, http.StatusBadGateway, "all attempts failed: "+sanitize.ErrorString(lastErr))
}

// roundTrip sends one attempt to a relay. The response context is the inbound
// request's, so a disconnected client also tears down the upstream call.
func (g *Gateway) roundTrip(r *http.Request, relay *pool.Relay, body []byte) (*http.Response, error) {
	out, err := http.NewRequestWithContext(r.Context(), r.Method, relay.URL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	copyFilteredHeaders(out.Header, r.Header)
	return g.client.Do(out)
}

// copyResponse streams the relay response straight through, flushing after
// every write — SSE chunks must reach the client immediately.
func (g *Gateway) copyResponse(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	h := w.Header()
	for k, vv := range resp.Header {
		if hopByHop[http.CanonicalHeaderKey(k)] || http.CanonicalHeaderKey(k) == "Content-Length" {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(flushWriter{w}, resp.Body)
}

type flushWriter struct{ w http.ResponseWriter }

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if flusher, ok := f.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}

// parseProvider resolves the requested pin: X-Relay-Provider header first,
// then the first path segment (a client pins by pointing its relay base URL
// at http://gateway/{provider} — the real path travels in
// X-Relay-Path, so the prefix costs nothing). ok=false means an explicit but
// unknown provider.
func parseProvider(r *http.Request, p *pool.Pool) (provider string, ok bool) {
	if v := strings.ToLower(strings.TrimSpace(r.Header.Get(HeaderProvider))); v != "" {
		if unpinned(v) {
			return "", true
		}
		if p.HasProvider(v) {
			return v, true
		}
		return "", false
	}
	seg := strings.TrimPrefix(r.URL.Path, "/")
	if i := strings.IndexByte(seg, '/'); i >= 0 {
		seg = seg[:i]
	}
	seg = strings.ToLower(seg)
	if unpinned(seg) {
		return "", true
	}
	if p.HasProvider(seg) {
		return seg, true
	}
	return "", false
}

// copyFilteredHeaders forwards end-to-end headers only. The gateway-internal
// provider pin and hop-by-hop headers are stripped — edge relays forward
// every header they receive straight to the provider. X-Forwarded-For is
// deliberately never added.
func copyFilteredHeaders(dst, src http.Header) {
	for k, vv := range src {
		ck := http.CanonicalHeaderKey(k)
		if hopByHop[ck] {
			continue
		}
		switch ck {
		case HeaderProvider, "Content-Length", "Host":
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func providerLabel(provider string) string {
	if provider == "" {
		return "auto"
	}
	return provider
}
