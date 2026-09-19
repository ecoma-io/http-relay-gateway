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

	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/sanitize"

	"github.com/rs/zerolog"
)

// HeaderProvider pins a provider ("vercel" | "cloudflare" | "deno" | ...).
// Empty, "none", "auto" or "all" means round-robin across every provider.
const HeaderProvider = "X-Relay-Provider"

// HeaderToken authenticates the gateway to a managed relay. A
// client-supplied value is always stripped: only the gateway's own stored
// token may ever authenticate, and only when the relay leg is built (the
// engine sends it once managed relays exist).
const HeaderToken = "X-Relay-Token"

func unpinned(v string) bool { return v == "" || v == "none" || v == "auto" || v == "all" }

// State is one coherent serving snapshot, swappable atomically. Everything
// the hot path reads is resolved here — request handling never re-derives
// limits or rebuilds clients.
type State struct {
	Pool   *pool.Pool
	Client *http.Client
	// MaxRetries is the number of failover attempts beyond the first.
	MaxRetries int
	// MaxBufferBytes caps the buffered request body: the largest configured
	// provider limit, so oversized bodies 413 before any relay is tried.
	MaxBufferBytes int64
	// StreamThresholdBytes switches request bodies above it to pass-through
	// streaming: a single attempt, no failover (the body is consumed), and
	// no gateway-side 413. 0 keeps every body buffered — the default, and
	// the only retryable mode.
	StreamThresholdBytes int64
}

// Gateway is the root http.Handler.
type Gateway struct {
	st      atomic.Pointer[State]
	version string
	// relayVersion is the worker generation this binary deploys; /stats
	// exposes it so the dashboard can compare it against the fleet.
	relayVersion string
	log          zerolog.Logger
}

// NewTransport builds the shared outbound transport from the timeout
// settings: keep-alive pooled (TLS handshakes amortize to zero), HTTP/2
// where available, and Proxy: nil so relay traffic can never leak through
// HTTP_PROXY/HTTPS_PROXY env vars.
func NewTransport(dialTimeout, responseHeaderTimeout time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
	}
}

// NewClient wraps a transport with the gateway's redirect policy: redirects
// are responses to pass through — the gateway is a dumb pipe.
func NewClient(transport *http.Transport) *http.Client {
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// New builds the Gateway around the initial state, which must carry a
// client. version is the gateway's own version, relayVersion the worker
// generation its deployers ship.
func New(st *State, version, relayVersion string, log zerolog.Logger) *Gateway {
	g := &Gateway{log: log, version: version, relayVersion: relayVersion}
	g.st.Store(st)
	return g
}

// Swap atomically replaces the active state (reload, settings change). The
// outgoing client's idle connections are closed so a timeout-settings
// change does not strand pooled keep-alive sockets; in-flight requests on
// the old client finish untouched.
func (g *Gateway) Swap(st *State) {
	if old := g.st.Swap(st); old != nil && old.Client != nil {
		old.Client.CloseIdleConnections()
	}
}

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
		"version":      g.version,
		"relayVersion": g.relayVersion,
		"relays":       st.Pool.Stats(),
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

	// Body acquisition. Below StreamThresholdBytes (or with streaming off,
	// the default) the body is buffered so failover can replay it; at or
	// above the threshold it streams through untouched — a single attempt,
	// no failover (the body is consumed), and the gateway never invents a
	// 413 for it.
	var body []byte
	var liveBody io.ReadCloser
	streaming := false
	if st.StreamThresholdBytes > 0 && r.ContentLength > st.StreamThresholdBytes {
		streaming = true
		liveBody = r.Body
	} else {
		readCap := st.MaxBufferBytes
		if st.StreamThresholdBytes > 0 {
			readCap = st.StreamThresholdBytes
		}
		buffered, err := io.ReadAll(io.LimitReader(r.Body, readCap+1))
		if err != nil {
			jsonError(w, http.StatusBadRequest, "read request body: "+sanitize.ErrorString(err))
			return
		}
		switch {
		case int64(len(buffered)) <= readCap:
			body = buffered
		case st.StreamThresholdBytes > 0:
			// Over the threshold: stream with the already-read bytes
			// prefixing the live remainder.
			streaming = true
			liveBody = io.NopCloser(io.MultiReader(bytes.NewReader(buffered), r.Body))
		default:
			jsonError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("body exceeds every configured provider limit (%d bytes)", st.MaxBufferBytes))
			return
		}
	}

	key := provider
	if key == "" {
		key = pool.KeyAll
	}

	attempts := st.MaxRetries + 1
	if streaming {
		attempts = 1
	}
	var lastErr error
	skippedBySize := 0
	for attempt := range attempts {
		relay := st.Pool.Pick(key)
		if relay == nil {
			jsonError(w, http.StatusServiceUnavailable, "no relay configured for "+key)
			return
		}
		// Provider body limits are only enforceable on a buffered body; a
		// streaming body goes to the picked relay as-is by design.
		if !streaming && int64(len(body)) > relay.MaxBody {
			skippedBySize++
			log.Debug().Str("relay", relay.Name).Str("provider", relay.Provider).
				Int64("body", int64(len(body))).Int64("maxBody", relay.MaxBody).
				Msg("body exceeds provider limit; skipping relay")
			continue
		}
		start := time.Now()
		resp, err := g.roundTrip(st.Client, r, relay, body, liveBody)
		if err != nil {
			// The request never produced a response — health-recorded
			// everywhere; replayed on the next relay only while the body was
			// buffered. Once response bytes have reached the client, or the
			// body is streaming, never retry.
			st.Pool.RecordFailure(relay, err)
			lastErr = err
			log.Warn().Str("relay", relay.Name).Str("provider", relay.Provider).
				Int("attempt", attempt+1).Str("err", sanitize.ErrorString(err)).
				Msg("relay attempt failed")
			if streaming {
				jsonError(w, http.StatusBadGateway, "streaming request failed: "+sanitize.ErrorString(lastErr))
				return
			}
			continue
		}
		relay.Requests.Add(1)
		// A success clears the failure streak: a flaky relay must not slide
		// into cooldown across interleaved successes.
		st.Pool.RecordSuccess(relay)
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

// roundTrip sends one attempt to a relay: a buffered body (bytes.Reader —
// replayable) or a streaming body (live reader, single attempt). The
// response context is the inbound request's, so a disconnected client also
// tears down the upstream call.
func (g *Gateway) roundTrip(client *http.Client, r *http.Request, relay *pool.Relay, body []byte, live io.ReadCloser) (*http.Response, error) {
	var out *http.Request
	var err error
	if live != nil {
		out, err = http.NewRequestWithContext(r.Context(), r.Method, relay.URL.String(), live)
		if err == nil && r.ContentLength > 0 {
			// Preserve the declared length instead of degrading to chunked.
			out.ContentLength = r.ContentLength
		}
	} else {
		out, err = http.NewRequestWithContext(r.Context(), r.Method, relay.URL.String(), bytes.NewReader(body))
	}
	if err != nil {
		return nil, err
	}
	copyFilteredHeaders(out.Header, r.Header, relay.Policy)
	if relay.Token != "" {
		// Managed-relay auth: the stored token is the only one that may ride
		// this header, and only here — client-supplied values were stripped.
		out.Header.Set(HeaderToken, relay.Token)
	}
	return client.Do(out)
}

// copyResponse streams the relay response straight through, flushing after
// every write — SSE chunks must reach the client immediately.
func (g *Gateway) copyResponse(w http.ResponseWriter, resp *http.Response) {
	// The Close error is discarded explicitly (errcheck): by the time this
	// defer runs the body has been streamed or abandoned, and a Close error
	// on a read-only response body carries no recovery the caller could
	// perform.
	defer func() { _ = resp.Body.Close() }()
	h := w.Header()
	for k, vv := range resp.Header {
		ck := http.CanonicalHeaderKey(k)
		if pool.HopByHop[ck] || ck == "Content-Length" {
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

// copyFilteredHeaders forwards end-to-end headers only. Hop-by-hop headers,
// the gateway-internal spec headers (provider pin, relay token) and
// Content-Length/Host are stripped; then the relay's resolved policy strips
// and sets. X-Forwarded-For is deliberately never added.
func copyFilteredHeaders(dst, src http.Header, policy *pool.HeaderPolicy) {
	for k, vv := range src {
		ck := http.CanonicalHeaderKey(k)
		if pool.HopByHop[ck] {
			continue
		}
		switch ck {
		case HeaderProvider, HeaderToken, "Content-Length", "Host":
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
	applyHeaderPolicy(dst, policy)
}

// applyHeaderPolicy applies a resolved policy to the outgoing header set:
// stripped names are deleted first, then static sets replace their headers —
// except names the policy also strips, where strip wins.
func applyHeaderPolicy(dst http.Header, policy *pool.HeaderPolicy) {
	if policy == nil {
		return
	}
	stripped := make(map[string]bool, len(policy.Strip))
	for _, name := range policy.Strip {
		dst.Del(name)
		stripped[name] = true
	}
	for name, values := range policy.Set {
		if stripped[name] {
			continue
		}
		dst.Del(name)
		for _, v := range values {
			dst.Add(name, v)
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
