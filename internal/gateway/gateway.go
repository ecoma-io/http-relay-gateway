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
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/sanitize"

	"github.com/rs/zerolog"
)

// HeaderProvider pins a provider ("vercel" | "cloudflare" | "deno").
// Empty, "none", "auto" or "all" means round-robin across every provider.
const HeaderProvider = "X-Relay-Provider"

// HeaderToken authenticates the gateway to a relay's worker. A
// client-supplied value is always stripped: only the verified snapshot's
// token may ever authenticate, and it is set only when the relay leg is
// built.
const HeaderToken = "X-Relay-Token"

// HeaderTarget and HeaderPath carry the relay spec's origin and real path.
// In spec mode they arrive from the client and ride to the worker verbatim;
// in proxy mode the gateway derives both from the absolute-form request
// target, so client-supplied values are overwritten, never trusted.
const (
	HeaderTarget = "X-Relay-Target"
	HeaderPath   = "X-Relay-Path"
)

func unpinned(v string) bool { return v == "" || v == "none" || v == "auto" || v == "all" }

// State is one coherent serving snapshot, swappable atomically. Everything
// the hot path reads is resolved here — request handling never re-derives
// limits or rebuilds clients. The pool is built exclusively from the
// readiness registry's verified serving snapshot, so membership here means
// "verified for the current incarnation".
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
	// Lifecycle is the readiness state machine snapshot this generation was
	// built from: one row per relay, verbatim from the registry. It renders
	// on /stats and never carries URLs or tokens.
	Lifecycle []LifecycleRow
}

// LifecycleRow is one relay's readiness phase as the registry saw it when
// this generation was built — the operator-facing state machine view. It
// never carries URLs or tokens: the registry snapshot is the only source,
// and /stats renders it verbatim.
type LifecycleRow struct {
	Name       string `json:"name"`
	Provider   string `json:"provider"`
	State      string `json:"state"`
	Reason     string `json:"reason,omitempty"`
	Generation uint64 `json:"generation"`
}

// Gateway is the root http.Handler.
type Gateway struct {
	st atomic.Pointer[State]
	// mu serializes the relay hot path's load+pick+track against Swap: a
	// request either observes the new pool, or it picked from the old one
	// and its in-flight count is already visible when Swap returns — which
	// is what makes AwaitIdle exact rather than advisory.
	mu sync.Mutex
	// inflight counts requests per relay identity for the replacement
	// rollout: a relay being replaced must not receive new traffic while
	// its old incarnation still carries requests.
	inflight inflight

	version string
	// relayVersion is the worker generation this binary deploys; /stats
	// exposes it so the operator can compare it against the fleet.
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

// Swap atomically replaces the active state. The mutex makes the swap exact
// for the drain: when Swap returns, every request that will still use the
// old pool is already counted in inflight, so an AwaitIdle call after Swap
// cannot miss one. The outgoing client's idle connections are closed so a
// timeout-settings change does not strand pooled keep-alive sockets;
// in-flight requests on the old client finish untouched.
func (g *Gateway) Swap(st *State) {
	g.mu.Lock()
	old := g.st.Swap(st)
	g.mu.Unlock()
	if old != nil && old.Client != nil {
		old.Client.CloseIdleConnections()
	}
}

// AwaitIdle blocks until the relay identity (provider, name) carries no
// in-flight request, or the timeout expires. The replacement rollout calls
// it after demotion and the pool swap, before the new deploy may cut over:
// serving traffic must have drained from the old incarnation first. It
// reports whether the drain completed.
func (g *Gateway) AwaitIdle(provider, name string, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		ch := g.inflight.zeroChan(provider, name)
		if ch == nil {
			return true
		}
		select {
		case <-ch:
			// The count reached zero; loop once to re-check in case a new
			// request began in the interim (the caller demoted first, so
			// this can only be a straggler finishing, not new traffic).
			continue
		case <-deadline.C:
			return false
		}
	}
}

// ServeHTTP dispatches by request shape: proxy-form inbound (absolute-form
// target or CONNECT) goes to the proxy adapter; the control endpoints
// answer origin-form only — an absolute-form /healthz targets some other
// host and must relay, not answer this process; everything else is the
// relay-spec path.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect || r.URL.Host != "" {
		g.handleProxy(w, r, g.log)
		return
	}
	switch r.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	case "/stats":
		g.handleStats(w)
	case "/readyz":
		g.handleReadyz(w)
	default:
		g.handleRelay(w, r, g.log)
	}
}

// handleProxy adapts forward-proxy inbound onto the relay pipeline. An
// absolute-form request target (GET http://api.example.com/v1 HTTP/1.1 —
// what every HTTP client sends through HTTP_PROXY) already carries the
// origin and path the relay spec otherwise reads from HeaderTarget /
// HeaderPath, so the adapter derives both from the URL — overwriting
// whatever the client supplied — and hands the request to the same buffered
// failover / streaming path. The pin is header-only here: the target's path
// belongs to the target, never to the gateway. CONNECT would need a raw TCP
// tunnel the edge relays cannot carry (and its MITM variant is TLS
// termination), so it is a documented 501.
func (g *Gateway) handleProxy(w http.ResponseWriter, r *http.Request, log zerolog.Logger) {
	if r.Method == http.MethodConnect {
		log.Info().Str("method", r.Method).Msg("CONNECT rejected: tunneling unsupported")
		jsonError(w, http.StatusNotImplemented,
			"CONNECT tunneling is not supported; send absolute-form http(s) requests")
		return
	}
	provider, ok, _ := headerProvider(r, g.st.Load().Pool)
	if !ok {
		jsonError(w, http.StatusNotFound,
			"unknown provider: "+HeaderProvider+" header (no header = round-robin all)")
		return
	}
	pr := r.Clone(r.Context())
	pr.Header.Set(HeaderTarget, targetOrigin(r.URL))
	pr.Header.Set(HeaderPath, r.URL.RequestURI())
	g.relay(w, pr, log, provider)
}

// targetOrigin renders the origin (scheme, userinfo, host) of an
// absolute-form request target as the relay spec's target value.
func targetOrigin(u *url.URL) string {
	return (&url.URL{Scheme: u.Scheme, User: u.User, Host: u.Host}).String()
}

func (g *Gateway) handleStats(w http.ResponseWriter) {
	st := g.st.Load()
	ready := st.Pool.ReadyCount()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"version":      g.version,
		"relayVersion": g.relayVersion,
		"relays":       st.Pool.Stats(),
		"readiness":    map[string]any{"ready": ready > 0, "readyRelays": ready},
		"lifecycle":    st.Lifecycle,
	})
}

// handleReadyz is the readiness endpoint: ready only when at least one relay
// is admitted to the serving pool. It is deliberately distinct from
// /healthz — the process liveness probe — so an orchestrator can tell "the
// process is alive" from "the process can actually serve". An alive gateway
// with zero ready relays (nothing verified yet, or the whole fleet demoted)
// answers 503.
func (g *Gateway) handleReadyz(w http.ResponseWriter) {
	st := g.st.Load()
	ready := st.Pool.ReadyCount()
	w.Header().Set("Content-Type", "application/json")
	if ready == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": false, "readyRelays": 0})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "readyRelays": ready})
}

// handleRelay is the origin-form relay-spec entry: the pin comes from the
// X-Relay-Provider header or the /{provider} path prefix.
func (g *Gateway) handleRelay(w http.ResponseWriter, r *http.Request, log zerolog.Logger) {
	provider, ok := parseProvider(r, g.st.Load().Pool)
	if !ok {
		jsonError(w, http.StatusNotFound,
			"unknown provider: use /{provider} prefix or "+HeaderProvider+" header (root / = round-robin all)")
		return
	}
	g.relay(w, r, log, provider)
}

// relay is the one pipeline every inbound shape funnels into: body
// acquisition with failover replay, provider size skips, streaming
// pass-through.
func (g *Gateway) relay(w http.ResponseWriter, r *http.Request, log zerolog.Logger, provider string) {
	st := g.st.Load()

	// Zero-ready short-circuit before body acquisition: with no verified
	// relay the buffer cap is 0, so any body would 413 on the cap check —
	// the honest answer is retryable 503, never 413.
	key := provider
	if key == "" {
		key = pool.KeyAll
	}
	if st.Pool.ReadyCount() == 0 {
		jsonError(w, http.StatusServiceUnavailable, "no relay configured for "+key)
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

	attempts := st.MaxRetries + 1

	if streaming {
		attempts = 1
	}
	var lastErr error
	skippedBySize := 0
	for attempt := range attempts {
		// Pick and register in flight under one lock: a relay picked here is
		// visible to AwaitIdle before any later Swap returns, so a
		// replacement never deploys under a request it cannot see.
		g.mu.Lock()
		relay := st.Pool.Pick(key)
		if relay != nil {
			g.inflight.begin(relay.Provider, relay.Name)
		}
		g.mu.Unlock()
		if relay == nil {
			jsonError(w, http.StatusServiceUnavailable, "no relay configured for "+key)
			return
		}
		// Provider body limits are only enforceable on a buffered body; a
		// streaming body goes to the picked relay as-is by design.
		if !streaming && int64(len(body)) > relay.MaxBody {
			g.inflight.end(relay.Provider, relay.Name)
			skippedBySize++
			log.Debug().Str("relay", relay.Name).Str("provider", relay.Provider).
				Int64("body", int64(len(body))).Int64("maxBody", relay.MaxBody).
				Msg("body exceeds provider limit; skipping relay")
			continue
		}
		start := time.Now()
		resp, err := g.roundTrip(st.Client, r, relay, body, liveBody)
		if err != nil {
			g.inflight.end(relay.Provider, relay.Name)
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
		g.inflight.end(relay.Provider, relay.Name)
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
	copyFilteredHeaders(out.Header, r.Header)
	if relay.Token != "" {
		// Relay auth: the verified snapshot's token is the only one that may
		// ride this header, and only here — client-supplied values were
		// stripped.
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

// headerProvider resolves the pin from the X-Relay-Provider header alone.
// present=false means the header carried nothing and the caller may apply
// its own fallback; ok=false means an explicit but unknown provider.
func headerProvider(r *http.Request, p *pool.Pool) (provider string, ok, present bool) {
	v := strings.ToLower(strings.TrimSpace(r.Header.Get(HeaderProvider)))
	if v == "" {
		return "", true, false
	}
	if unpinned(v) {
		return "", true, true
	}
	// An explicit but unknown provider is a 404 — except while the pool is
	// entirely empty (nothing verified yet): configured-but-unready and
	// never-configured are indistinguishable there, and 503 is the honest,
	// retryable answer. /stats carries the operator's ground truth.
	if p.HasProvider(v) || p.ReadyCount() == 0 {
		return v, true, true
	}
	return "", false, true
}

// parseProvider resolves the requested pin: X-Relay-Provider header first,
// then the first path segment (a client pins by pointing its relay base URL
// at http://gateway/{provider} — the real path travels in
// X-Relay-Path, so the prefix costs nothing). ok=false means an explicit but
// unknown provider. Proxy-form inbound never reaches the path fallback — the
// target's path belongs to the target, not to the gateway.
func parseProvider(r *http.Request, p *pool.Pool) (provider string, ok bool) {
	if provider, ok, present := headerProvider(r, p); present {
		return provider, ok
	}
	seg := strings.TrimPrefix(r.URL.Path, "/")
	if i := strings.IndexByte(seg, '/'); i >= 0 {
		seg = seg[:i]
	}
	seg = strings.ToLower(seg)
	if unpinned(seg) {
		return "", true
	}
	if p.HasProvider(seg) || p.ReadyCount() == 0 {
		return seg, true
	}
	return "", false
}

// copyFilteredHeaders forwards end-to-end headers only: hop-by-hop headers,
// the gateway-internal spec headers (provider pin, relay token) and
// Content-Length/Host are stripped. X-Forwarded-For is deliberately never
// added.
func copyFilteredHeaders(dst, src http.Header) {
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

// inflight is the per-relay-identity request counter behind AwaitIdle. Each
// identity keeps a count plus a channel closed whenever the count reaches
// zero; waiters observe the close instead of polling. begin must be called
// under the Gateway mutex (serializing against Swap); end is lock-free
// against the swap path.
type inflight struct {
	mu    sync.Mutex
	perms map[string]*inflightPerm
}

type inflightPerm struct {
	count int
	zero  chan struct{} // closed on every 1 -> 0 transition
}

func identityKey(provider, name string) string { return provider + "/" + name }

func (f *inflight) begin(provider, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.perms == nil {
		f.perms = map[string]*inflightPerm{}
	}
	id := identityKey(provider, name)
	p := f.perms[id]
	if p == nil {
		p = &inflightPerm{zero: make(chan struct{})}
		f.perms[id] = p
	}
	if p.count == 0 {
		p.zero = make(chan struct{})
	}
	p.count++
}

func (f *inflight) end(provider, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.perms[identityKey(provider, name)]
	if p == nil {
		return
	}
	p.count--
	if p.count == 0 {
		close(p.zero)
	}
}

// zeroChan returns a channel closed when the identity's count next reaches
// zero, or nil when it is already zero.
func (f *inflight) zeroChan(provider, name string) <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.perms[identityKey(provider, name)]
	if p == nil || p.count == 0 {
		return nil
	}
	return p.zero
}
