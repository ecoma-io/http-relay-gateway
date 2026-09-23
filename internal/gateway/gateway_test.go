package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
)

// --- harness ---

type capture struct {
	req  *http.Request
	body []byte
}

// newUpstream starts an edge-relay stand-in that records every request and
// answers with the given status, body and extra headers.
func newUpstream(t *testing.T, status int, body string, extra http.Header) (*httptest.Server, *atomic.Int64, <-chan capture) {
	t.Helper()
	hits := &atomic.Int64{}
	got := make(chan capture, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("upstream read body: %v", err)
		}
		for k, vv := range extra {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		got <- capture{req: r, body: b}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, hits, got
}

func readCapture(t *testing.T, ch <-chan capture) capture {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received the request")
		return capture{}
	}
}

func relayURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

// deadRelayURL always fails at transport level: the listener is closed
// before any request can connect.
func deadRelayURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return "http://" + addr
}

type relaySpec struct {
	name     string
	provider string
	rawURL   string
	token    string
	maxBody  int64
	// incarnation is the admission generation the relay was verified under;
	// the hot path's attempt lines quote it. Zero is legal and unlabeled.
	incarnation uint64
}

func buildPool(t *testing.T, specs []relaySpec) *pool.Pool {
	t.Helper()
	in := pool.Input{FailureThreshold: 3, Cooldown: time.Second}
	for _, s := range specs {
		token := s.token
		if token == "" {
			token = "token-" + s.name
		}
		in.Relays = append(in.Relays, pool.RelayInput{
			Name:        s.name,
			Provider:    s.provider,
			URL:         relayURL(t, s.rawURL),
			Token:       token,
			MaxBody:     s.maxBody,
			Incarnation: s.incarnation,
		})
	}
	p, err := pool.New(in)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	return p
}

func newGateway(t *testing.T, st *State) *Gateway {
	t.Helper()
	if st.Client == nil {
		st.Client = NewClient(NewTransport(time.Second, time.Second))
	}
	return New(st, "test-version", "worker-42", logging.Nop())
}

// captureLog is a mutex-guarded buffer the hot path's JSON attempt lines
// land in: request goroutines write it while the test goroutine reads it.
type captureLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *captureLog) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *captureLog) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// newCapturingGateway is newGateway with the attempt log captured, so a test
// can pin the outcome fields the hot path actually logged.
func newCapturingGateway(t *testing.T, st *State) (*Gateway, *captureLog) {
	t.Helper()
	if st.Client == nil {
		st.Client = NewClient(NewTransport(time.Second, time.Second))
	}
	lc := &captureLog{}
	return New(st, "test-version", "worker-42", logging.New(lc)), lc
}

// waitForLog waits until the hot path has logged the given outcome. The log
// write sits after the classification decision in every branch, so this is
// the happens-before edge that lets the stats assertions below be exact
// instead of racing the handler.
func waitForLog(t *testing.T, lc *captureLog, outcome string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(lc.String(), `"outcome":"`+outcome+`"`) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("attempt log never recorded outcome %q; captured:\n%s", outcome, lc.String())
}

// relayStatsBy builds the name -> stats row view over one pool snapshot.
func relayStatsBy(p *pool.Pool) map[string]pool.StatsRow {
	byName := map[string]pool.StatsRow{}
	for _, row := range p.Stats() {
		byName[row.Name] = row
	}
	return byName
}

func relayRequest(t *testing.T, method, path, body string, header http.Header) *http.Request {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, path, rdr)
	for k, vv := range header {
		for _, v := range vv {
			r.Header.Add(k, v)
		}
	}
	return r
}

// specHeaders carries the minimal relay spec a client must send.
func specHeaders() http.Header {
	h := http.Header{}
	h.Set(HeaderTarget, "https://api.example.com")
	h.Set(HeaderPath, "/v1/data?q=1")
	return h
}

func bufferBytes() int64 { return 1 << 20 }

// --- relay-spec forwarding ---

func TestRelayForwardsSpecVerbatim(t *testing.T) {
	up, hits, got := newUpstream(t, http.StatusCreated, "made",
		http.Header{"Content-Type": []string{"text/plain"}, "X-Upstream": []string{"yes"}})
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, token: "verified-token", maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	h := specHeaders()
	h.Set(HeaderProvider, "vercel")
	h.Set(HeaderToken, "client-forged") // stripped, replaced by the pool's token
	h.Set("Authorization", "Bearer upstream-secret")
	h.Set("X-Custom-Header", "custom-value")
	h.Set("Connection", "close") // hop-by-hop
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", "hello-relay", h))

	if hits.Load() != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits.Load())
	}
	c := readCapture(t, got)
	r := c.req
	if r.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", r.Method)
	}
	if string(c.body) != "hello-relay" {
		t.Fatalf("upstream body = %q, want the client body", c.body)
	}
	if got := r.Header.Get(HeaderTarget); got != "https://api.example.com" {
		t.Fatalf("X-Relay-Target = %q", got)
	}
	if got := r.Header.Get(HeaderPath); got != "/v1/data?q=1" {
		t.Fatalf("X-Relay-Path = %q", got)
	}
	if got := r.Header.Get("Authorization"); got != "Bearer upstream-secret" {
		t.Fatalf("Authorization = %q, want verbatim", got)
	}
	if got := r.Header.Get("X-Custom-Header"); got != "custom-value" {
		t.Fatalf("custom header = %q, want verbatim", got)
	}
	if got := r.Header.Get(HeaderToken); got != "verified-token" {
		t.Fatalf("relay token = %q, want the pool's verified token", got)
	}
	// Content-Length is absent from the forwarded header map (the strip is
	// pinned in TestCopyFilteredHeadersStripsForbidden); the transport then
	// re-derives it from the replayed body, which is correct, not a leak.
	for _, absent := range []string{HeaderProvider, "X-Forwarded-For", "Connection", "Host"} {
		if r.Header.Get(absent) != "" {
			t.Fatalf("upstream saw forbidden header %s: %q", absent, r.Header.Get(absent))
		}
	}

	if rec.Code != http.StatusCreated {
		t.Fatalf("client status = %d, want the upstream's 201", rec.Code)
	}
	if rec.Body.String() != "made" {
		t.Fatalf("client body = %q, want the upstream body", rec.Body.String())
	}
	if rec.Header().Get("X-Upstream") != "yes" {
		t.Fatal("end-to-end response header was dropped")
	}
}

// The relay leg is a dumb pipe for compression: the transport must neither
// advertise gzip the client never asked for nor transparently decode a
// compressed relay answer — the client receives the relay's exact bytes with
// Content-Encoding intact. Go's transport does both by default
// (DisableCompression unset), which twice breaks the verbatim contract.
func TestRelayLegForwardsCompressionVerbatim(t *testing.T) {
	var packed bytes.Buffer
	zw := gzip.NewWriter(&packed)
	if _, err := zw.Write([]byte("gzip-me")); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	gz := packed.Bytes()

	up, _, got := newUpstream(t, http.StatusOK, string(gz),
		http.Header{"Content-Encoding": []string{"gzip"}})
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))

	// The client sent no Accept-Encoding, so the relay leg must not invent one.
	c := readCapture(t, got)
	if ae := c.req.Header.Get("Accept-Encoding"); ae != "" {
		t.Fatalf("relay leg sent Accept-Encoding = %q; the client sent none", ae)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if enc := rec.Header().Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("client Content-Encoding = %q, want the relay's gzip passed through", enc)
	}
	if !bytes.Equal(rec.Body.Bytes(), gz) {
		t.Fatalf("client body = % x, want the relay's %d gzip bytes verbatim", rec.Body.Bytes(), len(gz))
	}
}

func TestCopyFilteredHeadersStripsForbidden(t *testing.T) {
	src := http.Header{}
	src.Set("Connection", "keep-alive")
	src.Set("Proxy-Connection", "keep-alive")
	src.Set("Keep-Alive", "timeout=5")
	src.Set("Proxy-Authenticate", "Basic realm=x")
	src.Set("Proxy-Authorization", "Basic y")
	src.Set("Te", "trailers")
	src.Set("Trailer", "X-Sum")
	src.Set("Transfer-Encoding", "chunked")
	src.Set("Upgrade", "h2c")
	src.Set(HeaderProvider, "vercel")
	src.Set(HeaderToken, "forged")
	src.Set("Content-Length", "10")
	src.Set("Host", "gateway.internal")
	src.Set("X-Keep", "yes")

	dst := http.Header{}
	copyFilteredHeaders(dst, src)
	if len(dst) != 1 || dst.Get("X-Keep") != "yes" {
		t.Fatalf("filtered headers = %v, want only X-Keep", dst)
	}
}

func TestCopyResponseStripsHopByHopAndLength(t *testing.T) {
	g := newGateway(t, &State{Pool: buildPool(t, nil)})
	rec := httptest.NewRecorder()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":     []string{"application/json"},
			"Content-Length":   []string{"2"},
			"Connection":       []string{"keep-alive"},
			"Proxy-Connection": []string{"keep-alive"},
			"X-End-To-End":     []string{"stay"},
		},
		Body: io.NopCloser(strings.NewReader("ok")),
	}
	_ = g.copyResponse(rec, resp)
	if rec.Header().Get("X-End-To-End") != "stay" {
		t.Fatal("end-to-end response header was dropped")
	}
	for _, stripped := range []string{"Content-Length", "Connection", "Proxy-Connection"} {
		if rec.Header().Get(stripped) != "" {
			t.Fatalf("response kept hop-by-hop header %s", stripped)
		}
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("response body = %q, want ok", rec.Body.String())
	}
}

// errReader yields nothing and fails immediately — the mid-body read failure
// a relay leg produces when it breaks after the headers.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// TestCopyResponseReturnsTheCopyError pins the signature: the copy's error
// is the outcome signal relay() classifies. A discarded error here is the
// silent mid-stream success issue #59 describes — truncated client, full
// success accounting, nothing in the log.
func TestCopyResponseReturnsTheCopyError(t *testing.T) {
	g := newGateway(t, &State{Pool: buildPool(t, nil)})
	boom := errors.New("relay leg died mid-body")
	rec := httptest.NewRecorder()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
		Body: io.NopCloser(io.MultiReader(
			strings.NewReader("partial"),
			errReader{err: boom},
		)),
	}
	if err := g.copyResponse(rec, resp); !errors.Is(err, boom) {
		t.Fatalf("copyResponse error = %v, want the body's %v", err, boom)
	}
	// The bytes that made it through before the failure still reached the
	// client — the copy ends, it does not unwind.
	if got := rec.Body.String(); got != "partial" {
		t.Fatalf("client body = %q, want the partial prefix", got)
	}
}

// --- provider pinning ---

func TestPinResolution(t *testing.T) {
	upV, hitsV, gotV := newUpstream(t, http.StatusOK, "vercel", nil)
	upC, hitsC, gotC := newUpstream(t, http.StatusOK, "cloudflare", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: upV.URL, maxBody: bufferBytes()},
			{name: "beta", provider: "cloudflare", rawURL: upC.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	t.Run("unpinned round-robins in config order", func(t *testing.T) {
		for range 2 {
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d", rec.Code)
			}
		}
		if hitsV.Load() != 1 || hitsC.Load() != 1 {
			t.Fatalf("hits vercel=%d cloudflare=%d, want the deterministic a,b rotation", hitsV.Load(), hitsC.Load())
		}
		readCapture(t, gotV)
		readCapture(t, gotC)
	})

	t.Run("header wins over path prefix", func(t *testing.T) {
		h := specHeaders()
		h.Set(HeaderProvider, "cloudflare")
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/vercel/ignored", "", h))
		if hitsC.Load() != 2 {
			t.Fatalf("cloudflare hits = %d, want the header pin to win", hitsC.Load())
		}
		readCapture(t, gotC)
	})

	t.Run("unpinned spellings", func(t *testing.T) {
		for _, pin := range []string{"", "none", "auto", "all"} {
			h := specHeaders()
			if pin != "" {
				h.Set(HeaderProvider, pin)
			}
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", h))
			if rec.Code != http.StatusOK {
				t.Fatalf("pin %q: status = %d, want 200", pin, rec.Code)
			}
		}
	})

	t.Run("header pin is case-insensitive", func(t *testing.T) {
		h := specHeaders()
		h.Set(HeaderProvider, "VERCEL")
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", h))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		readCapture(t, gotV)
	})

	t.Run("path prefix pins", func(t *testing.T) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/cloudflare/real/path", "", specHeaders()))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		c := readCapture(t, gotC)
		// The client's spec headers ride verbatim; the prefix is only a pin.
		if got := c.req.Header.Get(HeaderPath); got != "/v1/data?q=1" {
			t.Fatalf("X-Relay-Path = %q, want the client value untouched", got)
		}
	})

	t.Run("unknown pin is 404", func(t *testing.T) {
		cases := []struct {
			name, path, headerVal string
		}{
			{"header pin", "/", "deno"},
			{"path pin", "/deno/x", ""},
		}
		for _, tc := range cases {
			h := specHeaders()
			if tc.headerVal != "" {
				h.Set(HeaderProvider, tc.headerVal)
			}
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, relayRequest(t, http.MethodGet, tc.path, "", h))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s: status = %d, want 404", tc.name, rec.Code)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("%s: body %q: %v", tc.name, rec.Body.String(), err)
			}
			if !strings.Contains(body["error"], "unknown provider") {
				t.Fatalf("%s: error = %q", tc.name, body["error"])
			}
		}
	})
}

// --- zero-ready ---

func TestZeroReadyAnswers503(t *testing.T) {
	g := newGateway(t, &State{Pool: buildPool(t, nil), MaxRetries: 1})

	t.Run("explicit pin", func(t *testing.T) {
		h := specHeaders()
		h.Set(HeaderProvider, "deno")
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", h))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 while nothing is verified", rec.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(body["error"], "deno") {
			t.Fatalf("error = %q, want the pinned provider named", body["error"])
		}
	})

	t.Run("unpinned", func(t *testing.T) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})

	t.Run("never 413", func(t *testing.T) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", strings.Repeat("x", 64), specHeaders()))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("zero-ready with a body = %d, want the honest retryable 503", rec.Code)
		}
	})
}

// --- control endpoints ---

func TestControlEndpoints(t *testing.T) {
	empty := newGateway(t, &State{Pool: buildPool(t, nil)})

	rec := httptest.NewRecorder()
	empty.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 with zero ready relays", rec.Code)
	}
	var ready struct {
		Ready       bool `json:"ready"`
		ReadyRelays int  `json:"readyRelays"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Ready || ready.ReadyRelays != 0 {
		t.Fatalf("/readyz body = %+v, want ready:false readyRelays:0", ready)
	}

	rec = httptest.NewRecorder()
	empty.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("/healthz = %d %q, want 200 ok", rec.Code, rec.Body.String())
	}

	full := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: "http://alpha.internal", maxBody: bufferBytes()},
		}),
	})
	rec = httptest.NewRecorder()
	full.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 once a relay is admitted", rec.Code)
	}
	ready = struct {
		Ready       bool `json:"ready"`
		ReadyRelays int  `json:"readyRelays"`
	}{}
	if err := json.Unmarshal(rec.Body.Bytes(), &ready); err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.ReadyRelays != 1 {
		t.Fatalf("/readyz body = %+v, want ready:true readyRelays:1", ready)
	}
}

func TestStatsExposesNoSecrets(t *testing.T) {
	// The scenario is a mid-stream failure, not a clean answer: it is the
	// one that populates every counter the contract renders — including
	// midstreamFailures, whose allowlist entry the leak-scan would otherwise
	// never exercise (omitempty hides it while zero).
	up, _ := newTruncatingUpstream(t)
	g, lc := newCapturingGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, token: "very-secret-token", maxBody: pool.VercelMaxBody},
		}),
		MaxRetries:     0, // one attempt: a started response is never retried
		MaxBufferBytes: bufferBytes(),
		Lifecycle: []LifecycleRow{
			{Name: "alpha", Provider: "vercel", State: "ready", Generation: 7},
			{Name: "beta", Provider: "deno", State: "failed", Reason: "probe_failed", Generation: 2},
		},
	})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", "buffered", specHeaders()))
	if rec.Code != http.StatusOK {
		t.Fatalf("relay status = %d, want the relay's original 200 before /stats", rec.Code)
	}
	waitForLog(t, lc, "upstream_midstream")

	rec = httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/stats = %d, want 200", rec.Code)
	}
	raw := rec.Body.String()
	var doc struct {
		Version      string           `json:"version"`
		RelayVersion string           `json:"relayVersion"`
		Relays       []map[string]any `json:"relays"`
		Readiness    struct {
			Ready       bool `json:"ready"`
			ReadyRelays int  `json:"readyRelays"`
		} `json:"readiness"`
		Lifecycle []map[string]any `json:"lifecycle"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode /stats: %v\n%s", err, raw)
	}
	if doc.Version != "test-version" || doc.RelayVersion != "worker-42" {
		t.Fatalf("version fields = %q / %q", doc.Version, doc.RelayVersion)
	}
	if !doc.Readiness.Ready || doc.Readiness.ReadyRelays != 1 {
		t.Fatalf("readiness = %+v", doc.Readiness)
	}
	if len(doc.Relays) != 1 {
		t.Fatalf("relay rows = %d, want 1", len(doc.Relays))
	}
	allowed := map[string]bool{
		"name": true, "provider": true, "healthy": true, "maxBody": true,
		"requests": true, "failures": true, "lastError": true,
		"midstreamFailures": true,
	}
	for k := range doc.Relays[0] {
		if !allowed[k] {
			t.Fatalf("relay stats key %q would leak a URL or token", k)
		}
	}
	if doc.Relays[0]["requests"] != float64(1) || doc.Relays[0]["failures"] != float64(1) {
		t.Fatalf("counters = %v / %v, want 1 request, 1 failure", doc.Relays[0]["requests"], doc.Relays[0]["failures"])
	}
	// The contract field itself: rendered, non-zero, and equal to the count
	// the mid-stream classification recorded.
	if doc.Relays[0]["midstreamFailures"] != float64(1) {
		t.Fatalf("midstreamFailures = %v, want 1 — the leak-scan must cover it while rendered", doc.Relays[0]["midstreamFailures"])
	}
	if len(doc.Lifecycle) != 2 {
		t.Fatalf("lifecycle rows = %d, want one per configured relay", len(doc.Lifecycle))
	}
	var alpha map[string]any
	for _, row := range doc.Lifecycle {
		if row["name"] == "alpha" {
			alpha = row
		}
	}
	if alpha == nil {
		t.Fatal("lifecycle is missing the ready relay")
	}
	if alpha["state"] != "ready" || alpha["generation"] != float64(7) {
		t.Fatalf("alpha lifecycle = %v, want ready at generation 7", alpha)
	}
	if s := raw; strings.Contains(s, "very-secret-token") || strings.Contains(s, "127.0.0.1") || strings.Contains(s, "http://") {
		t.Fatalf("/stats expose secrets: %s", s)
	}
}

// --- failover ---

func TestFailoverReplaysBufferedBody(t *testing.T) {
	up, _, got := newUpstream(t, http.StatusOK, "served", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "dead", provider: "vercel", rawURL: deadRelayURL(t), maxBody: bufferBytes()},
			{name: "live", provider: "cloudflare", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", "replay-me", specHeaders()))
	if rec.Code != http.StatusOK || rec.Body.String() != "served" {
		t.Fatalf("status = %d body = %q, want the second relay's 200", rec.Code, rec.Body.String())
	}
	c := readCapture(t, got)
	if string(c.body) != "replay-me" {
		t.Fatalf("failover must replay the buffered body, upstream saw %q", c.body)
	}
	byName := map[string]pool.StatsRow{}
	for _, row := range g.st.Load().Pool.Stats() {
		byName[row.Name] = row
	}
	if byName["dead"].Failures != 1 || byName["dead"].Requests != 0 {
		t.Fatalf("dead relay row = %+v, want one recorded failure", byName["dead"])
	}
	if byName["dead"].LastError == "" {
		t.Fatal("dead relay must carry its last error")
	}
	if byName["live"].Requests != 1 || byName["live"].Failures != 0 {
		t.Fatalf("live relay row = %+v", byName["live"])
	}
}

func TestFailoverExhaustedAnswers502(t *testing.T) {
	dead1, dead2 := deadRelayURL(t), deadRelayURL(t)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "dead-a", provider: "vercel", rawURL: dead1, maxBody: bufferBytes()},
			{name: "dead-b", provider: "cloudflare", rawURL: dead2, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", "body", specHeaders()))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 after every attempt failed", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "all attempts failed") {
		t.Fatalf("body = %q, want the exhaustion message", rec.Body.String())
	}
}

func TestResponseStartedIsNeverRetried(t *testing.T) {
	up500, hits500, got500 := newUpstream(t, http.StatusInternalServerError, "boom", nil)
	up200, hits200, _ := newUpstream(t, http.StatusOK, "fine", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "errors", provider: "vercel", rawURL: up500.URL, maxBody: bufferBytes()},
			{name: "fine", provider: "cloudflare", rawURL: up200.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     3,
		MaxBufferBytes: bufferBytes(),
	})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))
	if rec.Code != http.StatusInternalServerError || rec.Body.String() != "boom" {
		t.Fatalf("status = %d body = %q, want the 500 to pass through untouched", rec.Code, rec.Body.String())
	}
	if hits200.Load() != 0 {
		t.Fatal("a relay that answered must never be retried past")
	}
	if hits500.Load() != 1 {
		t.Fatalf("first relay hits = %d, want exactly one attempt", hits500.Load())
	}
	readCapture(t, got500)
	for _, row := range g.st.Load().Pool.Stats() {
		if row.Name == "errors" && (row.Requests != 1 || row.Failures != 0) {
			t.Fatalf("an answered request is not a failure: %+v", row)
		}
	}
}

// newTruncatingUpstream answers 200 with a declared 128-byte body, ships a
// short prefix and hard-closes the connection — the relay leg that breaks
// after the headers. A handler that merely returns early is re-framed into a
// complete chunked response by the server, so the hijack is what makes the
// leg actually break mid-body. Every request it serves does this.
func newTruncatingUpstream(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("upstream server does not support hijacking")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		// The Fprintf error is discarded explicitly (errcheck): the write
		// target is the hijacked conn's own buffered writer, and the test's
		// assertions observe the client-visible effect either way.
		_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Length: 128\r\n"+
			"Content-Type: text/plain\r\n\r\ntruncated-body")
		if err := buf.Flush(); err != nil {
			t.Errorf("flush truncated answer: %v", err)
		}
		_ = conn.Close()
	}))
	t.Cleanup(up.Close)
	return up, hits
}

// TestMidstreamUpstreamErrorIsPassiveAndNeverRetried pins the issue #59
// accounting and the issue #62 fix: a relay that answers and then breaks its
// body mid-stream is a counted attempt AND a passive transport failure whose
// streak ACCUMULATES across consecutive attempts (no header-time success
// resets it), so threshold 2 trips on the second one — and the truncated
// answer is never retried (the client already holds part of the body).
func TestMidstreamUpstreamErrorIsPassiveAndNeverRetried(t *testing.T) {
	up, hits := newTruncatingUpstream(t)

	// threshold 2: the second consecutive mid-stream failure must trip the
	// cooldown. Under the pre-#62 ordering each attempt also recorded a
	// header-time success that reset the streak first, so it never could —
	// that is exactly the regression this pin guards. The cooldown is a day:
	// the trip is asserted immediately after the second failure against
	// RecordFailure's wall-clock now, and this test never waits for expiry —
	// a short cooldown here would make healthy=false race the clock on a
	// loaded CI runner for no gain.
	in := pool.Input{FailureThreshold: 2, Cooldown: 24 * time.Hour}
	in.Relays = append(in.Relays, pool.RelayInput{
		Name: "mid", Provider: "vercel", URL: relayURL(t, up.URL),
		Token: "token-mid", MaxBody: bufferBytes(),
	})
	p, err := pool.New(in)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	g, lc := newCapturingGateway(t, &State{
		Pool:           p,
		MaxRetries:     3, // available and never used: a started response is never retried
		MaxBufferBytes: bufferBytes(),
	})

	for i := range 2 {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", "buffered", specHeaders()))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want the relay's original 200", i+1, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "truncated-body") {
			t.Fatalf("request %d: client body = %q, want the truncated prefix", i+1, rec.Body.String())
		}
		waitForLog(t, lc, "upstream_midstream")

		row := relayStatsBy(g.st.Load().Pool)["mid"]
		if row.Requests != int64(i+1) || row.Failures != int64(i+1) || row.MidstreamFailures != int64(i+1) {
			t.Fatalf("request %d: row = %+v, want %d requests, %d failures, %d mid-stream",
				i+1, row, i+1, i+1, i+1)
		}
		if n := hits.Load(); n != int64(i+1) {
			t.Fatalf("request %d: upstream hits = %d, want exactly one attempt per request", i+1, n)
		}
		// The streak accumulates: after the first failure the relay is still
		// healthy (one failure below the threshold of 2), and only the second
		// one trips the cooldown.
		if want := i == 1; row.Healthy == want {
			t.Fatalf("request %d: row = %+v, want healthy=%t — the mid-stream streak must accumulate",
				i+1, row, !want)
		}
	}
}

// TestCompletedSuccessResetsTheMidstreamStreak pins the issue #62 evidence
// rule from the success side: the passive-health success record belongs to a
// COMPLETED response body, never to the header receipt. The pins, in order:
// the reset lands only once the body finished copying (it is still pending
// while the body is mid-flight), an interleaved completed response resets the
// failure streak so a subsequent mid-stream failure sits below the threshold,
// and a client aborted mid-copy records neither success nor failure — it must
// not clear the previous failure's evidence.
func TestCompletedSuccessResetsTheMidstreamStreak(t *testing.T) {
	hits := &atomic.Int64{}
	// releaseBody is closed by the test once it has read the complete
	// answer's bytes mid-flight; the upstream then lets the body EOF. The
	// fallback timer only fires on a failed test, so a regression can never
	// hang the suite.
	releaseBody := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch hits.Add(1) {
		case 1, 3:
			// Mid-stream break: prefix of the declared body, hard close.
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("upstream server does not support hijacking")
				return
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			// Fprintf's error is discarded explicitly (errcheck); the
			// assertions observe the client-visible effect either way.
			_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Length: 128\r\n"+
				"Content-Type: text/plain\r\n\r\ntruncated-body")
			if err := buf.Flush(); err != nil {
				t.Errorf("flush truncated answer: %v", err)
			}
			_ = conn.Close()
		case 2:
			// A complete answer whose body deliberately finishes late: the
			// client already holds every byte while the relay body is still
			// open — exactly the window where a header-time success would
			// have recorded too early. The body stays open until the test
			// releases it, so the mid-flight row assert below races nothing.
			fl := w.(http.Flusher)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("whole"))
			fl.Flush()
			select {
			case <-r.Context().Done():
			case <-releaseBody:
			case <-time.After(2 * time.Second):
			}
		case 4:
			// The client leaves mid-body: one flushed chunk, then wait.
			fl := w.(http.Flusher)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("chunk-0\n"))
			fl.Flush()
			<-r.Context().Done()
		}
	}))
	t.Cleanup(up.Close)

	in := pool.Input{FailureThreshold: 2, Cooldown: time.Second}
	in.Relays = append(in.Relays, pool.RelayInput{
		Name: "solo", Provider: "vercel", URL: relayURL(t, up.URL),
		Token: "token-solo", MaxBody: bufferBytes(),
	})
	p, err := pool.New(in)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	g, lc := newCapturingGateway(t, &State{
		Pool:           p,
		MaxRetries:     0, // one attempt: a started response is never retried
		MaxBufferBytes: bufferBytes(),
	})
	gwSrv := httptest.NewServer(g)
	t.Cleanup(gwSrv.Close)

	row := func() pool.StatsRow { return relayStatsBy(g.st.Load().Pool)["solo"] }

	// 1. Mid-stream break: one counted attempt, one passive failure, and the
	// failure's evidence on the row.
	body := gwPost(t, gwSrv)
	if !strings.HasPrefix(body, "truncated-body") {
		t.Fatalf("body = %q, want the truncated prefix", body)
	}
	waitForLog(t, lc, "upstream_midstream")
	if r := row(); r.Requests != 1 || r.Failures != 1 || r.MidstreamFailures != 1 ||
		r.LastError == "" || !r.Healthy {
		t.Fatalf("after the mid-stream break: row = %+v, want 1 request, 1 failure, 1 mid-stream, lastError set, healthy", r)
	}

	// 2. A complete answer whose body finishes late. Read its bytes WITHOUT
	// waiting for the body EOF, and check the row mid-flight: the previous
	// failure's evidence must still be there — the headers alone are no
	// success. Only the completed copy resets the streak.
	req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set(HeaderTarget, "https://target.example")
	req.Header.Set(HeaderPath, "/v1")
	resp, err := gwSrv.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	prefix := make([]byte, len("whole"))
	if _, err := io.ReadFull(resp.Body, prefix); err != nil {
		t.Fatalf("read body prefix: %v", err)
	}
	if string(prefix) != "whole" {
		t.Fatalf("body prefix = %q, want the complete answer's bytes", prefix)
	}
	// The relay body is still open here — the upstream holds it until
	// releaseBody below — so the copy cannot have reached EOF and the
	// success record cannot have landed yet. No sleep: the hold makes the
	// mid-flight window a fact, not a timing bet.
	if r := row(); r.LastError == "" || r.Failures != 1 {
		t.Fatalf("mid-flight of the completed body: row = %+v, want lastError still set and still 1 failure — headers are not success evidence", r)
	}
	close(releaseBody) // the upstream lets the body EOF: the copy completes
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("drain body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	waitForStats(t, row, func(r pool.StatsRow) bool { return r.LastError == "" },
		"the completed body to reset the streak")
	if r := row(); r.Requests != 2 || r.Failures != 1 || r.MidstreamFailures != 1 || !r.Healthy {
		t.Fatalf("after the completed body: row = %+v, want the streak reset, counters unchanged", r)
	}

	// 3. Another mid-stream break: it lands on a cleared streak, so the relay
	// stays healthy — the completed response in between is what saved it. An
	// unreset streak would have made this the second consecutive failure and
	// tripped the threshold-2 cooldown.
	body = gwPost(t, gwSrv)
	if !strings.HasPrefix(body, "truncated-body") {
		t.Fatalf("body = %q, want the truncated prefix", body)
	}
	waitForLog(t, lc, "upstream_midstream")
	if r := row(); r.Requests != 3 || r.Failures != 2 || r.MidstreamFailures != 2 || !r.Healthy {
		t.Fatalf("after the second break: row = %+v, want 2 failures on a healthy relay — the completed response reset the streak", r)
	}

	// 4. A client abort mid-copy: neither a success nor a failure. The old
	// failure's evidence must survive it — a success recorded here would
	// clear lastError and lift the streak.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
	if reqErr != nil {
		t.Fatalf("build request: %v", reqErr)
	}
	req.Header.Set(HeaderTarget, "https://target.example")
	req.Header.Set(HeaderPath, "/v1")
	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		resp, doErr := gwSrv.Client().Do(req)
		if doErr != nil {
			return // expected: the canceled request fails on the client
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	waitForStats(t, row, func(pool.StatsRow) bool { return hits.Load() >= 4 },
		"the relay to answer the fourth request")
	cancel() // the client goes away mid-body
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled request never returned")
	}
	waitForLog(t, lc, "client_aborted")
	if r := row(); r.Requests != 4 || r.Failures != 2 || r.MidstreamFailures != 2 ||
		r.LastError == "" || !r.Healthy {
		t.Fatalf("after the client abort: row = %+v, want counters unchanged, lastError preserved, healthy — an abort is neither success nor failure", r)
	}
}

// gwPost sends one relay-spec POST through the real gateway server and
// returns the fully read body (the classification it drives completes before
// the response does).
func gwPost(t *testing.T, gwSrv *httptest.Server) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set(HeaderTarget, "https://target.example")
	req.Header.Set(HeaderPath, "/v1")
	resp, err := gwSrv.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(raw)
}

// waitForStats polls a pool stats row until pred holds — the completion-time
// success record lands microseconds after the body EOFs, so the test waits
// for it instead of racing it.
func waitForStats(t *testing.T, row func() pool.StatsRow, pred func(pool.StatsRow) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pred(row()) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; last row = %+v", what, row())
}

// roundTripperFunc adapts one function to http.RoundTripper for a focused
// transport seam. It lets a test control the outbound leg's exact context
// without relying on a second TCP server's remote-close scheduling.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestClientCancellationIsNotAPassiveFailure pins the client_aborted half of
// the classification: a client that goes away — before the relay answers or
// mid-body — is never evidence against the relay. Its teardown takes the
// upstream leg with it, so recording a passive failure would cool a relay
// down for the caller's own hangup.
func TestClientCancellationIsNotAPassiveFailure(t *testing.T) {
	t.Run("before the relay answers", func(t *testing.T) {
		// Use the handler seam directly instead of asking two real TCP legs to
		// observe cancellation in an unspecified order. The transport receives
		// the exact context roundTrip gives its outbound leg; it parks until
		// that context is canceled, then returns the cancellation error. This
		// makes the pre-response client-abort state factual and pins its
		// classification without racing net/http's remote-close detection.
		entered := make(chan struct{})
		hits := &atomic.Int64{}
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			hits.Add(1)
			close(entered)
			<-req.Context().Done()
			return nil, req.Context().Err()
		})
		g, lc := newCapturingGateway(t, &State{
			Pool: buildPool(t, []relaySpec{
				{name: "solo", provider: "vercel", rawURL: "http://solo.internal", maxBody: bufferBytes()},
			}),
			Client:         &http.Client{Transport: transport},
			MaxRetries:     3, // available and never used: the client is gone
			MaxBufferBytes: bufferBytes(),
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req := relayRequest(t, http.MethodPost, "/", "x", specHeaders()).WithContext(ctx)
		req.Header.Set(HeaderProvider, "vercel")
		done := make(chan struct{})
		go func() {
			defer close(done)
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("request never reached the relay transport")
		}

		cancel() // the client goes away before any response byte
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("canceled request never returned from the gateway")
		}

		waitForLog(t, lc, "client_aborted")
		row := relayStatsBy(g.st.Load().Pool)["solo"]
		if row.Requests != 0 || row.Failures != 0 || row.MidstreamFailures != 0 || !row.Healthy {
			t.Fatalf("row = %+v, want zero counters, healthy — a client abort is not a relay failure", row)
		}
		if n := hits.Load(); n != 1 {
			t.Fatalf("upstream hits = %d, want exactly one attempt with no retry", n)
		}
	})

	t.Run("mid-body", func(t *testing.T) {
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fl := w.(http.Flusher)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("chunk-0\n"))
			fl.Flush()
			<-r.Context().Done() // the body stays open until the client leaves
		}))
		t.Cleanup(up.Close)

		g, lc := newCapturingGateway(t, &State{
			Pool: buildPool(t, []relaySpec{
				{name: "solo", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
			}),
			MaxRetries:     3,
			MaxBufferBytes: bufferBytes(),
		})
		gwSrv := httptest.NewServer(g)
		t.Cleanup(gwSrv.Close)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gwSrv.URL+"/", nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set(HeaderProvider, "vercel")
		resp, err := gwSrv.Client().Do(req)
		if err != nil {
			t.Fatalf("streaming request: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		prefix := make([]byte, len("chunk-0\n"))
		if _, err := io.ReadFull(resp.Body, prefix); err != nil {
			t.Fatalf("read first chunk: %v", err)
		}

		cancel() // the client goes away mid-body
		_, _ = io.Copy(io.Discard, resp.Body)
		waitForLog(t, lc, "client_aborted")
		if strings.Contains(lc.String(), `"outcome":"upstream_midstream"`) {
			t.Fatal("a client abort mid-body was classified against the relay")
		}
		row := relayStatsBy(g.st.Load().Pool)["solo"]
		if row.Requests != 1 || row.Failures != 0 || row.MidstreamFailures != 0 || !row.Healthy {
			t.Fatalf("row = %+v, want the counted request with zero failures — the answer itself was transport-level success", row)
		}
	})
}

// --- streaming ---

func TestStreamingIsSingleAttempt(t *testing.T) {
	up200, hits200, _ := newUpstream(t, http.StatusOK, "ok", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "dead", provider: "vercel", rawURL: deadRelayURL(t), maxBody: bufferBytes()},
			{name: "live", provider: "cloudflare", rawURL: up200.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:           3,
		MaxBufferBytes:       bufferBytes(),
		StreamThresholdBytes: 8,
	})

	body := strings.Repeat("a", 9) // above the threshold: streams
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", body, specHeaders()))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 for a failed streaming attempt", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "streaming request failed") {
		t.Fatalf("body = %q, want the streaming failure message", rec.Body.String())
	}
	if hits200.Load() != 0 {
		t.Fatal("a streaming body is consumed: there must be no failover attempt")
	}
}

// The threshold is strict: exactly at it the body stays buffered and
// failover-capable, one byte above it the request streams as one attempt.
func TestStreamingThresholdBoundary(t *testing.T) {
	up200, _, got := newUpstream(t, http.StatusOK, "ok", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "dead", provider: "vercel", rawURL: deadRelayURL(t), maxBody: bufferBytes()},
			{name: "live", provider: "cloudflare", rawURL: up200.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:           1,
		MaxBufferBytes:       bufferBytes(),
		StreamThresholdBytes: 8,
	})

	t.Run("exactly at the threshold is buffered", func(t *testing.T) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", strings.Repeat("a", 8), specHeaders()))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want failover to succeed for an at-threshold body", rec.Code)
		}
		if c := readCapture(t, got); len(c.body) != 8 {
			t.Fatalf("forwarded body length = %d, want 8", len(c.body))
		}
	})

	t.Run("one byte above streams", func(t *testing.T) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", strings.Repeat("a", 9), specHeaders()))
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want the single streaming attempt to fail on the dead relay", rec.Code)
		}
	})
}

func TestStreamingPreservesContentLength(t *testing.T) {
	up, _, got := newUpstream(t, http.StatusOK, "ok", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "live", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:           1,
		MaxBufferBytes:       bufferBytes(),
		StreamThresholdBytes: 8,
	})

	body := strings.Repeat("a", 9)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", body, specHeaders()))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	c := readCapture(t, got)
	if c.req.ContentLength != int64(len(body)) {
		t.Fatalf("streamed ContentLength = %d, want the declared %d", c.req.ContentLength, len(body))
	}
	if string(c.body) != body {
		t.Fatalf("streamed body = %q, want it passed through untouched", c.body)
	}
}

// --- body limits ---

func TestBodySizeSkipAnd413(t *testing.T) {
	t.Run("skips to a provider that accepts", func(t *testing.T) {
		up, hits, got := newUpstream(t, http.StatusOK, "ok", nil)
		g := newGateway(t, &State{
			Pool: buildPool(t, []relaySpec{
				{name: "small", provider: "vercel", rawURL: "http://small.internal", maxBody: 16},
				{name: "big", provider: "cloudflare", rawURL: up.URL, maxBody: 1024},
			}),
			MaxRetries:     2,
			MaxBufferBytes: 1024,
		})
		body := strings.Repeat("x", 32)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", body, specHeaders()))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want the accepting provider's 200", rec.Code)
		}
		if hits.Load() != 1 {
			t.Fatalf("upstream hits = %d, want exactly the accepting relay", hits.Load())
		}
		if c := readCapture(t, got); string(c.body) != body {
			t.Fatalf("forwarded body = %q, want %q", c.body, body)
		}
	})

	t.Run("no accepting provider is 413", func(t *testing.T) {
		up, hits, _ := newUpstream(t, http.StatusOK, "ok", nil)
		g := newGateway(t, &State{
			Pool: buildPool(t, []relaySpec{
				{name: "small", provider: "vercel", rawURL: up.URL, maxBody: 16},
			}),
			MaxRetries:     2,
			MaxBufferBytes: 1024,
		})
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", strings.Repeat("x", 32), specHeaders()))
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413 when every candidate rejects the size", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "exceeds") {
			t.Fatalf("body = %q, want the size explanation", rec.Body.String())
		}
		if hits.Load() != 0 {
			t.Fatal("an oversized body must never reach a relay")
		}
	})

	t.Run("over the buffer cap is 413 before any attempt", func(t *testing.T) {
		up, hits, _ := newUpstream(t, http.StatusOK, "ok", nil)
		g := newGateway(t, &State{
			Pool: buildPool(t, []relaySpec{
				{name: "a", provider: "vercel", rawURL: up.URL, maxBody: 1024},
				{name: "b", provider: "cloudflare", rawURL: up.URL, maxBody: 1024},
			}),
			MaxRetries:     2,
			MaxBufferBytes: 1024,
		})
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", strings.Repeat("x", 2048), specHeaders()))
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413 above the buffer cap", rec.Code)
		}
		if hits.Load() != 0 {
			t.Fatal("a body above the buffer cap must never reach a relay")
		}
	})
}

// --- proxy-form inbound ---

func TestProxyInboundDerivesSpec(t *testing.T) {
	up, _, got := newUpstream(t, http.StatusOK, "proxied", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	req := httptest.NewRequest(http.MethodGet, "http://target.example/v1/data?q=2", nil)
	req.Header.Set(HeaderProvider, "vercel")
	req.Header.Set(HeaderTarget, "https://forged.example") // overwritten, never trusted
	req.Header.Set(HeaderPath, "/forged")
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	c := readCapture(t, got)
	if got := c.req.Header.Get(HeaderTarget); got != "http://target.example" {
		t.Fatalf("derived X-Relay-Target = %q, want the absolute-form origin", got)
	}
	if got := c.req.Header.Get(HeaderPath); got != "/v1/data?q=2" {
		t.Fatalf("derived X-Relay-Path = %q, want the absolute-form request URI", got)
	}
}

func TestConnectIsRejected(t *testing.T) {
	g := newGateway(t, &State{Pool: buildPool(t, nil)})
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodConnect, "http://secret.example:443", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want the documented 501", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "CONNECT") {
		t.Fatalf("body = %q, want the CONNECT explanation", rec.Body.String())
	}
}

func TestProxyFormControlPathRelays(t *testing.T) {
	up, _, got := newUpstream(t, http.StatusOK, "origin-health", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	req := httptest.NewRequest(http.MethodGet, "http://control.example/healthz", nil)
	req.Header.Set(HeaderProvider, "vercel")
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "origin-health" {
		t.Fatalf("status = %d body = %q, want the absolute-form /healthz relayed, not shadowed",
			rec.Code, rec.Body.String())
	}
	c := readCapture(t, got)
	if got := c.req.Header.Get(HeaderTarget); got != "http://control.example" {
		t.Fatalf("X-Relay-Target = %q, want the proxied origin", got)
	}
}

// --- redirects and drain ---

func TestRedirectPassesThrough(t *testing.T) {
	up, _, got := newUpstream(t, http.StatusFound, "",
		http.Header{"Location": []string{"https://api.example.com/next"}})
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
	})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))
	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want the redirect to pass through untouched", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "https://api.example.com/next" {
		t.Fatalf("Location = %q, want it forwarded verbatim", loc)
	}
	readCapture(t, got)
}

// TestAwaitIdleExactness pins the drain contract: once Swap returns, a
// request picked from the old pool is already counted, so AwaitIdle cannot
// report idle until that request has actually finished.
func TestAwaitIdleExactness(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-gate
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	}))
	t.Cleanup(upA.Close)

	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "a", provider: "vercel", rawURL: upA.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     0,
		MaxBufferBytes: bufferBytes(),
	})
	gwSrv := httptest.NewServer(g)
	t.Cleanup(gwSrv.Close)

	req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set(HeaderProvider, "vercel")
	done := make(chan error, 1)
	go func() {
		resp, doErr := gwSrv.Client().Do(req)
		if doErr == nil {
			_ = resp.Body.Close()
		}
		done <- doErr
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the upstream")
	}

	// Swap in a pool without relay a while the request is still in flight.
	dead := deadRelayURL(t)
	g.Swap(&State{
		Pool: buildPool(t, []relaySpec{
			{name: "b", provider: "cloudflare", rawURL: dead, maxBody: bufferBytes()},
		}),
		Client:         NewClient(NewTransport(time.Second, time.Second)),
		MaxRetries:     0,
		MaxBufferBytes: bufferBytes(),
	})

	idle := make(chan bool, 1)
	go func() { idle <- g.AwaitIdle(context.Background(), "vercel", "a", 5*time.Second) }()
	select {
	case v := <-idle:
		t.Fatalf("AwaitIdle returned %v while the old-pool request was still in flight", v)
	case <-time.After(100 * time.Millisecond):
	}

	close(gate) // the request completes and drains
	select {
	case v := <-idle:
		if !v {
			t.Fatal("AwaitIdle reported a timeout even though the request drained")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitIdle did not return after the drain")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("relayed request failed: %v", err)
		}
	default:
	}

	// An identity the gateway never tracked is idle by definition.
	if !g.AwaitIdle(context.Background(), "deno", "nope", time.Millisecond) {
		t.Fatal("unknown identity must report idle immediately")
	}

	// A short timeout against a still-busy identity reports false.
	gate2 := make(chan struct{})
	entered2 := make(chan struct{}, 1)
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered2 <- struct{}{}
		<-gate2
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upB.Close)
	g.Swap(&State{
		Pool: buildPool(t, []relaySpec{
			{name: "b", provider: "cloudflare", rawURL: upB.URL, maxBody: bufferBytes()},
		}),
		Client:         NewClient(NewTransport(time.Second, time.Second)),
		MaxRetries:     0,
		MaxBufferBytes: bufferBytes(),
	})
	req2, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req2.Header.Set(HeaderProvider, "cloudflare")
	done2 := make(chan error, 1)
	go func() {
		resp, doErr := gwSrv.Client().Do(req2)
		if doErr == nil {
			_ = resp.Body.Close()
		}
		done2 <- doErr
	}()
	select {
	case <-entered2:
	case <-time.After(2 * time.Second):
		t.Fatal("second request never reached the upstream")
	}
	if g.AwaitIdle(context.Background(), "cloudflare", "b", 20*time.Millisecond) {
		t.Fatal("AwaitIdle must report false when the timeout expires in flight")
	}
	close(gate2)
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second request failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second request never completed")
	}
	if !g.AwaitIdle(context.Background(), "cloudflare", "b", 2*time.Second) {
		t.Fatal("identity must be idle after its request drained")
	}
}

// TestAwaitIdleStopsWhenTheContextEnds pins the shutdown half of the drain
// contract: the whole-process grace budget ends the wait even while the
// quiesce timer still has time left — a stalled stream must never push
// shutdown past the grace.
func TestAwaitIdleStopsWhenTheContextEnds(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-gate // the straggler: a stream that never finishes on its own
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)

	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "a", provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
		}),
		MaxRetries:     0,
		MaxBufferBytes: bufferBytes(),
	})
	gwSrv := httptest.NewServer(g)
	t.Cleanup(gwSrv.Close)

	req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set(HeaderProvider, "vercel")
	done := make(chan error, 1)
	go func() {
		resp, doErr := gwSrv.Client().Do(req)
		if doErr == nil {
			_ = resp.Body.Close()
		}
		done <- doErr
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the upstream")
	}

	ctx, cancel := context.WithCancel(context.Background())
	idle := make(chan bool, 1)
	go func() { idle <- g.AwaitIdle(ctx, "vercel", "a", 30*time.Second) }()
	select {
	case v := <-idle:
		t.Fatalf("AwaitIdle returned %v while the straggler was still in flight", v)
	case <-time.After(100 * time.Millisecond):
	}

	cancel() // the shutdown budget expires
	select {
	case v := <-idle:
		if v {
			t.Fatal("a context that ended mid-drain must report the drain incomplete")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitIdle did not return when the context ended (timer-only wait)")
	}
	close(gate) // let the request finish and the test goroutines unwind
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("straggler request failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("straggler request never completed after release")
	}
}

func TestPostSwapPickServesFromTheCurrentGeneration(t *testing.T) {
	// A request that entered before a Swap but picks after it (body
	// buffering can park it for seconds) must be served from the CURRENT
	// pool. Picking from the entry snapshot would let Swap + AwaitIdle
	// declare the old generation idle while the request was still to come,
	// and the replacement would deploy beneath the in-flight request.
	upA, hitsA, _ := newUpstream(t, http.StatusOK, "old-a", nil)
	upB, hitsB, gotB := newUpstream(t, http.StatusOK, "new-b", nil)

	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "a", provider: "vercel", rawURL: upA.URL, maxBody: 1 << 20},
		}),
		MaxBufferBytes: 1 << 20,
	})
	gwSrv := httptest.NewServer(g)
	t.Cleanup(gwSrv.Close)

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", pr)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.ContentLength = 8
	req.Header.Set("X-Relay-Target", "https://target.example")
	req.Header.Set("X-Relay-Path", "/v1")

	respCh := make(chan *http.Response, 1)
	go func() {
		resp, err := gwSrv.Client().Do(req)
		if err != nil {
			t.Errorf("client do: %v", err)
			respCh <- nil
			return
		}
		respCh <- resp
	}()

	// Give the handler time to park in the body read; every assertion below
	// holds no matter whether it has entered yet.
	time.Sleep(50 * time.Millisecond)

	client := NewClient(NewTransport(time.Second, time.Second))
	g.Swap(&State{
		Pool: buildPool(t, []relaySpec{
			{name: "b", provider: "deno", rawURL: upB.URL, maxBody: 1 << 20},
		}),
		Client:         client,
		MaxBufferBytes: 1 << 20,
	})

	// The replacement's drain concludes immediately: the parked request has
	// not begun on the old generation, so nothing is holding it.
	if !g.AwaitIdle(context.Background(), "vercel", "a", 500*time.Millisecond) {
		t.Fatal("AwaitIdle blocked on a request that never picked from the old generation")
	}

	if _, err := pw.Write([]byte("12345678")); err != nil {
		t.Fatalf("write body: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	select {
	case resp := <-respCh:
		if resp == nil {
			t.Fatal("no response")
		}
		defer func() { _ = resp.Body.Close() }()
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if string(got) != "new-b" {
			t.Fatalf("response = %q, want the current-generation relay's answer", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request never completed")
	}
	if hitsA.Load() != 0 {
		t.Fatalf("drained generation still served traffic: relay A hit %d times", hitsA.Load())
	}
	readCapture(t, gotB) // the request reached relay B of the current generation
	if hitsB.Load() == 0 {
		t.Fatal("current-generation relay never received the request")
	}
}

// TestHealthUpdatesReachThePickedGeneration is the issue #60 regression pin:
// an attempt that begins before a Swap and runs after it must record its
// passive health on the generation it picked FROM, not on the entry
// snapshot's. The two generations here are standalone pool.New builds —
// fresh RelayState objects even for the same relay identity — so a
// mis-bound record is directly observable: the picked generation's relay
// carries the outcome, the entry generation's stays at zero.
func TestHealthUpdatesReachThePickedGeneration(t *testing.T) {
	// gen builds one standalone generation whose only relay is named the
	// same in both generations, pointing at the given origin.
	gen := func(t *testing.T, rawURL string) *State {
		return &State{
			Pool: buildPool(t, []relaySpec{
				{name: "solo", provider: "vercel", rawURL: rawURL, maxBody: bufferBytes()},
			}),
			Client:         NewClient(NewTransport(time.Second, time.Second)),
			MaxRetries:     0,
			MaxBufferBytes: bufferBytes(),
		}
	}

	// parkRequest enters one request and holds it in body buffering, so the
	// pick happens only when the body is released — after the Swap below.
	// It returns the release closure and the channel the response arrives on.
	parkRequest := func(t *testing.T, gwSrv *httptest.Server) (release func(), respCh <-chan *http.Response) {
		t.Helper()
		pr, pw := io.Pipe()
		t.Cleanup(func() { _ = pw.Close() })
		req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", pr)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.ContentLength = 8
		req.Header.Set(HeaderTarget, "https://target.example")
		req.Header.Set(HeaderPath, "/v1")

		ch := make(chan *http.Response, 1)
		go func() {
			r, doErr := gwSrv.Client().Do(req)
			if doErr != nil {
				t.Errorf("client do: %v", doErr)
				ch <- nil
				return
			}
			ch <- r
		}()
		// Give the handler time to park in the body read; the Swap lands
		// while the request has picked nothing yet.
		time.Sleep(50 * time.Millisecond)
		return func() {
			if _, err := pw.Write([]byte("12345678")); err != nil {
				t.Errorf("write body: %v", err)
			}
			if err := pw.Close(); err != nil {
				t.Errorf("close body: %v", err)
			}
		}, ch
	}

	t.Run("success is recorded on the picked generation", func(t *testing.T) {
		upOK, hitsOK, _ := newUpstream(t, http.StatusOK, "served", nil)
		entry := gen(t, deadRelayURL(t)) // would have failed: a success here exposes the mis-binding
		picked := gen(t, upOK.URL)
		g, lc := newCapturingGateway(t, entry)
		gwSrv := httptest.NewServer(g)
		t.Cleanup(gwSrv.Close)

		release, respCh := parkRequest(t, gwSrv)
		g.Swap(picked)
		release()

		select {
		case resp := <-respCh:
			if resp == nil {
				t.Fatal("no response")
			}
			defer func() { _ = resp.Body.Close() }()
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if string(got) != "served" {
				t.Fatalf("response = %q, want the picked generation's answer", got)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("request never completed")
		}
		waitForLog(t, lc, "relayed")

		byName := relayStatsBy(picked.Pool)
		if byName["solo"].Requests != 1 || byName["solo"].Failures != 0 {
			t.Fatalf("picked generation row = %+v, want the success recorded here", byName["solo"])
		}
		if entryRow := relayStatsBy(entry.Pool)["solo"]; entryRow.Requests != 0 || entryRow.Failures != 0 {
			t.Fatalf("entry generation row = %+v, want untouched — the health update must follow the pick", entryRow)
		}
		if n := hitsOK.Load(); n != 1 {
			t.Fatalf("upstream hits = %d, want 1", n)
		}
	})

	t.Run("failure is recorded on the picked generation", func(t *testing.T) {
		upOK, _, _ := newUpstream(t, http.StatusOK, "served", nil)
		entry := gen(t, upOK.URL) // would have succeeded: a failure here exposes the mis-binding
		picked := gen(t, deadRelayURL(t))
		g, lc := newCapturingGateway(t, entry)
		gwSrv := httptest.NewServer(g)
		t.Cleanup(gwSrv.Close)

		release, respCh := parkRequest(t, gwSrv)
		g.Swap(picked)
		release()

		select {
		case resp := <-respCh:
			if resp == nil {
				t.Fatal("request errored on the client")
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502 from the failed attempt", resp.StatusCode)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("request never completed")
		}
		waitForLog(t, lc, "upstream_pre_response")

		byName := relayStatsBy(picked.Pool)
		if byName["solo"].Failures != 1 || byName["solo"].Requests != 0 {
			t.Fatalf("picked generation row = %+v, want exactly one recorded failure", byName["solo"])
		}
		if entryRow := relayStatsBy(entry.Pool)["solo"]; entryRow.Requests != 0 || entryRow.Failures != 0 {
			t.Fatalf("entry generation row = %+v, want untouched — the health update must follow the pick", entryRow)
		}
	})
}

// --- outcome classification pins ---

// stubRoundTripper answers every request with one canned response — the seam
// that reaches the classification switch with a copy the transport itself
// reported as clean.
type stubRoundTripper struct{ resp *http.Response }

func (s *stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) { return s.resp, nil }

// abortBody serves its payload, then cancels the request's context as it
// reports EOF — the EOF-flush abort shape: the copy itself succeeds
// (copyErr == nil) while the client is already gone. In the real stack this
// is a client dying while http.response flushes the final chunk: Flush
// reports no error, so the copy looks complete and only the dead context
// tells the two outcomes apart.
type abortBody struct {
	payload []byte
	cancel  context.CancelFunc
	eof     bool
}

func (b *abortBody) Read(p []byte) (int, error) {
	if b.eof {
		b.cancel()
		return 0, io.EOF
	}
	b.eof = true
	return copy(p, b.payload), nil
}

func (b *abortBody) Close() error { return nil }

// TestEOFFlushAbortIsClientAbortedNotSuccess pins the flush window: a copy
// that ends cleanly (copyErr == nil) with a DEAD request context is a client
// abort — recorded as neither success nor failure, outcome client_aborted —
// never a success. flushWriter drops the Flush error (http.response.Flush
// has no return), so without the live-context check this shape recorded
// RecordSuccess and silently cleared the relay's failure evidence. The
// second subtest is the control: the same harness with a live context must
// still record the success, so the first cannot pass vacuously.
func TestEOFFlushAbortIsClientAbortedNotSuccess(t *testing.T) {
	// run builds a seeded relay (one prior failure whose evidence a success
	// would clear) whose relay leg is the stub transport, and drives one
	// request through the real relay pipeline.
	run := func(t *testing.T, abort bool) (*captureLog, *httptest.ResponseRecorder, *pool.Pool) {
		t.Helper()
		p := buildPool(t, []relaySpec{
			{name: "solo", provider: "vercel", rawURL: "http://solo.internal", maxBody: bufferBytes()},
		})
		p.RecordFailure(p.Pick(pool.KeyAll), errors.New("dial solo: connection refused"))

		body := &abortBody{payload: []byte("done"), cancel: func() {}}
		req := relayRequest(t, http.MethodGet, "/", "", specHeaders())
		if abort {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body.cancel = cancel
			req = req.WithContext(ctx)
		}
		g, lc := newCapturingGateway(t, &State{
			Pool: p,
			Client: &http.Client{Transport: &stubRoundTripper{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/plain"}},
				Body:       body,
			}}},
			MaxRetries:     1,
			MaxBufferBytes: bufferBytes(),
		})
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		return lc, rec, p
	}

	t.Run("dead context at a clean copy is an abort", func(t *testing.T) {
		lc, rec, p := run(t, true)
		if got := rec.Body.String(); got != "done" {
			t.Fatalf("client body = %q, want the full relay body — the copy itself must have succeeded", got)
		}
		waitForLog(t, lc, "client_aborted")
		row := relayStatsBy(p)["solo"]
		if row.Requests != 1 || row.Failures != 1 || row.MidstreamFailures != 0 {
			t.Fatalf("row = %+v, want the seeded failure standing with nothing added — an abort records neither success nor failure", row)
		}
		if row.LastError != "dial solo: connection refused" {
			t.Fatalf("lastError = %q, want the seeded failure intact — a success here would have cleared it", row.LastError)
		}
		if !row.Healthy {
			t.Fatalf("row = %+v, want the relay still healthy — an abort is not a failure", row)
		}
	})

	t.Run("live context at a clean copy is a success", func(t *testing.T) {
		lc, rec, p := run(t, false)
		if got := rec.Body.String(); got != "done" {
			t.Fatalf("client body = %q, want the full relay body", got)
		}
		if strings.Contains(lc.String(), `"outcome":"client_aborted"`) {
			t.Fatal("a live context was classified as a client abort")
		}
		row := relayStatsBy(p)["solo"]
		if row.LastError != "" {
			t.Fatalf("lastError = %q, want it cleared — the control proves the harness records the success this test hangs on", row.LastError)
		}
		if row.Requests != 1 || !row.Healthy {
			t.Fatalf("row = %+v, want the completed success recorded", row)
		}
	})
}

// outcomes extracts every logged outcome value in order.
func outcomes(lc *captureLog) []string {
	var out []string
	for _, line := range strings.Split(lc.String(), "\n") {
		i := strings.Index(line, `"outcome":"`)
		if i < 0 {
			continue
		}
		rest := line[i+len(`"outcome":"`):]
		if j := strings.IndexByte(rest, '"'); j >= 0 {
			out = append(out, rest[:j])
		}
	}
	return out
}

// waitOutcome waits for the first attempt line and returns every outcome
// logged so far. The log write sits after the classification decision in
// every branch, so this is the happens-before edge the row comparisons
// below rely on.
func waitOutcome(t *testing.T, lc *captureLog) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if outs := outcomes(lc); len(outs) > 0 {
			return outs
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("attempt log never recorded an outcome; captured:\n%s", lc.String())
	return nil
}

// TestDrainDoesNotChangeClassification pins the BeginDrain contract: the
// draining flag is a log-line label and nothing else. http.Server.Shutdown
// never cancels request contexts, so a failure during the drain means
// exactly what it means mid-flight — a drain-special-cased outcome (a
// drain_aborted label, a skipped health record) would invent evidence the
// mid-flight classification never produced and punish or excuse relays for
// a process that is merely exiting.
func TestDrainDoesNotChangeClassification(t *testing.T) {
	// run issues one buffered request through a fresh gateway over rawURL
	// and returns the relay's stats row, the logged outcomes and the raw
	// log. Both runs of a scenario share one upstream, so the rows must be
	// equal down to the error text.
	run := func(t *testing.T, rawURL string, drained bool) (pool.StatsRow, []string, string) {
		t.Helper()
		p := buildPool(t, []relaySpec{
			{name: "solo", provider: "vercel", rawURL: rawURL, maxBody: bufferBytes()},
		})
		g, lc := newCapturingGateway(t, &State{
			Pool:           p,
			MaxRetries:     0, // one attempt: the transport scenario must not retry
			MaxBufferBytes: bufferBytes(),
		})
		if drained {
			g.BeginDrain()
		}
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodPost, "/", "buffered", specHeaders()))
		if rec.Code == 0 {
			t.Fatal("the request never completed")
		}
		return relayStatsBy(p)["solo"], waitOutcome(t, lc), lc.String()
	}

	scenarios := []struct {
		name   string
		rawURL func(t *testing.T) string
		want   string // the classification outcome the scenario must produce
	}{
		{"completed answer", func(t *testing.T) string {
			up, _, _ := newUpstream(t, http.StatusOK, "served", nil)
			return up.URL
		}, "relayed"},
		{"mid-stream failure", func(t *testing.T) string {
			up, _ := newTruncatingUpstream(t)
			return up.URL
		}, "upstream_midstream"},
		{"transport failure", func(t *testing.T) string { return deadRelayURL(t) }, "upstream_pre_response"},
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			rawURL := tc.rawURL(t)
			plainRow, plainOut, plainLog := run(t, rawURL, false)
			drainedRow, drainedOut, drainedLog := run(t, rawURL, true)

			if plainRow != drainedRow {
				t.Fatalf("the drain changed the relay's row: plain %+v vs drained %+v", plainRow, drainedRow)
			}
			if strings.Join(plainOut, ",") != strings.Join(drainedOut, ",") {
				t.Fatalf("the drain changed the logged outcomes: plain %v vs drained %v", plainOut, drainedOut)
			}
			if got := plainOut[len(plainOut)-1]; got != tc.want {
				t.Fatalf("outcome = %q, want %q — the scenario must classify before the drain can be compared", got, tc.want)
			}
			if !strings.Contains(drainedLog, `"draining":true`) {
				t.Fatal("the drained run's lines carry no draining label — the flag stopped being operator context")
			}
			if strings.Contains(plainLog, `"draining"`) {
				t.Fatal("the undrained run's lines carry a draining label")
			}
		})
	}
}

// outcomeEvents decodes every captured log line carrying the outcome into a
// JSON object, so a test can pin the fields the hot path logged alongside
// it.
func outcomeEvents(t *testing.T, lc *captureLog, outcome string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(lc.String(), "\n") {
		if !strings.Contains(line, `"outcome":"`+outcome+`"`) {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// TestAttemptLogCarriesThePickedIncarnation pins the incarnation label: a
// relay-naming attempt line quotes the admission generation of the relay
// that served it, and the value comes from the PICKED generation — the line
// after a Swap names the new incarnation, never the entry snapshot's — and
// equals the generation the lifecycle row renders for the same relay.
func TestAttemptLogCarriesThePickedIncarnation(t *testing.T) {
	upA, _, _ := newUpstream(t, http.StatusOK, "old", nil)
	upB, _, _ := newUpstream(t, http.StatusOK, "new", nil)
	stA := &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: upA.URL, maxBody: bufferBytes(), incarnation: 7},
		}),
		MaxRetries:     0,
		MaxBufferBytes: bufferBytes(),
		Lifecycle:      []LifecycleRow{{Name: "alpha", Provider: "vercel", State: "ready", Generation: 7}},
	}
	stB := &State{
		Pool: buildPool(t, []relaySpec{
			{name: "bravo", provider: "deno", rawURL: upB.URL, maxBody: bufferBytes(), incarnation: 9},
		}),
		Client:         NewClient(NewTransport(time.Second, time.Second)),
		MaxRetries:     0,
		MaxBufferBytes: bufferBytes(),
		Lifecycle:      []LifecycleRow{{Name: "bravo", Provider: "deno", State: "ready", Generation: 9}},
	}
	g, lc := newCapturingGateway(t, stA)

	// lifecycleGeneration reads the ready relay's generation off /stats —
	// the operator-facing view the log line must agree with.
	lifecycleGeneration := func(t *testing.T, name string) uint64 {
		t.Helper()
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
		var doc struct {
			Lifecycle []struct {
				Name       string `json:"name"`
				State      string `json:"state"`
				Generation uint64 `json:"generation"`
			} `json:"lifecycle"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatalf("decode /stats: %v\n%s", err, rec.Body.String())
		}
		for _, row := range doc.Lifecycle {
			if row.Name == name && row.State == "ready" {
				return row.Generation
			}
		}
		t.Fatalf("no ready lifecycle row for %s in %s", name, rec.Body.String())
		return 0
	}
	post := func(t *testing.T) {
		t.Helper()
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))
		if rec.Code != http.StatusOK {
			t.Fatalf("relay status = %d, want 200", rec.Code)
		}
	}

	post(t)
	waitForLog(t, lc, "relayed")
	events := outcomeEvents(t, lc, "relayed")
	if len(events) != 1 {
		t.Fatalf("relayed lines = %d, want 1", len(events))
	}
	if want := float64(lifecycleGeneration(t, "alpha")); events[0]["incarnation"] != want {
		t.Fatalf("incarnation = %v, want the serving relay's lifecycle generation %v", events[0]["incarnation"], want)
	}

	g.Swap(stB)
	post(t)
	deadline := time.Now().Add(2 * time.Second)
	for len(outcomeEvents(t, lc, "relayed")) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the post-swap attempt never logged")
		}
		time.Sleep(2 * time.Millisecond)
	}
	events = outcomeEvents(t, lc, "relayed")
	if events[1]["relay"] != "bravo" {
		t.Fatalf("post-swap relay = %v, want the current generation's bravo", events[1]["relay"])
	}
	if want := float64(lifecycleGeneration(t, "bravo")); events[1]["incarnation"] != want {
		t.Fatalf("incarnation after the swap = %v, want the picked generation's %v — the line must quote the generation that served, not the entry snapshot", events[1]["incarnation"], want)
	}
}

// TestSwapAndAwaitIdleAreAtomicWithPick pins the drain guarantee at its
// enforcement point: relay() takes Pick and inflight.begin under the same
// mutex section Swap holds, so once Swap returns every request that will
// still touch the old pool is already counted and AwaitIdle cannot miss it.
// The mutation this hunts moves Pick+begin after the unlock: a request that
// loaded the old generation just before the Swap then picks and begins after
// AwaitIdle already saw the identity idle, and the drained relay serves one
// more request — exactly the deploy-beneath-traffic the ordering exists to
// prevent. The unlock->pick gap is unreachable from outside, so this is a
// bounded stress: many short requests across many Swap rounds, failing the
// moment one drained relay takes a late hit.
func TestSwapAndAwaitIdleAreAtomicWithPick(t *testing.T) {
	const (
		workers = 8
		rounds  = 16
	)

	// pickGate blocks an attempt immediately after the atomic section. On
	// correct code it has already picked and registered before arriving; if
	// Pick+begin move below Unlock, it arrives before either action — the
	// test can then let Swap + AwaitIdle expose that untracked old-pool pick
	// deterministically instead of betting on scheduler timing.
	type pickGate struct {
		arrived  chan struct{}
		release  chan struct{}
		released sync.Once
	}
	releaseGate := func(g *pickGate) { g.released.Do(func() { close(g.release) }) }
	var (
		gateMu sync.Mutex
		gate   *pickGate
	)

	// A and B are the alternating serving generations. Each handler is
	// deliberately simple: the test's gate holds attempts at the precise
	// pick/register seam, then a hit says that one made it to this upstream.
	type side struct {
		name string
		hits atomic.Int64
		st   *State
	}
	client := NewClient(NewTransport(time.Second, time.Second))
	var sides [2]*side
	for i, name := range [2]string{"alpha", "bravo"} {
		s := &side{name: name}
		hits := &s.hits
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("done"))
		}))
		t.Cleanup(up.Close)
		s.st = &State{
			Pool: buildPool(t, []relaySpec{
				{name: name, provider: "vercel", rawURL: up.URL, maxBody: bufferBytes()},
			}),
			Client:         client,
			MaxRetries:     0, // one attempt: no retry may muddy relay hits
			MaxBufferBytes: bufferBytes(),
		}
		sides[i] = s
	}

	g := newGateway(t, sides[0].st)
	g.afterPick = func() {
		gateMu.Lock()
		current := gate
		gateMu.Unlock()
		if current == nil {
			return
		}
		current.arrived <- struct{}{}
		<-current.release
	}
	gwSrv := httptest.NewServer(g)
	t.Cleanup(gwSrv.Close)

	// K request goroutines loop one request per round. The round barrier
	// makes every attempt participate in the active pick gate, avoiding a
	// probabilistic timing window while preserving concurrent hot-path load.
	starts := make(chan struct{})
	finished := make(chan struct{}, workers*rounds)
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }) }
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				select {
				case <-stop:
					return
				case <-starts:
				}
				req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/", strings.NewReader("x"))
				if err != nil {
					t.Errorf("build worker request: %v", err)
					return
				}
				req.Header.Set(HeaderTarget, "https://target.example")
				req.Header.Set(HeaderPath, "/v1")
				resp, err := gwSrv.Client().Do(req)
				if err != nil {
					t.Errorf("worker request: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("worker request status = %d, want 200", resp.StatusCode)
					return
				}
				finished <- struct{}{}
			}
		}()
	}
	t.Cleanup(func() {
		gateMu.Lock()
		current := gate
		gateMu.Unlock()
		if current != nil {
			releaseGate(current)
		}
		stopWorkers()
		wg.Wait()
	})

	cur := 0
	for round := range rounds {
		old, next := sides[cur], sides[1-cur]
		currentGate := &pickGate{arrived: make(chan struct{}, workers), release: make(chan struct{})}
		gateMu.Lock()
		gate = currentGate
		gateMu.Unlock()
		for range workers {
			starts <- struct{}{}
		}
		for range workers {
			select {
			case <-currentGate.arrived:
			case <-time.After(2 * time.Second):
				t.Fatal("worker never reached the pick gate")
			}
		}

		// Correct code has already begun all K old-pool attempts before the
		// swap, so AwaitIdle cannot return while their gates remain closed.
		// The Pick+begin-after-Unlock mutation reaches this gate before it
		// counts anything, so Swap wins and AwaitIdle returns true here — the
		// exact violation, caught before a late old hit can hide in traffic.
		g.Swap(next.st)
		idle := make(chan bool, 1)
		go func() { idle <- g.AwaitIdle(context.Background(), "vercel", old.name, time.Second) }()
		select {
		case v := <-idle:
			releaseGate(currentGate)
			stopWorkers()
			wg.Wait()
			t.Fatalf("round %d: AwaitIdle returned %v before the old-pool pick gates released — Pick+begin escaped Swap's mutex", round, v)
		case <-time.After(20 * time.Millisecond):
		}

		releaseGate(currentGate)
		select {
		case v := <-idle:
			if !v {
				t.Fatalf("round %d: AwaitIdle timed out after the old attempts released", round)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: AwaitIdle remained blocked after old attempts released", round)
		}
		h0 := old.hits.Load()
		for range workers {
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatalf("round %d: worker did not finish its released request", round)
			}
		}
		if got := old.hits.Load(); got != h0 {
			t.Fatalf("round %d: old relay %s served %d more request(s) after AwaitIdle reported idle (%d -> %d)", round, old.name, got-h0, h0, got)
		}
		gateMu.Lock()
		gate = nil
		gateMu.Unlock()
		cur = 1 - cur
	}
}
