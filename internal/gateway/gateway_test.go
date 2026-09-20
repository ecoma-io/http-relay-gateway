package gateway

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
			Name:     s.name,
			Provider: s.provider,
			URL:      relayURL(t, s.rawURL),
			Token:    token,
			MaxBody:  s.maxBody,
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
	g.copyResponse(rec, resp)
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
	up, _, got := newUpstream(t, http.StatusOK, "ok", nil)
	g := newGateway(t, &State{
		Pool: buildPool(t, []relaySpec{
			{name: "alpha", provider: "vercel", rawURL: up.URL, token: "very-secret-token", maxBody: pool.VercelMaxBody},
		}),
		MaxRetries:     1,
		MaxBufferBytes: bufferBytes(),
		Lifecycle: []LifecycleRow{
			{Name: "alpha", Provider: "vercel", State: "ready", Generation: 7},
			{Name: "beta", Provider: "deno", State: "failed", Reason: "probe_failed", Generation: 2},
		},
	})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, relayRequest(t, http.MethodGet, "/", "", specHeaders()))
	if rec.Code != http.StatusOK {
		t.Fatalf("relay status = %d, want 200 before /stats", rec.Code)
	}
	readCapture(t, got)

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
	}
	for k := range doc.Relays[0] {
		if !allowed[k] {
			t.Fatalf("relay stats key %q would leak a URL or token", k)
		}
	}
	if doc.Relays[0]["requests"] != float64(1) {
		t.Fatalf("requests counter = %v, want 1", doc.Relays[0]["requests"])
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
	go func() { idle <- g.AwaitIdle("vercel", "a", 5*time.Second) }()
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
	if !g.AwaitIdle("deno", "nope", time.Millisecond) {
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
	if g.AwaitIdle("cloudflare", "b", 20*time.Millisecond) {
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
	if !g.AwaitIdle("cloudflare", "b", 2*time.Second) {
		t.Fatal("identity must be idle after its request drained")
	}
}
