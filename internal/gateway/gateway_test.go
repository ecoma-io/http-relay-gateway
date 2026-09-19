package gateway

import (
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
	mu            sync.Mutex
	headers       http.Header
	body          []byte
	contentLength int64
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
	pool    *pool.Pool
	in      *pool.Input
	saw     *capture
}

// newFixture starts one live upstream edge relay (echoing what it received)
// and builds a gateway whose "vercel" pool is [dead, upstream] and whose
// "cloudflare" pool is [upstream]. The mutate hook adjusts the raw pool
// input and the state knobs before the pool is built.
func newFixture(t *testing.T, mutate func(*pool.Input, *State)) *fixture {
	t.Helper()

	saw := &capture{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw.mu.Lock()
		saw.headers = r.Header.Clone()
		saw.body, _ = io.ReadAll(r.Body)
		saw.contentLength = r.ContentLength
		saw.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hi\n\n"))
	}))
	t.Cleanup(upstream.Close)

	in := &pool.Input{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "dead", Provider: "vercel", URL: mustURL(t, deadRelayURL(t)), Active: true, Origin: pool.OriginLegacy, MaxBody: 4_500_000},
			{ID: 2, Name: "good", Provider: "vercel", URL: mustURL(t, upstream.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 4_500_000},
			{ID: 3, Name: "good-cf", Provider: "cloudflare", URL: mustURL(t, upstream.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 100_000_000},
		},
	}
	state := &State{
		MaxRetries:     2,
		MaxBufferBytes: 100_000_000,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}
	if mutate != nil {
		mutate(in, state)
	}
	p, err := pool.New(*in)
	if err != nil {
		t.Fatal(err)
	}
	state.Pool = p
	g := New(state, "test", logging.Nop())
	return &fixture{gateway: g, pool: p, in: in, saw: saw}
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
	return relayRequestBody(t, g, target, strings.NewReader(body), headers)
}

// relayRequestBody is relayRequest with an arbitrary body reader; a reader
// of unknown length produces a chunked (ContentLength -1) request.
func relayRequestBody(t *testing.T, g *Gateway, target string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)

	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+u.Path, body)
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

func relaySpecHeaders() map[string]string {
	return map[string]string{
		"X-Relay-Target": "https://api.example.com",
		"X-Relay-Path":   "/v1/messages",
	}
}

func TestRelayForwardsAndStrips(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, _ *State) {
		// Remove the dead relay: this test is about the forwarding contract,
		// not failover.
		in.Relays = in.Relays[1:]
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

func TestHeaderPolicyAppliesOnWire(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, _ *State) {
		in.Relays = in.Relays[1:2] // good only
		raw := `{"strip":["x-internal-trace"],"set":{"x-relayed-by":["gateway"]}}`
		policy, err := pool.ParseHeaderPolicy(&raw)
		if err != nil {
			t.Fatal(err)
		}
		in.Relays[0].Policy = policy
	})

	res := relayRequest(t, f.gateway, "http://gateway/", `{}`, map[string]string{
		"X-Relay-Target":   "https://api.example.com",
		"X-Relay-Path":     "/v1/messages",
		"X-Internal-Trace": "client-value",
		"X-Relayed-By":     "client-value",
	})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if got := f.saw.header("X-Internal-Trace"); got != "" {
		t.Fatalf("stripped header reached upstream: %q", got)
	}
	if got := f.saw.header("X-Relayed-By"); got != "gateway" {
		t.Fatalf("X-Relayed-By = %q, want the policy's set value", got)
	}
}

func TestRelayTokenClientValueStrippedAndManagedTokenSent(t *testing.T) {
	t.Run("managed relay gets the stored token, never the client's", func(t *testing.T) {
		f := newFixture(t, func(in *pool.Input, _ *State) {
			in.Relays = in.Relays[1:2] // good only
			in.Relays[0].Token = "gw-stored-token"
		})
		res := relayRequest(t, f.gateway, "http://gateway/", `{}`, map[string]string{
			"X-Relay-Target": "https://api.example.com",
			"X-Relay-Path":   "/v1/messages",
			HeaderToken:      "client-forged-token",
		})
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", res.StatusCode)
		}
		if got := f.saw.header(HeaderToken); got != "gw-stored-token" {
			t.Fatalf("X-Relay-Token upstream = %q, want the stored token", got)
		}
	})

	t.Run("legacy relay forwards no token at all", func(t *testing.T) {
		f := newFixture(t, func(in *pool.Input, _ *State) {
			in.Relays = in.Relays[1:2] // good only, no token
		})
		res := relayRequest(t, f.gateway, "http://gateway/", `{}`, map[string]string{
			"X-Relay-Target": "https://api.example.com",
			"X-Relay-Path":   "/v1/messages",
			HeaderToken:      "client-forged-token",
		})
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", res.StatusCode)
		}
		if got := f.saw.header(HeaderToken); got != "" {
			t.Fatalf("client X-Relay-Token leaked upstream: %q", got)
		}
	})
}

func TestFailoverToNextRelay(t *testing.T) {
	f := newFixture(t, nil)

	res := relayRequest(t, f.gateway, "http://gateway/vercel", `{}`, relaySpecHeaders())
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
	f := newFixture(t, func(in *pool.Input, state *State) {
		// vercel's real limit is 4.5MB; shrink it so the test body qualifies.
		in.Relays[0].MaxBody = 8
		in.Relays[1].MaxBody = 8
		state.MaxBufferBytes = 100
	})

	// 12 bytes: over vercel's 8-byte limit, under cloudflare's. Pinned
	// vercel must 413; auto must skip to cloudflare and succeed.
	big := "123456789012"

	res := relayRequest(t, f.gateway, "http://gateway/vercel", big, relaySpecHeaders())
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("pinned vercel status = %d, want 413", res.StatusCode)
	}

	res = relayRequest(t, f.gateway, "http://gateway/", big, relaySpecHeaders())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("auto status = %d, want 200 via cloudflare", res.StatusCode)
	}
	if string(f.saw.body) != big {
		t.Fatalf("upstream body = %q, want %q", f.saw.body, big)
	}
}

func TestBodyOverEveryLimitRejected(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, state *State) {
		// Shrink every relay under the 10-byte test body.
		in.Relays[0].MaxBody = 4
		in.Relays[1].MaxBody = 4
		in.Relays[2].MaxBody = 4
		state.MaxBufferBytes = 4
	})

	res := relayRequest(t, f.gateway, "http://gateway/", "1234567890", relaySpecHeaders())
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 over every provider limit", res.StatusCode)
	}
}

func TestAllAttemptsExhaustedReturns502(t *testing.T) {
	in := pool.Input{
		FailureThreshold: 100,
		Cooldown:         time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "dead1", Provider: "vercel", URL: mustURL(t, deadRelayURL(t)), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
			{ID: 2, Name: "dead2", Provider: "vercel", URL: mustURL(t, deadRelayURL(t)), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	}
	p, err := pool.New(in)
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     1,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", logging.Nop())

	res := relayRequest(t, g, "http://gateway/", `{}`, relaySpecHeaders())
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when every attempt fails", res.StatusCode)
	}
}

func TestStreamingAboveThresholdNoFailover(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, state *State) {
		state.StreamThresholdBytes = 8
	})

	// 12 bytes over the 8-byte threshold: stream mode. The first (dead)
	// relay fails and there is exactly one attempt — the live relay is
	// never contacted and the client gets 502.
	res := relayRequest(t, f.gateway, "http://gateway/", "123456789012", relaySpecHeaders())
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (streaming never fails over)", res.StatusCode)
	}
	f.saw.mu.Lock()
	contacted := f.saw.headers != nil
	f.saw.mu.Unlock()
	if contacted {
		t.Fatal("live relay was contacted; streaming must use a single attempt")
	}
	for _, row := range f.pool.Stats() {
		if row.Name == "dead" && row.Failures != 1 {
			t.Fatalf("dead relay failures = %d, want exactly 1 (single attempt)", row.Failures)
		}
	}
}

func TestStreamingRelaysBodyAndKeepsLength(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, state *State) {
		in.Relays = in.Relays[1:2] // good only
		in.Relays[0].MaxBody = 8   // smaller than the body: the gateway must
		// never invent a 413 for a streaming body.
		state.StreamThresholdBytes = 8
	})

	res := relayRequest(t, f.gateway, "http://gateway/", "123456789012", relaySpecHeaders())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (streaming bypasses body limits)", res.StatusCode)
	}
	if string(f.saw.body) != "123456789012" {
		t.Fatalf("upstream body = %q", f.saw.body)
	}
	if f.saw.contentLength != 12 {
		t.Fatalf("upstream ContentLength = %d, want 12 (declared length preserved)", f.saw.contentLength)
	}
}

func TestStreamingChunkedBodyRelaysIntact(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, state *State) {
		in.Relays = in.Relays[1:2] // good only
		state.StreamThresholdBytes = 8
	})

	// An unknown-length reader makes the inbound request chunked
	// (ContentLength -1): the gateway learns the size only while reading,
	// crosses the threshold mid-body, and must switch to streaming with the
	// already-read bytes prefixing the live remainder.
	res := relayRequestBody(t, f.gateway, "http://gateway/",
		unknownLengthReader{r: strings.NewReader("123456789012")}, relaySpecHeaders())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if string(f.saw.body) != "123456789012" {
		t.Fatalf("upstream body = %q, want the intact chunked body", f.saw.body)
	}
}

// unknownLengthReader hides the concrete reader type from net/http so the
// request is sent chunked (ContentLength unknown).
type unknownLengthReader struct{ r io.Reader }

func (u unknownLengthReader) Read(p []byte) (int, error) { return u.r.Read(p) }

func TestBufferedBelowThresholdStillFailsOver(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, state *State) {
		in.Relays[0].MaxBody = 8
		in.Relays[1].MaxBody = 8
		state.StreamThresholdBytes = 8
	})

	// Under the threshold the body is buffered: the dead first relay is
	// failed over to the live one as always.
	res := relayRequest(t, f.gateway, "http://gateway/", "abc", relaySpecHeaders())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (buffered bodies still fail over)", res.StatusCode)
	}
}

func TestResponseHeaderTimeoutFailsFast(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)

	in := pool.Input{
		FailureThreshold: 100,
		Cooldown:         time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "slow", Provider: "vercel", URL: mustURL(t, slow.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	}
	p, err := pool.New(in)
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		Client:         NewClient(NewTransport(2*time.Second, 100*time.Millisecond)),
		MaxBufferBytes: 1 << 20,
	}, "test", logging.Nop())

	start := time.Now()
	res := relayRequest(t, g, "http://gateway/", `{}`, relaySpecHeaders())
	elapsed := time.Since(start)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 on response-header timeout", res.StatusCode)
	}
	if elapsed >= time.Second {
		t.Fatalf("request took %v, want a fail-fast well under the upstream's 2s", elapsed)
	}
}

func TestSuccessResetsFailureStreak(t *testing.T) {
	// Flaky upstream: odd hits abort the connection (transport error), even
	// hits succeed. With threshold 2, interleaved successes must reset the
	// streak — a relay that half-fails must never slide into cooldown.
	var hits atomic.Int64
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if hits.Add(1)%2 == 1 {
			panic(http.ErrAbortHandler) // connection dies before any response byte
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(flaky.Close)
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(live.Close)

	in := pool.Input{
		FailureThreshold: 2,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "flaky", Provider: "vercel", URL: mustURL(t, flaky.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
			{ID: 2, Name: "live", Provider: "vercel", URL: mustURL(t, live.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	}
	p, err := pool.New(in)
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     1,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", logging.Nop())

	// Round-robin alternates [flaky, live] per pick; MaxRetries 1 covers one
	// failover. Over six requests flaky is contacted on hits 1-4 (fail, ok,
	// fail, ok). Without the success reset, hit 3 trips cooldown (threshold
	// 2, 30s) and requests 5-6 never reach flaky — hits stay at 3.
	for i := range 6 {
		res := relayRequest(t, g, "http://gateway/vercel", `{}`, relaySpecHeaders())
		if res.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 via failover", i+1, res.StatusCode)
		}
	}
	if got := hits.Load(); got != 4 {
		t.Fatalf("flaky relay hits = %d, want 4 (success must keep it in rotation)", got)
	}
	for _, row := range p.Stats() {
		if row.Name != "flaky" {
			continue
		}
		if row.Failures != 2 {
			t.Fatalf("flaky failures = %d, want 2", row.Failures)
		}
		if !row.Healthy {
			t.Fatal("flaky relay is unhealthy; interleaved successes failed to reset the streak")
		}
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
	if !strings.Contains(string(raw), `"provider":"vercel"`) ||
		!strings.Contains(string(raw), `"version":"test"`) ||
		!strings.Contains(string(raw), `"origin":"legacy"`) {
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
