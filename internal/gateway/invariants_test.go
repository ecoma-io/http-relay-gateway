package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/pool"
)

// The tests here pin the wire-contract invariants the README promises and a
// regression would break silently: an answered response is never retried,
// redirects pass through, the response leg forwards end-to-end headers, and
// the spec's method and pins behave exactly as documented.

func TestAnsweredStatusIsNeverRetried(t *testing.T) {
	var liveHits atomic.Int64
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("relay says: overloaded"))
	}))
	t.Cleanup(failing.Close)
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		liveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(live.Close)

	p, err := pool.New(pool.Input{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "failing", Provider: "vercel", URL: mustURL(t, failing.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
			{ID: 2, Name: "live", Provider: "vercel", URL: mustURL(t, live.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     2,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", "1", logging.Nop())

	// Failover is bounded by transport errors: once the relay answered —
	// even with a 5xx — the response streams through untouched.
	res := relayRequest(t, g, "http://gateway/vercel", `{}`, relaySpecHeaders())
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the relay's own 503 (answered responses are never retried)", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if string(body) != "relay says: overloaded" {
		t.Fatalf("body = %q, want the relay's own answer", body)
	}
	if got := liveHits.Load(); got != 0 {
		t.Fatalf("live relay hit %d times; a 503 answer was retried", got)
	}
	// A status is not a health failure: passive health counts transport
	// errors only, so the answering relay stays in rotation.
	for _, row := range p.Stats() {
		if row.Name == "failing" && (row.Failures != 0 || row.Requests != 1) {
			t.Fatalf("failing relay health row = %+v, want 0 failures 1 request", row)
		}
	}
}

func TestPartialBodyIsDeliveredWithoutRetry(t *testing.T) {
	var liveHits atomic.Int64
	partial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: first\n\n"))
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush() // commit the response: any byte after this is client-visible
		}
		panic(http.ErrAbortHandler) // the relay dies mid-stream
	}))
	t.Cleanup(partial.Close)
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		liveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(live.Close)

	p, err := pool.New(pool.Input{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "partial", Provider: "vercel", URL: mustURL(t, partial.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
			{ID: 2, Name: "live", Provider: "vercel", URL: mustURL(t, live.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     2,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", "1", logging.Nop())

	res := relayRequest(t, g, "http://gateway/vercel", `{}`, relaySpecHeaders())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want the 200 the relay already committed", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if string(body) != "data: first\n\n" {
		t.Fatalf("body = %q, want the partial bytes the relay sent before dying", body)
	}
	if got := liveHits.Load(); got != 0 {
		t.Fatalf("live relay hit %d times; a committed response was retried", got)
	}
}

func TestRedirectPassesThrough(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(http.StatusFound)
		_, _ = w.Write([]byte("see other"))
	}))
	t.Cleanup(origin.Close)
	p, err := pool.New(pool.Input{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "redirecting", Provider: "vercel", URL: mustURL(t, origin.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     2,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", "1", logging.Nop())

	// The gateway is a dumb pipe: a relay's redirect is a response to pass
	// through, never one to follow (the pinned client below observes the
	// untouched 302 — a following client would chase the Location itself).
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/vercel", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range relaySpecHeaders() {
		req.Header.Set(k, v)
	}
	follower := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	res, err := follower.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want the relay's own 302", res.StatusCode)
	}
	if got := res.Header.Get("Location"); got != "/elsewhere" {
		t.Fatalf("Location = %q, want the relay's own", got)
	}
	body, _ := io.ReadAll(res.Body)
	if string(body) != "see other" {
		t.Fatalf("body = %q, want the redirect body", body)
	}
}

func TestResponseHeadersPassThrough(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Relay-Trace", "trace-123")
		w.Header().Add("Set-Cookie", "a=1; Path=/")
		w.Header().Add("Set-Cookie", "b=2; Path=/")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	}))
	t.Cleanup(origin.Close)
	p, err := pool.New(pool.Input{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Relays: []pool.RelayInput{
			{ID: 1, Name: "origin", Provider: "vercel", URL: mustURL(t, origin.URL), Active: true, Origin: pool.OriginLegacy, MaxBody: 1 << 20},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     2,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", "1", logging.Nop())

	res := relayRequest(t, g, "http://gateway/vercel", `{}`, relaySpecHeaders())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if got := res.Header.Get("X-Relay-Trace"); got != "trace-123" {
		t.Fatalf("X-Relay-Trace = %q, want the relay's response header forwarded", got)
	}
	if got := res.Header.Values("Set-Cookie"); len(got) != 2 {
		t.Fatalf("Set-Cookie values = %v, want both forwarded (multi-value leg)", got)
	}
}

func TestMethodsForwardVerbatim(t *testing.T) {
	f := newFixture(t, func(in *pool.Input, _ *State) {
		in.Relays = in.Relays[1:]
	})
	ts := httptest.NewServer(f.gateway)
	t.Cleanup(ts.Close)

	for _, tc := range []struct {
		method string
		body   string
	}{
		{http.MethodGet, ""},
		{http.MethodDelete, ""},
		{http.MethodPut, `{"updated":true}`},
	} {
		var reader io.Reader
		if tc.body != "" {
			reader = strings.NewReader(tc.body)
		}
		req, err := http.NewRequest(tc.method, ts.URL+"/vercel", reader)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range relaySpecHeaders() {
			req.Header.Set(k, v)
		}
		res, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d", tc.method, res.StatusCode)
		}
		if got := f.saw.method; got != tc.method {
			t.Fatalf("%s: upstream saw method %q", tc.method, got)
		}
		if tc.body != "" && string(f.saw.body) != tc.body {
			t.Fatalf("%s: upstream body = %q, want %q", tc.method, f.saw.body, tc.body)
		}
	}
}

func TestExplicitAllAndNonePinsRoundRobin(t *testing.T) {
	// One live relay per provider; the served sequence tells the pins apart.
	f := newFixture(t, func(in *pool.Input, _ *State) {
		in.Relays = in.Relays[1:] // [good(vercel), good-cf(cloudflare)]
	})

	for i, pin := range []string{"all", "none"} {
		headers := relaySpecHeaders()
		headers["X-Relay-Provider"] = pin
		for range 2 {
			res := relayRequest(t, f.gateway, "http://gateway/", `{}`, headers)
			if res.StatusCode != http.StatusOK {
				t.Fatalf("pin %q: status = %d", pin, res.StatusCode)
			}
		}
		// After each pair both providers must have served once more — an
		// explicit all/none pin behaves exactly like no pin at all.
		rows := f.pool.Stats()
		if rows[0].Requests != int64(i+1) || rows[1].Requests != int64(i+1) {
			t.Fatalf("pin %q: requests = [%d, %d], want both providers rotated",
				pin, rows[0].Requests, rows[1].Requests)
		}
	}
}

func TestEmptyPoolServes503(t *testing.T) {
	// A fresh database boots with zero relays: the data plane must answer a
	// clean 503, not panic or misroute.
	p, err := pool.New(pool.Input{FailureThreshold: 1, Cooldown: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	g := New(&State{
		Pool:           p,
		MaxRetries:     2,
		MaxBufferBytes: 1 << 20,
		Client:         NewClient(NewTransport(2*time.Second, 0)),
	}, "test", "1", logging.Nop())

	res := relayRequest(t, g, "http://gateway/", `{}`, relaySpecHeaders())
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 from an empty pool", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "no relay configured") {
		t.Fatalf("body = %q, want the no-relay error", body)
	}
}

func TestStatsExposesRelayVersion(t *testing.T) {
	f := newFixture(t, nil)
	ts := httptest.NewServer(f.gateway)
	t.Cleanup(ts.Close)

	res, err := ts.Client().Get(ts.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var payload struct {
		Version      string          `json:"version"`
		RelayVersion string          `json:"relayVersion"`
		Relays       []pool.StatsRow `json:"relays"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.Version != "test" || payload.RelayVersion != "1" {
		t.Fatalf("stats = %+v, want version test and relayVersion 1", payload)
	}
	if len(payload.Relays) != 3 {
		t.Fatalf("stats relays = %d rows, want 3", len(payload.Relays))
	}
}
