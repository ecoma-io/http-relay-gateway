package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The tests here drive the data-plane resilience contract black box:
// bounded failover with buffered-body replay, the never-retry rule once a
// relay has answered, the stream_threshold_bytes single-attempt mode, the
// passive-health layer beneath the readiness gate, and the explicit
// round-robin pin values.
//
// A dead relay is modeled with the worker sim's hang mode plus a short
// response_header_timeout: the request is accepted, nothing answers, and
// the gateway's own timeout is the transport failure — deterministic, and
// always before the first response byte. The verified readiness layer is
// quieted with a one-hour verify tick wherever passive failures are the
// subject: a hung worker sim is a *passive* data-plane event and must stay
// one — probe-driven demotion is TestE2E_LastRelayFailureZeroReady's
// subject, not this file's.
//
// Determinism: the pool is served in sorted (provider, name) order —
// c-rel (cloudflare) first, v-rel (vercel) second — with one round-robin
// cursor per selector key, so the relay each request lands on is pinned by
// the requests before it. Markers ride end to end and are observed at each
// sim's ingress; successes and transport failures are observed on /stats.

// twoRelayFleet boots cloudflare c-rel + vercel v-rel against fake with
// the given settings overrides and waits for both to admit. Credentials
// are unique per call, so the leak scans stay per-run.
func twoRelayFleet(t *testing.T, fake *fakeEdge, overrides map[string]string) *gatewayProc {
	t.Helper()
	key, vt := freshIdentity("fo-v")
	ct := "cf-tok-" + randSuffix(12)
	fake.setToken("vercel", vt)
	fake.setToken("cloudflare", ct)
	g := startGateway(t, fake, gwOptions{
		key: key,
		config: configYAMLOverrides(t, overrides,
			relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")),
			relayBlock("c-rel", "cloudflare", tokenEnvLine("E2E_CT")),
		),
		extraEnv: map[string]string{"E2E_VT": vt, "E2E_CT": ct},
	})
	g.waitForStats(t, 20*time.Second, "both relays to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})
	return g
}

// relayTo sends one relay-spec request against echo with a marker and
// fails the test unless it answered with want.
func relayTo(t *testing.T, g *gatewayProc, echo *echoServer, path, marker string, body []byte, want int) []byte {
	t.Helper()
	resp, raw := g.do(t, http.MethodPost, path, map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/" + marker, "X-Marker": marker,
	}, body)
	if resp.StatusCode != want {
		t.Fatalf("%s (marker %s) = %d (%s), want %d", path, marker, resp.StatusCode, raw, want)
	}
	return raw
}

func TestE2E_FailoverReplaysBufferedBodyOnTheNextRelay(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	g := twoRelayFleet(t, fake, map[string]string{
		// One transport failure puts a relay on cooldown before the retry
		// picks, so the replay provably lands on the healthy peer.
		"failure_threshold":       "1",
		"cooldown":                "2s",
		"response_header_timeout": "300ms",
		"verify_interval":         "1h",
	})

	// Two unpinned requests pin the served order — c-rel, then v-rel — and
	// leave the all-providers cursor on c-rel.
	for _, marker := range []string{"w1", "w2"} {
		relayTo(t, g, echo, "/", marker, []byte("warm"), http.StatusOK)
	}
	if got := fake.project("c-rel").sim.markers(); !equalStrings(got, []string{"w1"}) {
		t.Fatalf("c-rel served %v, want [w1]", got)
	}
	if got := fake.project("v-rel").sim.markers(); !equalStrings(got, []string{"w2"}) {
		t.Fatalf("v-rel served %v, want [w2]", got)
	}

	// The cloudflare worker goes silent mid-fleet: the next request on it
	// is a transport error, before any response byte.
	fake.project("c-rel").sim.setMode(simHang)

	body := relayTo(t, g, echo, "/", "f1", []byte("failover-payload"), http.StatusOK)
	if !strings.Contains(string(body), "failover-payload") {
		t.Fatalf("relayed answer %s lost the payload", body)
	}

	// The attempt landed on the dead relay — one recorded transport
	// failure, the warm-up success, nothing forwarded — and the same
	// buffered body was replayed on the peer.
	if row, ok := relayRow(g.stats(t), "cloudflare", "c-rel"); !ok ||
		row.Failures != 1 || row.Requests != 1 {
		t.Fatalf("c-rel stats row = %+v (ok=%v), want 1 request and exactly 1 failure", row, ok)
	}
	if row, _ := relayRow(g.stats(t), "vercel", "v-rel"); row.Requests != 2 {
		t.Fatalf("v-rel requests = %d, want 2 (warm-up + the replay)", row.Requests)
	}
	if got := fake.project("c-rel").sim.markers(); !equalStrings(got, []string{"w1"}) {
		t.Fatalf("c-rel served %v — the failed attempt must not reach the target", got)
	}
	if got := fake.project("v-rel").sim.markers(); !equalStrings(got, []string{"w2", "f1"}) {
		t.Fatalf("v-rel served %v, want [w2 f1] — the replay", got)
	}
	var seen int
	for _, rec := range echo.requests() {
		if rec.Header.Get("X-Marker") == "f1" {
			seen++
			if rec.BodyLen != len("failover-payload") {
				t.Fatalf("replayed body length = %d, want the full payload", rec.BodyLen)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("the target saw the replayed request %d times, want exactly 1", seen)
	}

	// The passive failure shows on /stats: unhealthy, labeled, and clean —
	// but still a member, still admitted, and no redeploy fired.
	doc := g.stats(t)
	row, ok := relayRow(doc, "cloudflare", "c-rel")
	if !ok || row.Healthy {
		t.Fatalf("c-rel stats row = %+v (ok=%v), want healthy=false on cooldown", row, ok)
	}
	if row.LastError == "" {
		t.Fatal("c-rel lastError missing after a passive transport failure")
	}
	assertNoLeaks(t, "/stats lastError", g, fake, []byte(row.LastError))
	if !doc.Readiness.Ready || doc.Readiness.ReadyRelays != 2 {
		t.Fatalf("readiness after a passive failure = %+v, want the fleet unchanged", doc.Readiness)
	}
	if lc, ok := lifecycleOf(doc, "cloudflare", "c-rel"); !ok || lc.State != "ready" {
		t.Fatalf("c-rel lifecycle = %+v (ok=%v), want ready — a passive failure never changes membership", lc, ok)
	}
	if n := fake.deployCount("c-rel"); n != 1 {
		t.Fatalf("deploys after a passive failure = %d, want 1 (the admission deploy)", n)
	}
	if n := fake.deleteCount("c-rel"); n != 0 {
		t.Fatalf("deletes after a passive failure = %d, want 0", n)
	}
}

func TestE2E_PinnedFailoverExhaustsOnThePinnedProvider(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	g := twoRelayFleet(t, fake, map[string]string{
		"failure_threshold":       "1",
		"cooldown":                "5s",
		"response_header_timeout": "300ms",
		"verify_interval":         "1h",
	})
	fake.project("c-rel").sim.setMode(simHang)

	// Pinned to the silent provider with the peer healthy: the pin bounds
	// failover, so max_retries attempts run against the pinned relay and
	// the gateway answers its own 502.
	raw := relayTo(t, g, echo, "/cloudflare", "x1", []byte("pinned-payload"), http.StatusBadGateway)
	assertNoLeaks(t, "exhausted 502 body", g, fake, raw)
	if strings.Contains(string(raw), "pinned-payload") {
		t.Fatal("the exhausted 502 must not carry the request payload")
	}

	if row, ok := relayRow(g.stats(t), "cloudflare", "c-rel"); !ok || row.Failures != 2 {
		t.Fatalf("c-rel failures = %+v (ok=%v), want exactly 2 (the first attempt plus one retry; max_retries = 1)", row, ok)
	}
	if row, _ := relayRow(g.stats(t), "vercel", "v-rel"); row.Requests != 0 {
		t.Fatalf("v-rel requests = %d, want 0 — the pin bounds failover to the pinned provider", row.Requests)
	}
	if n := len(echo.requests()); n != 0 {
		t.Fatalf("the target saw %d requests, want 0 — the payload never left", n)
	}
	if lc, ok := lifecycleOf(g.stats(t), "cloudflare", "c-rel"); !ok || lc.State != "ready" {
		t.Fatalf("c-rel lifecycle = %+v (ok=%v), want ready — exhaustion is passive, not a demotion", lc, ok)
	}
}

func TestE2E_ResponseStartedIsNeverRetried(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	g := twoRelayFleet(t, fake, map[string]string{
		"verify_interval": "1h",
	})

	// Cursor on c-rel.
	for _, marker := range []string{"a1", "a2"} {
		relayTo(t, g, echo, "/", marker, []byte("warm"), http.StatusOK)
	}

	// c-rel answers its own 502 (its upstream leg fails): an HTTP verdict,
	// not a transport error.
	fake.project("c-rel").sim.setMode(simProbe500)

	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/started", "X-Marker": "r1",
	}, []byte("started-payload"))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("answered 502 streamed through as %d (%s), want 502", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "upstream fetch failed") {
		t.Fatalf("the relay's own answer was not passed through verbatim: %s", body)
	}

	// The answering relay took the request exactly once and was not
	// penalized for its verdict; the healthy peer never saw a retry; the
	// payload never reached the target a second time.
	if row, ok := relayRow(g.stats(t), "cloudflare", "c-rel"); !ok || row.Failures != 0 || row.Requests != 2 {
		t.Fatalf("c-rel stats row = %+v (ok=%v), want 2 requests (warm-up + the answered 502) and 0 failures — an HTTP verdict is not a transport failure", row, ok)
	}
	if row, _ := relayRow(g.stats(t), "vercel", "v-rel"); row.Requests != 1 {
		t.Fatalf("v-rel requests = %d, want 1 (the warm-up only)", row.Requests)
	}
	if !fake.project("c-rel").sim.markerSeen("r1") {
		t.Fatal("the answering relay must have taken the request")
	}
	if fake.project("v-rel").sim.markerSeen("r1") {
		t.Fatal("the peer saw a retry after the response had started")
	}
	for _, rec := range echo.requests() {
		if rec.Header.Get("X-Marker") == "r1" {
			t.Fatal("the request reached the target after the relay had already answered")
		}
	}
	if doc := g.stats(t); doc.Readiness.ReadyRelays != 2 {
		t.Fatalf("readiness = %+v, want both relays still admitted", doc.Readiness)
	}
}

func TestE2E_StreamThresholdRelaysLiveWithoutFailover(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	g := twoRelayFleet(t, fake, map[string]string{
		"stream_threshold_bytes":  "1024",
		"failure_threshold":       "1",
		"cooldown":                "5s",
		"response_header_timeout": "300ms",
		"verify_interval":         "1h",
	})

	// Cursor on c-rel.
	for _, marker := range []string{"a1", "a2"} {
		relayTo(t, g, echo, "/", marker, []byte("warm"), http.StatusOK)
	}
	fake.project("c-rel").sim.setMode(simHang)

	// Above the threshold the body streams through in a single attempt: a
	// transport failure is a 502, with no replay on the peer despite
	// max_retries = 1.
	big := bytes.Repeat([]byte("s"), 8192)
	raw := relayTo(t, g, echo, "/", "s1", big, http.StatusBadGateway)
	if !strings.Contains(string(raw), "streaming request failed") {
		t.Fatalf("streaming failure answered %s, want the gateway's streaming 502", raw)
	}
	assertNoLeaks(t, "streaming 502 body", g, fake, raw)
	if row, ok := relayRow(g.stats(t), "cloudflare", "c-rel"); !ok || row.Failures != 1 {
		t.Fatalf("c-rel failures = %+v (ok=%v), want exactly 1 — a streaming body is one attempt", row, ok)
	}
	if row, _ := relayRow(g.stats(t), "vercel", "v-rel"); row.Requests != 1 {
		t.Fatalf("v-rel requests = %d, want 1 (the warm-up only) — streaming never fails over", row.Requests)
	}
	if fake.project("v-rel").sim.markerSeen("s1") {
		t.Fatal("the peer received a streaming retry")
	}

	// No gateway-side 413 above a provider cap: the same body class that
	// 413s on the buffered path relays live through the 4.5MB provider.
	huge := bytes.Repeat([]byte("x"), 4_600_000)
	relayTo(t, g, echo, "/vercel", "s2", huge, http.StatusOK)
	rec, ok := echo.lastRequest()
	if !ok || rec.BodyLen != 4_600_000 {
		t.Fatalf("echo body length = %d (ok=%v), want the 4.6MB body intact", rec.BodyLen, ok)
	}

	// Below the threshold the body still buffers and may fail over: the
	// cooled dead relay is skipped, the peer serves, the payload arrives
	// intact exactly once.
	relayTo(t, g, echo, "/", "s3", []byte("buffered-payload"), http.StatusOK)
	if got := fake.project("v-rel").sim.markers(); !equalStrings(got, []string{"a2", "s2", "s3"}) {
		t.Fatalf("v-rel served %v, want [a2 s2 s3]", got)
	}
	var seen int
	for _, r := range echo.requests() {
		if r.Header.Get("X-Marker") == "s3" {
			seen++
			if string(r.Body) != "buffered-payload" {
				t.Fatalf("buffered replay body = %q, want the payload intact", r.Body)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("the target saw the buffered request %d times, want exactly 1", seen)
	}
}

func TestE2E_PassiveHealthCooldownSkipAndBestEffort(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	g := twoRelayFleet(t, fake, map[string]string{
		"failure_threshold":       "2",
		"cooldown":                "2s",
		"max_retries":             "0",
		"response_header_timeout": "300ms",
		"verify_interval":         "1h",
	})
	cRelay := fake.project("c-rel").sim

	// Silence c-rel; the pin makes every request land on it,
	// deterministically.
	cRelay.setMode(simHang)

	// One failure: a streak, not a cooldown — the relay stays healthy and
	// labeled.
	relayTo(t, g, echo, "/cloudflare", "p1", nil, http.StatusBadGateway)
	row, ok := relayRow(g.stats(t), "cloudflare", "c-rel")
	if !ok || !row.Healthy || row.Failures != 1 || row.LastError == "" {
		t.Fatalf("c-rel after one failure = %+v (ok=%v), want healthy with 1 failure and a lastError", row, ok)
	}
	assertNoLeaks(t, "/stats lastError", g, fake, []byte(row.LastError))

	// Second consecutive failure: the threshold trips and the relay goes
	// on cooldown.
	relayTo(t, g, echo, "/cloudflare", "p2", nil, http.StatusBadGateway)
	if row, _ := relayRow(g.stats(t), "cloudflare", "c-rel"); row.Healthy {
		t.Fatalf("c-rel after %d consecutive failures = %+v, want healthy=false", row.Failures, row)
	}

	// The pin has no alternative: Pick still returns the cooled relay (best
	// effort beats refusing), so the attempt runs and fails.
	relayTo(t, g, echo, "/cloudflare", "p3", nil, http.StatusBadGateway)
	if row, _ := relayRow(g.stats(t), "cloudflare", "c-rel"); row.Failures != 3 {
		t.Fatalf("c-rel failures = %d, want 3 — the pinned attempt must still run", row.Failures)
	}

	// Unpinned traffic skips the cooled relay: the request succeeds on the
	// peer instead of dying on the dead one.
	relayTo(t, g, echo, "/", "u1", []byte("skip-payload"), http.StatusOK)
	if got := fake.project("v-rel").sim.markers(); !equalStrings(got, []string{"u1"}) {
		t.Fatalf("v-rel served %v, want [u1]", got)
	}

	// Membership was never touched: both relays admitted, c-rel still
	// `ready`, and the only deploy is its admission deploy.
	doc := g.stats(t)
	if doc.Readiness.ReadyRelays != 2 {
		t.Fatalf("readyRelays = %d, want 2 — passive failures never change membership", doc.Readiness.ReadyRelays)
	}
	if lc, ok := lifecycleOf(doc, "cloudflare", "c-rel"); !ok || lc.State != "ready" {
		t.Fatalf("c-rel lifecycle = %+v (ok=%v), want ready", lc, ok)
	}
	if n := fake.deployCount("c-rel"); n != 1 {
		t.Fatalf("deploys = %d, want 1 (the admission deploy)", n)
	}
	if n := fake.deleteCount("c-rel"); n != 0 {
		t.Fatalf("deletes = %d, want 0", n)
	}

	// An interleaved success resets the streak: after the relay answers
	// once, the *next* single failure must not trip the cooldown again.
	cRelay.setMode(simOK)
	relayTo(t, g, echo, "/cloudflare", "p4", nil, http.StatusOK)
	row, _ = relayRow(g.stats(t), "cloudflare", "c-rel")
	if !row.Healthy || row.LastError != "" {
		t.Fatalf("c-rel after recovery = %+v, want healthy with the lastError cleared", row)
	}
	cRelay.setMode(simHang)
	relayTo(t, g, echo, "/cloudflare", "p5", nil, http.StatusBadGateway)
	if row, _ := relayRow(g.stats(t), "cloudflare", "c-rel"); !row.Healthy {
		t.Fatalf("c-rel after a reset streak and one failure = %+v, want healthy (the success reset the streak)", row)
	}

	// The second failure after the reset does trip the cooldown.
	relayTo(t, g, echo, "/cloudflare", "p6", nil, http.StatusBadGateway)
	if row, _ := relayRow(g.stats(t), "cloudflare", "c-rel"); row.Healthy {
		t.Fatalf("c-rel = %+v, want back on cooldown", row)
	}

	// Drive the peer down too: every candidate is now cooled.
	fake.project("v-rel").sim.setMode(simHang)
	relayTo(t, g, echo, "/vercel", "q1", nil, http.StatusBadGateway)
	relayTo(t, g, echo, "/vercel", "q2", nil, http.StatusBadGateway)

	// Best effort beats a 503: with nothing healthy, Pick still returns a
	// relay and the request fails at the relay leg (502), never with the
	// empty-pool answer. The process is alive and the fleet is still
	// admitted.
	raw := relayTo(t, g, echo, "/", "u2", nil, http.StatusBadGateway)
	assertNoLeaks(t, "best-effort 502 body", g, fake, raw)
	resp, _ := g.do(t, http.MethodGet, "/readyz", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/readyz with a fully cooled fleet = %d, want 200 — cooldown is passive", resp.StatusCode)
	}
	if doc := g.stats(t); doc.Readiness.ReadyRelays != 2 {
		t.Fatalf("readyRelays = %d, want 2", doc.Readiness.ReadyRelays)
	}

	// Half-open recovery: past the cooldown the relay is picked again and a
	// success brings it fully back — healthy, unlabeled, serving.
	cRelay.setMode(simOK)
	fake.project("v-rel").sim.setMode(simOK)
	g.waitForStats(t, 10*time.Second, "c-rel to leave cooldown", func(d *statsDoc) bool {
		row, ok := relayRow(d, "cloudflare", "c-rel")
		return ok && row.Healthy
	})
	relayTo(t, g, echo, "/cloudflare", "p7", nil, http.StatusOK)
	row, _ = relayRow(g.stats(t), "cloudflare", "c-rel")
	if !row.Healthy || row.LastError != "" {
		t.Fatalf("c-rel after half-open success = %+v, want healthy with the label cleared", row)
	}
	relayTo(t, g, echo, "/", "u3", []byte("back-payload"), http.StatusOK)
}

func TestE2E_UnpinnedPinValuesRoundRobin(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	g := twoRelayFleet(t, fake, nil)

	// none / auto / all mean the same thing as no header: round-robin
	// across every provider, on the one shared cursor. Six requests pin the
	// exact served sequence over the sorted pool order.
	pins := []string{"none", "auto", "all", "none", "auto", "all"}
	for i, pin := range pins {
		resp, raw := g.do(t, http.MethodPost, "/", map[string]string{
			"X-Relay-Provider": pin,
			"X-Relay-Target":   echo.base,
			"X-Relay-Path":     "/pin",
			"X-Marker":         fmt.Sprintf("m%d", i),
		}, []byte("pin"))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("pin %q request %d = %d (%s)", pin, i+1, resp.StatusCode, raw)
		}
	}
	if got := fake.project("c-rel").sim.markers(); !equalStrings(got, []string{"m0", "m2", "m4"}) {
		t.Fatalf("c-rel served %v, want [m0 m2 m4]", got)
	}
	if got := fake.project("v-rel").sim.markers(); !equalStrings(got, []string{"m1", "m3", "m5"}) {
		t.Fatalf("v-rel served %v, want [m1 m3 m5]", got)
	}
	if n := len(echo.requests()); n != len(pins) {
		t.Fatalf("the target saw %d requests, want %d", n, len(pins))
	}
}

// TestE2E_AbruptRelayCloseIsSanitizedOnEverySurface: a relay that accepts a
// live-relayed request body and resets the connection without answering
// produces the "<op> tcp A->B:" error family — net.OpError's read/write
// forms and the readfrom wrapper a declared-length body write adds — and
// every layer carries both endpoint addresses. The sanitized text is what
// may reach the client's 502 body, /stats lastError, and the structured
// log — ports stay, addresses do not. This is the shape the hang-mode tests
// above deliberately avoid (their timeout errors are address-free by
// construction); it was the redaction gap the security follow-up closed.
func TestE2E_AbruptRelayCloseIsSanitizedOnEverySurface(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, ct := freshIdentity("rs-s")
	fake.setToken("cloudflare", ct)
	g := startGateway(t, fake, gwOptions{
		key: key,
		config: configYAMLOverrides(t, map[string]string{
			// Live-relay the body (one attempt, no failover) and keep the
			// verified-readiness layer quiet: the reset is a data-plane
			// event and must not demote the relay mid-test.
			"stream_threshold_bytes": "65536",
			"verify_interval":        "1h",
		}, relayBlock("c-rel", "cloudflare", tokenEnvLine("E2E_CT"))),
		extraEnv: map[string]string{"E2E_CT": ct},
	})
	g.waitForStats(t, 20*time.Second, "the relay to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 1
	})

	fake.project("c-rel").sim.setMode(simResetLeg)

	// Well past the loopback socket buffers, so the relay's close lands
	// mid-write and the transport error is deterministic.
	body := bytes.Repeat([]byte("x"), 8<<20)
	resp, raw := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Provider": "cloudflare",
		"X-Relay-Target":   echo.base,
		"X-Relay-Path":     "/reset",
		"X-Marker":         "reset-leg",
	}, body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("streamed request against a reset relay leg = %d (%s), want 502", resp.StatusCode, raw)
	}
	assertNoLeaks(t, "502 body", g, fake, raw)
	if bytes.Contains(raw, []byte("127.0.0.1")) {
		t.Fatalf("502 body carries a literal endpoint address: %s", raw)
	}
	if !bytes.Contains(raw, []byte("[redacted]")) {
		t.Fatalf("502 body lost the sanitized transport-error shape: %s", raw)
	}

	d := g.waitForStats(t, 10*time.Second, "the passive failure to record", func(d *statsDoc) bool {
		row, ok := relayRow(d, "cloudflare", "c-rel")
		return ok && row.Failures >= 1 && row.LastError != ""
	})
	row, _ := relayRow(d, "cloudflare", "c-rel")
	if !strings.Contains(row.LastError, "[redacted]") || strings.Contains(row.LastError, "127.0.0.1") {
		t.Fatalf("/stats lastError is not the sanitized OpError shape: %s", row.LastError)
	}
	assertNoLeaks(t, "/stats lastError", g, fake, []byte(row.LastError))

	log := g.logDump()
	assertNoLeaks(t, "gateway log", g, fake, []byte(log))
	if !strings.Contains(log, " tcp [redacted]") {
		t.Fatal("gateway log never records the sanitized transport-error shape for the reset")
	}
}
