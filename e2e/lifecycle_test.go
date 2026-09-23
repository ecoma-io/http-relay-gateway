package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestE2E_ReplacementRolloutStrategyA: a version-drifting relay is replaced
// in the documented order — demote, settle (the in-flight request drains
// against the old worker), only then deploy. While the replacement is in
// flight the pool serves nothing (zero-ready), and the drained request
// completes exactly once against the old worker.
func TestE2E_ReplacementRolloutStrategyA(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("roll")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)

	// One in-flight request, held inside the worker sim after admission —
	// the request the settle barrier must wait for.
	releaseHold := fake.project("v-rel").sim.blockMarker("hold")
	type result struct {
		status int
		body   string
		err    error
	}
	inflight := make(chan result, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, g.base+"/", bytes.NewReader([]byte("inflight")))
		if err != nil {
			inflight <- result{err: err}
			return
		}
		req.Header.Set("X-Relay-Target", echo.base)
		req.Header.Set("X-Relay-Path", "/inflight")
		req.Header.Set("X-Marker", "hold")
		resp, err := relayClient.Do(req)
		if err != nil {
			inflight <- result{err: err}
			return
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		inflight <- result{status: resp.StatusCode, body: string(raw)}
	}()
	waitFor(t, 10*time.Second, "the in-flight request to reach the relay", func() bool {
		return fake.project("v-rel").sim.markerSeen("hold")
	})

	// Version drift queues a replacement; the deploy is gated so the whole
	// rollout order is observable.
	releaseDeploy := fake.armDeployGate("v-rel")
	fake.project("v-rel").sim.setMode(simStale)

	// The demoted phase is intentionally short when the drain completes, so
	// observe the durable safety boundary instead: no relay is admitted before
	// the replacement deploy may start.
	g.waitForZeroReady(t, 5*time.Second)

	// The settle barrier: no deploy may start while the old worker still
	// carries the in-flight request. Observed by deploy *receipt* — the
	// fake records the call the moment it arrives, before its (gated)
	// completion — so a regression that starts the replacement deploy while
	// the drain is still waiting cannot pass on a technicality.
	time.Sleep(700 * time.Millisecond)
	if n := len(fake.callsFor("vercel", "deploy")); n != 1 {
		t.Fatalf("deploy calls received while a request was in flight = %d, want 1 (the admission deploy; settle and drain precede any deploy)", n)
	}
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys completed while a request was in flight = %d, want 1 (settle first)", n)
	}

	releaseHold()
	waitFor(t, 10*time.Second, "the drained request to answer", func() bool {
		select {
		case r := <-inflight:
			if r.err != nil || r.status != http.StatusOK {
				t.Errorf("in-flight request = %d/%v (%s), want 200", r.status, r.err, r.body)
			}
			return true
		default:
			return false
		}
	})

	// With the worker idle the replacement deploys — gated, so the window
	// between deploy-start and admission is observable.
	g.waitForLifecycle(t, "vercel", "v-rel", "deploying", "", 15*time.Second)
	resp, _ := g.do(t, http.MethodGet, "/readyz", nil, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz mid-replacement = %d, want 503", resp.StatusCode)
	}
	if doc := g.stats(t); len(doc.Relays) != 0 {
		t.Fatalf("pool mid-replacement = %+v, want nothing serving", doc.Relays)
	}

	releaseDeploy()
	g.waitForReady(t, 15*time.Second)

	if n := fake.deployCount("v-rel"); n != 2 {
		t.Fatalf("deploys after rollout = %d, want 2", n)
	}
	// The drained request hit the echo exactly once — a replacement never
	// replays a request the old worker already took.
	var seen int
	for _, rec := range echo.requests() {
		if rec.Header.Get("X-Marker") == "hold" {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("echo saw the in-flight request %d times, want exactly 1", seen)
	}
}

// TestE2E_ShutdownDrainStaysInsideTheGraceBudget: a SIGTERM that lands
// while a replacement is parked in the in-flight drain must unwind inside
// the single SHUTDOWN_GRACE budget (5s in this harness) — the reconciler's
// drain may not add its own 30s quiesce window on top before the HTTP drain
// even starts, or the orchestrator's kill timer closes the straggler
// anyway.
func TestE2E_ShutdownDrainStaysInsideTheGraceBudget(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("drain")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)
	sim := fake.project("v-rel").sim

	// One request held inside the worker sim: the straggler the rollout's
	// drain waits for. It never finishes on its own — process exit closes it.
	releaseHold := sim.blockMarker("hold")
	inflight := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, g.base+"/", bytes.NewReader([]byte("held")))
		if err != nil {
			inflight <- err
			return
		}
		req.Header.Set("X-Relay-Target", echo.base)
		req.Header.Set("X-Relay-Path", "/held")
		req.Header.Set("X-Marker", "hold")
		resp, err := relayClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		inflight <- err
	}()
	waitFor(t, 10*time.Second, "the held request to reach the relay", func() bool {
		return sim.markerSeen("hold")
	})

	// Version drift parks the replacement in the drain behind the held
	// request (verify_interval is 1s in the e2e settings).
	sim.setMode(simStale)
	g.waitForLifecycle(t, "vercel", "v-rel", "unready", "replacing", 15*time.Second)

	started := time.Now()
	g.exitOK(10 * time.Second) // SHUTDOWN_GRACE is 5s in this harness
	if took := time.Since(started); took > 9*time.Second {
		t.Fatalf("shutdown took %s with a 5s grace — the reconciler drain ran on its own budget", took)
	}
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys = %d, want 1 (a shutting-down process never deploys under the straggler)", n)
	}

	releaseHold()
	<-inflight
}

// trickleServer is an upstream whose response body never ends on its own:
// one numbered, flushed chunk per interval — a mid-stream response the
// gateway must keep draining across shutdown.
type trickleServer struct {
	base     string
	srv      *http.Server
	ln       net.Listener
	interval time.Duration
}

func newTrickle(t *testing.T, interval time.Duration) *trickleServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("trickle listen: %v", err)
	}
	s := &trickleServer{base: "http://" + ln.Addr().String(), ln: ln, interval: interval}
	s.srv = &http.Server{Handler: http.HandlerFunc(s.handle), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	t.Cleanup(func() { _ = s.srv.Close() })
	return s
}

func (s *trickleServer) handle(w http.ResponseWriter, r *http.Request) {
	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	for i := 0; ; i++ {
		if _, err := fmt.Fprintf(w, "chunk-%d\n", i); err != nil {
			return
		}
		if fl != nil {
			fl.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(s.interval):
		}
	}
}

// TestE2E_ShutdownDrainsAMidStreamResponse: SIGTERM lands while a response
// is already streaming through the gateway — the stream keeps flowing to
// the client afterwards (in-flight drain, streaming included), and the
// process still exits cleanly inside the one SHUTDOWN_GRACE budget instead
// of waiting on the never-ending body.
func TestE2E_ShutdownDrainsAMidStreamResponse(t *testing.T) {
	fake := newFakeEdge(t)
	key, vt := freshIdentity("midstream")
	fake.setToken("vercel", vt)
	trickle := newTrickle(t, 150*time.Millisecond)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)

	req, err := http.NewRequest(http.MethodGet, g.base+"/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Relay-Target", trickle.base)
	req.Header.Set("X-Relay-Path", "/stream")
	resp, err := relayClient.Do(req)
	if err != nil {
		t.Fatalf("streaming request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("streaming request = %d, want 200", resp.StatusCode)
	}

	lines := make(chan string, 16)
	readErr := make(chan error, 1)
	go func() {
		br := bufio.NewReader(resp.Body)
		for {
			line, rerr := br.ReadString('\n')
			if rerr != nil {
				readErr <- rerr
				return
			}
			lines <- line
		}
	}()

	// The response is live: chunks flow before any signal.
	select {
	case line := <-lines:
		if line != "chunk-0\n" {
			t.Fatalf("first chunk = %q, want chunk-0", line)
		}
	case rerr := <-readErr:
		t.Fatalf("stream ended before shutdown: %v", rerr)
	case <-time.After(10 * time.Second):
		t.Fatal("no chunk reached the client within 10s")
	}

	// Shutdown while the body is mid-flight.
	started := time.Now()
	if err := g.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}

	// The drain keeps the open stream alive: another chunk arrives after
	// the process was told to stop.
	select {
	case line := <-lines:
		if line != "chunk-1\n" {
			t.Fatalf("post-signal chunk = %q, want chunk-1", line)
		}
	case rerr := <-readErr:
		t.Fatalf("the stream was cut by shutdown instead of drained: %v", rerr)
	case <-time.After(3 * time.Second):
		t.Fatal("no chunk reached the client after SIGTERM — the in-flight stream was not drained")
	}

	// A body that never ends must not hold the process past its budget.
	g.exitOK(10 * time.Second) // SHUTDOWN_GRACE is 5s in this harness
	if took := time.Since(started); took > 9*time.Second {
		t.Fatalf("shutdown took %s with a 5s grace — the streaming request outlived the budget", took)
	}
}

// TestE2E_RemoveReAddWhileDeleteInFlight: removing a relay deletes its
// remote project; re-adding the same identity while that delete is still
// in flight waits for it (no concurrent deploy against a dying project)
// and then redeploys as a new generation.
func TestE2E_RemoveReAddWhileDeleteInFlight(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("rm")
	dt := "deno-tok-" + randSuffix(12)
	fake.setToken("vercel", vt)
	fake.setToken("deno", dt)

	both := configYAML(
		relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")),
		relayBlock("d-rel", "deno", tokenEnvLine("E2E_DT")),
	)
	onlyVercel := configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")))

	g := startGateway(t, fake, gwOptions{
		key:      key,
		config:   both,
		extraEnv: map[string]string{"E2E_VT": vt, "E2E_DT": dt},
	})
	g.waitForStats(t, 20*time.Second, "both relays to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})

	// Drop d-rel from the desired state; gate the remote delete so its
	// in-flight window is observable.
	releaseDelete := fake.armDeleteGate("d-rel")
	g.writeConfig(onlyVercel)

	waitFor(t, 20*time.Second, "the remote deno delete to start", func() bool {
		return len(fake.callsFor("deno", "delete")) > 0
	})
	g.waitForLifecycle(t, "deno", "d-rel", "removing", "", 10*time.Second)
	if n := fake.deleteCount("d-rel"); n != 1 {
		t.Fatalf("delete records while gated = %d, want 1", n)
	}
	resp, _ := g.do(t, http.MethodGet, "/readyz", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/readyz while one of two relays removes = %d, want 200 (v-rel still ready)", resp.StatusCode)
	}

	// Re-add the same identity while the delete is still in flight: no
	// deploy may race the dying project.
	g.writeConfig(both)
	time.Sleep(1500 * time.Millisecond)
	if n := fake.deployCount("d-rel"); n != 1 {
		t.Fatalf("deploys while the stale delete is in flight = %d, want 1 (busy hold)", n)
	}

	releaseDelete()
	g.waitForStats(t, 20*time.Second, "the re-added relay to redeploy and admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})

	if n := fake.deployCount("d-rel"); n != 2 {
		t.Fatalf("deploys for the re-added relay = %d, want 2", n)
	}
	if n := fake.deleteCount("d-rel"); n != 1 {
		t.Fatalf("deletes = %d, want exactly 1", n)
	}
	// The contract the generation counter protects: the new incarnation's
	// deploy only begins after the old project actually ceased to exist —
	// never concurrently with, or ahead of, the stale delete.
	done, ok := fake.deleteDoneAt("d-rel")
	deployCalls := fake.callsFor("deno", "deploy")
	if !ok || len(deployCalls) != 2 || deployCalls[1].At.Before(done) {
		t.Fatalf("delete completed at %v (ok=%v), second deploy call at %v of %d — the redeploy must follow the delete",
			done, ok, deployCalls[len(deployCalls)-1].At, len(deployCalls))
	}
	g.waitForLifecycle(t, "deno", "d-rel", "ready", "", 10*time.Second)
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("untouched vercel deploys = %d, want 1", n)
	}
	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/after-readd",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after re-add = %d (%s)", resp.StatusCode, body)
	}
}

// TestE2E_RestartRecoversByReuse: a restarted gateway rebuilds its entire
// view from the desired state alone — existing deployments are discovered
// and re-admitted without a single redeploy or delete.
func TestE2E_RestartRecoversByReuse(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("rest")
	ct := "cf-tok-" + randSuffix(12)
	fake.setToken("vercel", vt)
	fake.setToken("cloudflare", ct)

	opts := gwOptions{
		key: key,
		config: configYAML(
			relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")),
			relayBlock("c-rel", "cloudflare", tokenEnvLine("E2E_CT")),
		),
		extraEnv: map[string]string{"E2E_VT": vt, "E2E_CT": ct},
	}

	g := startGateway(t, fake, opts)
	g.waitForStats(t, 20*time.Second, "both relays to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})
	if n := fake.deployCount("v-rel") + fake.deployCount("c-rel"); n != 2 {
		t.Fatalf("deploys before restart = %d, want 2", n)
	}
	discV, discC := len(fake.callsFor("vercel", "discover")), len(fake.callsFor("cloudflare", "discover"))

	g.exitOK(15 * time.Second)

	g2 := startGateway(t, fake, opts)
	g2.waitForStats(t, 25*time.Second, "both relays to re-admit after restart", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})

	for _, slug := range []string{"v-rel", "c-rel"} {
		if n := fake.deployCount(slug); n != 1 {
			t.Fatalf("deploys for %s after restart = %d, want 1 (reuse, no redeploy)", slug, n)
		}
		if n := fake.deleteCount(slug); n != 0 {
			t.Fatalf("deletes for %s after restart = %d, want 0", slug, n)
		}
	}
	if n := len(fake.callsFor("vercel", "discover")); n <= discV {
		t.Fatalf("vercel discover calls after restart = %d, want > %d (fresh discovery)", n, discV)
	}
	if n := len(fake.callsFor("cloudflare", "discover")); n <= discC {
		t.Fatalf("cloudflare discover calls after restart = %d, want > %d", n, discC)
	}

	resp, body := g2.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/after-restart",
	}, []byte("again"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after restart = %d (%s)", resp.StatusCode, body)
	}
	doc := g2.stats(t)
	for slug, provider := range map[string]string{"v-rel": "vercel", "c-rel": "cloudflare"} {
		row, ok := lifecycleOf(doc, provider, slug)
		if !ok || row.State != "ready" || row.Generation != 1 {
			t.Fatalf("lifecycle for %s = %+v (ok=%v), want ready generation 1", slug, row, ok)
		}
	}
}

// TestE2E_InvalidConfigKeepsLastKnownGood: a desired-state file that fails
// validation is refused wholesale — the last known configuration keeps
// serving untouched until a valid file lands.
func TestE2E_InvalidConfigKeepsLastKnownGood(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("inv")
	ct := "cf-tok-" + randSuffix(12)
	fake.setToken("vercel", vt)
	fake.setToken("cloudflare", ct)

	good := configYAML(
		relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")),
		relayBlock("c-rel", "cloudflare", tokenEnvLine("E2E_CT")),
	)
	g := startGateway(t, fake, gwOptions{
		key:      key,
		config:   good,
		extraEnv: map[string]string{"E2E_VT": vt, "E2E_CT": ct},
	})
	g.waitForStats(t, 20*time.Second, "both relays to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})
	deploysV, deploysC := fake.deployCount("v-rel"), fake.deployCount("c-rel")

	// An unknown provider: the whole file is invalid, not just one relay.
	g.writeConfig(good + "relays:\n  - name: bad\n    provider: gcp\n    token: nope\n")
	waitFor(t, 10*time.Second, "the invalid file to be reported", func() bool {
		return strings.Contains(g.logDump(), "desired state file invalid; keeping the last known configuration")
	})

	// Nothing changed: the last known fleet keeps serving, no churn fires.
	doc := g.stats(t)
	if !doc.Readiness.Ready || len(doc.Relays) != 2 {
		t.Fatalf("/stats after an invalid reload = %+v, want the last known two-relay fleet", doc)
	}
	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/still-serving",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after an invalid reload = %d (%s)", resp.StatusCode, body)
	}
	time.Sleep(2200 * time.Millisecond)
	if fake.deployCount("v-rel") != deploysV || fake.deployCount("c-rel") != deploysC {
		t.Fatalf("deploys churned on an invalid reload (%d, %d), want %d, %d",
			fake.deployCount("v-rel"), fake.deployCount("c-rel"), deploysV, deploysC)
	}

	// A valid file is adopted again.
	g.writeConfig(good)
	g.waitForStats(t, 10*time.Second, "the fleet to keep serving the valid state", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})
}

// TestE2E_SuspensionPausesAndRevives: a platform suspension (HTTP 402 or
// the DEPLOYMENT_DISABLED marker) pauses a relay on its own cadence —
// never a redeploy — and the revival cadence re-admits it untouched once
// the platform answers again.
func TestE2E_SuspensionPausesAndRevives(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("susp")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)
	sim := fake.project("v-rel").sim

	// Quota-page suspension.
	sim.setMode(simSuspended402)
	g.waitForLifecycle(t, "vercel", "v-rel", "paused", "paused", 20*time.Second)
	resp, _ := g.do(t, http.MethodGet, "/readyz", nil, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while paused = %d, want 503", resp.StatusCode)
	}
	resp, _ = g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/paused",
	}, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("data plane while paused = %d, want 503", resp.StatusCode)
	}
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys after suspension = %d, want 1 (never redeploy at a suspension)", n)
	}

	// The platform recovers; the revival cadence re-admits the same deploy.
	sim.setMode(simOK)
	g.waitForReady(t, 15*time.Second)
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys after revival = %d, want 1", n)
	}

	// Marker-style suspension, same cycle.
	sim.setMode(simSuspendedMarker)
	g.waitForLifecycle(t, "vercel", "v-rel", "paused", "paused", 20*time.Second)
	sim.setMode(simOK)
	g.waitForReady(t, 15*time.Second)

	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys across both suspensions = %d, want 1", n)
	}
	if n := fake.deleteCount("v-rel"); n != 0 {
		t.Fatalf("deletes across suspensions = %d, want 0", n)
	}
	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/revived",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after revival = %d (%s)", resp.StatusCode, body)
	}
}

// midstreamRow is one relay's /stats row decoded with the mid-stream
// counter. The shared harness row predates the counter and stays shared
// with the other in-flight PR, so this file decodes the field locally
// instead of widening the shared type.
type midstreamRow struct {
	Name              string `json:"name"`
	Requests          int64  `json:"requests"`
	Failures          int64  `json:"failures"`
	MidstreamFailures int64  `json:"midstreamFailures"`
	Healthy           bool   `json:"healthy"`
}

func midstreamRowOf(t *testing.T, g *gatewayProc, name string) (midstreamRow, bool) {
	t.Helper()
	_, raw := g.do(t, http.MethodGet, "/stats", nil, nil)
	var doc struct {
		Relays []midstreamRow `json:"relays"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("/stats: decode %s: %v", raw, err)
	}
	for _, row := range doc.Relays {
		if row.Name == name {
			return row, true
		}
	}
	return midstreamRow{}, false
}

// TestE2E_MidstreamFailureCountsOnceAndNeverRetries: a relay leg that dies
// after the response headers have gone through is one counted attempt and
// one passive failure — never a replay (the client already holds part of
// the body), and the truncated 200 simply ends on the client instead of
// turning into an error status.
func TestE2E_MidstreamFailureCountsOnceAndNeverRetries(t *testing.T) {
	fake := newFakeEdge(t)
	key, vt := freshIdentity("midcut")
	fake.setToken("vercel", vt)
	trickle := newTrickle(t, 150*time.Millisecond)

	g := startGateway(t, fake, gwOptions{
		config: configYAMLOverrides(t, map[string]string{
			// The severed leg is a data-plane event and must stay one: a live
			// verify tick would probe the dead origin, demote the relay and
			// pull its row off /stats before the counters below are read.
			"verify_interval": "1h",
		}, relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)
	sim := fake.project("v-rel").sim

	req, err := http.NewRequest(http.MethodGet, g.base+"/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Relay-Target", trickle.base)
	req.Header.Set("X-Relay-Path", "/stream")
	req.Header.Set("X-Marker", "mid1")
	resp, err := relayClient.Do(req)
	if err != nil {
		t.Fatalf("streaming request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("streaming request = %d, want 200", resp.StatusCode)
	}

	lines := make(chan string, 16)
	readErr := make(chan error, 1)
	go func() {
		br := bufio.NewReader(resp.Body)
		for {
			line, rerr := br.ReadString('\n')
			if rerr != nil {
				readErr <- rerr
				return
			}
			lines <- line
		}
	}()

	// The response is live: the relay leg carried the request and the first
	// chunk reached the client through it.
	waitFor(t, 10*time.Second, "the relay leg to admit the request", func() bool {
		return sim.markerSeen("mid1")
	})
	select {
	case line := <-lines:
		if line != "chunk-0\n" {
			t.Fatalf("first chunk = %q, want chunk-0", line)
		}
	case rerr := <-readErr:
		t.Fatalf("stream ended before the leg was severed: %v", rerr)
	case <-time.After(10 * time.Second):
		t.Fatal("no chunk reached the client within 10s")
	}

	// Sever the gateway↔relay leg mid-body. The worker sim re-frames an
	// upstream that stops early as a complete chunked response, so the
	// transport event the outcome classification owns has to happen on this
	// leg itself: closing the fake origin force-closes the in-flight relay
	// connection — the headers are long gone, the body is not.
	fake.Close()

	// Once the relay answered, the response streams through untouched and
	// never turns into an error status: the truncated body just ends.
	rest, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		t.Fatalf("truncated body did not end cleanly: %v (read %q)", rerr, rest)
	}

	// Never retried: the request reached the relay exactly once.
	waitFor(t, 10*time.Second, "the sim's ingress to settle at one request", func() bool {
		return len(sim.markers()) == 1
	})
	if got := sim.markers(); !equalStrings(got, []string{"mid1"}) {
		t.Fatalf("relay served %v, want exactly [mid1] — a started response is never retried", got)
	}

	// The accounting: one counted attempt, one passive failure, the
	// mid-stream counter up once — and the relay stays healthy, because one
	// failure sits below the harness threshold of 2.
	waitFor(t, 10*time.Second, "/stats to count the mid-stream failure", func() bool {
		row, ok := midstreamRowOf(t, g, "v-rel")
		return ok && row.Requests == 1 && row.Failures == 1 && row.MidstreamFailures == 1
	})
	row, ok := midstreamRowOf(t, g, "v-rel")
	if !ok || !row.Healthy {
		t.Fatalf("v-rel stats row = %+v (ok=%v), want healthy after a single mid-stream failure", row, ok)
	}
}
