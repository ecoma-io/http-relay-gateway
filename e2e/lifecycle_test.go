package e2e

import (
	"bytes"
	"io"
	"net/http"
	"strings"
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

	g.waitForLifecycle(t, "vercel", "v-rel", "unready", "replacing", 15*time.Second)
	g.waitForZeroReady(t, 5*time.Second)

	// The settle barrier: no deploy may start while the old worker still
	// carries the in-flight request.
	time.Sleep(700 * time.Millisecond)
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys started while a request was in flight = %d, want 1 (settle first)", n)
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
// never a redeploy — and the revival scan re-admits it untouched once the
// platform answers again.
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

	// The platform recovers; the revival scan re-admits the same deploy.
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
