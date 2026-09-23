package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// freshIdentity returns a unique relay key and a unique provider token so
// leak scans can never match another run's values.
func freshIdentity(prefix string) (key, token string) {
	return prefix + "-key-" + randSuffix(12), prefix + "-tok-" + randSuffix(12)
}

// TestE2E_BootstrapNoConfig boots with the desired-state file absent: the
// empty fleet answers healthz, refuses everything else with the retryable
// zero-ready 503, and adopts the fleet the moment the file appears — no
// restart.
func TestE2E_BootstrapNoConfig(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("boot")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})

	resp, body := g.do(t, http.MethodGet, "/healthz", nil, nil)
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("/healthz = %d %q, want 200 ok", resp.StatusCode, body)
	}
	resp, _ = g.do(t, http.MethodGet, "/readyz", nil, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 before any relay verifies", resp.StatusCode)
	}
	resp, body = g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/x", "X-Relay-Token": "forged",
	}, []byte("payload"))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("relay with an empty fleet = %d (%s), want 503", resp.StatusCode, body)
	}
	doc := g.stats(t)
	if len(doc.Relays) != 0 || doc.Readiness.Ready || len(doc.Lifecycle) != 0 {
		t.Fatalf("/stats with no config = %+v, want an empty pool", doc)
	}

	// The file appears; the watcher adopts it and the relay deploys.
	g.writeConfig(configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))))
	g.waitForReady(t, 20*time.Second)

	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys after adoption = %d, want 1", n)
	}
	resp, body = g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/adopted?q=1",
	}, []byte("hello"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after adoption = %d (%s)", resp.StatusCode, body)
	}
	if n := len(echo.requests()); n != 1 {
		t.Fatalf("echo saw %d requests, want 1", n)
	}
}

// TestE2E_ReadinessCheckSubcommand: the readinesscheck subcommand gates on
// a verified fleet — non-zero while nothing is admitted, zero once one is,
// against the real running listener — and healthcheck answers for the live
// process throughout, fleet or no fleet.
func TestE2E_ReadinessCheckSubcommand(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("rc")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})

	// The empty fleet: the process is alive, but nothing is verified — the
	// readiness gate must refuse to hand traffic over.
	if code, out := runSubcommand(t, g, "readinesscheck"); code == 0 {
		t.Fatalf("readinesscheck with an empty fleet exited %d (%s), want failure", code, out)
	}
	if code, out := runSubcommand(t, g, "healthcheck"); code != 0 {
		t.Fatalf("healthcheck with an empty fleet exited %d (%s), want 0", code, out)
	}

	// A relay verifies; the same subcommand now admits traffic.
	g.writeConfig(configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))))
	g.waitForReady(t, 20*time.Second)
	if code, out := runSubcommand(t, g, "readinesscheck"); code != 0 {
		t.Fatalf("readinesscheck with a verified relay exited %d (%s), want 0", code, out)
	}
	if code, out := runSubcommand(t, g, "healthcheck"); code != 0 {
		t.Fatalf("healthcheck with a verified relay exited %d (%s), want 0", code, out)
	}
	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/after-check",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after readinesscheck = %d (%s)", resp.StatusCode, body)
	}
}

// TestE2E_FirstRelayAdmission pins the first bring-up contract: discover
// honoring the scope pin, then exactly one deploy carrying the current
// worker version and the relay key; the deployed relay forwards end to end
// with caller headers intact, gateway-internal headers stripped, and
// nothing identifying the caller added. No delete ever fires.
func TestE2E_FirstRelayAdmission(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("first")
	fake.setToken("vercel", vt)
	tokenFile := filepath.Join(t.TempDir(), "vercel-token")
	if err := os.WriteFile(tokenFile, []byte(vt+"\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	g := startGateway(t, fake, gwOptions{
		key: key,
		config: configYAML(
			relayBlock("v-rel", "vercel",
				tokenFileLine(tokenFile)+"    team: team-e2e\n"),
		),
	})
	g.waitForReady(t, 20*time.Second)

	disc := fake.callsFor("vercel", "discover")
	if len(disc) == 0 {
		t.Fatal("no vercel discover call recorded")
	}
	if got := disc[0].Query.Get("teamId"); got != "team-e2e" {
		t.Fatalf("discover teamId = %q, want the pinned scope team-e2e", got)
	}
	for _, c := range fake.callsFor("vercel", "deploy") {
		if got := c.Query.Get("teamId"); got != "team-e2e" {
			t.Fatalf("deploy teamId = %q, want team-e2e", got)
		}
	}

	doc := g.stats(t)
	deploys := fake.deploysOf("v-rel")
	if len(deploys) != 1 {
		t.Fatalf("deploys = %d, want exactly 1", len(deploys))
	}
	if deploys[0].Version != doc.RelayVersion {
		t.Fatalf("deployed version %q != /stats relayVersion %q", deploys[0].Version, doc.RelayVersion)
	}
	if deploys[0].Token != key {
		t.Fatalf("deployed relay key does not match RELAY_AUTH_TOKEN (len %d vs %d)",
			len(deploys[0].Token), len(key))
	}
	if !strings.Contains(deploys[0].Source, "__relay/version") {
		t.Fatal("deployed source is not the relay worker")
	}
	if n := fake.deleteCount("v-rel"); n != 0 {
		t.Fatalf("deletes on first admission = %d, want 0", n)
	}

	// End-to-end relay-spec forward through the deployed worker.
	resp, body := g.do(t, http.MethodPost, "/vercel", map[string]string{
		"X-Relay-Target": echo.base,
		"X-Relay-Path":   "/v1/chat?beta=true",
		"X-Relay-Token":  "caller-forged-token",
		"X-Custom":       "keepme",
		"User-Agent":     "e2e-probe/1",
	}, []byte("relay me"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay POST = %d (%s)", resp.StatusCode, body)
	}
	var eb parsedEcho
	if err := json.Unmarshal(body, &eb); err != nil {
		t.Fatalf("echo body %s: %v", body, err)
	}
	if eb.Method != http.MethodPost || eb.Path != "/v1/chat" || eb.Query != "beta=true" {
		t.Fatalf("echo routed %+v, want POST /v1/chat?beta=true", eb)
	}
	if eb.Body != "relay me" {
		t.Fatalf("echo body %q, want the forwarded payload", eb.Body)
	}
	if eb.Headers["x-custom"] != "keepme" || eb.Headers["user-agent"] != "e2e-probe/1" {
		t.Fatalf("end-to-end headers not forwarded verbatim: %+v", eb.Headers)
	}
	for name, want := range map[string]string{
		"x-relay-token":    "", // caller-supplied, stripped
		"x-relay-provider": "", // pin, stripped
		"x-relay-target":   "", // consumed by the worker
		"x-relay-path":     "",
		"x-forwarded-for":  "", // never added
	} {
		if got := eb.Headers[name]; got != want {
			t.Fatalf("echo header %s = %q, want stripped/absent", name, got)
		}
	}

	row, ok := lifecycleOf(doc, "vercel", "v-rel")
	if !ok || row.State != "ready" || row.Generation != 1 {
		t.Fatalf("lifecycle = %+v (ok=%v), want ready generation 1", row, ok)
	}
	// Re-read: the forward above must show up in the serving counters.
	doc = g.stats(t)
	if len(doc.Relays) != 1 || doc.Relays[0].MaxBody != 4_500_000 || doc.Relays[0].Requests < 1 {
		t.Fatalf("relays = %+v, want one serving vercel relay with its 4.5MB cap", doc.Relays)
	}
	if doc.Version != "e2e-build" {
		t.Fatalf("/stats version = %q, want the built-in version", doc.Version)
	}
}

// TestE2E_UnreachableRelayNeverRedeploys: a discovered deployment that
// refuses connections classifies unreachable and retries under backoff —
// never a redeploy — and recovers by reuse once it answers again.
func TestE2E_UnreachableRelayNeverRedeploys(t *testing.T) {
	fake := newFakeEdge(t)
	key, vt := freshIdentity("down")
	fake.setToken("vercel", vt)
	p := fake.addProject("vercel", "v-rel", simDown)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})

	g.waitForLifecycle(t, "vercel", "v-rel", "failed", "unreachable", 20*time.Second)
	if n := fake.deployCount("v-rel"); n != 0 {
		t.Fatalf("deploys on an unreachable relay = %d, want 0", n)
	}

	// The relay comes back — still the same deployment: verify re-admits it
	// with zero deploys. Recovery is reuse, never a rebuild.
	p.sim.setDeployed(g.stats(t).RelayVersion, key)
	g.waitForStats(t, 20*time.Second, "the relay to re-admit", func(d *statsDoc) bool {
		row, ok := lifecycleOf(d, "vercel", "v-rel")
		return ok && row.State == "ready" && row.Reason == ""
	})
	if n := fake.deployCount("v-rel"); n != 0 {
		t.Fatalf("deploys after recovery = %d, want 0 (reuse)", n)
	}
}

// TestE2E_AuthDriftRedeploysOnKeyRotation: rotating RELAY_AUTH_TOKEN_FILE
// makes the next pass see the key rejected, redeploys with the new key,
// and re-admits — rotation without restart.
func TestE2E_AuthDriftRedeploysOnKeyRotation(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	oldKey, vt := freshIdentity("auth1")
	newKey := "rotated-key-" + randSuffix(12)
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		key:      oldKey,
		keyFile:  true,
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)

	if err := os.WriteFile(filepath.Join(g.dir, "relay-key"), []byte(newKey+"\n"), 0o600); err != nil {
		t.Fatalf("rotate key file: %v", err)
	}

	waitFor(t, 20*time.Second, "a redeploy carrying the rotated key", func() bool {
		deploys := fake.deploysOf("v-rel")
		return len(deploys) == 2 && deploys[1].Token == newKey
	})
	g.waitForReady(t, 10*time.Second)

	// The gateway now speaks the new key end to end.
	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/after-rotation",
	}, []byte("rotated"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after key rotation = %d (%s)", resp.StatusCode, body)
	}
	if fake.deleteCount("v-rel") != 0 {
		t.Fatal("key rotation must not delete the remote")
	}
}

// TestE2E_ProviderTokenRotationReuses: rotating the provider credential
// (token_file) needs no redeploy — discovery and verification simply
// succeed under the new token.
func TestE2E_ProviderTokenRotationReuses(t *testing.T) {
	fake := newFakeEdge(t)
	oldTok := "vercel-tok1-" + randSuffix(12)
	newTok := "vercel-tok2-" + randSuffix(12)
	key := "reuse-key-" + randSuffix(12)
	fake.setToken("vercel", oldTok)
	tokenFile := filepath.Join(t.TempDir(), "vercel-token")
	if err := os.WriteFile(tokenFile, []byte(oldTok+"\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	g := startGateway(t, fake, gwOptions{
		key:    key,
		config: configYAML(relayBlock("v-rel", "vercel", tokenFileLine(tokenFile))),
	})
	g.waitForReady(t, 20*time.Second)

	fake.setToken("vercel", newTok)
	if err := os.WriteFile(tokenFile, []byte(newTok+"\n"), 0o600); err != nil {
		t.Fatalf("rotate token file: %v", err)
	}

	waitFor(t, 20*time.Second, "a platform call under the rotated token", func() bool {
		for _, c := range append(fake.callsFor("vercel", "discover"), fake.callsFor("vercel", "deploy")...) {
			if c.Auth == "Bearer "+newTok {
				return true
			}
		}
		return false
	})
	g.waitForStats(t, 10*time.Second, "the relay to re-verify under the new token", func(d *statsDoc) bool {
		return d.Readiness.Ready
	})

	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys after provider token rotation = %d, want 1 (reuse)", n)
	}
	if n := fake.deleteCount("v-rel"); n != 0 {
		t.Fatalf("deletes after provider token rotation = %d, want 0", n)
	}
}

// TestE2E_VersionDriftRedeploys: a worker reporting a stale version is
// drift a deploy fixes; the replacement demotes first (zero-ready on a
// single relay) and only admits after the new worker verifies.
func TestE2E_VersionDriftRedeploys(t *testing.T) {
	fake := newFakeEdge(t)
	key, vt := freshIdentity("drift")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)
	relayVersion := g.stats(t).RelayVersion

	// Gate the replacement deploy so the demoted window is observable.
	release := fake.armDeployGate("v-rel")
	fake.project("v-rel").sim.setMode(simStale)

	// The deploy is gated immediately after the required demotion, so the
	// durable observable is zero admission rather than the transient
	// pre-deploy lifecycle phase.
	g.waitForZeroReady(t, 5*time.Second)
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("completed deploys while replacement gated = %d, want 1", n)
	}

	release()
	g.waitForReady(t, 15*time.Second)

	deploys := fake.deploysOf("v-rel")
	if len(deploys) != 2 || deploys[1].Version != relayVersion {
		t.Fatalf("deploys = %+v, want a second deploy of version %q", deploys, relayVersion)
	}
	resp, body := g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": "http://127.0.0.1:1", "X-Relay-Path": "/x",
	}, nil)
	// The relay forwards; the unreachable upstream turns into the worker's
	// own 502 — anything but the gateway's zero-ready 503.
	if resp.StatusCode == http.StatusServiceUnavailable {
		t.Fatalf("relay after replacement still zero-ready: %d (%s)", resp.StatusCode, body)
	}
}

// TestE2E_LastRelayFailureZeroReady: a verified relay whose forwarding
// round trip starts failing demotes after verify_demote_after failures —
// zero-ready 503s, healthz stays ok, a redeploy is never fired, and the
// same relay re-admits on its own once it verifies again.
func TestE2E_LastRelayFailureZeroReady(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("blip")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)

	fake.project("v-rel").sim.setMode(simProbe500)

	g.waitForZeroReady(t, 15*time.Second)
	g.waitForLifecycle(t, "vercel", "v-rel", "unready", "probe_failed", 5*time.Second)

	resp, body := g.do(t, http.MethodGet, "/healthz", nil, nil)
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("/healthz during outage = %d %q, want 200 ok", resp.StatusCode, body)
	}
	resp, _ = g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/unreachable",
	}, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("data plane during outage = %d, want 503", resp.StatusCode)
	}
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys after probe failures = %d, want 1 (the admission deploy; no redeploy)", n)
	}

	fake.project("v-rel").sim.setMode(simOK)
	g.waitForReady(t, 15*time.Second)
	if n := fake.deployCount("v-rel"); n != 1 {
		t.Fatalf("deploys after recovery = %d, want 1", n)
	}
	resp, body = g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/recovered",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relay after recovery = %d (%s)", resp.StatusCode, body)
	}
}

// TestE2E_MultiRelayAdmissionAndOrder: staggered admission across three
// providers (gated deploys), the zero-ready gate with unknown pins, the
// exact round-robin over the sorted serving order — cloudflare, deno,
// vercel, whatever the config order was — and per-selector cursor
// isolation for pins.
func TestE2E_MultiRelayAdmissionAndOrder(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("multi")
	ct := "cf-tok-" + randSuffix(12)
	dt := "deno-tok-" + randSuffix(12)
	fake.setToken("vercel", vt)
	fake.setToken("cloudflare", ct)
	fake.setToken("deno", dt)

	// Gate all three deploys before boot: admission then happens strictly
	// in the order the test releases them. The file order (vercel, deno,
	// cloudflare) deliberately differs from the served (sorted) order.
	releaseCF := fake.armDeployGate("c-rel")
	releaseDeno := fake.armDeployGate("d-rel")
	releaseVercel := fake.armDeployGate("v-rel")

	g := startGateway(t, fake, gwOptions{
		key: key,
		config: configYAML(
			relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")),
			relayBlock("d-rel", "deno", tokenEnvLine("E2E_DT")),
			relayBlock("c-rel", "cloudflare", tokenEnvLine("E2E_CT")+"    account: acct-e2e\n"),
		),
		extraEnv: map[string]string{"E2E_VT": vt, "E2E_DT": dt, "E2E_CT": ct},
	})

	// While nothing has verified, every pin — even a bogus one — answers
	// the retryable zero-ready 503.
	resp, _ := g.do(t, http.MethodGet, "/", nil, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unpinned before any admission = %d, want 503", resp.StatusCode)
	}
	resp, _ = g.do(t, http.MethodGet, "/nosuch", nil, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unknown pin before any admission = %d, want 503", resp.StatusCode)
	}

	// Staged admission: one relay at a time.
	releaseCF()
	g.waitForStats(t, 15*time.Second, "the cloudflare relay to admit alone", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 1
	})
	resp, _ = g.do(t, http.MethodGet, "/nosuch", nil, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown pin with a non-empty pool = %d, want 404", resp.StatusCode)
	}

	releaseDeno()
	g.waitForStats(t, 15*time.Second, "the deno relay to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})
	releaseVercel()
	g.waitForStats(t, 15*time.Second, "the vercel relay to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 3
	})

	// Six unpinned requests pin the exact served sequence: sorted
	// (provider, name) — c-rel, d-rel, v-rel — twice over. Requests target
	// the echo so forwards succeed and passive health stays green; the
	// marker header rides end to end and is observed at each relay's
	// ingress.
	rrHeaders := func(marker string) map[string]string {
		return map[string]string{
			"X-Relay-Target": echo.base, "X-Relay-Path": "/rr", "X-Marker": marker,
		}
	}
	for i := 1; i <= 6; i++ {
		resp, body := g.do(t, http.MethodPost, "/", rrHeaders(fmt.Sprintf("m%d", i)), []byte("rr"))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("round-robin request %d = %d (%s)", i, resp.StatusCode, body)
		}
	}

	// Pinned traffic never skews the all-providers cursor, and the header
	// pin beats the path prefix.
	for i, marker := range []string{"p1", "p2"} {
		h := rrHeaders(marker)
		h["X-Relay-Provider"] = "vercel"
		resp, body := g.do(t, http.MethodPost, "/deno", h, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("pinned request %d = %d (%s)", i+1, resp.StatusCode, body)
		}
	}
	// The all-providers cursor continues where m6 left it: m7 lands on c-rel.
	resp, body := g.do(t, http.MethodPost, "/", rrHeaders("m7"), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-pin unpinned request = %d (%s)", resp.StatusCode, body)
	}

	want := map[string][]string{
		"c-rel": {"m1", "m4", "m7"},
		"d-rel": {"m2", "m5"},
		"v-rel": {"m3", "m6", "p1", "p2"},
	}
	for slug, wantMarkers := range want {
		if got := fake.project(slug).sim.markers(); !equalStrings(got, wantMarkers) {
			t.Fatalf("relay %s served markers %v, want %v", slug, got, wantMarkers)
		}
	}

	// The cloudflare scope pin was honored: the account list was never
	// consulted, and discovery authenticated with the configured token.
	if n := len(fake.callsFor("cloudflare", "accounts")); n != 0 {
		t.Fatalf("account resolution calls = %d, want 0 with an explicit account pin", n)
	}
	if disc := fake.callsFor("cloudflare", "discover"); len(disc) == 0 ||
		disc[0].Auth != "Bearer "+ct {
		t.Fatalf("cloudflare discover = %+v, want an authenticated call", disc)
	}
}

// TestE2E_ScopeFlipReincarnatesTheRelay: moving a relay's provider scope pin
// (the vercel team) under an unchanged name is a new identity, not an update.
// Each flip bumps the generation and revokes admission first, drains the
// request the OLD scope's worker still carries before anything deploys, then
// creates a fresh deployment in the new scope — and leaves the old scope's
// deployment orphaned on the platform: never addressed again, never deleted.
// Flipping back never resurrects the orphan; the relay serves a third, fresh
// deployment built in the original scope.
func TestE2E_ScopeFlipReincarnatesTheRelay(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("scope")
	fake.setToken("vercel", vt)
	tokenFile := filepath.Join(t.TempDir(), "vercel-token")
	if err := os.WriteFile(tokenFile, []byte(vt+"\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	relayWithTeam := func(team string) string {
		return configYAML(relayBlock("v-rel", "vercel",
			tokenFileLine(tokenFile)+"    team: "+team+"\n"))
	}

	g := startGateway(t, fake, gwOptions{key: key, config: relayWithTeam("team-a")})
	g.waitForReady(t, 20*time.Second)
	if row, ok := lifecycleOf(g.stats(t), "vercel", "v-rel"); !ok || row.State != "ready" || row.Generation != 1 {
		t.Fatalf("lifecycle after admission = %+v (ok=%v), want ready generation 1", row, ok)
	}
	if n := fake.deployCountIn("vercel", "team-a", "v-rel"); n != 1 {
		t.Fatalf("deploys in team-a = %d, want exactly 1", n)
	}

	// holdOneRequest parks one request inside the serving worker's sim — the
	// request a flip's settle+drain barrier must wait for. The returned func
	// releases it and waits for the answer: a flip drains the old scope's
	// worker, it never cuts the request mid-flight.
	holdOneRequest := func(marker string) (release func()) {
		releaseHold := fake.project("v-rel").sim.blockMarker(marker)
		done := make(chan error, 1)
		go func() {
			req, err := http.NewRequest(http.MethodPost, g.base+"/", bytes.NewReader([]byte("inflight")))
			if err != nil {
				done <- err
				return
			}
			req.Header.Set("X-Relay-Target", echo.base)
			req.Header.Set("X-Relay-Path", "/"+marker)
			req.Header.Set("X-Marker", marker)
			resp, err := relayClient.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					err = fmt.Errorf("status %d", resp.StatusCode)
				}
			}
			done <- err
		}()
		waitFor(t, 10*time.Second, "the in-flight request to reach the serving worker", func() bool {
			return fake.project("v-rel").sim.markerSeen(marker)
		})
		return func() {
			releaseHold()
			waitFor(t, 10*time.Second, "the drained request to answer", func() bool {
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("in-flight request failed: %v", err)
					}
					return true
				default:
					return false
				}
			})
		}
	}

	// flip moves the pin to the next team and returns once the relay has
	// reincarnated: generation wantGen with admission revoked, while the
	// still-held request keeps the barrier closed — no deploy may even be
	// received in the new scope. (The discovery pass relabels the entry
	// discovered/missing microseconds after the flip's configured/
	// scope_changed, so the label is not observable; the generation and the
	// orphan warning are.)
	flip := func(next string, wantGen uint64) (releaseDeploy func()) {
		releaseDeploy = fake.armFreshDeployGate("v-rel")
		g.writeConfig(relayWithTeam(next))
		g.waitForStats(t, 15*time.Second, "the "+next+" flip to reincarnate the relay", func(d *statsDoc) bool {
			r, ok := lifecycleOf(d, "vercel", "v-rel")
			return ok && r.Generation == wantGen && r.State != "ready"
		})
		// The flip warns about the orphan it leaves behind — scopes named by
		// fingerprint, never by the pin value the credential resolved.
		log := g.logDump()
		if !strings.Contains(log, "relay scope changed") ||
			!strings.Contains(log, "previous_scope") {
			t.Fatalf("the %s flip logged no orphaned-deployment warning", next)
		}
		for _, pin := range []string{"team-a", "team-b"} {
			if strings.Contains(log, pin) {
				t.Fatalf("the %s flip logged the raw pin value, not its fingerprint", next)
			}
		}
		// The identity changed before any new deployment exists: the pool
		// serves nothing even though the old scope's worker still answers.
		resp, _ := g.do(t, http.MethodGet, "/readyz", nil, nil)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("/readyz mid-flip = %d, want 503", resp.StatusCode)
		}
		if doc := g.stats(t); len(doc.Relays) != 0 {
			t.Fatalf("pool mid-flip = %+v, want nothing serving", doc.Relays)
		}
		// The deploy is gated anyway; the fake records the call the moment it
		// arrives, so a regression that deploys while the drain waits cannot
		// pass on a technicality. Boot admission accounts for generation 1.
		time.Sleep(700 * time.Millisecond)
		if n := len(fake.callsFor("vercel", "deploy")); n != int(wantGen)-1 {
			t.Fatalf("deploy calls received after the %s flip = %d, want %d (settle and drain precede any deploy)",
				next, n, int(wantGen)-1)
		}
		if n := fake.deployCountIn("vercel", next, "v-rel"); n != 0 {
			t.Fatalf("deploys completed in %s = %d, want 0 while the drain waits", next, n)
		}
		return releaseDeploy
	}

	// relay marker lands in exactly one scope's worker sim — after a flip,
	// only in the new scope's.
	markerServedBy := func(marker, scope string) {
		t.Helper()
		resp, body := g.do(t, http.MethodPost, "/", map[string]string{
			"X-Relay-Target": echo.base, "X-Relay-Path": "/" + marker, "X-Marker": marker,
		}, []byte("after"))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request after the flip = %d (%s)", resp.StatusCode, body)
		}
		if !fake.projectIn("vercel", scope, "v-rel").sim.markerSeen(marker) {
			t.Fatalf("marker %s never reached the %s worker", marker, scope)
		}
		for _, other := range []string{"team-a", "team-b"} {
			if other == scope {
				continue
			}
			if fake.projectIn("vercel", other, "v-rel") != nil &&
				fake.projectIn("vercel", other, "v-rel").sim.markerSeen(marker) {
				t.Fatalf("marker %s reached the %s worker — the old scope is still being served", marker, other)
			}
		}
	}

	// --- flip one: team-a → team-b, under one in-flight request ---
	releaseA := holdOneRequest("hold-a")
	releaseDeployB := flip("team-b", 2)
	releaseA()
	if n := len(echo.requests()); n != 1 {
		t.Fatalf("echo requests = %d, want 1: the drained request answered exactly once", n)
	}
	releaseDeployB()
	g.waitForStats(t, 15*time.Second, "the team-b worker to admit", func(d *statsDoc) bool {
		r, ok := lifecycleOf(d, "vercel", "v-rel")
		return ok && r.State == "ready" && r.Generation == 2
	})
	if n := fake.deployCountIn("vercel", "team-b", "v-rel"); n != 1 {
		t.Fatalf("deploys in team-b = %d, want exactly 1 fresh deploy for the flip", n)
	}
	// The old scope's deployment is orphaned, never deleted: the gateway
	// addresses only the scope it pins now.
	if !fake.projectExistsIn("vercel", "team-a", "v-rel") || fake.deleteCountIn("vercel", "team-a", "v-rel") != 0 {
		t.Fatalf("team-a after the flip: exists=%v deletes=%d, want the orphan still present, never deleted",
			fake.projectExistsIn("vercel", "team-a", "v-rel"), fake.deleteCountIn("vercel", "team-a", "v-rel"))
	}
	markerServedBy("post-b", "team-b")

	// The documented remediation for an orphan: remove it by hand on the
	// platform console. The fake drops it with no API call — nothing the
	// gateway does may reach the old scope.
	fake.removeProjectManually("vercel", "team-a", "v-rel")

	// --- flip two: team-b → team-a, same discipline, no orphan reuse ---
	releaseB := holdOneRequest("hold-b")
	releaseDeployA := flip("team-a", 3)
	releaseB()
	releaseDeployA()
	g.waitForStats(t, 15*time.Second, "the team-a replacement to admit", func(d *statsDoc) bool {
		r, ok := lifecycleOf(d, "vercel", "v-rel")
		return ok && r.State == "ready" && r.Generation == 3
	})
	if n := fake.deployCountIn("vercel", "team-a", "v-rel"); n != 1 {
		t.Fatalf("deploys in team-a = %d, want exactly 1 fresh deploy for the flip back", n)
	}
	if n := fake.deployCountIn("vercel", "team-b", "v-rel") + fake.deployCountIn("vercel", "team-a", "v-rel"); n != 2 {
		t.Fatalf("total flip deploys = %d, want 2 (one fresh deployment per flip)", n)
	}
	if !fake.projectExistsIn("vercel", "team-b", "v-rel") ||
		fake.deleteCountIn("vercel", "team-b", "v-rel") != 0 {
		t.Fatalf("team-b after the flip back: exists=%v deletes=%d, want the orphan still present, never deleted",
			fake.projectExistsIn("vercel", "team-b", "v-rel"), fake.deleteCountIn("vercel", "team-b", "v-rel"))
	}
	markerServedBy("post-a", "team-a")

	if strings.Contains(g.logDump(), "in-flight drain timed out") {
		t.Fatal("a flip waited out the drain timeout; the barrier must see the held request answer")
	}
	g.scanLog(t, fake)
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
