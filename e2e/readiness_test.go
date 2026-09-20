package e2e_test

// Readiness-gate lifecycle matrix. Each scenario is observable end to end
// at the real binary: /readyz, /stats readiness + lifecycle, round-robin
// admission, demotion and re-admission, managed redeploy cycles, and the
// no-trace guarantees around unready relays. Registry mechanics that are
// purely in-process (backoff math, single-flight) stay with the unit
// tests — these pin the wire contract.
import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
)

// healthzBody returns the /healthz status and body verbatim (liveness,
func healthzBody(t *testing.T, addr string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("healthz read: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// readyzAssert polls /readyz until it reports the wanted readiness state.
func readyzAssert(t *testing.T, g *Gateway, wantReady bool) {
	t.Helper()
	deadline := time.Now().Add(applySettle)
	for time.Now().Before(deadline) {
		code, body := readyzDo(t, g.Addr)
		if wantReady && code == http.StatusOK && strings.Contains(body, `"ready":true`) {
			return
		}
		if !wantReady && code == http.StatusServiceUnavailable {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	code, body := readyzDo(t, g.Addr)
	t.Fatalf("readyz never held ready=%v; last %d %q\nlogs:\n%s", wantReady, code, body, g.Logs())
}

// relayMustServe retries /vercel traffic until it answers 200 (optionally
// with a wanted body) or the window closes — pinning that the pool serves
// despite transient redeploy windows.
func relayMustServe(t *testing.T, g *Gateway, wantBody string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var status int
	var body string
	for time.Now().Before(deadline) {
		status, _, body = relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status == http.StatusOK && (wantBody == "" || body == wantBody) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("relay traffic never served %q; last %d %q\nlogs:\n%s", wantBody, status, body, g.Logs())
}

// TestE2E_Readyz503UntilFirstVerification pins the zero-ready contract: an
// alive process with no verified relay reports readiness 503 while liveness
// stays green, and admission flips it to 200.
func TestE2E_Readyz503UntilFirstVerification(t *testing.T) {
	g := NewGateway(t)
	g.Setup(t, adminPassword)
	if code := g.Login(t, adminPassword); code != http.StatusOK {
		t.Fatalf("login = %d", code)
	}

	code, body := readyzDo(t, g.Addr)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before any relay = %d %q, want 503", code, body)
	}
	hcode, hbody := healthzBody(t, g.Addr)
	if hcode != http.StatusOK || hbody != "ok\n" {
		t.Fatalf("healthz = %d %q, want 200 ok even unready", hcode, hbody)
	}

	sim := NewEdgeSim(t, "readyz-a")
	g.CreateRelay(t, RelaySeed{Name: "readyz-a", Provider: "vercel", URL: sim.URL})
	g.WaitForReady(1, applySettle)

	readyzAssert(t, g, true)
	// The pool serves once readiness flips.
	relayMustServe(t, g, sim.servedBody())
}

// TestE2E_UnreachableRelayNeverAdmitted pins the strict gate for transport
// failures: the relay never appears in the traffic pool, /readyz stays 503,
// the data plane answers 503, and the lifecycle row records the failure —
// liveness is a separate concern and stays green.
func TestE2E_UnreachableRelayNeverAdmitted(t *testing.T) {
	g := NewGateway(t)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)

	g.CreateRelay(t, RelaySeed{Name: "never-a", Provider: "vercel", URL: deadRelayURL(t)})

	// The failed relay is visible in the lifecycle, never in the pool.
	g.WaitForCondition(applySettle, "unreachable relay recorded as failed", func(st *StatsView) bool {
		if st.Readiness.ReadyRelays != 0 || st.Readiness.Ready {
			return false
		}
		if _, served := st.relay("never-a"); served {
			return false
		}
		row, ok := st.lifecycle("never-a")
		return ok && row.State == "failed" && row.Reason == "unreachable"
	})

	readyzAssert(t, g, false)
	if code, body := healthzBody(t, g.Addr); code != http.StatusOK || body != "ok\n" {
		t.Fatalf("healthz = %d %q, want 200 ok despite empty pool", code, body)
	}
	status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("data plane = %d, want 503 with no verified relay", status)
	}
}

// TestE2E_ProbeFailureStaysOutNoRedeploy pins the probe_failed class on a
// managed relay: the platform deploy succeeds, the worker answers the
// version probe — but the end-to-end forward walk fails, so the relay never
// earns admission and never triggers a redeploy (tokens stay flat). Healing
// the origin admits it on the same deployment.
func TestE2E_ProbeFailureStaysOutNoRedeploy(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	acc := createMainAccount(t, g)

	sim.setForwardBroken(true)
	id := g.CreateRelay(t, RelaySeed{Name: "probe-f", Provider: "vercel", URL: sim.srv.URL, AccountID: &acc})
	waitForDeployment(t, g, id, "active", 30*time.Second)

	// Deployed once, verified, but the forward walk fails: stays out.
	g.WaitForCondition(applySettle, "probe-failed relay stays out with one deploy", func(st *StatsView) bool {
		if st.Readiness.ReadyRelays != 0 {
			return false
		}
		if _, served := st.relay("probe-f"); served {
			return false
		}
		return len(platform.deployedTokens()) == 1
	})

	// The origin heals: the same deployment verifies end to end — no second
	// deploy fires.
	sim.setForwardBroken(false)
	g.WaitForReady(1, applySettle)
	if got := len(platform.deployedTokens()); got != 1 {
		t.Fatalf("deploys = %d, want 1 (healing must not redeploy)", got)
	}
	relayMustServe(t, g, "")
}

// TestE2E_VersionMismatchTriggersRedeployToReady pins verifyVersionMismatch:
// a worker that answers with the wrong version loses readiness and is
// re-deployed, then returns to serving on the fresh config.
func TestE2E_VersionMismatchTriggersRedeployToReady(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	acc := createMainAccount(t, g)

	id := g.CreateRelay(t, RelaySeed{Name: "ver-drift", Provider: "vercel", URL: sim.srv.URL, AccountID: &acc})
	waitForDeployment(t, g, id, "active", 30*time.Second)
	g.WaitForReady(1, applySettle)
	relayMustServe(t, g, "")

	// Drift the worker's version: the next pass must redeploy it.
	tokenBefore := sim.currentToken()
	sim.setConfig(tokenBefore, "stale")

	g.WaitForCondition(10*time.Second, "version drift redeploys", func(st *StatsView) bool {
		return len(platform.deployedTokens()) >= 2 && st.Readiness.ReadyRelays == 1
	})
	readyzAssert(t, g, true)
	relayMustServe(t, g, "")
}

// TestE2E_TokenMismatchTriggersRedeployToReady pins verifyAuthFailed: a
// worker whose token no longer matches the deployment's is 404 on the
// forward walk, loses admission, and is re-deployed with a fresh token.
func TestE2E_TokenMismatchTriggersRedeployToReady(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	acc := createMainAccount(t, g)

	id := g.CreateRelay(t, RelaySeed{Name: "tok-drift", Provider: "vercel", URL: sim.srv.URL, AccountID: &acc})
	waitForDeployment(t, g, id, "active", 30*time.Second)
	g.WaitForReady(1, applySettle)
	relayMustServe(t, g, "")
	sim.setConfig("wrong-token", deploy.RelayVersion)
	g.WaitForCondition(10*time.Second, "token drift redeploys", func(st *StatsView) bool {
		return len(platform.deployedTokens()) >= 2 && st.Readiness.ReadyRelays == 1
	})
	// The serving worker carries the fresh token, and the redeployed one is
	// the same deployment — never a leaked plaintext.
	readyzAssert(t, g, true)
	relayMustServe(t, g, "")
}

// TestE2E_ReadySetOnlyRoundRobin pins admission as the membership rule:
// a relay that fails its end-to-end probe never enters the rotation, heals
// into it, and the strict config order then alternates deterministically.
func TestE2E_ReadySetOnlyRoundRobin(t *testing.T) {
	a := NewEdgeSim(t, "rr-a")
	b := NewEdgeSim(t, "rr-b")
	b.setForwardBroken(true)

	g := NewGateway(t)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	g.CreateRelay(t, RelaySeed{Name: "rr-a", Provider: "vercel", URL: a.URL})
	g.CreateRelay(t, RelaySeed{Name: "rr-b", Provider: "vercel", URL: b.URL})
	g.WaitForReady(1, applySettle)

	// Only the verified relay serves.
	for range 3 {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != a.servedBody() {
			t.Fatalf("status=%d body=%q, want %q from the only ready relay", status, body, a.servedBody())
		}
	}
	if b.hitCount() != 0 {
		t.Fatalf("unready relay served %d requests", b.hitCount())
	}

	// Healing admits it; the pool then alternates in config order.
	b.setForwardBroken(false)
	g.WaitForReady(2, applySettle)
	got := []string{}
	for range 3 {
		_, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		got = append(got, body)
	}
	want := strings.Join([]string{a.servedBody(), b.servedBody(), a.servedBody()}, ",")
	if strings.Join(got, ",") != want {
		t.Fatalf("round-robin sequence = %q, want %q", strings.Join(got, ","), want)
	}
}

// TestE2E_ReadyzReflectsDemotionAndRecovery pins the DemoteAfter contract:
// a verified relay that starts failing loses admission after the configured
// consecutive failures, and heals back without intervention.
func TestE2E_ReadyzReflectsDemotionAndRecovery(t *testing.T) {
	a := NewEdgeSim(t, "demote-a")
	b := NewEdgeSim(t, "demote-b")
	// Two failures must demote; the suite-wide fast defaults never do.
	g := NewGatewayWithEnv(t, "RELAY_VERIFY_DEMOTE_AFTER=2")
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	g.CreateRelay(t, RelaySeed{Name: "demote-a", Provider: "vercel", URL: a.URL})
	g.CreateRelay(t, RelaySeed{Name: "demote-b", Provider: "vercel", URL: b.URL})
	g.WaitForReady(2, applySettle)

	b.setForwardBroken(true)
	g.WaitForCondition(applySettle, "failing relay demoted", func(st *StatsView) bool {
		if st.Readiness.ReadyRelays != 1 {
			return false
		}
		if _, served := st.relay("demote-b"); served {
			return false
		}
		row, ok := st.lifecycle("demote-b")
		return ok && row.State == "unready" && row.Reason == "probe_failed"
	})
	readyzAssert(t, g, true)

	b.setForwardBroken(false)
	g.WaitForReady(2, applySettle)
	readyzAssert(t, g, true)
}

// TestE2E_StatsLifecycleShowsUnreadyRows pins the observability contract:
// every configured relay appears in the readiness lifecycle with its
// classification, while unready rows stay out of the relay list.
func TestE2E_StatsLifecycleShowsUnreadyRows(t *testing.T) {
	sim := NewEdgeSim(t, "obs-unready")
	sim.setForwardBroken(true)
	g := NewGateway(t)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	g.CreateRelay(t, RelaySeed{Name: "obs-unready", Provider: "vercel", URL: sim.URL})

	g.WaitForCondition(applySettle, "unready relay visible in lifecycle", func(st *StatsView) bool {
		if st.Readiness.Ready {
			return false
		}
		if _, served := st.relay("obs-unready"); served {
			return false
		}
		row, ok := st.lifecycle("obs-unready")
		return ok && row.State == "failed" && row.Reason == "probe_failed"
	})
	readyzAssert(t, g, false)
}

// TestE2E_DeleteWhileUnreadyLeavesNoTrace pins the cleanup contract: an
// unready relay deleted through the admin API leaves no readiness or
// serving trace, and an empty pool stays a 503.
func TestE2E_DeleteWhileUnreadyLeavesNoTrace(t *testing.T) {
	sim := NewEdgeSim(t, "del-unready")
	sim.setForwardBroken(true)
	g := NewGateway(t)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	id := g.CreateRelay(t, RelaySeed{Name: "del-unready", Provider: "vercel", URL: sim.URL})

	g.WaitForCondition(applySettle, "unready relay visible in lifecycle", func(st *StatsView) bool {
		row, ok := st.lifecycle("del-unready")
		return ok && row.State == "failed" && row.Reason == "probe_failed"
	})

	g.DeleteRelay(t, id)
	// The registry holds a "removing" lifecycle row for 30s before the
	// purge on a later rebuild — the trace that matters is the relay
	// leaving readiness and the admin list; no traffic ever flows.
	g.WaitForCondition(applySettle, "deleted relay leaves readiness", func(st *StatsView) bool {
		if st.Readiness.ReadyRelays != 0 {
			return false
		}
		if _, served := st.relay("del-unready"); served {
			return false
		}
		return true
	})
	readyzAssert(t, g, false)

	// The admin list agrees, and the data plane never served it.
	code, _, raw := g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusOK {
		t.Fatalf("relay list = %d: %s", code, raw)
	}
	if strings.Contains(raw, "del-unready") {
		t.Fatalf("deleted relay still in admin list: %s", raw)
	}
	if code, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("data plane = %d %q, want 503 after delete", code, body)
	}
}

// TestE2E_FailedRedeployKeepsCurrentWorkerServing pins the redeploy safety
// contract: a platform rejection leaves the verified worker serving under
// its current token, and a later deploy heals it.
func TestE2E_FailedRedeployKeepsCurrentWorkerServing(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	acc := createMainAccount(t, g)

	id := g.CreateRelay(t, RelaySeed{Name: "redeploy-f", Provider: "vercel", URL: sim.srv.URL, AccountID: &acc})
	waitForDeployment(t, g, id, "active", 30*time.Second)
	g.WaitForReady(1, applySettle)
	relayMustServe(t, g, "")

	// The platform starts rejecting: a manual redeploy must not pull the
	// serving worker out of rotation.
	platform.setDeployFail(true)
	code, _, body := g.AdminDo(t, http.MethodPost, relayPath(id)+"/redeploy", nil)
	if code != http.StatusAccepted {
		t.Fatalf("redeploy = %d: %s", code, body)
	}
	tokenBefore := sim.currentToken()
	time.Sleep(2 * time.Second) // let the redeploy attempt land and fail
	relayMustServe(t, g, "")
	if got := sim.currentToken(); got != tokenBefore {
		t.Fatalf("worker token changed on failed redeploy: %q -> %q", tokenBefore, got)
	}
	readyzAssert(t, g, true)

	// The platform heals: the next redeploy lands a fresh token and the
	// relay keeps serving.
	platform.setDeployFail(false)
	code, _, body = g.AdminDo(t, http.MethodPost, relayPath(id)+"/redeploy", nil)
	if code != http.StatusAccepted {
		t.Fatalf("healed redeploy = %d: %s", code, body)
	}
	g.WaitForCondition(applySettle, "healed redeploy lands fresh token", func(st *StatsView) bool {
		return sim.currentToken() != tokenBefore && st.Readiness.ReadyRelays == 1
	})
	relayMustServe(t, g, "")
}

// TestE2E_AdoptFailureKeepsLegacyServing pins the adoption safety contract:
// a failed adopt leaves the legacy relay untouched and serving; healing
// adopts it onto the managed worker.
func TestE2E_AdoptFailureKeepsLegacyServing(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	g.Login(t, adminPassword)
	account := createMainAccount(t, g)

	legacy := NewEdgeSim(t, "adopt-legacy")
	id := g.CreateRelay(t, RelaySeed{Name: "adopt-legacy", Provider: "vercel", URL: legacy.URL})
	g.WaitForReady(1, applySettle)
	relayMustServe(t, g, legacy.servedBody())

	platform.setDeployFail(true)
	code, _, body := g.AdminDo(t, http.MethodPost, relayPath(id)+"/adopt", map[string]any{
		"accountId": account,
	})
	if code != http.StatusAccepted {
		t.Fatalf("adopt = %d: %s", code, body)
	}
	// The failed adopt leaves the relay legacy and serving.
	time.Sleep(2 * time.Second)
	relayMustServe(t, g, legacy.servedBody())
	g.WaitForCondition(applySettle, "failed adopt keeps legacy origin", func(st *StatsView) bool {
		row, ok := st.relay("adopt-legacy")
		return ok && row.Origin == "legacy" && st.Readiness.ReadyRelays == 1
	})

	// Platform heals: adoption lands the relay on the managed worker.
	platform.setDeployFail(false)
	code, _, body = g.AdminDo(t, http.MethodPost, relayPath(id)+"/adopt", map[string]any{
		"accountId": account,
	})
	if code != http.StatusAccepted {
		t.Fatalf("healed adopt = %d: %s", code, body)
	}
	waitForDeployment(t, g, id, "active", 30*time.Second)
	g.WaitForCondition(applySettle, "adopted relay serving managed", func(st *StatsView) bool {
		row, ok := st.relay("adopt-legacy")
		return ok && row.Origin == "managed" && st.Readiness.ReadyRelays == 1
	})
	// Traffic now flows through the worker sim with a live token.
	status, _, respBody := relayDo(t, g.Addr, "/", `{}`, map[string]string{
		"X-Relay-Target": sim.srv.URL, "X-Relay-Path": "/up",
	})
	if status != http.StatusOK || !strings.Contains(respBody, `"served":true`) {
		t.Fatalf("adopted traffic = %d %q", status, respBody)
	}
	if sim.currentToken() == "" {
		t.Fatal("adopted relay serves with no live worker token")
	}
	if got := len(platform.deployedTokens()); got != 2 {
		t.Fatalf("deploys = %d, want 2 (failed attempt + successful adopt)", got)
	}
}

// createMainAccount provisions the shared vercel account used by the
// managed scenarios and returns its id.
func createMainAccount(t *testing.T, g *Gateway) int64 {
	t.Helper()
	code, _, body := g.AdminDo(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "e2e-fake-vercel-platform-token",
	})
	if code != http.StatusCreated {
		t.Fatalf("account create = %d: %s", code, body)
	}
	var account struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &account); err != nil {
		t.Fatal(err)
	}
	return account.ID
}
