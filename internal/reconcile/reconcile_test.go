package reconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/readiness"
	"http-relay-gateway/internal/store"
)

// versionServer is a stand-in live relay: it answers the version endpoint
// with whatever the test last set, can be switched off (a transport failure,
// as far as the prober is concerned), can answer like a platform-paused
// deployment instead of a worker, and counts who came asking. It also stands
// in for the worker's relay-spec ingress: a request carrying X-Relay-Target
// is the outer leg of an end-to-end forward probe, which the real worker
// answers by checking the token and then fetching back into its own version
// route — this fake produces the same observable answers (404 wrong token,
// 502 broken fetch, version JSON round trip).
type versionServer struct {
	srv         *httptest.Server
	mu          sync.Mutex
	ver         string
	dn          bool
	paused      bool
	token       string // non-empty: relay-spec forward probes must carry it
	fwdBroken   bool   // worker answers but its own fetch fails: 502
	hits        int
	forwardHits int
	versionHits int
}

func newVersionServer(version string) *versionServer {
	vs := &versionServer{ver: version}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		vs.mu.Lock()
		vs.hits++
		down, paused, ver, wantToken, broken := vs.dn, vs.paused, vs.ver, vs.token, vs.fwdBroken
		outer := r.Header.Get("X-Relay-Target") != ""
		if outer {
			vs.forwardHits++
		} else if r.URL.Path == "/__relay/version" {
			vs.versionHits++
		}
		vs.mu.Unlock()
		if down {
			panic(http.ErrAbortHandler) // the connection dies before any byte
		}
		if paused {
			// The shape a quota-suspended Vercel deployment answers with:
			// the platform's page, never the worker's JSON.
			w.Header().Set("X-Vercel-Error", "DEPLOYMENT_DISABLED")
			w.WriteHeader(http.StatusPaymentRequired)
			_, _ = w.Write([]byte("Payment required\n\nDEPLOYMENT_DISABLED\n\n"))
			return
		}
		if outer {
			// Outer leg of a relay-spec forward: token check, then the
			// forwarding fetch back into the worker's own version route.
			if wantToken != "" && r.Header.Get("X-Relay-Token") != wantToken {
				w.WriteHeader(http.StatusNotFound) // the worker's key rejection
				return
			}
			if broken {
				w.WriteHeader(http.StatusBadGateway) // the worker's fetch failed
				_, _ = w.Write([]byte("Fetch to the relay target failed"))
				return
			}
			// The forwarded inner request lands on the version route, which
			// answers unauthenticated — a completed round trip.
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"version":%q}`, ver)
			return
		}
		if r.URL.Path == "/__relay/version" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"version":%q}`, ver)
			return
		}
		http.NotFound(w, r)
	})
	vs.srv = httptest.NewServer(mux)
	return vs
}

func (v *versionServer) set(version string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.ver = version
}

func (v *versionServer) setDown(down bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.dn = down
}

func (v *versionServer) setPaused(paused bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.paused = paused
}

func (v *versionServer) setToken(token string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.token = token
}

func (v *versionServer) setForwardBroken(broken bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.fwdBroken = broken
}

func (v *versionServer) hitCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.hits
}

// fakeClient is the deploy.Client the tests hand the worker; it counts
// deploys and returns a canned result or failure. A non-nil deployGate
// blocks every deploy until closed — the single-flight tests hold a deploy
// open to prove a second request refuses to start.
type fakeClient struct {
	mu         sync.Mutex
	platform   string
	deploys    int
	lastSpec   deploy.Spec
	failDeploy error
	url        string
	deployGate chan struct{}
}

func (f *fakeClient) Platform() string { return f.platform }

func (f *fakeClient) Verify(_ context.Context) (string, error) { return "acc-ref", nil }

func (f *fakeClient) Deploy(_ context.Context, spec deploy.Spec) (deploy.Result, error) {
	f.mu.Lock()
	f.deploys++
	f.lastSpec = spec
	fail, url, gate := f.failDeploy, f.url, f.deployGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if fail != nil {
		return deploy.Result{}, fail
	}
	return deploy.Result{Project: spec.Project, ExternalID: fmt.Sprintf("ext-%d", f.deploys), URL: url}, nil
}

func (f *fakeClient) Delete(_ context.Context, _ string) error { return nil }

func (f *fakeClient) deployCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deploys
}

type fakeFactory struct{ client deploy.Client }

func (f fakeFactory) For(_, _, _ string) (deploy.Client, error) { return f.client, nil }

// seedManaged plants one managed relay with one active deployment answering
// oldVersion from its own server. The server expects the deployment's token,
// so the end-to-end forward probe passes for relays that verify current.
func seedManaged(t *testing.T, db *store.Store, oldVersion string) (int64, int64, *versionServer) {
	t.Helper()
	accountID, err := db.CreateAccount("main", "vercel", "platform-token", "acc-ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	relayID, err := db.CreateRelay("web-relay", "vercel", "https://own.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay: %v", err)
	}
	old := newVersionServer(oldVersion)
	old.setToken("old-relay-token")
	t.Cleanup(old.srv.Close)
	if err := db.UpsertDeployment(store.DeploymentRow{
		RelayID: relayID, AccountID: accountID, Platform: "vercel", Project: "web-relay",
		URL: old.srv.URL, Version: oldVersion, AuthToken: "old-relay-token",
		Status: store.DeployActive, LastCheckedAt: 1, DeployedAt: 1,
	}); err != nil {
		t.Fatalf("UpsertDeployment: %v", err)
	}
	return relayID, accountID, old
}

// newIdleWorker builds a worker without launching the background loop: the
// tests drive passes and redeploys synchronously, so no startup batch can
// interleave with their assertions (a racing loop turned
// TestStartupRedeploysDrift into a two-deploy flake in CI). The loop
// mechanics themselves — wake, drain, orderly stop — are covered in
// TestLoopRunsQueuedJobs and by the e2e fleet endpoints.
func newIdleWorker(t *testing.T, client deploy.Client) *Worker {
	return newIdleWorkerReg(t, client, readiness.New(readiness.Config{}))
}

// newIdleWorkerReg is newIdleWorker with a tuned readiness registry — tests
// that need immediate re-verification or a shorter demote threshold drive
// backoff and demotion deterministically instead of waiting out the defaults.
func newIdleWorkerReg(t *testing.T, client deploy.Client, reg *readiness.Registry) *Worker {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Worker{
		db:        db,
		factory:   fakeFactory{client: client},
		probeHTTP: &http.Client{Timeout: 10 * time.Second},
		log:       logging.Nop(),
		queue:     newQueue(),
		interval:  func() int64 { return 0 },
		reg:       reg,
		platformLocks: map[string]*sync.Mutex{
			deploy.PlatformVercel:     {},
			deploy.PlatformCloudflare: {},
			deploy.PlatformDeno:       {},
		},
		ctx:    context.Background(),
		cancel: func() {},
		done:   make(chan struct{}),
	}
}

// newLoopWorker is a Start-launched worker — a live loop, stopped on
// cleanup. Only the loop-behavior tests use it.
func newLoopWorker(t *testing.T, client deploy.Client) *Worker {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	w := Start(db, fakeFactory{client: client}, logging.Nop(), func() int64 { return 0 }, readiness.New(readiness.Config{}))
	t.Cleanup(w.Stop)
	return w
}

// newRevivingLoopWorker is a Start-launched worker whose revival scan runs
// at test-scale cadence instead of the production ten minutes.
func newRevivingLoopWorker(t *testing.T, client deploy.Client, reviveEvery time.Duration) *Worker {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	w := startWorker(db, fakeFactory{client: client}, logging.Nop(), func() int64 { return 0 }, readiness.New(readiness.Config{}), reviveEvery, 0)
	t.Cleanup(w.Stop)
	return w
}

func TestStartupRedeploysDrift(t *testing.T) {
	live := newVersionServer(deploy.RelayVersion)
	t.Cleanup(live.srv.Close)
	client := &fakeClient{platform: "vercel", url: live.srv.URL}
	w := newIdleWorker(t, client)
	relayID, accountID, old := seedManaged(t, w.db, "0")
	old.set("0")

	stale := w.verifyFleet(true)
	if len(stale) != 1 || stale[0] != relayID {
		t.Fatalf("verifyFleet stale = %v", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployStale || dep.AuthToken != "old-relay-token" {
		t.Fatalf("after verify: %+v, %v", dep, err)
	}

	w.redeployRelay(relayID)
	if got := client.deployCount(); got != 1 {
		t.Fatalf("deploys = %d", got)
	}
	dep, err = w.db.Deployment(relayID)
	if err != nil {
		t.Fatalf("Deployment: %v", err)
	}
	if dep.Status != store.DeployActive || dep.URL != live.srv.URL || dep.Version != deploy.RelayVersion {
		t.Fatalf("after redeploy: %+v", dep)
	}
	if dep.AuthToken == "old-relay-token" || len(dep.AuthToken) != 43 {
		t.Fatalf("token not rotated: %q", dep.AuthToken)
	}
	if dep.AccountID != accountID || dep.Project != "web-relay" {
		t.Fatalf("identity lost: %+v", dep)
	}
	// The pool must now see the relay serving through the new deployment.
	rows, err := w.db.Relays()
	if err != nil || len(rows) != 1 || rows[0].Deployment == nil || rows[0].Deployment.URL != live.srv.URL {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
	// After the redeploy the registry admits the relay, and a follow-up
	// verify agrees: active, no error.
	if !w.reg.IsReady(readiness.Key{Provider: "vercel", Name: "web-relay"}) {
		t.Fatal("redeployed relay not admitted to the pool")
	}
	w.verifyFleet(false)
	dep, _ = w.db.Deployment(relayID)
	if dep.Status != store.DeployActive || dep.LastError != "" {
		t.Fatalf("post-check: %+v", dep)
	}
}

func TestStartupNeverRedeploysUnreachable(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, old := seedManaged(t, w.db, deploy.RelayVersion)
	old.srv.Close() // the relay stopped answering

	stale := w.verifyFleet(true)
	if len(stale) != 0 {
		t.Fatalf("unreachable relay queued for redeploy: %v", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployUnreachable || dep.LastError == "" {
		t.Fatalf("after verify: %+v, %v", dep, err)
	}
	if client.deployCount() != 0 {
		t.Fatal("unreachable relay was redeployed")
	}
}

func TestCheckIsPassive(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, _ := seedManaged(t, w.db, "0")

	stale := w.verifyFleet(false)
	if len(stale) != 0 {
		t.Fatalf("passive check returned redeploys: %v", stale)
	}
	dep, _ := w.db.Deployment(relayID)
	if dep.Status != store.DeployStale {
		t.Fatalf("status = %q", dep.Status)
	}
	if client.deployCount() != 0 {
		t.Fatal("passive check deployed")
	}
}

func TestRedeployFailureKeepsActiveServing(t *testing.T) {
	client := &fakeClient{platform: "vercel", failDeploy: errors.New("platform exploded")}
	w := newIdleWorker(t, client)
	relayID, _, _ := seedManaged(t, w.db, deploy.RelayVersion)

	w.redeployRelay(relayID)
	dep, err := w.db.Deployment(relayID)
	if err != nil {
		t.Fatalf("Deployment: %v", err)
	}
	if dep.Status != store.DeployActive || dep.URL == "" || dep.AuthToken != "old-relay-token" {
		t.Fatalf("failed redeploy disturbed the serving row: %+v", dep)
	}
	if dep.LastError == "" {
		t.Fatal("failure not recorded")
	}
	// A still-serving relay stays in the pool.
	rows, err := w.db.Relays()
	if err != nil || len(rows) != 1 || rows[0].Deployment == nil {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
}

func TestFirstDeployFailureRecordsError(t *testing.T) {
	client := &fakeClient{platform: "vercel", failDeploy: errors.New("quota exhausted")}
	w := newIdleWorker(t, client)
	accountID, err := w.db.CreateAccount("main", "vercel", "platform-token", "acc-ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	relayID, err := w.db.CreateRelay("fresh-relay", "vercel", "https://own.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay: %v", err)
	}

	w.redeployRelay(relayID)
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployError || dep.LastError == "" || dep.URL != "" {
		t.Fatalf("first-deploy failure row = %+v, %v", dep, err)
	}
	// An error row keeps the relay out of the pool.
	rows, err := w.db.Relays()
	if err != nil || len(rows) != 1 || rows[0].Deployment != nil {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
}

func TestAdoptHappyAndMismatch(t *testing.T) {
	live := newVersionServer(deploy.RelayVersion)
	t.Cleanup(live.srv.Close)
	client := &fakeClient{platform: "vercel", url: live.srv.URL}
	w := newIdleWorker(t, client)
	if _, err := w.db.SyncLegacy(
		store.RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000},
		nil,
		[]store.LegacyRelay{
			{Name: "imported", Provider: "vercel", URL: "https://public.example", Active: true},
			{Name: "wrong-pin", Provider: "deno", URL: "https://other.example", Active: true},
		},
	); err != nil {
		t.Fatalf("SyncLegacy: %v", err)
	}
	rows, err := w.db.Relays()
	if err != nil || len(rows) != 2 {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
	byName := map[string]int64{}
	for _, row := range rows {
		byName[row.Name] = row.ID
	}
	accountID, err := w.db.CreateAccount("main", "vercel", "platform-token", "acc-ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	// Platform mismatch is refused without a deploy call.
	w.adoptRelay(byName["wrong-pin"], accountID)
	if client.deployCount() != 0 {
		t.Fatal("mismatched adopt deployed")
	}

	w.adoptRelay(byName["imported"], accountID)
	if got := client.deployCount(); got != 1 {
		t.Fatalf("deploys = %d", got)
	}
	row, err := w.db.Relay(byName["imported"])
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if row.Origin != store.OriginManaged || row.AccountID == nil || row.Deployment == nil {
		t.Fatalf("adopted row = %+v", row)
	}
	if row.Deployment.URL != live.srv.URL || row.Deployment.Status != store.DeployActive {
		t.Fatalf("deployment = %+v", row.Deployment)
	}
	if client.lastSpec.Project != "imported" {
		t.Fatalf("project = %q", client.lastSpec.Project)
	}
	// A successful adoption admits the relay to the pool.
	if !w.reg.IsReady(readiness.Key{Provider: "vercel", Name: "imported"}) {
		t.Fatal("adopted relay not admitted")
	}
}

// TestAdoptFailureLeavesRelayAdoptable pins the adoption failure contract:
// a failed deploy leaves the relay exactly as it was — legacy, own URL, no
// deployment — and the same adoption succeeds once the platform recovers.
func TestAdoptFailureLeavesRelayAdoptable(t *testing.T) {
	live := newVersionServer(deploy.RelayVersion)
	t.Cleanup(live.srv.Close)
	client := &fakeClient{platform: "vercel", url: live.srv.URL, failDeploy: errors.New("quota exhausted")}
	w := newIdleWorker(t, client)
	if _, err := w.db.SyncLegacy(
		store.RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000},
		nil,
		[]store.LegacyRelay{{Name: "imported", Provider: "vercel", URL: "https://public.example", Active: true}},
	); err != nil {
		t.Fatalf("SyncLegacy: %v", err)
	}
	rows, err := w.db.Relays()
	if err != nil || len(rows) != 1 {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
	legacyID := rows[0].ID
	accountID, err := w.db.CreateAccount("main", "vercel", "platform-token", "acc-ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	w.adoptRelay(legacyID, accountID)
	row, err := w.db.Relay(legacyID)
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if row.Origin != store.OriginLegacy || row.AccountID != nil || row.Deployment != nil {
		t.Fatalf("failed adopt disturbed the relay: %+v", row)
	}

	// The platform recovers; the retry manages the relay.
	client.failDeploy = nil
	w.adoptRelay(legacyID, accountID)
	row, err = w.db.Relay(legacyID)
	if err != nil {
		t.Fatalf("Relay after retry: %v", err)
	}
	if row.Origin != store.OriginManaged || row.AccountID == nil || row.Deployment == nil ||
		row.Deployment.URL != live.srv.URL {
		t.Fatalf("retried adopt left = %+v", row)
	}
}

// TestLegacyProbedForwardOnly pins the readiness boundary for legacy relays:
// they own their URLs, so a fleet pass may never version-probe or redeploy
// them — but their readiness IS proven, by a relay-spec request forwarded
// back through their own URL and answered below 500. Managed deployments get
// the version probe and the forward leg.
func TestLegacyProbedForwardOnly(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	quiet := newVersionServer(deploy.RelayVersion)
	t.Cleanup(quiet.srv.Close)
	if _, err := w.db.SyncLegacy(
		store.RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000},
		nil,
		[]store.LegacyRelay{{Name: "imported", Provider: "vercel", URL: quiet.srv.URL, Active: true}},
	); err != nil {
		t.Fatalf("SyncLegacy: %v", err)
	}
	managedID, _, managed := seedManaged(t, w.db, deploy.RelayVersion)

	if stale := w.verifyFleet(true); len(stale) != 0 {
		t.Fatalf("healthy fleet queued redeploys: %v", stale)
	}
	if got := quiet.forwardHits; got != 1 {
		t.Fatalf("legacy forward probes = %d, want exactly one", got)
	}
	if got := quiet.versionHits; got != 0 {
		t.Fatalf("legacy was version-probed %d times; only forward probes apply", got)
	}
	if client.deployCount() != 0 {
		t.Fatal("legacy relay was redeployed")
	}
	// The forward round trip admits the legacy relay to the pool.
	if !w.reg.IsReady(readiness.Key{Provider: "vercel", Name: "imported"}) {
		t.Fatal("legacy relay with a passing forward probe not admitted")
	}
	if got := managed.hitCount(); got != 2 {
		t.Fatalf("managed relay hit %d times, want version probe + forward probe", got)
	}
	row, err := w.db.Relay(managedID)
	if err != nil || row.Deployment == nil || row.Deployment.Status != store.DeployActive {
		t.Fatalf("managed relay after verify = %+v, %v", row, err)
	}
}

// TestManagedNotAdmittedBeforeVerification pins the gate: a configured
// managed relay serves nothing until a fleet pass proves it end to end.
func TestManagedNotAdmittedBeforeVerification(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	seedManaged(t, w.db, deploy.RelayVersion)
	key := readiness.Key{Provider: "vercel", Name: "web-relay"}

	if w.reg.IsReady(key) || w.reg.ReadyCount() != 0 {
		t.Fatal("unverified relay must not hold admission")
	}

	// First healthy pass admits it; a re-verify agrees without churn.
	w.verifyFleet(false)
	if !w.reg.IsReady(key) || w.reg.ReadyCount() != 1 {
		t.Fatalf("healthy relay not admitted: ready=%v count=%d", w.reg.IsReady(key), w.reg.ReadyCount())
	}
	w.verifyFleet(false)
	if !w.reg.IsReady(key) {
		t.Fatal("re-verified relay lost admission")
	}
}

// TestLegacyGateClosedUntilProbePasses pins the same gate for legacy rows: a
// relay whose forward probe does not round-trip stays out; one that answers
// is admitted — recovery on the next scan, no operator action.
func TestLegacyGateClosedUntilProbePasses(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorkerReg(t, client, readiness.New(readiness.Config{BackoffBase: time.Millisecond}))
	legacy := newVersionServer(deploy.RelayVersion)
	t.Cleanup(legacy.srv.Close)
	if _, err := w.db.SyncLegacy(
		store.RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000},
		nil,
		[]store.LegacyRelay{{Name: "imported", Provider: "vercel", URL: legacy.srv.URL, Active: true}},
	); err != nil {
		t.Fatalf("SyncLegacy: %v", err)
	}
	key := readiness.Key{Provider: "vercel", Name: "imported"}

	legacy.srv.Close() // the relay dropped off
	w.verifyFleet(false)
	if w.reg.IsReady(key) {
		t.Fatal("unreachable legacy relay admitted")
	}
	rec, ok := w.reg.StateOf(key)
	if !ok || rec.State != readiness.StateFailed {
		t.Fatalf("unreachable legacy record = %+v, %v; want failed", rec, ok)
	}

	// A fresh server stands where the relay was; the next scan admits it.
	up := newVersionServer(deploy.RelayVersion)
	t.Cleanup(up.srv.Close)
	if err := updateRelayURL(w.db, "imported", up.srv.URL); err != nil {
		t.Fatalf("update relay URL: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	w.verifyFleet(false)
	if !w.reg.IsReady(key) {
		t.Fatal("legacy relay with a passing forward probe not admitted")
	}
}

func TestUnreachableRecoversOnNextVerify(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorkerReg(t, client, readiness.New(readiness.Config{BackoffBase: time.Millisecond}))
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	live.setDown(true)
	if stale := w.verifyFleet(false); len(stale) != 0 {
		t.Fatalf("unreachable relay queued for redeploy: %v", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployUnreachable || dep.LastError == "" {
		t.Fatalf("down verify = %+v, %v", dep, err)
	}

	time.Sleep(2 * time.Millisecond) // let the failure backoff elapse
	live.setDown(false)
	w.verifyFleet(false)
	dep, err = w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployActive || dep.LastError != "" {
		t.Fatalf("recovered verify = %+v, %v; want active with the error cleared", dep, err)
	}
}

// TestBackoffGatesRepeatedFailingVerifies pins the bounded retry: a relay
// that just failed is not re-probed until its backoff elapses, so a broken
// relay is never hot-looped by the always-on verify tick.
func TestBackoffGatesRepeatedFailingVerifies(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorkerReg(t, client, readiness.New(readiness.Config{BackoffBase: time.Hour}))
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)
	live.setDown(true)

	w.verifyFleet(false)
	hits := live.hitCount()
	if hits != 1 {
		t.Fatalf("first failing verify = %d hits, want 1", hits)
	}
	w.verifyFleet(true) // an hour of backoff: the gate must skip the relay
	if got := live.hitCount(); got != hits {
		t.Fatalf("gated relay probed again: %d hits", got)
	}
	if dep, _ := w.db.Deployment(relayID); client.deployCount() != 0 {
		t.Fatalf("gated relay redeployed: %+v", dep)
	}
}

// TestDemotedAfterConsecutiveFailures pins the demote threshold: a verified
// relay rides out transient blips (passive health is the fast layer), and
// only a run of consecutive failed verifications pulls it out of the pool.
func TestDemotedAfterConsecutiveFailures(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorkerReg(t, client, readiness.New(readiness.Config{BackoffBase: time.Millisecond, DemoteAfter: 2}))
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)
	key := readiness.Key{Provider: "vercel", Name: "web-relay"}

	w.verifyFleet(false) // baseline: admitted
	if !w.reg.IsReady(key) {
		t.Fatal("precondition: healthy relay not admitted")
	}

	live.setDown(true)
	w.verifyFleet(false) // first blip: below the threshold, still serving
	if !w.reg.IsReady(key) {
		t.Fatal("one transient failure demoted the relay")
	}
	time.Sleep(2 * time.Millisecond)
	w.verifyFleet(false) // second consecutive failure: at the threshold, out
	if w.reg.IsReady(key) || w.reg.ReadyCount() != 0 {
		t.Fatalf("relay not demoted after %d failures: ready=%v count=%d", 2, w.reg.IsReady(key), w.reg.ReadyCount())
	}
	rec, _ := w.reg.StateOf(key)
	if rec.State != readiness.StateUnready || rec.FailStreak != 2 {
		t.Fatalf("demoted record = %+v; want unready with streak 2", rec)
	}
	if dep, _ := w.db.Deployment(relayID); dep.Status != store.DeployUnreachable {
		t.Fatalf("demoted deployment status = %q", dep.Status)
	}
}

// TestAuthRejectedQueuesRedeployAndHeals pins the auth-failed lifecycle: a
// worker that rejects its relay key stays current (the URL answers) but
// loses the end-to-end check and queues a redeploy; the redeploy's fresh
// token heals it and returns it to the pool.
func TestAuthRejectedQueuesRedeployAndHeals(t *testing.T) {
	current := newVersionServer(deploy.RelayVersion)
	t.Cleanup(current.srv.Close)
	client := &fakeClient{platform: "vercel", url: current.srv.URL}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)
	key := readiness.Key{Provider: "vercel", Name: "web-relay"}

	w.verifyFleet(false) // baseline: the deployment's own server accepts it
	if !w.reg.IsReady(key) {
		t.Fatal("precondition: healthy relay not admitted")
	}

	live.setToken("tampered") // the worker now rejects the stored key
	stale := w.verifyFleet(true)
	if len(stale) != 1 || stale[0] != relayID {
		t.Fatalf("auth failure queued %v, want the relay", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployActive || !strings.Contains(dep.LastError, "key") {
		t.Fatalf("auth-failed deployment = %+v, %v; want active with a key error", dep, err)
	}

	time.Sleep(2 * time.Millisecond)
	w.run([]job{{kind: jobRedeploy, relayID: relayID}})
	dep, err = w.db.Deployment(relayID)
	if err != nil {
		t.Fatalf("Deployment: %v", err)
	}
	if dep.Status != store.DeployActive || dep.URL != current.srv.URL || dep.AuthToken == "old-relay-token" {
		t.Fatalf("healed deployment = %+v", dep)
	}
	if !w.reg.IsReady(key) {
		t.Fatal("healed relay not admitted")
	}
}

// TestSingleFlightRedeploy pins the single-flight hold: while one redeploy
// of a relay is in flight, a second request refuses to start — a concurrent
// reconcile and operator action can never launch duplicate deploys.
func TestSingleFlightRedeploy(t *testing.T) {
	live := newVersionServer(deploy.RelayVersion)
	t.Cleanup(live.srv.Close)
	client := &fakeClient{platform: "vercel", url: live.srv.URL}
	w := newIdleWorker(t, client)
	relayID, _, _ := seedManaged(t, w.db, deploy.RelayVersion)

	gate := make(chan struct{})
	client.deployGate = gate
	first := make(chan struct{})
	go func() {
		w.redeployRelay(relayID)
		close(first)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for client.deployCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first redeploy never reached the platform")
		}
		time.Sleep(time.Millisecond)
	}
	w.redeployRelay(relayID) // in flight: must refuse without deploying
	close(gate)
	<-first
	if got := client.deployCount(); got != 1 {
		t.Fatalf("single-flight violated: %d deploys for one relay", got)
	}
}

// TestProbeFailedKeepsWorkerButPullsAdmission pins the probe-failed verdict:
// the worker answers and accepts the key, but the forwarding round trip
// fails on its side — the deployment stays current (no redeploy) while the
// relay's admission is withdrawn after the demote run.
func TestProbeFailedKeepsWorkerButPullsAdmission(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	w.verifyFleet(false) // baseline: admitted
	live.setForwardBroken(true)
	stale := w.verifyFleet(true)
	if len(stale) != 0 {
		t.Fatalf("probe-failed relay queued for redeploy: %v; the worker is current", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployActive || !strings.Contains(dep.LastError, "502") {
		t.Fatalf("probe-failed deployment = %+v, %v; want active with the round-trip error", dep, err)
	}
	if client.deployCount() != 0 {
		t.Fatal("probe-failed relay was redeployed")
	}
}

func TestPausedProbeClassifiesAndWaits(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	live.setPaused(true)
	if stale := w.verifyFleet(true); len(stale) != 0 {
		t.Fatalf("paused relay queued for redeploy: %v", stale)
	}
	if got := live.hitCount(); got != 1 {
		t.Fatalf("probes = %d, want 1", got)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployPaused {
		t.Fatalf("paused verify = %+v, %v", dep, err)
	}
	if dep.LastError == "" || !strings.Contains(dep.LastError, "DEPLOYMENT_DISABLED") {
		t.Fatalf("pause reason not recorded: %q", dep.LastError)
	}

	// The revival scan owns paused relays: a fleet pass must not re-probe
	// (and risk flipping the pause to unreachable on a transport blip).
	w.verifyFleet(true)
	if got := live.hitCount(); got != 1 {
		t.Fatalf("fleet pass re-probed a paused relay: %d hits", got)
	}
	dep, _ = w.db.Deployment(relayID)
	if dep.Status != store.DeployPaused || client.deployCount() != 0 {
		t.Fatalf("paused relay disturbed: %+v deploys=%d", dep, client.deployCount())
	}
}

// TestReviveScanWaitsWhilePaused pins the waiting half of the revival scan:
// while the platform still answers instead of the worker, the deployment
// stays paused — the reason refreshed, no deploy — and nothing comes back
// for redeployment.
func TestReviveScanWaitsWhilePaused(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	live.setPaused(true)
	w.verifyFleet(false)
	dep, _ := w.db.Deployment(relayID)
	if dep.Status != store.DeployPaused {
		t.Fatalf("setup: status = %q, want paused", dep.Status)
	}

	if redeploys := w.revivePaused(); len(redeploys) != 0 {
		t.Fatalf("still-paused relay queued for redeploy: %v", redeploys)
	}
	if got := live.hitCount(); got != 2 { // classify + one revive scan
		t.Fatalf("revive scan probes = %d, want 1 more", got)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployPaused || dep.LastError == "" {
		t.Fatalf("after revive scan = %+v, %v; want still paused with its reason", dep, err)
	}
	if client.deployCount() != 0 {
		t.Fatal("revive scan deployed against a paused platform")
	}
}

// TestReviveScanRevivesPaused pins the revival half: the pause lifts, the
// worker answers the current version again, and one scan returns the
// deployment to active with a clean error — no redeploy, same token.
func TestReviveScanRevivesPaused(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	live.setPaused(true)
	w.verifyFleet(false)
	live.setPaused(false)

	if redeploys := w.revivePaused(); len(redeploys) != 0 {
		t.Fatalf("same-version revival queued a redeploy: %v", redeploys)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployActive || dep.LastError != "" {
		t.Fatalf("revived deployment = %+v, %v", dep, err)
	}
	if dep.AuthToken != "old-relay-token" {
		t.Fatalf("revival rotated the token: %q", dep.AuthToken)
	}
	if client.deployCount() != 0 {
		t.Fatal("revival deployed")
	}
	// The pool must see it again.
	rows, err := w.db.Relays()
	if err != nil || len(rows) != 1 || rows[0].Deployment == nil {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
}

// TestReviveScanRedeploysStaleAfterRevival pins the catch-up path: the
// platform lifts the pause but the worker that answers is an old version —
// the scan marks it stale and a full revive batch redeploys it to current.
func TestReviveScanRedeploysStaleAfterRevival(t *testing.T) {
	current := newVersionServer(deploy.RelayVersion)
	t.Cleanup(current.srv.Close)
	client := &fakeClient{platform: "vercel", url: current.srv.URL}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	live.setPaused(true)
	w.verifyFleet(false)
	live.setPaused(false)
	live.set("0") // revived, but the platform brought back an old deployment

	w.run([]job{{kind: jobRevive}})
	dep, err := w.db.Deployment(relayID)
	if err != nil {
		t.Fatalf("Deployment: %v", err)
	}
	if dep.Status != store.DeployActive || dep.Version != deploy.RelayVersion || dep.URL != current.srv.URL {
		t.Fatalf("after revive batch = %+v", dep)
	}
	if got := client.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want the catch-up redeploy", got)
	}
}

// TestLoopRevivesPausedAutomatically covers the always-on scanner end to
// end: a deployment paused while the loop runs — without any operator
// action, and with the reconcile interval off — comes back active once the
// platform lifts the suspension, on the scanner's own cadence.
func TestLoopRevivesPausedAutomatically(t *testing.T) {
	current := newVersionServer(deploy.RelayVersion)
	t.Cleanup(current.srv.Close)
	client := &fakeClient{platform: "vercel", url: current.srv.URL}
	w := newRevivingLoopWorker(t, client, 30*time.Millisecond)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	// The startup pass ran against a healthy relay; the pause lands after,
	// so classification goes through an explicit fleet pass — the same path
	// an operator's dashboard check or a restart's startup pass would take.
	live.setPaused(true)
	w.CheckAll()
	deadline := time.Now().Add(10 * time.Second)
	for {
		dep, err := w.db.Deployment(relayID)
		if err != nil {
			t.Fatalf("Deployment: %v", err)
		}
		if dep.Status == store.DeployPaused {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loop never classified the pause: %+v", dep)
		}
		time.Sleep(10 * time.Millisecond)
	}

	live.setPaused(false)
	deadline = time.Now().Add(10 * time.Second)
	for {
		dep, err := w.db.Deployment(relayID)
		if err != nil {
			t.Fatalf("Deployment: %v", err)
		}
		if dep.Status == store.DeployActive && dep.LastError == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loop never revived the relay: %+v", dep)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if client.deployCount() != 0 {
		t.Fatalf("revival deployed %d times; want none on a same-version revival", client.deployCount())
	}
}

// TestLoopRunsQueuedJobs covers the background loop the direct-driving
// tests deliberately avoid: an enqueued redeploy must be executed by the
// loop, land active on the current version and rotate the token. The
// seeded deployment is current, so the startup pass finds no drift and
// the explicit redeploy is the loop's only deploy source — the count is
// exact.
func TestLoopRunsQueuedJobs(t *testing.T) {
	live := newVersionServer(deploy.RelayVersion)
	t.Cleanup(live.srv.Close)
	client := &fakeClient{platform: "vercel", url: live.srv.URL}
	w := newLoopWorker(t, client)
	relayID, _, _ := seedManaged(t, w.db, deploy.RelayVersion)

	w.Redeploy(relayID)

	deadline := time.Now().Add(10 * time.Second)
	for {
		dep, err := w.db.Deployment(relayID)
		if err != nil {
			t.Fatalf("Deployment: %v", err)
		}
		if dep.Status == store.DeployActive && dep.Version == deploy.RelayVersion &&
			client.deployCount() == 1 && dep.AuthToken != "old-relay-token" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loop never completed the redeploy: %+v deploys=%d", dep, client.deployCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestQueueCoalescesAndPrioritizes(t *testing.T) {
	q := newQueue()
	q.push(job{kind: jobRedeploy, relayID: 5})
	q.push(job{kind: jobCheck})
	q.push(job{kind: jobRedeploy, relayID: 5})
	q.push(job{kind: jobAdopt, relayID: 7, accountID: 2})
	q.push(job{kind: jobReconcile})

	jobs := q.drain()
	if len(jobs) != 4 {
		t.Fatalf("drained %d jobs: %+v", len(jobs), jobs)
	}
	if jobs[0].kind != jobCheck || jobs[1].kind != jobReconcile {
		t.Fatalf("fleet passes must come first: %+v", jobs)
	}
	keys := map[string]bool{}
	for _, j := range jobs[2:] {
		keys[j.key()] = true
	}
	if len(keys) != 2 || !keys["redeploy:5"] || !keys["adopt:7:2"] {
		t.Fatalf("per-relay jobs wrong: %v", keys)
	}
	if more := q.drain(); len(more) != 0 {
		t.Fatalf("queue not empty: %+v", more)
	}
}

func TestRunWidthBoundsConcurrency(t *testing.T) {
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	const total = 20
	run(3, total, func(i int) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		mu.Lock()
		inFlight--
		mu.Unlock()
	})
	if maxInFlight > 3 {
		t.Fatalf("max in flight = %d", maxInFlight)
	}
}

// updateRelayURL rewrites a relay's own URL — the test equivalent of an
// operator editing the row.
func updateRelayURL(db *store.Store, name, url string) error {
	rows, err := db.Relays()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.Name == name {
			return db.UpdateRelay(row.ID, store.RelayPatch{URL: &url})
		}
	}
	return fmt.Errorf("relay %q not found", name)
}
