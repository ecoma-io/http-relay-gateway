package reconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/store"
)

// versionServer is a stand-in live relay: it answers the version endpoint
// with whatever the test last set, can be switched off (a transport failure,
// as far as the prober is concerned), and counts who came asking.
type versionServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	ver  string
	dn   bool
	hits int
}

func newVersionServer(version string) *versionServer {
	vs := &versionServer{ver: version}
	mux := http.NewServeMux()
	mux.HandleFunc("/__relay/version", func(w http.ResponseWriter, _ *http.Request) {
		vs.mu.Lock()
		vs.hits++
		down := vs.dn
		vs.mu.Unlock()
		if down {
			panic(http.ErrAbortHandler) // the connection dies before any byte
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"version":%q}`, vs.ver)
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

func (v *versionServer) hitCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.hits
}

// fakeClient is the deploy.Client the tests hand the worker; it counts
// deploys and returns a canned result or failure.
type fakeClient struct {
	mu         sync.Mutex
	platform   string
	deploys    int
	lastSpec   deploy.Spec
	failDeploy error
	url        string
}

func (f *fakeClient) Platform() string { return f.platform }

func (f *fakeClient) Verify(_ context.Context) (string, error) { return "acc-ref", nil }

func (f *fakeClient) Deploy(_ context.Context, spec deploy.Spec) (deploy.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deploys++
	f.lastSpec = spec
	if f.failDeploy != nil {
		return deploy.Result{}, f.failDeploy
	}
	return deploy.Result{Project: spec.Project, ExternalID: fmt.Sprintf("ext-%d", f.deploys), URL: f.url}, nil
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
// oldVersion from its own server.
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
	w := Start(db, fakeFactory{client: client}, logging.Nop(), func() int64 { return 0 })
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

	stale := w.probeFleet(true)
	if len(stale) != 1 || stale[0] != relayID {
		t.Fatalf("probeFleet stale = %v", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployStale || dep.AuthToken != "old-relay-token" {
		t.Fatalf("after probe: %+v, %v", dep, err)
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
	// And a follow-up probe agrees: active, no error.
	w.probeFleet(false)
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

	stale := w.probeFleet(true)
	if len(stale) != 0 {
		t.Fatalf("unreachable relay queued for redeploy: %v", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployUnreachable || dep.LastError == "" {
		t.Fatalf("after probe: %+v, %v", dep, err)
	}
	if client.deployCount() != 0 {
		t.Fatal("unreachable relay was redeployed")
	}
}

func TestCheckIsPassive(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, _ := seedManaged(t, w.db, "0")

	stale := w.probeFleet(false)
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

// TestProbeNeverTouchesLegacyRelays pins the probe boundary: legacy relays
// own their URLs, so no fleet pass may ever contact them — only managed
// deployments with accounts get probed.
func TestProbeNeverTouchesLegacyRelays(t *testing.T) {
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

	if stale := w.probeFleet(true); len(stale) != 0 {
		t.Fatalf("healthy fleet queued redeploys: %v", stale)
	}
	if got := quiet.hitCount(); got != 0 {
		t.Fatalf("legacy relay was probed %d times", got)
	}
	if got := managed.hitCount(); got != 1 {
		t.Fatalf("managed relay probed %d times, want exactly once", got)
	}
	row, err := w.db.Relay(managedID)
	if err != nil || row.Deployment == nil || row.Deployment.Status != store.DeployActive {
		t.Fatalf("managed relay after probe = %+v, %v", row, err)
	}
}

// TestUnreachableRecoversOnNextProbe pins the unreachable lifecycle: a probe
// transport failure marks unreachable (and never redeploys); the next pass
// that gets an answer returns the deployment to active with a clean error.
func TestUnreachableRecoversOnNextProbe(t *testing.T) {
	client := &fakeClient{platform: "vercel", url: "https://unused.example"}
	w := newIdleWorker(t, client)
	relayID, _, live := seedManaged(t, w.db, deploy.RelayVersion)

	live.setDown(true)
	if stale := w.probeFleet(false); len(stale) != 0 {
		t.Fatalf("unreachable relay queued for redeploy: %v", stale)
	}
	dep, err := w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployUnreachable || dep.LastError == "" {
		t.Fatalf("down probe = %+v, %v", dep, err)
	}

	live.setDown(false)
	w.probeFleet(false)
	dep, err = w.db.Deployment(relayID)
	if err != nil || dep.Status != store.DeployActive || dep.LastError != "" {
		t.Fatalf("recovered probe = %+v, %v; want active with the error cleared", dep, err)
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
