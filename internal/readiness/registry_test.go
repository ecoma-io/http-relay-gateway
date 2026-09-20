package readiness

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests drive backoff and removal deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)}
}

// newTestRegistry builds a registry with a fake clock and tiny knobs so the
// test matrix can walk every transition without waiting.
func newTestRegistry() (*Registry, *fakeClock) {
	clock := newFakeClock()
	r := New(Config{
		BackoffBase: 5 * time.Second,
		BackoffMax:  40 * time.Second,
		RecoverMax:  2 * time.Second,
		DemoteAfter: 3,
		Now:         clock.Now,
	})
	return r, clock
}

var (
	keyA = Key{Provider: "vercel", Name: "alpha"}
	keyB = Key{Provider: "cloudflare", Name: "bravo"}
)

// drain eats one pending notification, reporting whether any fired.
func drain(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestSyncAnnouncesConfigured(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	rec, ok := r.StateOf(keyA)
	if !ok || rec.State != StateConfigured {
		t.Fatalf("record = %+v ok=%v, want configured", rec, ok)
	}
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatalf("a configured relay must never serve: ready=%v count=%d",
			r.IsReady(keyA), r.ReadyCount())
	}
	if drain(r.Changes()) {
		t.Fatal("announcing a configured relay must not notify — membership did not change")
	}
}

func TestReadyAdmitsAndNotifies(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, 50*time.Millisecond)
	if !r.IsReady(keyA) || r.ReadyCount() != 1 {
		t.Fatalf("ready relay not admitted: ready=%v count=%d", r.IsReady(keyA), r.ReadyCount())
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateReady || rec.Reason != "" || rec.FailStreak != 0 || rec.LastResult != 50*time.Millisecond {
		t.Fatalf("record = %+v, want ready with streak 0 and no reason", rec)
	}
	if !drain(r.Changes()) {
		t.Fatal("first admission must notify the applier")
	}
	// A second Ready (another successful scan tick) is a no-op admission-wise
	// and must not fire a second notification for the same membership.
	r.Ready(keyA, 40*time.Millisecond)
	if drain(r.Changes()) {
		t.Fatal("re-verifying an already-serving relay must not re-notify")
	}
}

func TestFailingDemotesOnlyAfterThreshold(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	drain(r.Changes()) // consume the admission notification

	// Blip one: still serving, still counted, no notification.
	r.Failing(keyA, ReasonProbeFailed, time.Second)
	if drain(r.Changes()) {
		t.Fatal("a single blip must not notify")
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateReady || rec.FailStreak != 1 {
		t.Fatalf("record = %+v, want ready with streak 1", rec)
	}
	// Blip two: still the fast layer's job.
	r.Failing(keyA, ReasonProbeFailed, time.Second)
	if !r.IsReady(keyA) {
		t.Fatal("two blips must still not demote")
	}
	// Blip three: past the threshold, demoted, notified.
	r.Failing(keyA, ReasonProbeFailed, time.Second)
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatalf("demoted relay must not serve: ready=%v count=%d", r.IsReady(keyA), r.ReadyCount())
	}
	rec, _ = r.StateOf(keyA)
	if rec.State != StateUnready || rec.Reason != ReasonProbeFailed {
		t.Fatalf("record = %+v, want unready with %q", rec, ReasonProbeFailed)
	}
	if !drain(r.Changes()) {
		t.Fatal("demotion must notify the applier")
	}

	// Later failures keep it in unready, never re-admitting silently.
	r.Failing(keyA, ReasonUnreachable, time.Second)
	rec, _ = r.StateOf(keyA)
	if rec.State != StateUnready {
		t.Fatalf("record = %+v, want unready to persist", rec)
	}
	if r.Allow(keyA) {
		t.Fatal("a fresh failure must close the backoff gate")
	}
}

func TestDemoteRevokesAdmissionImmediately(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	drain(r.Changes()) // consume the admission notification

	// A failing replacement drops admission NOW — one Demote, no streak
	// wait: the previous verified deployment no longer exists.
	r.Demote(keyA, ReasonProbeFailed)
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatalf("demoted relay must not serve: ready=%v count=%d", r.IsReady(keyA), r.ReadyCount())
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateUnready || rec.Reason != ReasonProbeFailed {
		t.Fatalf("record = %+v, want unready with %q", rec, ReasonProbeFailed)
	}
	if !drain(r.Changes()) {
		t.Fatal("demotion must notify the applier")
	}
	if r.Allow(keyA) {
		t.Fatal("a demotion must close the backoff gate")
	}

	// Re-admission happens only through a successful Ready.
	r.Ready(keyA, time.Millisecond)
	if !r.IsReady(keyA) || r.ReadyCount() != 1 {
		t.Fatalf("ready after demote not admitted: ready=%v count=%d", r.IsReady(keyA), r.ReadyCount())
	}
}

func TestDemoteNeverServedGoesFailed(t *testing.T) {
	r, _ := newTestRegistry()
	r.Enter(keyA, StateDiscovered, "")

	// A relay that never verified ends up failed, not unready — same
	// semantics as a Failing streak's never-serving branch.
	r.Demote(keyA, ReasonAuthFailed)
	rec, _ := r.StateOf(keyA)
	if rec.State != StateFailed || rec.Reason != ReasonAuthFailed {
		t.Fatalf("record = %+v, want failed with %q", rec, ReasonAuthFailed)
	}
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatalf("never-served relay must stay out: ready=%v count=%d", r.IsReady(keyA), r.ReadyCount())
	}
}

func TestEnterVerifyingDoesNotResetStreak(t *testing.T) {
	// Routine scans interleave Enter(verifying) with every attempt. If that
	// reset the streak, a serving relay failing every scan tick would sit at
	// streak 1 forever and never demote — the flagship guarantee silently
	// dies. Verify the demote still fires across interleaved verifying
	// phases.
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	for range 3 {
		r.Enter(keyA, StateVerifying, "")
		r.Failing(keyA, ReasonProbeFailed, time.Second)
	}
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatalf("interleaved verifying phases defeated the demote threshold: ready=%v count=%d",
			r.IsReady(keyA), r.ReadyCount())
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateUnready || rec.FailStreak != 3 {
		t.Fatalf("record = %+v, want unready with streak 3", rec)
	}
}

func TestEnterDeployingResetsStreakAndBackoff(t *testing.T) {
	// A deploy is a genuinely fresh attempt: it clears the failure history
	// so the new deployment is not pre-judged by the old one's failures.
	r, clock := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	for range 3 {
		r.Failing(keyA, ReasonUnreachable, time.Second)
	}
	if r.IsReady(keyA) {
		t.Fatal("precondition: three failures demote")
	}
	r.Enter(keyA, StateDeploying, "")
	rec, _ := r.StateOf(keyA)
	if rec.State != StateDeploying || rec.FailStreak != 0 {
		t.Fatalf("record = %+v, want deploying with streak 0", rec)
	}
	if !r.Allow(keyA) {
		t.Fatal("a deploying relay must not be backoff-gated")
	}
	if r.IsReady(keyA) {
		t.Fatal("deploying must never admit a relay")
	}
	clock.Advance(0)
}

func TestFailingNeverReadyGoesFailed(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Failing(keyA, ReasonAuthFailed, time.Second)
	rec, _ := r.StateOf(keyA)
	if rec.State != StateFailed || rec.Reason != ReasonAuthFailed {
		t.Fatalf("record = %+v, want failed with %q", rec, ReasonAuthFailed)
	}
	if r.IsReady(keyA) {
		t.Fatal("a failed relay must never serve")
	}
	// The relay never served, so pool membership is unchanged — but the
	// lifecycle transition must still reach the applier: /stats and the
	// admin plane render the failed row from a rebuilt generation, and
	// without this notification the transition stays invisible forever.
	if !drain(r.Changes()) {
		t.Fatal("failure of a never-serving relay must notify for lifecycle visibility")
	}
}

func TestUnreadyRecoversToReady(t *testing.T) {
	r, clock := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	drain(r.Changes()) // admission notification
	for range 3 {
		r.Failing(keyA, ReasonUnreachable, time.Second)
	}
	drain(r.Changes()) // demotion notification
	if r.IsReady(keyA) {
		t.Fatal("precondition: three failures demote")
	}
	clock.Advance(time.Hour) // backoff elapses before the recovery attempt
	r.Ready(keyA, 30*time.Millisecond)
	if !r.IsReady(keyA) || r.ReadyCount() != 1 {
		t.Fatalf("recovered relay must serve: ready=%v count=%d", r.IsReady(keyA), r.ReadyCount())
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateReady || rec.Reason != "" {
		t.Fatalf("record = %+v, want clean ready", rec)
	}
	if !drain(r.Changes()) {
		t.Fatal("recovery to readiness must notify")
	}
}

func TestDeployTransitionsPreserveServingOldDeployment(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	// A replacement deploys: phase shows deploying, the old verified relay
	// keeps serving — nothing unverified is admitted, nothing healthy is
	// dropped early.
	r.Enter(keyA, StateDeploying, "")
	if !r.IsReady(keyA) || r.ReadyCount() != 1 {
		t.Fatalf("replacing a healthy relay must keep serving it: ready=%v count=%d",
			r.IsReady(keyA), r.ReadyCount())
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateDeploying {
		t.Fatalf("record = %+v, want deploying phase", rec)
	}
	// The replacement finished verifying: back to ready, still serving.
	r.Ready(keyA, 20*time.Millisecond)
	rec, _ = r.StateOf(keyA)
	if rec.State != StateReady || !r.IsReady(keyA) {
		t.Fatalf("record = %+v ready=%v, want ready and serving", rec, r.IsReady(keyA))
	}
}

func TestFirstDeployDoesNotServeUntilReady(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Enter(keyA, StateDeploying, "")
	if r.IsReady(keyA) {
		t.Fatal("a never-verified relay must not serve while deploying")
	}
	r.Failing(keyA, ReasonDeployFailed, time.Second)
	rec, _ := r.StateOf(keyA)
	if rec.State != StateFailed {
		t.Fatalf("record = %+v, want failed after a failed first deploy", rec)
	}
}

func TestBackoffGrowthAndCap(t *testing.T) {
	// keyB stays serving so keyA's growth is never folded into the
	// nothing-serves recovery cap — this test pins the pure exponential.
	r, clock := newTestRegistry()
	r.Sync([]Key{keyA, keyB})
	r.Ready(keyB, time.Millisecond)

	failAndCheck := func(wantWait time.Duration) {
		t.Helper()
		r.Failing(keyA, ReasonProbeFailed, time.Second)
		// Closed until wantWait (checked at wantWait-1s), open at wantWait.
		clock.Advance(wantWait - time.Second)
		if r.Allow(keyA) {
			t.Fatalf("gate open at %s; want closed until %s after failure", wantWait-time.Second, wantWait)
		}
		clock.Advance(time.Second)
		if !r.Allow(keyA) {
			t.Fatalf("gate still closed at %s; want open", wantWait)
		}
	}

	// Failure 1 → 5s. Failure 2 → 10s. Failure 3 → 20s. Failure 4 → 40s,
	// the cap: it must not keep doubling.
	failAndCheck(5 * time.Second)
	failAndCheck(10 * time.Second)
	failAndCheck(20 * time.Second)
	failAndCheck(40 * time.Second) // would be 80s un-capped
	failAndCheck(40 * time.Second)
}

func TestBackoffCappedHardWhileNothingServes(t *testing.T) {
	r, clock := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	for range 3 {
		r.Failing(keyA, ReasonUnreachable, time.Second)
	}
	if r.ReadyCount() != 0 {
		t.Fatalf("precondition: nothing serves, count=%d", r.ReadyCount())
	}
	// The demoting failure itself gates the next attempt by the grown wait;
	// from then on, with zero relays serving, the gate reopens within
	// RecoverMax of every failure — a total outage heals at a steady, fast
	// cadence instead of waiting out a long backoff.
	clock.Advance(20 * time.Second)
	for range 5 {
		// Wait out the recover cap, then the gate must be open and one more
		// failure re-gates it for no more than RecoverMax.
		clock.Advance(2 * time.Second)
		if !r.Allow(keyA) {
			t.Fatal("gate must reopen within RecoverMax while nothing serves")
		}
		r.Failing(keyA, ReasonProbeFailed, time.Second)
	}
}
func TestStreakCountsPerRelay(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA, keyB})
	r.Ready(keyA, time.Millisecond)
	r.Ready(keyB, time.Millisecond)
	for range 3 {
		r.Failing(keyB, ReasonProbeFailed, time.Second)
	}
	if !r.IsReady(keyA) || r.ReadyCount() != 1 {
		t.Fatalf("independent relays must not share demotion: keyA ready=%v count=%d",
			r.IsReady(keyA), r.ReadyCount())
	}
	recB, _ := r.StateOf(keyB)
	if recB.State != StateUnready {
		t.Fatalf("record = %+v, want unready", recB)
	}
}

func TestBeginSingleFlight(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	end, ok := r.Begin(keyA)
	if !ok {
		t.Fatal("first Begin must win")
	}
	if _, ok := r.Begin(keyA); ok {
		t.Fatal("concurrent Begin must lose the single-flight hold")
	}
	end()
	if _, ok := r.Begin(keyA); !ok {
		t.Fatal("Begin must succeed again after release")
	}
}

func TestSyncRemovesDeletedRelay(t *testing.T) {
	r, clock := newTestRegistry()
	r.Sync([]Key{keyA, keyB})
	r.Ready(keyA, time.Millisecond)
	r.Ready(keyB, time.Millisecond)

	r.Sync([]Key{keyA}) // keyB deleted from config
	if r.IsReady(keyB) || r.ReadyCount() != 1 {
		t.Fatalf("deleted relay must leave the pool: ready=%v count=%d", r.IsReady(keyB), r.ReadyCount())
	}
	rec, ok := r.StateOf(keyB)
	if !ok || rec.State != StateRemoving {
		t.Fatalf("record = %+v ok=%v, want removing phase", rec, ok)
	}
	if !drain(r.Changes()) {
		t.Fatal("removing a serving relay must notify")
	}

	// After the hold, the removing entry is purged; a re-added relay must
	// re-verify from scratch, never remembered-ready.
	clock.Advance(removingHold + time.Second)
	r.Sync([]Key{keyA})
	if _, ok := r.StateOf(keyB); ok {
		t.Fatal("removing entry must be purged after the hold")
	}
	r.Sync([]Key{keyA, keyB})
	if r.IsReady(keyB) {
		t.Fatal("re-added relay must start configured, never remembered-ready")
	}
}

func TestSyncResurrectsReaddedWithinHold(t *testing.T) {
	// A relay deleted and re-added before the removal hold elapses must read
	// configured again — the registry mirrors the database rows, and the row
	// now exists.
	r, clock := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Ready(keyA, time.Millisecond)
	r.Sync(nil) // deleted
	clock.Advance(10 * time.Second)
	r.Sync([]Key{keyA}) // re-added within the hold
	rec, ok := r.StateOf(keyA)
	if !ok || rec.State != StateConfigured {
		t.Fatalf("record = %+v ok=%v, want resurrected as configured", rec, ok)
	}
	if r.IsReady(keyA) {
		t.Fatal("resurrected relay must re-verify from scratch")
	}
}

func TestSyncRemovesUnreadyWithoutNotify(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA})
	r.Failing(keyA, ReasonAuthFailed, time.Second)
	drain(r.Changes()) // the never-serving failure transition notified
	r.Sync(nil)
	if drain(r.Changes()) {
		t.Fatal("removing a never-serving relay must not notify — the pool did not change")
	}
}

func TestSnapshotSortedDeterministic(t *testing.T) {
	r, _ := newTestRegistry()
	r.Sync([]Key{keyA, keyB})
	r.Ready(keyB, time.Millisecond)
	r.Enter(keyA, StateVerifying, "")
	records := r.Snapshot()
	if len(records) != 2 {
		t.Fatalf("snapshot = %d records, want 2", len(records))
	}
	// Sorted by "provider/name": cloudflare/bravo before vercel/alpha, and
	// only keyB holds admission.
	if records[0].Key != keyB || records[1].Key != keyA {
		t.Fatalf("snapshot order = %v, %v; want keyB then keyA", records[0].Key, records[1].Key)
	}
	if records[0].State != StateReady || records[1].State != StateVerifying {
		t.Fatalf("records = %+v, %+v; want ready and verifying", records[0], records[1])
	}
}
