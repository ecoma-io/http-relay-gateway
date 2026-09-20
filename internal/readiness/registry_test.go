package readiness

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// clock is the injected test clock: tests advance it explicitly instead of
// sleeping.
type clock struct {
	nanos int64
}

func newClock() *clock {
	return &clock{nanos: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()}
}

func (c *clock) Now() time.Time { return time.Unix(0, c.nanos) }

func (c *clock) Advance(d time.Duration) { c.nanos += int64(d) }

func testRegistry(c *clock) *Registry {
	return New(Config{
		BackoffBase: time.Second,
		BackoffMax:  8 * time.Second,
		RecoverMax:  4 * time.Second,
		PauseRetry:  10 * time.Minute,
		DemoteAfter: 3,
		Now:         c.Now,
	})
}

func key(provider, name string) Key { return Key{Provider: provider, Name: name} }

var (
	keyA = key("cloudflare", "alpha")
	keyB = key("vercel", "beta")
	keyC = key("deno", "gamma")
)

// admit brings a relay to ready through the normal Begin/Ready flow.
func admit(t *testing.T, r *Registry, k Key, url, token string) uint64 {
	t.Helper()
	release, gen, ok := r.Begin(k)
	if !ok {
		t.Fatal("Begin failed on an idle relay")
	}
	r.Ready(k, gen, url, token, 10*time.Millisecond)
	release()
	if !r.IsReady(k) {
		t.Fatal("relay not admitted after Ready")
	}
	return gen
}

// generationOf fetches a relay's current incarnation, failing the test when
// the entry is gone.
func generationOf(t *testing.T, r *Registry, k Key) uint64 {
	t.Helper()
	gen, ok := r.GenerationOf(k)
	if !ok {
		t.Fatalf("no registry entry for %v", k)
	}
	return gen
}

func TestSyncAnnouncesNewKeysAndRemovesMissing(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Key{keyA, keyB})
	rec, ok := r.StateOf(keyA)
	if !ok || rec.State != StateConfigured || rec.Generation != 1 {
		t.Fatalf("new key = %+v ok=%t, want configured generation 1", rec, ok)
	}
	if r.NotifySeq() != 2 {
		t.Fatalf("seq = %d, want 2 (one announcement per new key)", r.NotifySeq())
	}

	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	if got := r.ReadyCount(); got != 1 {
		t.Fatalf("readyCount = %d, want 1", got)
	}

	r.Sync(nil) // keyA leaves the configuration
	rec, ok = r.StateOf(keyA)
	if !ok || rec.State != StateRemoving {
		t.Fatalf("removed key state = %+v ok=%t, want removing", rec, ok)
	}
	if rec.Generation != gen+1 {
		t.Fatalf("generation = %d, want %d (bumped on removal)", rec.Generation, gen+1)
	}
	if r.IsReady(keyA) || r.ReadyCount() != 0 || len(r.Serving()) != 0 {
		t.Fatal("removal must revoke admission atomically")
	}
	// Two announcements (keyA, keyB), the admission grant, then two removals
	// (keyA and the never-admitted keyB both left the configuration).
	if r.NotifySeq() != 5 {
		t.Fatalf("seq = %d, want 5", r.NotifySeq())
	}
}

func TestSyncReaddStartsNewIncarnation(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Key{keyA})
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Sync(nil) // removed: generation bumps, admission revoked
	r.Sync([]Key{keyA})

	rec, ok := r.StateOf(keyA)
	if !ok || rec.State != StateConfigured {
		t.Fatalf("re-added key state = %+v ok=%t, want configured", rec, ok)
	}
	if rec.Generation != gen+2 {
		t.Fatalf("generation = %d, want %d (removal + re-add)", rec.Generation, gen+2)
	}
	if rec.Reason != "" || rec.FailStreak != 0 {
		t.Fatalf("re-added record = %+v, want a clean slate", rec)
	}
	if !r.Allow(keyA) {
		t.Fatal("re-added relay must not inherit the old incarnation's backoff")
	}
	if r.IsReady(keyA) {
		t.Fatal("a new incarnation must re-verify from scratch, never from memory")
	}
}

func TestReadyPublishesURLAndTokenAtomically(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})

	release, gen, ok := r.Begin(keyA)
	if !ok {
		t.Fatal("Begin failed")
	}
	r.Ready(keyA, gen, "https://alpha.example", "rk-1", 5*time.Millisecond)
	release()

	serving := r.Serving()
	if len(serving) != 1 {
		t.Fatalf("serving = %+v, want exactly one admission", serving)
	}
	s := serving[0]
	if s.Key != keyA || s.URL != "https://alpha.example" || s.Token != "rk-1" || s.Version != gen {
		t.Fatalf("serving entry = %+v, want the exact verified URL+key pair at generation %d", s, gen)
	}

	seq := r.NotifySeq()
	r.Ready(keyA, gen, "https://alpha.example", "rk-1", 5*time.Millisecond)
	if r.NotifySeq() != seq {
		t.Error("re-verifying an already-serving relay must not notify")
	}
	if got := r.ReadyCount(); got != 1 {
		t.Fatalf("readyCount = %d after re-verification, want 1", got)
	}
}

func TestStaleCompletionsAreDiscarded(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	release, gen, ok := r.Begin(keyA)
	if !ok {
		t.Fatal("Begin failed")
	}
	r.Sync(nil) // removal invalidates every in-flight operation on gen

	// Every completion API from the OLD incarnation must land in the void.
	r.Ready(keyA, gen, "https://stale.example", "rk", time.Millisecond)
	if r.IsReady(keyA) || len(r.Serving()) != 0 {
		t.Fatal("stale Ready admitted the old incarnation")
	}
	r.Enter(keyA, gen, StateDeploying, "")
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	r.Demote(keyA, gen, ReasonReplacing)
	r.Pause(keyA, gen, ReasonPaused)
	r.DeleteDone(keyA, gen)
	r.DeleteFailed(keyA, gen, "stale delete")
	release()

	rec, ok := r.StateOf(keyA)
	if !ok {
		t.Fatal("stale DeleteDone purged the entry")
	}
	if rec.State != StateRemoving || rec.Generation != gen+1 {
		t.Fatalf("record after stale completions = %+v, want untouched removing at generation %d", rec, gen+1)
	}
	if rec.Reason != "" {
		t.Fatalf("reason = %q, want untouched", rec.Reason)
	}

	// The current incarnation is configured again: even a CURRENT-generation
	// DeleteDone is discarded because the relay is not removing.
	r.Sync([]Key{keyA})
	rec, _ = r.StateOf(keyA)
	r.DeleteDone(keyA, rec.Generation)
	if _, ok := r.StateOf(keyA); !ok {
		t.Fatal("DeleteDone purged a relay that was not removing")
	}
}

func TestFailingBlipsKeepServingUntilDemoteAfter(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	admit(t, r, keyA, "https://alpha.example", "rk")
	seq := r.NotifySeq()

	r.Failing(keyA, generationOf(t, r, keyA), ReasonUnreachable, 10*time.Millisecond)
	r.Failing(keyA, generationOf(t, r, keyA), ReasonUnreachable, 10*time.Millisecond)
	if !r.IsReady(keyA) {
		t.Fatal("two blips must not pull a verified relay out of rotation")
	}
	if r.NotifySeq() != seq {
		t.Error("blips below the threshold must not notify")
	}

	rec, _ := r.StateOf(keyA)
	if rec.FailStreak != 2 || rec.Reason != ReasonUnreachable {
		t.Fatalf("record = %+v, want streak 2 with the failure reason recorded", rec)
	}

	r.Failing(keyA, generationOf(t, r, keyA), ReasonUnreachable, 10*time.Millisecond)
	if r.IsReady(keyA) {
		t.Fatal("the third consecutive failure must revoke admission")
	}
	rec, _ = r.StateOf(keyA)
	if rec.State != StateUnready {
		t.Fatalf("state = %s, want unready", rec.State)
	}
	if r.NotifySeq() != seq+1 {
		t.Fatalf("seq = %d, want %d (demote notifies)", r.NotifySeq(), seq+1)
	}
}

func TestDemoteRevokesImmediatelyAndIdempotently(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	admit(t, r, keyA, "https://alpha.example", "rk")

	gen := generationOf(t, r, keyA)
	r.Demote(keyA, gen, ReasonReplacing)
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatal("Demote must revoke admission immediately, bypassing the streak")
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateUnready || rec.Reason != ReasonReplacing {
		t.Fatalf("state/reason = %s/%s, want unready/replacing", rec.State, rec.Reason)
	}
	if rec.FailStreak != 3 {
		t.Fatalf("FailStreak = %d, want the DemoteAfter threshold", rec.FailStreak)
	}

	seq := r.NotifySeq()
	r.Demote(keyA, gen, ReasonReplacing)
	if r.NotifySeq() != seq {
		t.Error("a second Demote of an already-demoted relay must not notify")
	}
	if rec, _ := r.StateOf(keyA); rec.State != StateUnready {
		t.Fatalf("state after the second Demote = %s, want still unready", rec.State)
	}
}

func TestDemoteNeverServingLabelsFailed(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	seq := r.NotifySeq()

	r.Demote(keyA, generationOf(t, r, keyA), ReasonVersionFailed)
	rec, _ := r.StateOf(keyA)
	if rec.State != StateFailed || rec.Reason != ReasonVersionFailed {
		t.Fatalf("state/reason = %s/%s, want failed/version_failed", rec.State, rec.Reason)
	}
	if r.IsReady(keyA) {
		t.Fatal("a never-serving relay gained admission via Demote")
	}
	if r.NotifySeq() != seq+1 {
		t.Fatalf("seq = %d, want %d", r.NotifySeq(), seq+1)
	}
}

func TestBackoffDoublesAndCapsWhileSomethingServes(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA, keyB})
	admit(t, r, keyB, "https://beta.example", "rk") // readyCount > 0: no RecoverMax cap

	gen := generationOf(t, r, keyA)
	waits := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for i, want := range waits {
		r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
		if r.Allow(keyA) {
			t.Fatalf("streak %d: attempt allowed immediately after failure", i+1)
		}
		c.Advance(want - time.Second)
		if r.Allow(keyA) {
			t.Fatalf("streak %d: attempt allowed after %s, want to wait %s", i+1, want-time.Second, want)
		}
		c.Advance(time.Second)
		if !r.Allow(keyA) {
			t.Fatalf("streak %d: attempt still blocked after the full %s wait", i+1, want)
		}
	}
}

func TestRecoverMaxCapsBackoffWhenNothingServes(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA}) // nothing serving at all

	gen := generationOf(t, r, keyA)
	for range 4 {
		r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	}
	// The exponential wait would be 8s (BackoffMax); RecoverMax caps it at 4s.
	if r.Allow(keyA) {
		t.Fatal("attempt allowed immediately after failure")
	}
	c.Advance(3 * time.Second)
	if r.Allow(keyA) {
		t.Fatal("attempt allowed before the RecoverMax window")
	}
	c.Advance(1 * time.Second)
	if !r.Allow(keyA) {
		t.Fatal("attempt blocked past the RecoverMax window")
	}
}

func TestReadyAndDeployingResetTheGate(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	gen := generationOf(t, r, keyA)
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if r.Allow(keyA) {
		t.Fatal("gate should be closed after failures")
	}

	// Ready resets streak and gate.
	r.Ready(keyA, gen, "https://alpha.example", "rk", time.Millisecond)
	if !r.Allow(keyA) {
		t.Fatal("Ready must reopen the backoff gate")
	}
	rec, _ := r.StateOf(keyA)
	if rec.FailStreak != 0 {
		t.Fatalf("FailStreak = %d after Ready, want 0", rec.FailStreak)
	}

	// Enter(deploying) counts as a fresh attempt; routine phases do not.
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if r.Allow(keyA) {
		t.Fatal("gate should be closed again")
	}
	r.Enter(keyA, gen, StateVerifying, "")
	if r.Allow(keyA) {
		t.Fatal("Enter(verifying) must not reset the backoff gate")
	}
	r.Enter(keyA, gen, StateDeploying, "")
	if !r.Allow(keyA) {
		t.Fatal("Enter(deploying) must reset the backoff gate")
	}
	rec, _ = r.StateOf(keyA)
	if rec.FailStreak != 0 {
		t.Fatalf("FailStreak = %d after Enter(deploying), want 0", rec.FailStreak)
	}
}

func TestPauseUsesTheRevivalCadence(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA, keyB})
	admit(t, r, keyA, "https://alpha.example", "rk")
	admit(t, r, keyB, "https://beta.example", "rk")

	r.Pause(keyA, generationOf(t, r, keyA), ReasonPaused)
	if r.IsReady(keyA) || r.ReadyCount() != 1 {
		t.Fatal("Pause must revoke admission immediately")
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StatePaused {
		t.Fatalf("state = %s, want paused", rec.State)
	}

	// The gate is armed at the revival cadence, not the ordinary backoff —
	// 10 minutes out even though the failure backoff would have been 1s.
	c.Advance(9 * time.Minute)
	if r.Allow(keyA) {
		t.Fatal("paused relay re-probed before the revival cadence")
	}
	c.Advance(1 * time.Minute)
	if !r.Allow(keyA) {
		t.Fatal("paused relay still blocked at the revival cadence")
	}
}

func TestFailingKeepsPausedRelaysPaused(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	gen := generationOf(t, r, keyA)
	r.Pause(keyA, gen, ReasonPaused)

	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	rec, _ := r.StateOf(keyA)
	if rec.State != StatePaused {
		t.Fatalf("state = %s, want still paused (the revival scan owns it)", rec.State)
	}
	if rec.Reason != ReasonUnreachable {
		t.Fatalf("reason = %q, want refreshed", rec.Reason)
	}
	if r.IsReady(keyA) {
		t.Fatal("paused relay admitted")
	}
}

func TestDeleteLifecycleGuardsAndPurge(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	// BeginDelete refuses anything that is not labeled removing.
	r.Sync([]Key{keyA})
	if _, _, ok := r.BeginDelete(keyA); ok {
		t.Fatal("BeginDelete succeeded on a configured relay")
	}

	r.Sync(nil)
	release, gen, ok := r.BeginDelete(keyA)
	if !ok {
		t.Fatal("BeginDelete failed on a removing relay")
	}
	if _, _, ok := r.BeginDelete(keyA); ok {
		t.Fatal("a second delete started while the first holds the relay")
	}
	if _, _, ok := r.Begin(keyA); ok {
		t.Fatal("a deploy started while a delete holds the relay")
	}

	r.DeleteFailed(keyA, gen, "platform 500")
	rec, ok := r.StateOf(keyA)
	if !ok || rec.State != StateRemoving {
		t.Fatalf("record after DeleteFailed = %+v ok=%t, want still removing", rec, ok)
	}
	if rec.Reason != "platform 500" {
		t.Fatalf("reason = %q, want the delete failure", rec.Reason)
	}
	if rec.FailStreak != 1 {
		t.Fatalf("FailStreak = %d, want 1 (a failed delete is a failed attempt)", rec.FailStreak)
	}
	if r.Allow(keyA) {
		t.Fatal("the delete retry must be backoff-gated")
	}
	release()

	r.DeleteDone(keyA, gen)
	if _, ok := r.StateOf(keyA); ok {
		t.Fatal("DeleteDone must purge the completed removal")
	}
	if r.NotifySeq() == 0 {
		t.Error("the purge never notified")
	}
}

func TestSingleFlightSpansTheWholeOperation(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})

	release, gen, ok := r.Begin(keyA)
	if !ok {
		t.Fatal("first Begin failed")
	}
	if _, _, ok := r.Begin(keyA); ok {
		t.Fatal("second Begin succeeded while the first holds the relay")
	}
	release()
	release2, gen2, ok := r.Begin(keyA)
	if !ok {
		t.Fatal("Begin failed after release")
	}
	if gen2 != gen {
		t.Fatalf("generation moved from %d to %d without a removal", gen, gen2)
	}
	release2()
}

func TestNotificationSemantics(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA})
	gen := generationOf(t, r, keyA)
	base := r.NotifySeq() // two announcements so far: keyA + keyB absent

	// Plain phase moves never notify.
	r.Enter(keyA, gen, StateDiscovering, "")
	if r.NotifySeq() != base {
		t.Fatalf("seq = %d after Enter, want %d", r.NotifySeq(), base)
	}

	// A first failure of a never-serving relay changes the lifecycle record.
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if r.NotifySeq() != base+1 {
		t.Fatalf("seq = %d after first Failing, want %d", r.NotifySeq(), base+1)
	}

	// Granting admission notifies; re-verifying it does not.
	r.Ready(keyA, gen, "https://alpha.example", "rk", time.Millisecond)
	if r.NotifySeq() != base+2 {
		t.Fatalf("seq = %d after admission, want %d", r.NotifySeq(), base+2)
	}
	r.Ready(keyA, gen, "https://alpha.example", "rk", time.Millisecond)
	if r.NotifySeq() != base+2 {
		t.Fatalf("seq = %d after re-verification, want %d", r.NotifySeq(), base+2)
	}

	// Revocation notifies.
	r.Pause(keyA, gen, ReasonPaused)
	if r.NotifySeq() != base+3 {
		t.Fatalf("seq = %d after Pause, want %d", r.NotifySeq(), base+3)
	}
}

func TestChangesChannelCoalescesBursts(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyA, keyB, keyC})
	admit(t, r, keyA, "https://alpha.example", "rk")
	admit(t, r, keyB, "https://beta.example", "rk")
	r.Demote(keyA, generationOf(t, r, keyA), ReasonReplacing)

	select {
	case <-r.Changes():
	default:
		t.Fatal("a burst of notifications left no pending signal")
	}
	select {
	case <-r.Changes():
		t.Fatal("the coalesced channel delivered more than one signal per burst")
	default:
	}
}

func TestServingAndSnapshotAreSorted(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync([]Key{keyC, keyA, keyB})
	admit(t, r, keyA, "https://alpha.example", "rk")
	admit(t, r, keyC, "https://gamma.example", "rk")
	admit(t, r, keyB, "https://beta.example", "rk")

	var got []string
	for _, s := range r.Serving() {
		got = append(got, s.Key.String())
	}
	want := []string{keyA.String(), keyC.String(), keyB.String()} // sorted by key string
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("serving order = %v, want %v", got, want)
	}

	recs := r.Snapshot()
	if len(recs) != 3 {
		t.Fatalf("snapshot = %d records, want 3", len(recs))
	}
	for i, rec := range recs {
		if rec.Key.String() != want[i] {
			t.Fatalf("snapshot[%d] = %s, want %s", i, rec.Key.String(), want[i])
		}
	}
}

func TestDefaultsTakeTheDocumentedValues(t *testing.T) {
	c := newClock()
	r := New(Config{Now: c.Now})
	r.Sync([]Key{keyA})
	gen := generationOf(t, r, keyA)
	admit(t, r, keyA, "https://alpha.example", "rk")

	// DemoteAfter defaults to 3.
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if !r.IsReady(keyA) {
		t.Fatal("default DemoteAfter is not 3")
	}
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if r.IsReady(keyA) {
		t.Fatal("relay still serving after three failures at the default threshold")
	}

	// After the demote the gate is armed at 3 failures × 5s default base =
	// 20s. The gate was computed while the relay still counted as ready, so
	// the 15s RecoverMax did not apply to this final failure.
	c.Advance(19 * time.Second)
	if r.Allow(keyA) {
		t.Fatal("default backoff after three failures is not 3 × 5s")
	}
	c.Advance(1 * time.Second)
	if !r.Allow(keyA) {
		t.Fatal("relay still blocked after the full default backoff")
	}
}

func TestConcurrencyKeepsCountersConsistent(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	keys := []Key{keyA, keyB, keyC,
		key("cloudflare", "delta"), key("vercel", "epsilon"), key("deno", "zeta")}
	r.Sync(keys)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range 120 {
				k := keys[(i+len(keys))%len(keys)]
				switch i % 4 {
				case 0:
					release, gen, ok := r.Begin(k)
					if !ok {
						continue
					}
					r.Ready(k, gen, "https://"+k.Name+".example", "rk", time.Millisecond)
					release()
				case 1:
					if gen, ok := r.GenerationOf(k); ok {
						r.Failing(k, gen, ReasonUnreachable, time.Millisecond)
					}
				case 2:
					if rel, gen, ok := r.Begin(k); ok {
						r.Demote(k, gen, ReasonReplacing)
						rel()
					}
				default:
					_ = r.Serving()
					_ = r.ReadyCount()
					_ = r.Snapshot()
					_, _ = r.StateOf(k)
					_ = r.Allow(k)
					_ = r.IsReady(k)
				}
			}
		}(i)
	}
	wg.Wait()

	if r.ReadyCount() != len(r.Serving()) {
		t.Fatalf("readyCount = %d, len(Serving) = %d — counters diverged",
			r.ReadyCount(), len(r.Serving()))
	}
	ready := map[Key]bool{}
	for _, s := range r.Serving() {
		ready[s.Key] = true
		if !r.IsReady(s.Key) {
			t.Fatalf("serving relay %v is not admitted", s.Key)
		}
	}
	for _, rec := range r.Snapshot() {
		if ready[rec.Key] != r.IsReady(rec.Key) {
			t.Fatalf("snapshot/serving disagreement for %v", rec.Key)
		}
	}
}
