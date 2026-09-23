package readiness

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
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

// members wraps keys as scope-resolved Sync members with no pins — the shape
// every relay without provider pins presents, and the one these tests need
// unless they exercise scope flips explicitly (ScopeKnown true, empty scope:
// the credential resolved fine, it just pins nothing).
func members(keys ...Key) []Member {
	out := make([]Member, 0, len(keys))
	for _, k := range keys {
		out = append(out, Member{Key: k, ScopeKnown: true})
	}
	return out
}

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

	r.Sync(members(keyA, keyB))
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

	r.Sync(members(keyA))
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Sync(nil) // removed: generation bumps, admission revoked
	r.Sync(members(keyA))

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

// pinned wraps a key as a Sync member whose credential resolves to one
// explicit team pin — the smallest concrete scope a flip can move.
func pinned(k Key, team string) Member {
	return Member{Key: k, Scope: deploy.Scope{Team: team}, ScopeKnown: true}
}

// A scope that moves under a stable name is a new incarnation: the old
// scope's admission is revoked FIRST (a verification earned in one scope must
// never serve an identity addressed in another), the generation bumps, and
// the entry restarts as configured with a clean slate — but a deferred-drain
// marker survives, because the incarnation's replacement story is still open.
func TestSyncBumpsGenerationWhenScopeChanges(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Member{pinned(keyA, "team-a")})
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	if r.ReadyCount() != 1 {
		t.Fatalf("readyCount = %d, want 1", r.ReadyCount())
	}
	// Arm a deferred-drain marker and a backoff gate so the flip must show
	// which incarnation state it carries over and which it leaves behind.
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	r.DeferDrain(keyA, gen)
	if r.Allow(keyA) {
		t.Fatal("precondition: the failing streak armed the backoff gate")
	}

	r.Sync([]Member{pinned(keyA, "team-b")})
	rec, ok := r.StateOf(keyA)
	if !ok {
		t.Fatal("entry vanished on a scope flip")
	}
	if rec.Generation != gen+1 {
		t.Fatalf("generation = %d, want %d (the flip is a new incarnation)", rec.Generation, gen+1)
	}
	if rec.State != StateConfigured || rec.Reason != ReasonScopeChanged {
		t.Fatalf("state/reason = %s/%q, want configured/%q", rec.State, rec.Reason, ReasonScopeChanged)
	}
	if rec.FailStreak != 0 {
		t.Fatalf("fail streak = %d, want 0 on the fresh incarnation", rec.FailStreak)
	}
	if !r.Allow(keyA) {
		t.Fatal("a fresh incarnation must not inherit the old one's backoff")
	}
	if r.IsReady(keyA) || r.ReadyCount() != 0 || len(r.Serving()) != 0 {
		t.Fatal("the old scope's admission must be revoked by the flip")
	}
	if !rec.DrainPending {
		t.Fatal("a deferred-drain marker must survive the scope flip: the replacement is still open")
	}
	if r.NotifySeq() == 0 {
		t.Fatal("a scope flip that revokes admission must notify the applier")
	}
}

// Flipping back re-bumps: generation is a strict incarnation counter, so a
// relay that returns to a previously used scope is still a NEW incarnation —
// nothing about the old one, its admission included, may be remembered.
func TestScopeFlipBackBumpsAgainWithoutReuse(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Member{pinned(keyA, "team-a")})
	genA := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Sync([]Member{pinned(keyA, "team-b")})
	genB := generationOf(t, r, keyA)
	if genB != genA+1 {
		t.Fatalf("generation after the first flip = %d, want %d", genB, genA+1)
	}

	// Back to team-a: the fresh incarnation must not resurrect the team-a
	// admission the previous incarnation held.
	r.Sync([]Member{pinned(keyA, "team-a")})
	genBack := generationOf(t, r, keyA)
	if genBack != genB+1 {
		t.Fatalf("generation after the flip back = %d, want %d", genBack, genB+1)
	}
	if r.IsReady(keyA) || r.ReadyCount() != 0 {
		t.Fatal("returning to a former scope must not resurrect its admission")
	}
	rec, _ := r.StateOf(keyA)
	if rec.State != StateConfigured || rec.Reason != ReasonScopeChanged {
		t.Fatalf("state/reason = %s/%q, want configured/%q", rec.State, rec.Reason, ReasonScopeChanged)
	}
}

// Every completion captured under the flipped-out incarnation is stale: the
// generation moved on, so a verification that finishes after the flip must
// land in the void — most of all a Ready, which would otherwise re-admit a
// relay whose URL now answers for a different scope.
func TestScopeFlipDiscardsInFlightCompletions(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Member{pinned(keyA, "team-a")})
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Sync([]Member{pinned(keyA, "team-b")})

	r.Ready(keyA, gen, "https://alpha.example", "rk", time.Millisecond)
	if r.IsReady(keyA) || r.ReadyCount() != 0 || len(r.Serving()) != 0 {
		t.Fatal("a pre-flip Ready must complete into the void")
	}
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if rec, _ := r.StateOf(keyA); rec.FailStreak != 0 || rec.State != StateConfigured {
		t.Fatalf("record = %+v, want the fresh incarnation untouched by a stale Failing", rec)
	}
	if r.TakeScopeChanged(keyA, gen) {
		t.Fatal("a stale generation must read no scope marker")
	}
	if !r.TakeScopeChanged(keyA, gen+1) {
		t.Fatal("the flip's marker must read under the new incarnation")
	}
	if r.TakeScopeChanged(keyA, gen+1) {
		t.Fatal("the marker is read-and-clear: the second read must be false")
	}
}

// A pass whose credential resolution failed claims no scope, and Sync must
// not read that as a flip: an unrelated credential hiccup may not evict
// every pinned relay. The first resolved scope after unknown ones is a
// baseline too, never a change — and that baseline is live: a later real
// move against it flips, bumping the generation and (when the relay was
// serving at the move) revoking admission and raising the drain marker.
func TestSyncUnknownScopeNeverFlips(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	// Each Sync below mirrors the whole desired set, the way the reconciler
	// feeds it — every member every pass, only the scope claim varying.
	fleet := func(ms ...Member) []Member { return ms }

	r.Sync(fleet(pinned(keyA, "team-a")))
	gen := admit(t, r, keyA, "https://alpha.example", "rk")

	// Credential unresolvable this pass: no scope claimed, nothing compared.
	r.Sync(fleet(Member{Key: keyA}))
	if got := generationOf(t, r, keyA); got != gen {
		t.Fatalf("generation = %d, want %d (an unknown scope is never a flip)", got, gen)
	}
	if !r.IsReady(keyA) {
		t.Fatal("an unknown-scope pass must not revoke admission")
	}

	// Still unknown, then resolved again at the same scope: no flip either.
	r.Sync(fleet(Member{Key: keyA}))
	r.Sync(fleet(pinned(keyA, "team-a")))
	if got := generationOf(t, r, keyA); got != gen {
		t.Fatalf("generation = %d, want %d (re-resolving the same scope is not a flip)", got, gen)
	}

	// The baseline rule runs the other way too: a relay whose scope was
	// never known before its first resolution must not flip on that first
	// resolution.
	r.Sync(fleet(pinned(keyA, "team-a"), Member{Key: keyB}))
	r.Sync(fleet(pinned(keyA, "team-a"), pinned(keyB, "org-a")))
	if got := generationOf(t, r, keyB); got != 1 {
		t.Fatalf("generation = %d, want 1 (the first known scope is a baseline)", got)
	}

	// And the unknown passes must not have disarmed flip detection: a real
	// move against the remembered baseline still flips.
	r.Sync(fleet(pinned(keyA, "team-b"), pinned(keyB, "org-a")))
	if got := generationOf(t, r, keyA); got != gen+1 {
		t.Fatalf("generation = %d, want %d (the remembered baseline still flips)", got, gen+1)
	}
	if r.IsReady(keyA) {
		t.Fatal("the real flip must revoke admission")
	}
	// keyB still has not moved, so it stays on its first incarnation.
	if got := generationOf(t, r, keyB); got != 1 {
		t.Fatalf("keyB generation = %d, want 1", got)
	}

	// The adopted baseline is live, not an armistice: keyB's first real move
	// flips too. Without admission the flip only bumps the incarnation.
	r.Sync(fleet(pinned(keyA, "team-b"), pinned(keyB, "org-b")))
	if got := generationOf(t, r, keyB); got != 2 {
		t.Fatalf("keyB generation = %d, want 2 (the adopted baseline still flips)", got)
	}
	if r.TakeScopeChanged(keyB, 2) {
		t.Fatal("a flip without admission must not raise the drain marker")
	}

	// The serving variant: admitted at org-b, then moved to org-c — the flip
	// revokes admission, labels the reason, and raises the barrier marker.
	genB := admit(t, r, keyB, "https://beta.example", "rk")
	if genB != 2 {
		t.Fatalf("keyB admitted at generation %d, want 2", genB)
	}
	r.Sync(fleet(pinned(keyA, "team-b"), pinned(keyB, "org-c")))
	if got := generationOf(t, r, keyB); got != genB+1 {
		t.Fatalf("keyB generation = %d, want %d (a serving flip is a new incarnation)", got, genB+1)
	}
	if r.IsReady(keyB) || r.ReadyCount() != 0 {
		t.Fatal("a serving flip must revoke admission")
	}
	if rec, _ := r.StateOf(keyB); rec.Reason != ReasonScopeChanged {
		t.Fatalf("reason = %q, want %q", rec.Reason, ReasonScopeChanged)
	}
	if !r.TakeScopeChanged(keyB, genB+1) {
		t.Fatal("a serving flip must raise the drain marker")
	}
}

// A flip on a relay that never held admission revokes nothing and raises no
// barrier marker: with no admission there is no in-flight request to
// protect, so the ordinary rollout path may deploy straight into the new
// scope.
func TestScopeFlipWithoutAdmissionRaisesNoBarrierMarker(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Member{pinned(keyA, "team-a")})
	r.Sync([]Member{pinned(keyA, "team-b")})

	rec, _ := r.StateOf(keyA)
	if rec.Generation != 2 || rec.Reason != ReasonScopeChanged {
		t.Fatalf("record = %+v, want generation 2 with %q", rec, ReasonScopeChanged)
	}
	if r.TakeScopeChanged(keyA, rec.Generation) {
		t.Fatal("a flip without admission must not raise the drain marker")
	}
}

// A re-added identity adopts the member's scope as its fresh baseline: the
// generation bump of the re-add IS the incarnation change, and double-bumping
// it for a scope the old incarnation never used would be a phantom flip.
func TestSyncReaddAdoptsScopeAsBaselineWithoutASecondBump(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	r.Sync([]Member{pinned(keyA, "team-a")})
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Sync(nil) // removal: generation bumps once
	r.Sync([]Member{pinned(keyA, "team-b")})

	if got := generationOf(t, r, keyA); got != gen+2 {
		t.Fatalf("generation = %d, want %d (removal + re-add, no phantom flip)", got, gen+2)
	}
	rec, _ := r.StateOf(keyA)
	if rec.Reason != "" {
		t.Fatalf("reason = %q, want a clean re-add without a flip marker", rec.Reason)
	}
	if r.TakeScopeChanged(keyA, gen+2) {
		t.Fatal("a re-add must not raise the drain marker")
	}
	// And the adopted baseline is live: the next real move flips.
	r.Sync([]Member{pinned(keyA, "team-c")})
	if got := generationOf(t, r, keyA); got != gen+3 {
		t.Fatalf("generation = %d, want %d after the next real flip", got, gen+3)
	}
}

func TestReadyPublishesURLAndTokenAtomically(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA))

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
	r.Sync(members(keyA))
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
	r.Sync(members(keyA))
	rec, _ = r.StateOf(keyA)
	r.DeleteDone(keyA, rec.Generation)
	if _, ok := r.StateOf(keyA); !ok {
		t.Fatal("DeleteDone purged a relay that was not removing")
	}
}

func TestFailingBlipsKeepServingUntilDemoteAfter(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA))
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
	r.Sync(members(keyA))
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
	r.Sync(members(keyA))
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
	r.Sync(members(keyA, keyB))
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
	r.Sync(members(keyA)) // nothing serving at all

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

func TestUpdateSettingsAppliesExistingRetryGates(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA, keyB))
	genA := generationOf(t, r, keyA)
	admit(t, r, keyB, "https://beta.example", "rk") // avoids the recover cap initially

	// Build a two-failure gate under the original 1s base, then reload the
	// settings. The existing gate must immediately recompute from LastAttempt:
	// base 3s with a 3s ceiling, rather than retaining the old 2s deadline.
	r.Failing(keyA, genA, ReasonUnreachable, time.Millisecond)
	c.Advance(time.Second)
	r.Failing(keyA, genA, ReasonUnreachable, time.Millisecond)
	r.UpdateSettings(Settings{
		BackoffBase: 3 * time.Second,
		BackoffMax:  3 * time.Second,
		RecoverMax:  time.Second,
		PauseRetry:  2 * time.Second,
		DemoteAfter: 2,
	})
	c.Advance(2 * time.Second)
	if r.Allow(keyA) {
		t.Fatal("reloaded BackoffBase/BackoffMax did not extend the existing gate")
	}
	c.Advance(time.Second)
	if !r.Allow(keyA) {
		t.Fatal("reloaded BackoffBase/BackoffMax did not release at the new deadline")
	}

	// A zero-ready failure uses the reloaded RecoverMax immediately.
	r.Demote(keyB, generationOf(t, r, keyB), ReasonReplacing)
	r.Failing(keyA, genA, ReasonUnreachable, time.Millisecond)
	if r.Allow(keyA) {
		t.Fatal("zero-ready retry was allowed immediately")
	}
	c.Advance(time.Second)
	if !r.Allow(keyA) {
		t.Fatal("reloaded RecoverMax did not cap the zero-ready gate")
	}

	// A newly paused relay follows the reloaded revival cadence.
	r.Ready(keyA, genA, "https://alpha.example", "rk", time.Millisecond)
	r.Pause(keyA, genA, ReasonPaused)
	c.Advance(time.Second)
	if r.Allow(keyA) {
		t.Fatal("reloaded PauseRetry released a paused relay early")
	}
	c.Advance(time.Second)
	if !r.Allow(keyA) {
		t.Fatal("reloaded PauseRetry did not release a paused relay on time")
	}

	// DemoteAfter is read on the next verification result without a restart.
	r.Ready(keyA, genA, "https://alpha.example", "rk", time.Millisecond)
	r.Failing(keyA, genA, ReasonUnreachable, time.Millisecond)
	if !r.IsReady(keyA) {
		t.Fatal("reloaded DemoteAfter demoted before its second failure")
	}
	r.Failing(keyA, genA, ReasonUnreachable, time.Millisecond)
	if r.IsReady(keyA) {
		t.Fatal("reloaded DemoteAfter did not demote on its second failure")
	}
}

func TestReadyAndDeployingResetTheGate(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA))
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
	r.Sync(members(keyA, keyB))
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
	r.Sync(members(keyA))
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

// A sync pass runs Enter(discovering) before every probe — including the
// revival probe of a suspended relay. Routine progress must not clobber the
// pause marker, or a transport blip during revival finds the relay
// mid-discover, Failing labels it failed, and the ordinary backoff erodes
// the revival cadence. Definitive transitions (a deploy) still leave the
// pause.
func TestEnterKeepsPausedRelaysPausedThroughRoutineProgress(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA))
	gen := generationOf(t, r, keyA)
	r.Pause(keyA, gen, ReasonPaused)

	r.Enter(keyA, gen, StateDiscovering, "")
	r.Enter(keyA, gen, StateVerifying, "")
	if rec, _ := r.StateOf(keyA); rec.State != StatePaused {
		t.Fatalf("state = %s after routine progress, want paused", rec.State)
	}

	// The blip: with the marker intact, Failing keeps the paused label and
	// the PauseRetry gate instead of the ordinary backoff.
	r.Failing(keyA, gen, ReasonUnreachable, 0)
	if rec, _ := r.StateOf(keyA); rec.State != StatePaused {
		t.Fatalf("state = %s after a blip during revival, want paused", rec.State)
	}
	c.Advance(2 * time.Minute) // past every ordinary backoff ceiling here (4s)
	if r.Allow(keyA) {
		t.Fatal("routine progress eroded the paused revival gate into the ordinary backoff")
	}
	c.Advance(8 * time.Minute)
	if !r.Allow(keyA) {
		t.Fatal("pause gate did not release at the revival cadence")
	}

	// A deploy is definitive: it takes the relay out of the pause.
	r.Enter(keyA, gen, StateDeploying, "")
	if rec, _ := r.StateOf(keyA); rec.State != StateDeploying {
		t.Fatalf("state = %s after Enter(deploying), want deploying", rec.State)
	}
}

func TestDeleteLifecycleGuardsAndPurge(t *testing.T) {
	c := newClock()
	r := testRegistry(c)

	// BeginDelete refuses anything that is not labeled removing.
	r.Sync(members(keyA))
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
	r.Sync(members(keyA))

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
	r.Sync(members(keyA))
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
	r.Sync(members(keyA, keyB, keyC))
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
	r.Sync(members(keyC, keyA, keyB))
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
	r.Sync(members(keyA))
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

	// After the demote the fleet sits at zero-ready: the relay dropped out
	// of readyCount before the gate was computed, so the very first gate is
	// capped at the default 15s RecoverMax — a total outage heals fast, not
	// at the uncapped 3 × 5s = 20s.
	c.Advance(14 * time.Second)
	if r.Allow(keyA) {
		t.Fatal("zero-ready demotion gate must be capped at the 15s RecoverMax")
	}
	c.Advance(1 * time.Second)
	if !r.Allow(keyA) {
		t.Fatal("relay still blocked after the RecoverMax ceiling")
	}
}

func TestConcurrencyKeepsCountersConsistent(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	keys := []Key{keyA, keyB, keyC,
		key("cloudflare", "delta"), key("vercel", "epsilon"), key("deno", "zeta")}
	r.Sync(members(keys...))

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

func TestDemoteIntoTotalOutageCapsTheFirstGateAtRecoverMax(t *testing.T) {
	c := newClock()
	// A base far above RecoverMax: if the demotion counted itself as ready
	// when the gate was computed, the first gate would be 30s << 2 uncapped;
	// the contract caps a total outage at RecoverMax from the very first
	// retry.
	r := New(Config{
		BackoffBase: 30 * time.Second,
		BackoffMax:  5 * time.Minute,
		RecoverMax:  15 * time.Second,
		PauseRetry:  10 * time.Minute,
		DemoteAfter: 3,
		Now:         c.Now,
	})
	r.Sync(members(keyA))
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Demote(keyA, gen, ReasonReplacing)
	c.Advance(14 * time.Second)
	if r.Allow(keyA) {
		t.Fatal("first gate after a total-outage demote exceeded RecoverMax")
	}
	c.Advance(1 * time.Second)
	if !r.Allow(keyA) {
		t.Fatal("RecoverMax ceiling did not release the retry gate")
	}
}

func TestPausedRelayKeepsItsRevivalGateThroughOrdinaryFailures(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA))
	gen := admit(t, r, keyA, "https://alpha.example", "rk")
	r.Pause(keyA, gen, ReasonPaused)

	// A generic probe failure while paused must not erode the PauseRetry
	// gate into ordinary backoff: the revival cadence holds until a marked
	// suspension answer or the revival itself re-arms it.
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	if rec, _ := r.StateOf(keyA); rec.State != StatePaused {
		t.Fatalf("state = %q, want paused", rec.State)
	}
	if r.Allow(keyA) {
		t.Fatal("paused relay retry gate did not hold at PauseRetry after an ordinary failure")
	}
	c.Advance(8 * time.Second) // far beyond any ordinary backoff step
	if r.Allow(keyA) {
		t.Fatal("ordinary failure eroded the pause gate below PauseRetry")
	}
	c.Advance(10 * time.Minute) // past the PauseRetry cadence
	if !r.Allow(keyA) {
		t.Fatal("pause gate did not release at the PauseRetry cadence")
	}
}

func TestDrainPendingMarkerSurvivesChurnAndClearsAtTheBarrier(t *testing.T) {
	c := newClock()
	r := testRegistry(c)
	r.Sync(members(keyA))
	gen := generationOf(t, r, keyA)

	r.DeferDrain(keyA, gen)
	if rec, _ := r.StateOf(keyA); !rec.DrainPending {
		t.Fatal("DeferDrain did not mark the record")
	}

	// Phase and reason churn between passes must not erase the marker: a
	// Failing refreshes the reason, a Pause replaces the phase, and the
	// resumed pass always re-enters discovering first.
	r.Failing(keyA, gen, ReasonUnreachable, time.Millisecond)
	r.Pause(keyA, gen, ReasonPaused)
	r.Enter(keyA, gen, StateDiscovering, "")
	if rec, _ := r.StateOf(keyA); !rec.DrainPending {
		t.Fatal("reason/phase churn erased the deferred-drain marker")
	}

	// Stale completions are discarded like every other transition.
	r.DrainCompleted(keyA, gen+1)
	if rec, _ := r.StateOf(keyA); !rec.DrainPending {
		t.Fatal("a stale generation cleared the deferred-drain marker")
	}

	// The barrier completing clears it — and so does re-admission, which
	// ends the deferral outright.
	r.DrainCompleted(keyA, gen)
	if rec, _ := r.StateOf(keyA); rec.DrainPending {
		t.Fatal("the barrier completion must clear the marker")
	}
	r.DeferDrain(keyA, gen)
	admit(t, r, keyA, "https://alpha.example", "rk")
	if rec, _ := r.StateOf(keyA); rec.DrainPending {
		t.Fatal("Ready must clear the deferred-drain marker")
	}

	// A new incarnation starts clean: the marker never leaks across a
	// removal or a re-add.
	r.DeferDrain(keyA, gen)
	r.Sync(nil)
	r.Sync(members(keyA))
	rec, ok := r.StateOf(keyA)
	if !ok || rec.DrainPending {
		t.Fatal("the marker leaked across an incarnation change")
	}
	if rec.Generation != gen+2 {
		t.Fatalf("generation = %d, want %d (removal + re-add)", rec.Generation, gen+2)
	}
}

// TestServingCarriesTheRecordedScopeFingerprint: the serving snapshot is
// what the pool builder keys a relay's runtime health by, so it must carry
// the scope fingerprint recorded with the entry. Until a scope is recorded
// (the reconciler records it with admission, in the scope-incarnation
// change) the field publishes empty — and empty must stay empty, not read
// as a scope change that would reset passive health on every rebuild.
func TestServingCarriesTheRecordedScopeFingerprint(t *testing.T) {
	r := New(Config{})
	keyA := Key{Provider: "vercel", Name: "alpha"}
	r.Sync(members(keyA))
	admit(t, r, keyA, "https://alpha.example", "rk")

	if got := r.Serving()[0].ScopeFP; got != "" {
		t.Fatalf("ScopeFP with nothing recorded = %q, want empty", got)
	}

	r.mu.Lock()
	r.entries[keyA].scopeFP = "scope-fp-1"
	r.mu.Unlock()
	if got := r.Serving()[0].ScopeFP; got != "scope-fp-1" {
		t.Fatalf("Serving ScopeFP = %q, want the recorded fingerprint", got)
	}
}

// TestServingPublishesTheSyncedScopeFingerprint wires the recording the
// runtime-key change anticipated: a member synced with a resolved pin
// publishes that pin's fingerprint with its admission — the value the pool's
// runtime keying folds into the endpoint key, so a relay's passive health
// never crosses a scope move under an unchanged name. A flip republishes the
// new scope's fingerprint with the new incarnation's admission, and a pass
// whose credential does not resolve leaves the last recorded value alone —
// an unknown scope is "no new information", not a scope change (the same
// rule TestSyncUnknownScopeNeverFlips pins for the incarnation).
func TestServingPublishesTheSyncedScopeFingerprint(t *testing.T) {
	r := New(Config{})
	keyA := Key{Provider: "vercel", Name: "alpha"}

	r.Sync([]Member{pinned(keyA, "team-a")})
	admit(t, r, keyA, "https://alpha.example", "rk")
	if got := r.Serving()[0].ScopeFP; got != (deploy.Scope{Team: "team-a"}).FP() {
		t.Fatalf("ScopeFP after a pinned sync = %q, want the pin's fingerprint", got)
	}

	r.Sync([]Member{pinned(keyA, "team-b")})
	if gen := admit(t, r, keyA, "https://alpha.example", "rk"); gen != 2 {
		t.Fatalf("generation after the flip = %d, want 2", gen)
	}
	if got := r.Serving()[0].ScopeFP; got != (deploy.Scope{Team: "team-b"}).FP() {
		t.Fatalf("ScopeFP after the flip = %q, want the new pin's fingerprint", got)
	}

	r.Sync([]Member{{Key: keyA}})
	if got := r.Serving()[0].ScopeFP; got != (deploy.Scope{Team: "team-b"}).FP() {
		t.Fatalf("ScopeFP with an unresolved scope = %q, want the last recorded fingerprint", got)
	}
}
