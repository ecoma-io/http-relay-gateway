package pool

import (
	"errors"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustAttached builds an attached pool over one relay input set, with the
// endpoint keys derived the way buildState derives them (identity + URL +
// token fingerprint, no scope recorded).
func mustAttached(t *testing.T, rt *RuntimeState, in Input) *Pool {
	t.Helper()
	for i := range in.Relays {
		r := in.Relays[i]
		in.Relays[i].Runtime = RuntimeKey{
			Provider: r.Provider,
			Name:     r.Name,
			URL:      r.URL.String(),
			TokenFP:  TokenFingerprint(r.Token),
		}
	}
	p, err := NewAttached(in, rt)
	if err != nil {
		t.Fatalf("pool.NewAttached: %v", err)
	}
	return p
}

// TestAttachReusesCountersAcrossRebuilds is the issue #15 pin: two pools
// built on one RuntimeState — the shape of two consecutive serving
// generations — must share the endpoint's counters, failure streak and
// cooldown, and the rotation must continue where the previous generation
// left it instead of restarting at the first relay.
func TestAttachReusesCountersAcrossRebuilds(t *testing.T) {
	rt := NewRuntimeState()
	in := Input{
		FailureThreshold: 1,
		Cooldown:         time.Hour,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel"), relayInput(t, "beta", "cloudflare")},
	}
	first := mustAttached(t, rt, in)

	// Warm the counters, cool alpha down, and advance the rotation twice.
	alpha, beta := first.Pick(KeyAll), first.Pick(KeyAll)
	if alpha.Name != "alpha" || beta.Name != "beta" {
		t.Fatalf("first picks = %s, %s; want alpha, beta (input order)", alpha.Name, beta.Name)
	}
	alpha.Requests.Add(4)
	first.RecordFailure(alpha, errors.New("dial relay: connection refused"))
	if row := first.Stats()[indexOf(first, "alpha")]; row.Healthy || row.Failures != 1 {
		t.Fatalf("alpha after the failure = %+v, want cooled down with 1 failure", row)
	}

	second := mustAttached(t, rt, in) // the "rebuild": unrelated new generation

	rows := map[string]StatsRow{}
	for _, row := range second.Stats() {
		rows[row.Name] = row
	}
	if row := rows["alpha"]; !reflect.DeepEqual(row, StatsRow{
		Name: "alpha", Provider: "vercel", MaxBody: VercelMaxBody,
		Failures: 1, Requests: 4,
		LastError: "dial relay: connection refused",
	}) {
		t.Fatalf("alpha's runtime state did not survive the rebuild: %+v", row)
	}
	if row := rows["beta"]; !row.Healthy || row.Requests != 0 || row.Failures != 0 {
		t.Fatalf("beta must be untouched: %+v", row)
	}

	// The rotation continues, and skips the cooled relay the way Pick
	// always has: healthy = [beta], so beta serves, then alpha (best
	// effort) once it is the only candidate left.
	if got := second.Pick(KeyAll); got.Name != "beta" {
		t.Fatalf("rebuild pick = %s, want beta (rotation carried, cooldown respected)", got.Name)
	}

	// And a third generation sees the same state the second did — attach,
	// not accumulate.
	third := mustAttached(t, rt, in)
	if row := third.Stats()[indexOf(third, "alpha")]; row.Failures != 1 || row.Requests != 4 {
		t.Fatalf("alpha's counters drifted across generations: %+v", row)
	}
}

// indexOf returns a relay's position in a pool's stats snapshot.
func indexOf(p *Pool, name string) int {
	for i, row := range p.Stats() {
		if row.Name == name {
			return i
		}
	}
	return -1
}

// TestAttachDoesNotCrossScopeURLorToken: the configuration identity is not
// the whole endpoint — a changed scope pin, URL or relay key is a different
// endpoint, its state must start clean, and the old endpoint's state must
// not survive behind the identity to reattach later.
func TestAttachDoesNotCrossScopeURLorToken(t *testing.T) {
	base := RuntimeKey{
		Provider: "vercel", Name: "alpha",
		ScopeFP: "scope-a", URL: "http://alpha.example.internal", TokenFP: "tok-1",
	}

	for _, tc := range []struct {
		name string
		next RuntimeKey
	}{
		{"scope moved", RuntimeKey{Provider: "vercel", Name: "alpha", ScopeFP: "scope-b", URL: base.URL, TokenFP: base.TokenFP}},
		{"url moved", RuntimeKey{Provider: "vercel", Name: "alpha", ScopeFP: base.ScopeFP, URL: "http://elsewhere.example.internal", TokenFP: base.TokenFP}},
		{"relay key rotated", RuntimeKey{Provider: "vercel", Name: "alpha", ScopeFP: base.ScopeFP, URL: base.URL, TokenFP: "tok-2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewRuntimeState()
			first := rt.Attach(base)
			first.Requests.Add(7)
			first.RecordFailure(errors.New("boom"), 1, time.Hour)

			second := rt.Attach(tc.next)
			if second == first {
				t.Fatal("a changed endpoint attached the old endpoint's state")
			}
			if second.Requests.Load() != 0 || second.Failures.Load() != 0 {
				t.Fatalf("new endpoint starts dirty: requests=%d failures=%d",
					second.Requests.Load(), second.Failures.Load())
			}

			// The old state is gone, not merely displaced: re-attaching the
			// old key hands out a fresh state, never the mutated one.
			again := rt.Attach(base)
			if again == first || again.Requests.Load() != 0 || again.Failures.Load() != 0 {
				t.Fatalf("the replaced endpoint's state survived: %+v", again)
			}
		})
	}

	t.Run("same key reattaches the same state", func(t *testing.T) {
		rt := NewRuntimeState()
		k := base
		first := rt.Attach(k)
		first.Requests.Add(2)
		if rt.Attach(k) != first {
			t.Fatal("an unchanged endpoint must keep its state across rebuilds")
		}
	})
}

// TestPruneDropsPurgedRelays: a relay that left the desired configuration
// loses its counters and cooldown (a re-added identity starts clean), a
// still-desired relay keeps them, and the rotation cursors of selectors
// that can no longer occur are dropped.
func TestPruneDropsPurgedRelays(t *testing.T) {
	rt := NewRuntimeState()
	in := Input{
		FailureThreshold: 1,
		Cooldown:         time.Hour,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel"), relayInput(t, "beta", "cloudflare")},
	}
	p := mustAttached(t, rt, in)
	alpha := p.Pick(KeyAll)
	p.RecordFailure(alpha, errors.New("boom"))
	// Arm every selector's cursor so the prune has something to keep and
	// something to drop: cloudflare is about to lose its last provider.
	p.Pick("vercel")
	p.Pick("cloudflare")

	rt.Prune(map[string]struct{}{"vercel/alpha": {}}) // beta left the desired config

	// alpha is still desired: its cooldown survives the prune...
	if got := rt.Attach(RuntimeKey{
		Provider: "vercel", Name: "alpha",
		URL: alpha.URL.String(), TokenFP: TokenFingerprint(alpha.Token),
	}); got != alpha.RelayState {
		t.Fatal("prune dropped the state of a relay that is still desired")
	}
	// ...beta's is gone: re-attaching its key hands out a fresh state.
	fresh := rt.Attach(RuntimeKey{
		Provider: "cloudflare", Name: "beta",
		URL: "http://beta.cloudflare.internal", TokenFP: TokenFingerprint("token-beta"),
	})
	if fresh.Failures.Load() != 0 || fresh.Requests.Load() != 0 {
		t.Fatalf("a removed relay's state survived the prune: %+v", fresh)
	}

	// The cloudflare selector can no longer occur: its cursor was dropped.
	rt.mu.Lock()
	_, vercelCursor := rt.rr["vercel"]
	_, cloudflareCursor := rt.rr["cloudflare"]
	_, allCursor := rt.rr[KeyAll]
	rt.mu.Unlock()
	if !vercelCursor || !allCursor {
		t.Fatal("prune must keep the cursors of selectors that can still occur (vercel, all)")
	}
	if cloudflareCursor {
		t.Fatal("prune must drop the cursor of a provider that left the desired config")
	}
}

// TestCursorForWrapsAcrossMembershipChanges: a persisted cursor outlives
// the list it indexed and the modulo keeps it in range — the rotation
// continues rather than restarting or panicking when membership shrinks or
// grows under it.
// TestAttachCarriesASubThresholdStreak pins the part of endpoint runtime
// health counters alone cannot show: an attached rebuild carries the
// consecutive-failure streak while it is still below the threshold. A second
// failure on the rebuilt pool must therefore trip the cooldown. The mutation
// this kills rezeros consecFails in Attach: counters still survive and every
// threshold-1 test still passes, but the relay remains healthy here.
func TestAttachCarriesASubThresholdStreak(t *testing.T) {
	rt := NewRuntimeState()
	in := Input{
		FailureThreshold: 2,
		Cooldown:         24 * time.Hour,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel")},
	}
	first := mustAttached(t, rt, in)
	alpha := first.Pick(KeyAll)
	first.RecordFailure(alpha, errors.New("first failure"))
	if row := first.Stats()[0]; !row.Healthy || row.Failures != 1 {
		t.Fatalf("first sub-threshold failure = %+v, want healthy with one failure", row)
	}

	second := mustAttached(t, rt, in) // the serving-generation rebuild
	second.RecordFailure(second.Pick(KeyAll), errors.New("second failure"))
	if row := second.Stats()[0]; row.Healthy || row.Failures != 2 {
		t.Fatalf("second failure after attach = %+v, want cooldown from the carried sub-threshold streak", row)
	}
}

// TestRuntimePickAttachPruneStress exposes the runtime state against the
// race detector: request goroutines Pick from one attached pool while the
// applier shape concurrently attaches same and other identities and prunes
// changing desired sets. Production gives Attach/Prune one applier owner;
// this deliberately violates that ownership to prove the locks that protect
// Pick's shared cursors and endpoint mapping are complete. Assertions are
// intentionally minimal — every non-nil Pick must be one of the relays in
// the immutable serving pool — because `go test -race` is the point.
func TestRuntimePickAttachPruneStress(t *testing.T) {
	rt := NewRuntimeState()
	in := Input{
		FailureThreshold: 3,
		Cooldown:         time.Second,
		Relays: []RelayInput{
			relayInput(t, "alpha", "vercel"),
			relayInput(t, "bravo", "cloudflare"),
		},
	}
	p := mustAttached(t, rt, in)
	stop := make(chan struct{})
	var bad atomic.Bool
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r := p.Pick(KeyAll)
				if r == nil || (r.Name != "alpha" && r.Name != "bravo") {
					bad.Store(true)
					return
				}
			}
		}()
	}

	// Attach the same endpoint (the common serving-rebuild path), alternate
	// another key under the same identity (replacement path), attach a
	// distinct identity, and prune it again. All run alongside Pick above.
	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		alpha := in.Relays[0]
		alpha.Runtime = RuntimeKey{
			Provider: alpha.Provider, Name: alpha.Name,
			URL: alpha.URL.String(), TokenFP: TokenFingerprint(alpha.Token),
		}
		rt.Attach(alpha.Runtime)
		if i%2 == 0 {
			rt.Attach(RuntimeKey{
				Provider: "vercel", Name: "alpha", URL: "http://alpha-next.vercel.internal", TokenFP: "rotated",
			})
		}
		bravo := in.Relays[1]
		rt.Attach(RuntimeKey{
			Provider: bravo.Provider, Name: bravo.Name,
			URL: bravo.URL.String(), TokenFP: TokenFingerprint(bravo.Token),
		})
		rt.Prune(map[string]struct{}{
			"vercel/alpha":     {},
			"cloudflare/bravo": {},
		})
	}
	close(stop)
	wg.Wait()
	if bad.Load() {
		t.Fatal("Pick returned a relay outside the immutable attached pool")
	}
}

func TestCursorForWrapsAcrossMembershipChanges(t *testing.T) {
	rt := NewRuntimeState()
	for range 5 {
		rt.CursorFor(KeyAll, 3)
	}
	// cursor = 5; the list shrinks to one relay (a fleet collapsed to its
	// last healthy member) and the cursor must simply wrap onto it.
	if got := rt.CursorFor(KeyAll, 1); got != 0 {
		t.Fatalf("CursorFor(n=1) = %d, want 0", got)
	}
	// ...and grow again: still in range, still advancing.
	if got := rt.CursorFor(KeyAll, 2); got != 1 {
		t.Fatalf("CursorFor(n=2) after a wrap = %d, want 1 (cursor advanced, then mod 2)", got)
	}
}

// TestAttachedPoolSharesCursors pins the pool-level wiring: two pools built
// on one RuntimeState rotate through the same cursor, so a rebuild does not
// restart the served sequence at the first relay (the skew the maintainer
// noted on issue #15).
func TestAttachedPoolSharesCursors(t *testing.T) {
	rt := NewRuntimeState()
	in := Input{
		FailureThreshold: 3,
		Cooldown:         time.Second,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel"), relayInput(t, "beta", "cloudflare")},
	}
	first := mustAttached(t, rt, in)
	for range 3 {
		first.Pick(KeyAll) // alpha, beta, alpha — cursor now at 3
	}

	second := mustAttached(t, rt, in)
	if got := second.Pick(KeyAll); got.Name != "beta" {
		t.Fatalf("first pick of the rebuilt pool = %s, want beta (cursor carried over, 3 mod 2 = 1)", got.Name)
	}

	// A standalone pool is untouched by all of this: fresh cursors, fresh
	// state, starting the rotation at its own first relay.
	standalone := mustPool(t, in)
	if got := standalone.Pick(KeyAll); got.Name != "alpha" {
		t.Fatalf("standalone pool pick = %s, want alpha (fresh rotation)", got.Name)
	}
	if standalone.Stats()[indexOf(standalone, "alpha")].Requests != 0 {
		t.Fatal("a standalone pool must not share counters with an attached one")
	}
}

// TestTokenFingerprintIsShortAndDistinct: the fingerprint separates relay
// keys inside a runtime key without ever resembling one.
func TestTokenFingerprintIsShortAndDistinct(t *testing.T) {
	a := TokenFingerprint("relay-key-one")
	b := TokenFingerprint("relay-key-two")
	if len(a) != 16 || len(b) != 16 {
		t.Fatalf("fingerprint length = %d/%d, want 16 hex chars", len(a), len(b))
	}
	if a == b {
		t.Fatal("distinct relay keys must fingerprint apart")
	}
	if TokenFingerprint("relay-key-one") != a {
		t.Fatal("the fingerprint must be deterministic")
	}
}

// TestRuntimeNilStateBehavesLikeNew: NewAttached tolerates a nil
// RuntimeState — the standalone shape — so a caller that has no
// process-lifetime state to attach gets exactly the disposable pool.
func TestRuntimeNilStateBehavesLikeNew(t *testing.T) {
	u, err := url.Parse("http://alpha.vercel.internal")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	in := Input{
		FailureThreshold: 1,
		Cooldown:         time.Hour,
		Relays:           []RelayInput{{Name: "alpha", Provider: "vercel", URL: u, Token: "t", MaxBody: VercelMaxBody}},
	}
	p, err := NewAttached(in, nil)
	if err != nil {
		t.Fatalf("NewAttached(nil): %v", err)
	}
	r := p.Pick(KeyAll)
	p.RecordFailure(r, errors.New("boom"))
	if row := p.Stats()[0]; row.Healthy || row.Failures != 1 {
		t.Fatalf("nil-state pool health broken: %+v", row)
	}
}
