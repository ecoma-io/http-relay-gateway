// Runtime health is the half of the pool that must outlive the serving
// generation it was earned in. A Pool is immutable once built and a rebuild
// constructs fresh Relay values — but the counters, the cooldown and the
// rotation position are runtime state of the endpoint, not of the membership
// snapshot, so they live here, keyed by RuntimeState and attached to every
// generation that serves the same endpoint (issue #15: a flapping sibling
// relay used to wipe a healthy relay's cooldown on every registry
// notification, and every rebuild restarted the rotation at the first
// relay).
//
// Nothing here is persisted: a restart re-verifies the fleet and starts
// every endpoint's state from zero, by design.
package pool

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"http-relay-gateway/internal/sanitize"
)

// RuntimeKey fingerprints everything the counters are meaningful for: the
// upstream endpoint identity. Provider+Name is the relay's configuration
// identity, but it is not the whole endpoint — the same identity answers
// from a different endpoint when a scope pin moves the name to another
// platform scope, when discovery lands on a different URL, or when the relay
// key rotates (a different worker sits behind the same URL). ScopeFP, URL
// and TokenFP pin those; a change in any of them is a different endpoint
// whose runtime health starts clean.
//
// The readiness generation is deliberately NOT part of the key: runtime
// health follows the endpoint, not control-plane churn. A rebuild for an
// unrelated relay, an accepted settings reload, or a replacement rollout
// must re-attach the same state, never reset it.
//
// ScopeFP is a plain string so this package stays dependency-light — the
// caller fills it from its own scope fingerprint; pool never imports
// internal/deploy. TokenFP is a short fingerprint of the relay key: it must
// never be logged, served or rendered anywhere.
type RuntimeKey struct {
	Provider string
	Name     string
	ScopeFP  string
	URL      string
	TokenFP  string
}

// identity is the configuration identity ("provider/name") the key belongs
// to — the string Prune filters by and /stats renders.
func (k RuntimeKey) identity() string { return k.Provider + "/" + k.Name }

// TokenFingerprint reduces a relay key to 16 hex chars: enough to tell keys
// apart inside a runtime key, nothing like enough to recover or display one.
func TokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:8])
}

// RelayState is one endpoint's runtime health: the usage counters, the
// consecutive-failure streak, the active cooldown and the sanitized last
// error. It is deliberately not owned by a Pool — Relay embeds the shared
// value handed out by RuntimeState.Attach, so a serving rebuild re-attaches
// the same state instead of constructing a fresh one.
type RelayState struct {
	// Requests and Failures are the serving counters /stats renders.
	Requests atomic.Int64
	Failures atomic.Int64
	// MidstreamFailures counts failures observed after the relay had
	// already answered (the relay hot path populates it when the midstream
	// rework lands); carried now so the runtime keying and the /stats
	// surface land together, and so a later change does not move the stats
	// contract twice.
	MidstreamFailures atomic.Int64

	mu          sync.Mutex
	consecFails int
	downUntil   time.Time
	lastErr     string
}

func (rs *RelayState) healthy(now time.Time) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.downUntil.IsZero() || now.After(rs.downUntil)
}

// health is the cooldown view /stats renders per relay: whether the relay
// picks (half-open included) and its sanitized last error, under one lock.
func (rs *RelayState) health(now time.Time) (up bool, lastErr string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.downUntil.IsZero() || now.After(rs.downUntil), rs.lastErr
}

// RecordSuccess clears the failure streak and brings the relay back up.
// The caller records it only for a COMPLETED response — a body that fully
// copied — never for a header receipt: a success that resets the streak must
// be evidence the relay can still carry a whole answer, so consecutive
// post-header failures accumulate to the threshold like any transport streak.
func (rs *RelayState) RecordSuccess() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.consecFails = 0
	rs.downUntil = time.Time{}
	rs.lastErr = ""
}

// RecordFailure bumps the failure streak; after `threshold` consecutive
// failures the relay goes on cooldown (passive health) and is skipped until
// it expires (half-open recovery). The stored label is sanitized: transport
// errors embed the relay URL, which never reaches /stats.
func (rs *RelayState) RecordFailure(err error, threshold int, cooldown time.Duration) {
	rs.Failures.Add(1)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.consecFails++
	rs.lastErr = sanitize.ErrorString(err)
	if rs.consecFails >= threshold {
		rs.downUntil = time.Now().Add(cooldown)
		rs.consecFails = 0
	}
}

// RuntimeState is the process-lifetime home of runtime health. It outlives
// every Pool: each serving generation attaches the same RelayState for the
// same endpoint and reads the same per-selector rotation cursors, so
// rebuilds carry the state instead of discarding it.
//
// Attach and Prune are called only from the applier goroutine — the single
// writer of serving generations. Pick reads the shared cursors from request
// goroutines, so everything is mutex-guarded regardless; the ownership rule
// exists to serialize the endpoint→state mapping against the rebuilds, not
// to replace the lock.
type RuntimeState struct {
	mu      sync.Mutex
	entries map[RuntimeKey]*RelayState
	// owners maps the configuration identity ("provider/name") to the key
	// currently attached for it, so a changed scope, URL or credential can
	// purge the endpoint state it replaced.
	owners map[string]RuntimeKey
	rr     map[string]int
}

// NewRuntimeState builds the empty runtime state a process carries for its
// whole lifetime.
func NewRuntimeState() *RuntimeState {
	return &RuntimeState{
		entries: map[RuntimeKey]*RelayState{},
		owners:  map[string]RuntimeKey{},
		rr:      map[string]int{},
	}
}

// Attach returns the RelayState for key, creating it on first sight — and
// purging whatever other key the same configuration identity owned before:
// a changed scope, URL or relay key is a different endpoint, and the old
// endpoint's counters and cooldown must not attach to (or survive behind)
// the new one. Called only from the applier goroutine.
func (rt *RuntimeState) Attach(k RuntimeKey) *RelayState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if prev, ok := rt.owners[k.identity()]; ok && prev != k {
		delete(rt.entries, prev)
	}
	rs, ok := rt.entries[k]
	if !ok {
		rs = &RelayState{}
		rt.entries[k] = rs
	}
	rt.owners[k.identity()] = k
	return rs
}

// Prune drops the runtime state of every identity absent from desired — the
// "provider/name" set of the last-known-good configuration — together with
// the rotation cursors that can no longer occur ("all" and every provider
// still present are kept). A removed relay's counters and cooldown end
// here, so a re-added identity starts clean; a transiently demoted relay (a
// Strategy A replacement) is still desired, so its state survives — the
// endpoint did not change. Called only from the applier goroutine, before
// the rebuild it precedes is swapped in.
func (rt *RuntimeState) Prune(desired map[string]struct{}) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	providers := map[string]bool{}
	for id := range desired {
		provider, _, _ := strings.Cut(id, "/")
		providers[provider] = true
	}
	for k := range rt.entries {
		if _, ok := desired[k.identity()]; !ok {
			delete(rt.entries, k)
			if rt.owners[k.identity()] == k {
				delete(rt.owners, k.identity())
			}
		}
	}
	for sel := range rt.rr {
		if sel == KeyAll || providers[sel] {
			continue
		}
		delete(rt.rr, sel)
	}
}

// CursorFor returns the next rotation index for a healthy-candidate list of
// n relays and advances the key's cursor past it (`i := cursor % n; cursor
// = i + 1`). The modulo makes a persisted cursor safe across membership
// changes: a cursor that outlives the list it indexed simply wraps. The keys
// are the selector keys ("all" or a provider) and the cursors are shared by
// every pool built on this RuntimeState, so a rebuild continues the rotation
// instead of restarting it at the first relay.
func (rt *RuntimeState) CursorFor(key string, n int) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	i := rt.rr[key] % n
	rt.rr[key] = i + 1
	return i
}
