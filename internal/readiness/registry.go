// Package readiness is the in-memory runtime gate between a relay's
// configured identity and the serving pool. A relay exists in the desired
// configuration long before it may answer clients; readiness records the
// only truth that matters for admission: has this relay's CURRENT
// incarnation been positively verified — reachable deployment, expected
// worker version, accepted relay key, at least one successful end-to-end
// forwarding request — and nothing since then invalidated that proof?
//
// The registry is deliberately stateless across restarts: nothing here is
// persisted, because verification must be re-proven after every restart,
// never remembered. The desired-state configuration stays the only source
// of intent; this package is derived runtime state.
//
// Two orthogonal dimensions live in each entry:
//
//   - the lifecycle phase (configured, discovering, deploying, verifying,
//     ready, unready, failed, paused, removing) reported to operators. It
//     progresses as discovery and verification happen, independently of
//     admission.
//   - serving: whether the relay may answer clients. Serving flips only
//     together with the verified snapshot it was earned for — URL and relay
//     key publish atomically with admission (Ready), and every revocation
//     (Failing past the streak, Demote, Pause, removal) drops both at once.
//     The pool builder reads Serving(); a relay therefore never appears in
//     the pool without the exact URL+key pair that passed verification.
//
// Every entry carries a generation — the relay's incarnation. It is bumped
// whenever the identity leaves the desired configuration (removal) and when
// it is re-added, so every in-flight operation (verify, deploy, delete)
// that captured an older generation completes into the void: Ready,
// Failing, Demote and DeleteDone discard stale completions instead of
// mutating the newer incarnation. An operation holds the single-flight lock
// from Begin to release, which also guarantees a re-added identity cannot
// start its own deploy until a stale delete in flight has finished deleting
// the OLD project.
package readiness

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"http-relay-gateway/internal/deploy"
)

// Key is one relay's identity: (provider, name) — the deploy package's
// RelayKey under a local name. All reconciliation, single-flight and
// incarnation decisions are per key.
type Key = deploy.RelayKey

// State is the observable lifecycle phase of one relay.
type State string

const (
	StateConfigured  State = "configured"  // desired; discovery pending
	StateDiscovering State = "discovering" // resolving the deployment from the provider
	StateDiscovered  State = "discovered"  // deployment located; verification pending
	StateDeploying   State = "deploying"   // a deploy/redeploy is in flight
	StateVerifying   State = "verifying"   // a verification pass is in flight
	StateReady       State = "ready"       // verified end to end; serving
	StateUnready     State = "unready"     // was serving; verification now fails
	StateFailed      State = "failed"      // never (or no longer) verified; retrying under backoff
	StatePaused      State = "paused"      // the platform answers instead of the worker
	StateRemoving    State = "removing"    // left the desired config; remote delete in flight
)

// Failure reasons distinguish why a relay is not ready. They ride on the
// unready/failed/paused states as the sanitized, operator-facing
// explanation.
const (
	ReasonVersionFailed = "version_failed"    // deployment answers a different worker version
	ReasonAuthFailed    = "auth_failed"       // deployment rejected the relay key
	ReasonProbeFailed   = "probe_failed"      // forwarding round trip did not complete
	ReasonUnreachable   = "unreachable"       // no HTTP answer at all
	ReasonDeployFailed  = "deploy_failed"     // platform deploy errored
	ReasonPaused        = "paused"            // the platform suspended the deployment
	ReasonReplacing     = "replacing"         // admission revoked on purpose: replacement in flight
	ReasonMissing       = "missing"           // no deployment exists for this identity
	ReasonDeleteFailed  = "delete_failed"     // the remote delete errored; retried under backoff
	ReasonCredentials   = "credential_failed" // provider credential rejected, ambiguous scope, or unreadable
)

// Record is the operator-facing view of one relay's readiness.
type Record struct {
	Key         Key
	State       State
	Reason      string        // sanitized; "" when there is none
	Generation  uint64        // the relay's incarnation; bumps on removal and re-add
	Since       time.Time     // when the current phase was entered
	LastAttempt time.Time     // when the last verification (or deploy) ended
	LastResult  time.Duration // how long the last attempt took
	FailStreak  int           // consecutive failed verifications
	// DrainPending reports a replacement deferred at the drain timeout: this
	// incarnation's next rollout must re-run the settle+drain barrier before
	// deploying. It is deliberately not carried by the (state, reason) pair
	// — Failing refreshes the reason and Pause replaces the phase between
	// passes — and it clears only when the barrier completes, the relay is
	// re-admitted, or the incarnation changes.
	DrainPending bool
}

// Serving is one relay's verified admission: the identity, and the exact
// URL+relay-key pair that passed the readiness gate. The pool builder
// consumes these verbatim — a serving relay is in the pool if and only if
// it appears here.
type Serving struct {
	Key     Key
	URL     string
	Token   string
	Version uint64
	// ScopeFP is the non-reversible fingerprint of the provider scope this
	// admission was verified under — the resolved credential pins, recorded
	// with the entry alongside the serving snapshot. It is empty until the
	// registry has recorded a scope (the scope-incarnation change wires the
	// recording), and empty must publish as empty: "scope unknown" attaches
	// in the pool's runtime keying, it must not read as a scope change that
	// resets a relay's passive health on every rebuild. The pool builder
	// folds the fingerprint into the relay's runtime key so passive health
	// never crosses a scope move under an unchanged relay name.
	ScopeFP string
}

// Settings are the live-updatable retry and readiness thresholds. Every
// field must be positive; UpdateSettings leaves a non-positive field as-is.
type Settings struct {
	// BackoffBase is the retry delay after the first failure. Default 5s.
	BackoffBase time.Duration
	// BackoffMax caps the exponential growth. Default 5m.
	BackoffMax time.Duration
	// RecoverMax caps the retry delay while no relay is serving at all, so a
	// total outage heals quickly instead of waiting out a long backoff.
	// Default 15s.
	RecoverMax time.Duration
	// PauseRetry is how long a paused relay waits before its next probe —
	// the revival cadence. A platform suspension lifts on the platform's
	// schedule, not ours; probing sooner burns the platform API for nothing.
	// Default 10m.
	PauseRetry time.Duration
	// DemoteAfter is the consecutive verification failures before a serving
	// relay loses admission. Below it transient blips leave it serving —
	// passive pool health is the fast layer, this is the verified one.
	// Default 3.
	DemoteAfter int
}

// Config tunes retry/backoff behavior at construction. Zero fields take the
// defaults; tests shrink them (and inject time) to drive transitions
// deterministically.
type Config struct {
	BackoffBase time.Duration
	BackoffMax  time.Duration
	RecoverMax  time.Duration
	PauseRetry  time.Duration
	DemoteAfter int
	// Now is the clock; tests inject a fake. Defaults to time.Now.
	Now func() time.Time
}

type entry struct {
	record  Record
	serving bool
	url     string // the verified serving snapshot, published with admission
	token   string
	// scopeFP is the fingerprint of the credential scope the entry's
	// serving snapshot was verified under, recorded with admission. Empty
	// until a scope has been recorded for this incarnation.
	scopeFP string
	busy    bool      // single-flight: a verifier/deployer/deleter holds this relay
	nextTry time.Time // backoff gate for the next attempt
}

// Registry is the in-memory readiness state. All methods are safe for
// concurrent use; transitions that change pool membership notify the
// coalesced Changes channel so the applier rebuilds the serving generation.
type Registry struct {
	mu         sync.Mutex
	cfg        Config
	entries    map[Key]*entry
	readyCount int
	changes    chan struct{}
	seq        atomic.Uint64 // monotonic count of notifications, for settle barriers
}

// New builds a registry with the given configuration; zero fields take the
// documented defaults.
func New(cfg Config) *Registry {
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 5 * time.Minute
	}
	if cfg.RecoverMax <= 0 {
		cfg.RecoverMax = 15 * time.Second
	}
	if cfg.PauseRetry <= 0 {
		cfg.PauseRetry = 10 * time.Minute
	}
	if cfg.DemoteAfter <= 0 {
		cfg.DemoteAfter = 3
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Registry{cfg: cfg, entries: map[Key]*entry{}, changes: make(chan struct{}, 1)}
}

// UpdateSettings applies retry and readiness settings from a successful
// desired-state reload. It is safe to call concurrently with all registry
// transitions. Existing failed and paused entries have their gates recomputed
// from LastAttempt, so a lowered ceiling or revival cadence takes effect
// without waiting for an unrelated future failure; a running verification
// keeps its captured work and uses the new settings when it completes.
//
// The desired-state loader rejects non-positive values. The guards here make
// the exported method defensive for other callers: a non-positive field keeps
// the registry's current valid value.
func (r *Registry) UpdateSettings(settings Settings) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if settings.BackoffBase > 0 {
		r.cfg.BackoffBase = settings.BackoffBase
	}
	if settings.BackoffMax > 0 {
		r.cfg.BackoffMax = settings.BackoffMax
	}
	if settings.RecoverMax > 0 {
		r.cfg.RecoverMax = settings.RecoverMax
	}
	if settings.PauseRetry > 0 {
		r.cfg.PauseRetry = settings.PauseRetry
	}
	if settings.DemoteAfter > 0 {
		r.cfg.DemoteAfter = settings.DemoteAfter
	}
	for _, e := range r.entries {
		if e.record.LastAttempt.IsZero() {
			continue
		}
		if e.record.State == StatePaused {
			e.nextTry = e.record.LastAttempt.Add(r.cfg.PauseRetry)
			continue
		}
		if e.record.FailStreak > 0 {
			r.backoffLocked(e, e.record.LastAttempt)
		}
	}
}

// Sync reconciles the registry with the desired relay set: keys that first
// appear are announced as configured, keys that reappear while a removal
// was in flight are resurrected as a NEW incarnation (generation bumped — a
// re-added relay must re-verify from scratch, never from memory, and any
// delete/verify/deploy still in flight for the old incarnation completes
// into the void), and keys that disappeared are moved to removing with
// their generation bumped, invalidating their in-flight operations. The
// reconciler calls this on every pass, so the registry always mirrors the
// desired configuration.
func (r *Registry) Sync(present []Key) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.cfg.Now()
	seen := make(map[Key]bool, len(present))
	for _, k := range present {
		seen[k] = true
		e, ok := r.entries[k]
		if !ok {
			r.entries[k] = &entry{record: Record{Key: k, State: StateConfigured, Generation: 1, Since: now}}
			r.notifyLocked() // lifecycle changed; the operator-facing snapshot rebuilds
			continue
		}
		if e.record.State == StateRemoving {
			// Re-added while (or after) its delete was in flight: a new
			// incarnation. The stale delete finishes harmlessly — it holds
			// the single-flight, so this incarnation's own deploy waits
			// until the OLD project has actually been deleted — and its
			// completion lands on a bumped generation and is discarded.
			e.record.Generation++
			e.record.State = StateConfigured
			e.record.Reason = ""
			e.record.FailStreak = 0
			e.record.DrainPending = false
			e.nextTry = time.Time{}
			e.record.Since = now
			r.notifyLocked()
		}
	}
	for k, e := range r.entries {
		if seen[k] {
			continue
		}
		if e.record.State != StateRemoving {
			// Left the desired configuration: revoke admission first, then
			// label for removal. The generation bump invalidates any verify,
			// deploy or delete this incarnation had in flight.
			if e.serving {
				e.serving = false
				r.readyCount--
			}
			e.record.State = StateRemoving
			e.record.Reason = ""
			e.record.Generation++
			e.record.DrainPending = false // the incarnation's replacement story ends here
			e.record.Since = now
			r.notifyLocked()
		}
	}
}

// IsReady reports whether key is admitted to the serving pool — the gate
// every generation build applies per relay.
func (r *Registry) IsReady(key Key) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	return ok && e.serving
}

// ReadyCount reports how many relays currently hold admission.
func (r *Registry) ReadyCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readyCount
}

// Serving returns every relay's verified admission snapshot, sorted by key
// for deterministic pool builds. This is the ONLY view the pool builder
// consumes: membership and the URL+key pair flip atomically under the same
// lock, so a serving relay can never be built into a pool with a URL or key
// its verification did not pass for.
func (r *Registry) Serving() []Serving {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Serving, 0, r.readyCount)
	for _, e := range r.entries {
		if e.serving {
			out = append(out, Serving{
				Key: e.record.Key, URL: e.url, Token: e.token,
				Version: e.record.Generation, ScopeFP: e.scopeFP,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key.String() < out[j].Key.String()
	})
	return out
}

// StateOf returns the current record for key.
func (r *Registry) StateOf(key Key) (Record, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return Record{}, false
	}
	return e.record, true
}

// Snapshot returns every entry's record, sorted by key for deterministic
// output. /stats renders this.
func (r *Registry) Snapshot() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := make([]Record, 0, len(r.entries))
	for _, e := range r.entries {
		records = append(records, e.record)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Key.String() < records[j].Key.String()
	})
	return records
}

// Changes returns the coalesced notification channel: a buffered signal
// fires (at most once per burst) whenever pool membership could have
// changed, so the applier can rebuild the serving generation.
func (r *Registry) Changes() <-chan struct{} { return r.changes }

// NotifySeq reports the monotonic count of notifications emitted so far.
// The applier records the value it has applied; Settle-style barriers wait
// until the rebuild triggered by a given notification has been swapped in.
func (r *Registry) NotifySeq() uint64 { return r.seq.Load() }

// GenerationOf returns the relay's current incarnation. Callers capture it
// before starting work and pass it to the completion APIs, which discard
// the outcome when the generation has moved on.
func (r *Registry) GenerationOf(key Key) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return 0, false
	}
	return e.record.Generation, true
}

// Begin takes the single-flight hold on key and returns the incarnation the
// caller operates on. The release function MUST be called exactly once.
// ok=false when another verifier, deployer or deleter already holds the
// relay — that caller is the only one operating on it. This is what keeps
// concurrent reconciliation paths from launching duplicate work for one
// relay, and what guarantees a re-added identity cannot deploy until an
// in-flight stale delete has finished deleting the old project.
func (r *Registry) Begin(key Key) (release func(), generation uint64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entry(key)
	if e.busy {
		return nil, 0, false
	}
	e.busy = true
	gen := e.record.Generation
	return func() {
		r.mu.Lock()
		e.busy = false
		r.mu.Unlock()
	}, gen, true
}

// BeginDelete takes the single-flight hold for a remote delete: it succeeds
// only while the relay is actually labeled removing. A re-added identity
// (state configured again) can no longer be deleted by the stale operation.
func (r *Registry) BeginDelete(key Key) (release func(), generation uint64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok || e.busy || e.record.State != StateRemoving {
		return nil, 0, false
	}
	e.busy = true
	gen := e.record.Generation
	return func() {
		r.mu.Lock()
		e.busy = false
		r.mu.Unlock()
	}, gen, true
}

// current returns the entry only when it still exists under the given
// incarnation — the guard every completion API applies.
func (r *Registry) current(key Key, generation uint64) *entry {
	e, ok := r.entries[key]
	if !ok || e.record.Generation != generation {
		return nil
	}
	return e
}

// Allow reports whether key's backoff gate permits another attempt now.
// Failed attempts push nextTry forward exponentially; success resets it, so
// a healthy relay is re-checked every scan tick and a failing one never
// hot-loops.
func (r *Registry) Allow(key Key) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[key]; ok {
		return !r.cfg.Now().Before(e.nextTry)
	}
	return true
}

// Enter moves key to an explicit phase (discovering, deploying, verifying,
// …) without touching admission: a relay being replaced keeps serving under
// its previous verification, and a relay that never verified stays out. A
// paused relay stays paused through the routine phases (discovering,
// verifying): a sync pass runs Enter(discovering) before every probe, and a
// transport blip during revival must not relabel the suspension as an
// ordinary failure — the revival cadence owns the relay until a definitive
// verdict (ready, a deploy, a missing worker) changes it.
// Stale completions (generation moved on) are discarded. Only the deploying
// phase counts as a genuinely fresh attempt — it clears the failure streak
// and reopens the backoff gate. Routine verification (verifying) must NOT
// reset them: a serving relay that starts failing racks up its DemoteAfter
// streak across consecutive scan ticks, and a failing relay's backoff keeps
// the scan from hot-looping it.
//
// A phase that actually changed notifies so the operator view (/stats
// renders the lifecycle from the applied generation) follows it — with one
// exception: entering the discovering phase never notifies, because every
// scan pass re-enters it for every relay, healthy ones included, and
// notifying it would rebuild the serving view on every pass. A repeated
// Enter of the phase already held is always silent.
func (r *Registry) Enter(key Key, generation uint64, state State, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion: the incarnation moved on
	}
	if e.record.State == StatePaused && (state == StateDiscovering || state == StateVerifying) {
		return // routine progress must not clobber the pause marker
	}
	changed := e.record.State != state || e.record.Reason != reason
	e.record.State = state
	e.record.Reason = reason
	e.record.Since = r.cfg.Now()
	if changed && state != StateDiscovering {
		r.notifyLocked()
	}
	if state == StateDeploying {
		e.record.FailStreak = 0
		e.nextTry = time.Time{}
	}
}

// Ready records a completed verification that passed end to end and
// publishes the relay's admission atomically with the exact URL+relay-key
// pair that earned it. Stale completions (generation moved on, entry
// deleted) are discarded. Notifies only when the relay newly gains
// admission — re-verifying an already-serving relay leaves the pool
// unchanged.
func (r *Registry) Ready(key Key, generation uint64, url, token string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion
	}
	now := r.cfg.Now()
	e.url, e.token = url, token
	e.record.State = StateReady
	e.record.Reason = ""
	e.record.Since = now
	e.record.LastAttempt = now
	e.record.LastResult = duration
	e.record.FailStreak = 0
	e.record.DrainPending = false // re-admission ends any deferred replacement
	e.nextTry = time.Time{}
	if !e.serving {
		e.serving = true
		r.readyCount++
		r.notifyLocked()
	}
}

// Failing records one verification failure. A serving relay keeps serving
// until DemoteAfter consecutive failures — transient blips are the passive
// health layer's job, not this one's; past the threshold it loses admission
// as unready. A relay that never verified goes to failed; a paused relay
// stays paused (the pause gate owns it) with its reason refreshed. The
// backoff gate doubles with each failure, capped, and capped again harder
// while nothing at all is serving so a total outage heals quickly. Stale
// completions are discarded.
func (r *Registry) Failing(key Key, generation uint64, reason string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion
	}
	now := r.cfg.Now()
	e.record.LastAttempt = now
	e.record.LastResult = duration
	e.record.FailStreak++
	e.record.Reason = reason
	if !e.serving {
		if e.record.State == StatePaused {
			// A non-suspension answer during a pause (a generic 5xx, a
			// transport blip) must not erode the revival cadence into the
			// ordinary backoff: keep the pause gate at PauseRetry until a
			// marked suspension answer or revival re-arms it.
			e.nextTry = now.Add(r.cfg.PauseRetry)
			return
		}
		switch e.record.State {
		case StateUnready, StateFailed:
			// already labeled; keep the phase stable across retries
		default:
			e.record.State = StateFailed
			e.record.Since = now
			// The relay never served, so pool membership is unchanged — but
			// its lifecycle record did change, and the serving generation is
			// what /stats renders it from. Coalescing keeps a failing fleet
			// at one pending rebuild per burst anyway.
			r.notifyLocked()
		}
		r.backoffLocked(e, now)
		return
	}
	if e.record.FailStreak < r.cfg.DemoteAfter {
		// One or two blips do not pull a verified relay out of rotation,
		// but the failed attempt still arms the retry gate.
		r.backoffLocked(e, now)
		return
	}
	// The streak is spent: revoke admission first, THEN arm the gate — the
	// demotion itself must see readyCount drop before the gate is computed,
	// or the last relay to fail in a total outage arms its first retry
	// outside the RecoverMax ceiling and the outage heals slowly.
	e.serving = false
	r.readyCount--
	e.record.State = StateUnready
	e.record.Since = now
	r.backoffLocked(e, now)
	r.notifyLocked()
}

// Demote revokes key's admission immediately, bypassing the DemoteAfter blip
// tolerance, and labels the relay unready. Used when a replacement rollout
// starts (Strategy A: the old admission must be gone before the platform can
// switch the production URL) and when a replacement the platform already
// switched to fails verification. A relay that never served is labeled
// failed; the backoff gate is armed so the scan does not hot-loop it. Stale
// completions are discarded.
func (r *Registry) Demote(key Key, generation uint64, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion
	}
	now := r.cfg.Now()
	e.record.LastAttempt = now
	e.record.Reason = reason
	e.record.FailStreak = r.cfg.DemoteAfter
	if !e.serving {
		if e.record.State != StateUnready && e.record.State != StateFailed {
			e.record.State = StateFailed
			e.record.Since = now
			r.notifyLocked()
		}
		r.backoffLocked(e, now)
		return
	}
	// Revoke admission before arming the gate: the demotion must count in
	// readyCount when the gate is computed, so a demotion that empties the
	// serving set gets the RecoverMax ceiling on its very first retry.
	e.serving = false
	r.readyCount--
	e.record.State = StateUnready
	e.record.Since = now
	r.backoffLocked(e, now)
	r.notifyLocked()
}

// Pause revokes key's admission immediately and labels it paused: the
// platform answered instead of the worker (a suspension page), and no
// client may ever see that page through the gateway. No deploy can lift a
// platform suspension, so revival is just the ordinary pass re-probing
// once the pause gate opens: the gate is armed at the revival cadence,
// not the ordinary backoff, and the relay rejoins automatically when the
// worker answers again. Stale completions are discarded.
func (r *Registry) Pause(key Key, generation uint64, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion
	}
	now := r.cfg.Now()
	e.record.LastAttempt = now
	e.record.Reason = reason
	e.nextTry = now.Add(r.cfg.PauseRetry)
	if e.record.State != StatePaused {
		e.record.State = StatePaused
		e.record.Since = now
	}
	if e.serving {
		e.serving = false
		r.readyCount--
	}
	r.notifyLocked()
}

// DeferDrain marks this incarnation's replacement as deferred at the drain
// timeout: the retry pass must re-run the settle+drain barrier before
// deploying. The marker is deliberately NOT the (state, reason) pair —
// Failing refreshes the reason and Pause replaces the phase between passes,
// and either would silently drop the barrier and let the deploy fire under
// the very straggler the ordering exists to protect. It survives that churn
// and clears on DrainCompleted, Ready, or an incarnation change. Stale
// completions are discarded.
func (r *Registry) DeferDrain(key Key, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion: the incarnation moved on
	}
	e.record.DrainPending = true
}

// DrainCompleted clears the deferred-drain marker: the settle+drain barrier
// passed and the old incarnation is quiet. Deploy retries from here no
// longer re-drain — admission stays revoked, so in-flight traffic can only
// shrink. Stale completions are discarded.
func (r *Registry) DrainCompleted(key Key, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil {
		return // stale completion: the incarnation moved on
	}
	e.record.DrainPending = false
}

// DeleteDone purges key after its remote delete completed under the given
// incarnation. Stale completions are discarded: a re-added identity (new
// incarnation) stays exactly as it is.
func (r *Registry) DeleteDone(key Key, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil || e.record.State != StateRemoving {
		return // stale completion, or the identity was re-added
	}
	delete(r.entries, key)
	r.notifyLocked()
}

// DeleteFailed records a failed remote delete so the operator sees why the
// relay still shows as removing; the retry is backoff-gated like every
// other attempt — the failure counts toward the streak, so repeated
// failures arm an ever-longer gate. Stale completions are discarded.
func (r *Registry) DeleteFailed(key Key, generation uint64, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current(key, generation)
	if e == nil || e.record.State != StateRemoving {
		return
	}
	e.record.FailStreak++
	e.record.Reason = reason
	r.backoffLocked(e, r.cfg.Now())
}

// backoffLocked arms key's retry gate after a failure: exponential base,
// capped at BackoffMax, and capped harder at RecoverMax while nothing at
// all is serving so a total outage heals quickly. Every caller increments
// FailStreak first, so the streak is at least one here — the clamp keeps a
// future zero-streak caller at the base delay instead of a negative-shift
// panic that would take the process down.
func (r *Registry) backoffLocked(e *entry, now time.Time) {
	wait := r.cfg.BackoffBase << min(max(e.record.FailStreak-1, 0), 30)
	if wait > r.cfg.BackoffMax || wait <= 0 {
		wait = r.cfg.BackoffMax
	}
	if r.readyCount == 0 && wait > r.cfg.RecoverMax {
		wait = r.cfg.RecoverMax
	}
	e.nextTry = now.Add(wait)
}

func (r *Registry) entry(key Key) *entry {
	e, ok := r.entries[key]
	if !ok {
		e = &entry{record: Record{Key: key, State: StateConfigured, Generation: 1, Since: r.cfg.Now()}}
		r.entries[key] = e
	}
	return e
}

func (r *Registry) notifyLocked() {
	r.seq.Add(1)
	select {
	case r.changes <- struct{}{}:
	default:
	}
}
