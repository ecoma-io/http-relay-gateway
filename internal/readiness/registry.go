// Package readiness is the in-memory runtime gate between a relay's
// configuration/deployment and the serving pool. A relay exists in the
// database long before it may answer clients; readiness records the only
// truth that matters for admission: has this relay been positively verified
// — reachable deployment, expected version, accepted token, at least one
// successful end-to-end forwarding request — since it last changed?
//
// The registry is deliberately stateless: nothing here survives a restart,
// and nothing here is persisted, because verification must be re-proven
// after every restart, never remembered. The database stays the single
// source of configuration truth; this package is derived runtime state.
//
// Two orthogonal facts live in each entry:
//
//   - phase: the lifecycle label (configured, deploying, verifying, …)
//     reported to operators. It progresses as verification and deployment
//     happen, independently of admission.
//   - serving: whether the relay's last completed verification still holds
//     AND the relay may answer clients. This is what the pool builder reads.
//
// They diverge in exactly one place: replacing a healthy relay. While the
// replacement deploys, phase reads "deploying" but the relay keeps serving
// under its previous verified deployment — nothing unverified is ever
// admitted, and the previous healthy relay stays in rotation until the
// replacement verifies (or the platform-side deploy succeeded, in which
// case the old deployment no longer exists and the new one serves only
// after it verifies).
package readiness

import (
	"sort"
	"sync"
	"time"
)

// Key is one relay's identity: (provider, name). All reconciliation and
// single-flight decisions are per key.
type Key struct {
	Provider string
	Name     string
}

func (k Key) String() string { return k.Provider + "/" + k.Name }

// State is the observable lifecycle phase of one relay.
type State string

const (
	StateConfigured State = "configured" // row exists, nothing verified yet
	StateDiscovered State = "discovered" // deployment URL known, verification pending
	StateDeploying  State = "deploying"  // a deploy/redeploy is in flight
	StateVerifying  State = "verifying"  // a verification pass is in flight
	StateReady      State = "ready"      // last verification passed, serving
	StateUnready    State = "unready"    // was ready, consecutive verifications now fail
	StateFailed     State = "failed"     // never verified; verification keeps failing
	StateRemoving   State = "removing"   // deleted from config, awaiting purge
)

// Failure reasons distinguish why a relay is not ready. They ride on the
// unready/failed states as the sanitized, operator-facing explanation.
const (
	ReasonVersionFailed = "version_failed" // deployment answers a different relay version
	ReasonAuthFailed    = "auth_failed"    // deployment rejected the deployed token
	ReasonProbeFailed   = "probe_failed"   // forwarding round trip did not complete
	ReasonUnreachable   = "unreachable"    // no HTTP answer at all
	ReasonDeployFailed  = "deploy_failed"  // platform deploy errored
	ReasonPaused        = "paused"         // platform answers instead of the worker
)

// Record is the operator-facing view of one relay's readiness.
type Record struct {
	Key         Key
	State       State
	Reason      string        // sanitized; "" when there is none
	Since       time.Time     // when the current phase was entered
	LastAttempt time.Time     // when the last verification (or deploy) ended
	LastResult  time.Duration // how long the last attempt took
	FailStreak  int           // consecutive failed verifications
}

// Config tunes retry/backoff behavior. Zero fields take the defaults;
// tests shrink them (and inject time) to drive transitions deterministically.
type Config struct {
	// BackoffBase is the retry delay after the first failure. Default 5s.
	BackoffBase time.Duration
	// BackoffMax caps the exponential growth. Default 5m.
	BackoffMax time.Duration
	// RecoverMax caps the retry delay while no relay is serving at all, so a
	// total outage heals quickly instead of waiting out a long backoff.
	// Default 15s.
	RecoverMax time.Duration
	// DemoteAfter is the consecutive verification failures before a serving
	// relay loses admission. Below it transient blips leave it serving —
	// passive pool health is the fast layer, this is the verified one.
	// Default 3.
	DemoteAfter int
	// Now is the clock; tests inject a fake. Defaults to time.Now.
	Now func() time.Time
}

// removingHold is how long a removed relay lingers in the "removing" phase
// before it is purged — long enough for the next pool snapshot to observe
// the removal, short enough to never build up.
const removingHold = 30 * time.Second

type entry struct {
	record  Record
	serving bool
	busy    bool      // single-flight: a verifier/deployer holds this relay
	nextTry time.Time // backoff gate for the next verification attempt
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
	if cfg.DemoteAfter <= 0 {
		cfg.DemoteAfter = 3
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Registry{cfg: cfg, entries: map[Key]*entry{}, changes: make(chan struct{}, 1)}
}

// Sync reconciles the registry with the set of configured relays: keys that
// first appear are announced as configured, keys that reappear while still
// labeled removing are resurrected as configured (a re-added relay must
// re-verify from scratch, never from memory), and keys that disappeared are
// moved to removing and purged after removingHold. The pool builder calls
// this on every generation build, so the registry always mirrors the
// database rows.
func (r *Registry) Sync(present []Key) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.cfg.Now()
	seen := make(map[Key]bool, len(present))
	for _, k := range present {
		seen[k] = true
		e, ok := r.entries[k]
		if !ok {
			r.entries[k] = &entry{record: Record{Key: k, State: StateConfigured, Since: now}}
			continue
		}
		if e.record.State == StateRemoving {
			e.record.State = StateConfigured
			e.record.Reason = ""
			e.record.Since = now
		}
	}
	for k, e := range r.entries {
		if seen[k] {
			continue
		}
		if e.record.State != StateRemoving {
			if e.serving {
				e.serving = false
				r.readyCount--
				r.notifyLocked()
			}
			e.record.State = StateRemoving
			e.record.Reason = ""
			e.record.Since = now
			continue
		}
		// Already removing: purge once the hold elapses. A removed relay
		// never serves again, so no rebuild is owed for the purge itself.
		if !e.serving && now.Sub(e.record.Since) >= removingHold {
			delete(r.entries, k)
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
// output. The admin plane and /stats render this.
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

// Begin takes the single-flight hold on key. It returns a release function
// that MUST be called exactly once, and ok=false when another verifier or
// deployer already holds the relay — that caller is the only one operating
// on it. This is what keeps concurrent reconciliation paths from launching
// duplicate deploys for one relay.
func (r *Registry) Begin(key Key) (release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entry(key)
	if e.busy {
		return nil, false
	}
	e.busy = true
	return func() {
		r.mu.Lock()
		e.busy = false
		r.mu.Unlock()
	}, true
}

// Allow reports whether key's backoff gate permits another verification
// attempt now. Failed attempts push nextTry forward exponentially; success
// resets it, so a healthy relay is re-checked every scan tick and a failing
// one never hot-loops.
func (r *Registry) Allow(key Key) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[key]; ok {
		return !r.cfg.Now().Before(e.nextTry)
	}
	return true
}

// Enter moves key to an explicit phase (deploying, verifying, discovered,
// …) without touching admission: a relay being replaced keeps serving under
// its previous verification, and a relay that never verified stays out.
// Only the deploying phase counts as a genuinely fresh attempt — it clears
// the failure streak and reopens the backoff gate. Routine verification
// (verifying) must NOT reset them: a serving relay that starts failing
// racks up its DemoteAfter streak across consecutive scan ticks, and a
// failing relay's backoff keeps the scan from hot-looping it.
func (r *Registry) Enter(key Key, state State, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entry(key)
	e.record.State = state
	e.record.Reason = reason
	e.record.Since = r.cfg.Now()
	if state == StateDeploying {
		e.record.FailStreak = 0
		e.nextTry = time.Time{}
	}
}

// Ready records a completed verification that passed end-to-end: the relay
// is admitted to the pool and the streak resets. Notifies only when the
// relay newly gains admission — re-verifying an already-serving relay
// leaves the pool unchanged.
func (r *Registry) Ready(key Key, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entry(key)
	now := r.cfg.Now()
	e.record.State = StateReady
	e.record.Reason = ""
	e.record.Since = now
	e.record.LastAttempt = now
	e.record.LastResult = duration
	e.record.FailStreak = 0
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
// as unready. A relay that never verified goes straight to failed. The
// backoff gate doubles with each failure, capped, and capped again harder
// while nothing at all is serving so a total outage heals quickly.
func (r *Registry) Failing(key Key, reason string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entry(key)
	now := r.cfg.Now()
	e.record.LastAttempt = now
	e.record.LastResult = duration
	e.record.FailStreak++
	e.record.Reason = reason
	wait := r.cfg.BackoffBase << min(e.record.FailStreak-1, 30)
	if wait > r.cfg.BackoffMax || wait <= 0 {
		wait = r.cfg.BackoffMax
	}
	if r.readyCount == 0 && wait > r.cfg.RecoverMax {
		wait = r.cfg.RecoverMax
	}
	e.nextTry = now.Add(wait)
	if !e.serving {
		if e.record.State != StateUnready && e.record.State != StateFailed {
			e.record.State = StateFailed
			e.record.Since = now
			// The relay never served, so pool membership is unchanged — but
			// its lifecycle record did change, and the serving generation
			// is what /stats and the admin plane render it from. Without a
			// rebuild here the transition stays invisible; coalescing keeps
			// a failing fleet at one pending rebuild per burst anyway.
			r.notifyLocked()
		}
		return
	}
	if e.record.FailStreak < r.cfg.DemoteAfter {
		// One or two blips do not pull a verified relay out of rotation.
		return
	}
	e.serving = false
	r.readyCount--
	e.record.State = StateUnready
	e.record.Since = now
	r.notifyLocked()
}

func (r *Registry) entry(key Key) *entry {
	e, ok := r.entries[key]
	if !ok {
		e = &entry{record: Record{Key: key, State: StateConfigured, Since: r.cfg.Now()}}
		r.entries[key] = e
	}
	return e
}

func (r *Registry) notifyLocked() {
	select {
	case r.changes <- struct{}{}:
	default:
	}
}
