// Package pool holds the serving set: the relays a client's request may be
// forwarded through right now. Membership is earned, never configured — the
// generation builder admits only relays whose verified readiness still holds
// (deployment reachable, expected worker version, accepted relay key, a
// forwarding round trip) — so everything here is, by construction, safe to
// receive production traffic.
//
// Two layers live side by side, deliberately separate: the verified
// readiness that decides membership is slow, control-plane work owned by
// internal/readiness; the passive health below is the fast, data-plane
// layer — consecutive transport failures put a relay on cooldown and
// half-open recovery lets it back in. Passive failures never change
// membership: a failing relay is skipped, not evicted.
//
// That passive layer (counters, cooldown, rotation position) is runtime
// state of the endpoint, not of the membership snapshot, so it lives in
// RuntimeState (runtime.go) and is attached to every serving generation
// that serves the same endpoint — a rebuild carries it instead of
// discarding it.
package pool

import (
	"net/url"
	"sync"
	"time"
)

// KeyAll is the selector key for "every provider" (round-robin across all).
const KeyAll = "all"

// HopByHop headers are connection-scoped and dropped on both legs.
var HopByHop = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// Platform request-body limits: the hard-coded provider set the gateway
// deploys to, with each platform's documented cap. The generation builder
// resolves them before the pool is built — the hot path never re-derives
// them.
const (
	// Vercel caps request bodies for edge functions at ~4.5 MB.
	VercelMaxBody = int64(4_500_000)
	// Cloudflare Workers accept up to ~100 MB.
	CloudflareMaxBody = int64(100 << 20)
	// Deno Deploy publishes no per-request body limit; 100 MB matches the
	// largest documented cap in the provider set so a deno relay rejects
	// only what the gateway would have rejected elsewhere.
	DenoMaxBody = int64(100 << 20)
)

// ProviderMaxBody resolves the body limit for one hard-coded provider.
func ProviderMaxBody(provider string) int64 {
	switch provider {
	case "vercel":
		return VercelMaxBody
	case "cloudflare":
		return CloudflareMaxBody
	case "deno":
		return DenoMaxBody
	default:
		return VercelMaxBody
	}
}

// Relay is one serving relay: verified readiness plus the runtime health
// state and usage counters. The Token authenticates the gateway to the
// relay's worker; it is presented on the relay leg only and must never reach
// logs or stats.
//
// The embedded *RelayState is what makes rebuilds harmless: standalone
// builds (New) hand every relay a fresh state, a serving build (NewAttached)
// attaches the endpoint's shared state — the pool itself never owns counters
// it would drop on the next swap.
type Relay struct {
	*RelayState

	Name     string
	Provider string
	URL      *url.URL
	Token    string
	MaxBody  int64
}

// StatsRow is a point-in-time snapshot for /stats. It carries no URL and no
// token: relay origins are private and never leave the process.
type StatsRow struct {
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Healthy   bool   `json:"healthy"`
	MaxBody   int64  `json:"maxBody"`
	Requests  int64  `json:"requests"`
	Failures  int64  `json:"failures"`
	LastError string `json:"lastError,omitempty"`
	// MidstreamFailures rides the same no-secrets rule as everything else
	// here: a count, omitted while zero.
	MidstreamFailures int64 `json:"midstreamFailures,omitempty"`
}

func (r *Relay) snapshot(now time.Time) StatsRow {
	up, lastErr := r.health(now)
	return StatsRow{
		Name:              r.Name,
		Provider:          r.Provider,
		Healthy:           up,
		MaxBody:           r.MaxBody,
		Requests:          r.Requests.Load(),
		Failures:          r.Failures.Load(),
		LastError:         lastErr,
		MidstreamFailures: r.MidstreamFailures.Load(),
	}
}

// RelayInput is one fully resolved serving relay handed to New: URL parsed,
// body limit resolved — the hot path never re-derives them.
type RelayInput struct {
	Name     string
	Provider string
	URL      *url.URL
	Token    string
	MaxBody  int64
	// Runtime is the endpoint key a RuntimeState attaches this relay's
	// shared health by — the caller computes it from the same identity,
	// URL and token as the fields above. New ignores it (fresh state every
	// build); NewAttached consumes it verbatim, so a caller that leaves it
	// zero attaches by the zero key and gets fresh state there too.
	Runtime RuntimeKey
}

// Input is everything New needs to build a pool.
type Input struct {
	FailureThreshold int
	Cooldown         time.Duration
	Relays           []RelayInput
}

// Pool is the set of serving relays plus one round-robin cursor per
// selector key. A Pool is immutable once built — membership changes build a
// new one and swap it in atomically — but per-relay health state and
// counters are live: attached to the process's RuntimeState when the
// generation was built with NewAttached, so a swap re-attaches instead of
// discarding them.
type Pool struct {
	mu        sync.Mutex
	relays    []*Relay
	rr        map[string]int // selector key -> next index into the healthy list
	threshold int
	cooldown  time.Duration
	// rt is the shared runtime state this pool attached its relays from.
	// nil for a standalone New: rotation then uses the pool-local rr map.
	rt *RuntimeState
}

// New builds a Pool from resolved relay inputs, preserving input order as
// the round-robin order. Every relay gets fresh runtime state — this is the
// standalone form, and every helper and test that needs a disposable pool
// keeps using it.
func New(in Input) (*Pool, error) { return newPool(in, nil) }

// NewAttached builds a Pool the way a serving generation does: each relay
// attaches its RuntimeState entry — the same endpoint keeps its counters,
// cooldown, failure streak and rotation position across rebuilds — and the
// rotation reads the shared per-selector cursors instead of pool-local ones.
// A nil rt is exactly New.
func NewAttached(in Input, rt *RuntimeState) (*Pool, error) { return newPool(in, rt) }

func newPool(in Input, rt *RuntimeState) (*Pool, error) {
	p := &Pool{
		rr:        map[string]int{},
		threshold: in.FailureThreshold,
		cooldown:  in.Cooldown,
		rt:        rt,
	}
	for i := range in.Relays {
		r := in.Relays[i]
		relay := &Relay{
			RelayState: &RelayState{},
			Name:       r.Name,
			Provider:   r.Provider,
			URL:        r.URL,
			Token:      r.Token,
			MaxBody:    r.MaxBody,
		}
		if rt != nil {
			relay.RelayState = rt.Attach(r.Runtime)
		}
		p.relays = append(p.relays, relay)
	}
	return p, nil
}

// HasProvider reports whether provider matches at least one serving relay.
func (p *Pool) HasProvider(provider string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.relays {
		if r.Provider == provider {
			return true
		}
	}
	return false
}

// Pick returns the next relay for key (KeyAll or a provider name), rotating
// round-robin among healthy relays. The cursor is kept per key so pinned
// traffic cannot skew the "all" rotation and vice versa — and, on an
// attached pool, across serving rebuilds. Unhealthy relays are skipped;
// when every candidate is unhealthy it still returns one (best effort beats
// a 503 when the whole pool is down).
func (p *Pool) Pick(key string) *Relay {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var candidates, healthy []*Relay
	for _, r := range p.relays {
		if key != KeyAll && r.Provider != key {
			continue
		}
		candidates = append(candidates, r)
		if r.healthy(now) {
			healthy = append(healthy, r)
		}
	}
	use := healthy
	if len(use) == 0 {
		use = candidates
	}
	if len(use) == 0 {
		return nil
	}
	return use[p.cursor(key, len(use))]
}

// cursor resolves the rotation index for key over a healthy-candidate list
// of n relays: the shared runtime cursors when the pool was built attached
// (they survive serving rebuilds), a pool-local one otherwise. Called under
// p.mu, like every other Pick step.
func (p *Pool) cursor(key string, n int) int {
	if p.rt != nil {
		return p.rt.CursorFor(key, n)
	}
	i := p.rr[key] % n
	p.rr[key] = i + 1
	return i
}

// RecordSuccess / RecordFailure apply the pool's health policy to a relay.
func (p *Pool) RecordSuccess(r *Relay) { r.RecordSuccess() }

func (p *Pool) RecordFailure(r *Relay, err error) {
	r.RecordFailure(err, p.threshold, p.cooldown)
}

// Stats snapshots every relay.
func (p *Pool) Stats() []StatsRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	rows := make([]StatsRow, 0, len(p.relays))
	for _, r := range p.relays {
		rows = append(rows, r.snapshot(now))
	}
	return rows
}

// ReadyCount reports how many relays are in the serving set. The pool only
// ever contains verified relays — the generation builder filtered by the
// readiness registry before the build — so this is the pool's own size, the
// number /readyz and the zero-ready short-circuit answer from.
func (p *Pool) ReadyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.relays)
}
