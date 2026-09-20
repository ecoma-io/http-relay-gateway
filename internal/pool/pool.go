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
package pool

import (
	"net/url"
	"sync"
	"sync/atomic"
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

// Relay is one serving relay: verified readiness plus passive health state
// and usage counters. The Token authenticates the gateway to the relay's
// worker; it is presented on the relay leg only and must never reach logs
// or stats.
type Relay struct {
	Name     string
	Provider string
	URL      *url.URL
	Token    string
	MaxBody  int64

	Requests atomic.Int64
	Failures atomic.Int64

	mu          sync.Mutex
	consecFails int
	downUntil   time.Time
	lastErr     string
}

func (r *Relay) healthy(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.downUntil.IsZero() || now.After(r.downUntil)
}

// RecordSuccess clears the failure streak and brings the relay back up.
func (r *Relay) RecordSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consecFails = 0
	r.downUntil = time.Time{}
	r.lastErr = ""
}

// RecordFailure bumps the failure streak; after `threshold` consecutive
// failures the relay goes on cooldown (passive health) and is skipped until
// it expires (half-open recovery).
func (r *Relay) RecordFailure(err error, threshold int, cooldown time.Duration) {
	r.Failures.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consecFails++
	r.lastErr = err.Error()
	if r.consecFails >= threshold {
		r.downUntil = time.Now().Add(cooldown)
		r.consecFails = 0
	}
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
}

func (r *Relay) snapshot(now time.Time) StatsRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	return StatsRow{
		Name:      r.Name,
		Provider:  r.Provider,
		Healthy:   r.downUntil.IsZero() || now.After(r.downUntil),
		MaxBody:   r.MaxBody,
		Requests:  r.Requests.Load(),
		Failures:  r.Failures.Load(),
		LastError: r.lastErr,
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
// counters are live.
type Pool struct {
	mu        sync.Mutex
	relays    []*Relay
	rr        map[string]int // selector key -> next index into the healthy list
	threshold int
	cooldown  time.Duration
}

// New builds a Pool from resolved relay inputs, preserving input order as
// the round-robin order.
func New(in Input) (*Pool, error) {
	p := &Pool{
		rr:        map[string]int{},
		threshold: in.FailureThreshold,
		cooldown:  in.Cooldown,
	}
	for i := range in.Relays {
		r := in.Relays[i]
		p.relays = append(p.relays, &Relay{
			Name:     r.Name,
			Provider: r.Provider,
			URL:      r.URL,
			Token:    r.Token,
			MaxBody:  r.MaxBody,
		})
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
// traffic cannot skew the "all" rotation and vice versa. Unhealthy relays
// are skipped; when every candidate is unhealthy it still returns one (best
// effort beats a 503 when the whole pool is down).
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
	i := p.rr[key] % len(use)
	p.rr[key] = i + 1
	return use[i]
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
// readiness registry before New — so this is the pool's own size, the
// number /readyz and the zero-ready short-circuit answer from.
func (p *Pool) ReadyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.relays)
}
