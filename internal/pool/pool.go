// Package pool holds the relay registry: round-robin selection per pool key
// and passive health state per relay.
package pool

import (
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// KeyAll is the selector key for "every provider" (round-robin across all).
const KeyAll = "all"

// Origin classifies where a relay came from: the legacy YAML bridge or the
// management plane.
type Origin string

const (
	// OriginLegacy marks relays imported from the legacy YAML config; they
	// carry no auth token and are synced by the config bridge.
	OriginLegacy Origin = "legacy"
	// OriginManaged marks relays deployed and owned by the management plane;
	// the engine authenticates to them with their stored token.
	OriginManaged Origin = "managed"
)

// HopByHop headers are connection-scoped and dropped on both legs; header
// policies may never touch them.
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

// Relay is a runtime relay entry with health state and usage counters.
type Relay struct {
	ID       int64
	Name     string
	Provider string
	URL      *url.URL
	Active   bool
	Origin   Origin
	// Token authenticates the gateway to a managed relay. It is presented on
	// the relay leg only and must never reach logs or stats.
	Token string
	// Policy is the resolved header policy; nil forwards verbatim.
	Policy *HeaderPolicy
	// MaxBody is the provider's request-body limit, resolved by the caller
	// before the pool is built — the hot path never re-derives it.
	MaxBody int64

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

// StatsRow is a point-in-time snapshot for /stats.
type StatsRow struct {
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Origin    string `json:"origin"`
	Managed   bool   `json:"managed"`
	Active    bool   `json:"active"`
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
		Origin:    string(r.Origin),
		Managed:   r.Origin == OriginManaged,
		Active:    r.Active,
		Healthy:   r.Active && (r.downUntil.IsZero() || now.After(r.downUntil)),
		MaxBody:   r.MaxBody,
		Requests:  r.Requests.Load(),
		Failures:  r.Failures.Load(),
		LastError: r.lastErr,
	}
}

// RelayInput is one fully resolved relay handed to New: URL parsed, body
// limit and header policy already resolved — the hot path never re-derives
// them.
type RelayInput struct {
	ID       int64
	Name     string
	Provider string
	URL      *url.URL
	Active   bool
	Origin   Origin
	Token    string
	Policy   *HeaderPolicy
	MaxBody  int64
}

// Input is everything New needs to build a pool.
type Input struct {
	FailureThreshold int
	Cooldown         time.Duration
	Relays           []RelayInput
}

// Pool is the set of relays plus one round-robin cursor per selector key.
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
			ID:       r.ID,
			Name:     r.Name,
			Provider: r.Provider,
			URL:      r.URL,
			Active:   r.Active,
			Origin:   r.Origin,
			Token:    r.Token,
			Policy:   r.Policy,
			MaxBody:  r.MaxBody,
		})
	}
	return p, nil
}

// HasProvider reports whether provider matches at least one configured relay.
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
// traffic cannot skew the "all" rotation and vice versa. Unhealthy relays are
// skipped; when every candidate is unhealthy it still returns one (best
// effort beats a 503 when the whole pool is down).
func (p *Pool) Pick(key string) *Relay {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var candidates, healthy []*Relay
	for _, r := range p.relays {
		if !r.Active {
			continue
		}
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
