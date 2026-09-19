// Package pool holds the relay registry: round-robin selection per pool key
// and passive health state per relay.
package pool

import (
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"http-relay-gateway/internal/config"
)

// KeyAll is the selector key for "every provider" (round-robin across all).
const KeyAll = "all"

// Relay is a runtime relay entry with health state and usage counters.
type Relay struct {
	Name     string
	Provider string
	URL      *url.URL
	Active   bool
	// MaxBody is the provider's request-body limit, resolved at pool
	// construction from the runtime config's providers block.
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
		Active:    r.Active,
		Healthy:   r.Active && (r.downUntil.IsZero() || now.After(r.downUntil)),
		MaxBody:   r.MaxBody,
		Requests:  r.Requests.Load(),
		Failures:  r.Failures.Load(),
		LastError: r.lastErr,
	}
}

// Pool is the set of relays plus one round-robin cursor per selector key.
type Pool struct {
	mu        sync.Mutex
	relays    []*Relay
	rr        map[string]int // selector key -> next index into the healthy list
	threshold int
	cooldown  time.Duration
}

// New builds a Pool from a validated runtime config. URLs are pre-parsed and
// provider body limits resolved once so the hot path never re-derives them.
func New(cfg *config.RuntimeConfig) (*Pool, error) {
	p := &Pool{
		rr:        map[string]int{},
		threshold: cfg.FailureThreshold,
		cooldown:  cfg.Cooldown,
	}
	for i := range cfg.Relays {
		spec := cfg.Relays[i]
		p.relays = append(p.relays, &Relay{
			Name:     spec.Name,
			Provider: spec.Provider,
			URL:      spec.URL,
			Active:   spec.Active,
			MaxBody:  cfg.ProviderMaxBody(spec.Provider),
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
