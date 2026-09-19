package auth

import (
	"sync"
	"time"
)

// Login limits: five failed attempts per window, enforced per key (client
// IP) and globally. The global cap stops a distributed brute force from
// laundering through many addresses; it deliberately blocks everyone once
// tripped, including correct logins, until the window drains.
const (
	loginMaxFails   = 5
	loginFailWindow = 15 * time.Minute
)

// LoginLimiter counts failed logins within a sliding window.
type LoginLimiter struct {
	mu     sync.Mutex
	fails  map[string][]time.Time
	global []time.Time
}

// NewLoginLimiter builds the limiter with the package defaults.
func NewLoginLimiter() *LoginLimiter {
	return &LoginLimiter{fails: map[string][]time.Time{}}
}

// Allow reports whether a login attempt for key may proceed now, and if not,
// how long until it may. Entries older than the window are forgotten first.
func (l *LoginLimiter) Allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-loginFailWindow)
	l.pruneLocked(cutoff)
	perKey := l.fails[key]
	if len(perKey) >= loginMaxFails {
		return false, perKey[0].Add(loginFailWindow).Sub(now)
	}
	if len(l.global) >= loginMaxFails {
		return false, l.global[0].Add(loginFailWindow).Sub(now)
	}
	return true, 0
}

// RecordFailure notes one failed attempt for key and globally.
func (l *LoginLimiter) RecordFailure(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fails[key] = append(l.fails[key], now)
	l.global = append(l.global, now)
}

// Reset clears the key's failure history after a successful login. The
// global history stays: it is an abuse meter, not a per-client one.
func (l *LoginLimiter) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.fails, key)
}

// pruneLocked drops expired entries; keys whose history emptied are removed
// so the map stays bounded by recently-active attacker keys, not by every
// key ever seen.
func (l *LoginLimiter) pruneLocked(cutoff time.Time) {
	for key, times := range l.fails {
		kept := times[:0]
		for _, t := range times {
			if t.After(cutoff) {
				kept = append(kept, t)
			}
		}
		if len(kept) == 0 {
			delete(l.fails, key)
		} else {
			l.fails[key] = kept
		}
	}
	kept := l.global[:0]
	for _, t := range l.global {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	l.global = kept
}
