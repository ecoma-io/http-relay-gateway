package auth

import (
	"testing"
	"time"
)

var base = time.Unix(1_700_000_000, 0)

func TestLimiterAllowsUnderThreshold(t *testing.T) {
	l := NewLoginLimiter()
	for i := range loginMaxFails - 1 {
		l.RecordFailure("10.0.0.1", base.Add(time.Duration(i)*time.Second))
	}
	if ok, _ := l.Allow("10.0.0.1", base.Add(time.Minute)); !ok {
		t.Fatal("blocked under the failure threshold")
	}
	if ok, _ := l.Allow("10.0.0.2", base.Add(time.Minute)); !ok {
		t.Fatal("unrelated key blocked under threshold")
	}
}

func TestLimiterBlocksAfterFiveFails(t *testing.T) {
	l := NewLoginLimiter()
	for i := range loginMaxFails {
		l.RecordFailure("10.0.0.1", base.Add(time.Duration(i)*time.Second))
	}
	ok, retryAfter := l.Allow("10.0.0.1", base.Add(time.Minute))
	if ok {
		t.Fatal("sixth attempt allowed after five failures")
	}
	if retryAfter <= 0 || retryAfter > loginFailWindow {
		t.Fatalf("retryAfter = %v, want within the window", retryAfter)
	}
}

func TestLimiterGlobalCapBlocksOtherKeys(t *testing.T) {
	l := NewLoginLimiter()
	// Five distinct IPs each fail once: the global counter saturates.
	for i := range loginMaxFails {
		l.RecordFailure("10.0.0."+string(rune('a'+i)), base)
	}
	if ok, _ := l.Allow("10.9.9.9", base.Add(time.Second)); ok {
		t.Fatal("fresh IP allowed while the global counter is saturated")
	}
}

func TestLimiterResetOnSuccess(t *testing.T) {
	l := NewLoginLimiter()
	for range loginMaxFails {
		l.RecordFailure("10.0.0.1", base)
	}
	if ok, _ := l.Allow("10.0.0.1", base.Add(time.Second)); ok {
		t.Fatal("blocked key allowed before reset")
	}
	l.Reset("10.0.0.1")
	if len(l.fails) != 0 {
		t.Fatalf("key history survived the reset: %v", l.fails)
	}
	// The global counter is an abuse meter and deliberately survives resets.
	if len(l.global) != loginMaxFails {
		t.Fatalf("global history was cleared by a per-key reset: %v", l.global)
	}
}

func TestLimiterWindowExpiry(t *testing.T) {
	l := NewLoginLimiter()
	for i := range loginMaxFails {
		l.RecordFailure("10.0.0.1", base.Add(time.Duration(i)*time.Second))
	}
	if ok, _ := l.Allow("10.0.0.1", base.Add(time.Minute)); ok {
		t.Fatal("blocked key allowed inside the window")
	}
	if ok, _ := l.Allow("10.0.0.1", base.Add(loginFailWindow+time.Minute)); !ok {
		t.Fatal("key still blocked after the window drained")
	}
}
