package pool

import (
	"errors"
	"net/url"
	"testing"
	"time"

	"http-relay-gateway/internal/config"
)

func testPool(t *testing.T, mutate func(*config.RuntimeConfig)) *Pool {
	t.Helper()
	cfg := &config.RuntimeConfig{
		FailureThreshold: 2,
		Cooldown:         1 * time.Second,
		Providers: map[string]config.ProviderSpec{
			"vercel": {MaxBody: 100},
		},
		Relays: []config.RelaySpec{
			{Name: "v1", Provider: "vercel", URL: mustURL(t, "https://v1.example"), Active: true},
			{Name: "v2", Provider: "vercel", URL: mustURL(t, "https://v2.example"), Active: true},
			{Name: "c1", Provider: "cloudflare", URL: mustURL(t, "https://c1.example"), Active: true},
		},
	}
	if mutate != nil {
		mutate(cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func relayByName(t *testing.T, p *Pool, name string) *Relay {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.relays {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("relay %q not found", name)
	return nil
}

func TestRoundRobinFairness(t *testing.T) {
	p := testPool(t, nil)
	count := map[string]int{}
	for i := 0; i < 6; i++ {
		count[p.Pick(KeyAll).Name]++
	}
	for name, n := range count {
		if n != 2 {
			t.Fatalf("relay %s picked %d times, want 2", name, n)
		}
	}
}

func TestPinProvider(t *testing.T) {
	p := testPool(t, nil)
	for i := 0; i < 4; i++ {
		if got := p.Pick("vercel"); got.Provider != "vercel" {
			t.Fatalf("picked provider %q, want vercel", got.Provider)
		}
	}
	if !p.HasProvider("vercel") || p.HasProvider("deno") {
		t.Fatal("HasProvider is broken")
	}
}

func TestCursorsArePerKey(t *testing.T) {
	p := testPool(t, nil)
	// Drive the "vercel" cursor hard; the "all" rotation must stay even.
	for i := 0; i < 10; i++ {
		p.Pick("vercel")
	}
	count := map[string]int{}
	for i := 0; i < 6; i++ {
		count[p.Pick(KeyAll).Name]++
	}
	for name, n := range count {
		if n != 2 {
			t.Fatalf("relay %s picked %d times on 'all' after pinned traffic, want 2", name, n)
		}
	}
}

func TestMaxBodyResolvedFromConfig(t *testing.T) {
	p := testPool(t, nil)
	if got := relayByName(t, p, "v1").MaxBody; got != 100 {
		t.Fatalf("vercel relay MaxBody = %d, want 100 (providers entry)", got)
	}
	if got := relayByName(t, p, "c1").MaxBody; got != config.DefaultMaxBody {
		t.Fatalf("cloudflare relay MaxBody = %d, want default %d", got, config.DefaultMaxBody)
	}
}

func TestPassiveHealthSkipsAndRecovers(t *testing.T) {
	p := testPool(t, nil)
	v1 := relayByName(t, p, "v1")
	boom := errors.New("boom")

	// One failure is under the threshold — still eligible.
	p.RecordFailure(v1, boom)
	if !v1.healthy(time.Now()) {
		t.Fatal("v1 tripped after a single failure, want threshold 2")
	}

	// Second consecutive failure trips cooldown (1s).
	p.RecordFailure(v1, boom)
	if v1.healthy(time.Now()) {
		t.Fatal("v1 should be on cooldown after 2 consecutive failures")
	}
	for i := 0; i < 10; i++ {
		if got := p.Pick("vercel"); got.Name == "v1" {
			t.Fatalf("v1 should be skipped while unhealthy, got it on pick %d", i+1)
		}
	}

	// A success resets the streak.
	v1.RecordSuccess()
	if !v1.healthy(time.Now()) {
		t.Fatal("v1 should be healthy after RecordSuccess")
	}

	// Half-open recovery: cooldown expiry makes it eligible again.
	p.RecordFailure(v1, boom)
	p.RecordFailure(v1, boom)
	time.Sleep(1100 * time.Millisecond)
	if !v1.healthy(time.Now()) {
		t.Fatal("v1 should recover after cooldown expiry")
	}
}

func TestAllDownIsBestEffort(t *testing.T) {
	p := testPool(t, nil)
	boom := errors.New("boom")
	for _, name := range []string{"v1", "v2", "c1"} {
		r := relayByName(t, p, name)
		p.RecordFailure(r, boom)
		p.RecordFailure(r, boom)
	}
	if got := p.Pick(KeyAll); got == nil {
		t.Fatal("Pick returned nil with all relays down; want best-effort relay")
	}
}

func TestInactiveRelayNeverPicked(t *testing.T) {
	p := testPool(t, func(c *config.RuntimeConfig) {
		c.Relays[0].Active = false
	})
	for i := 0; i < 6; i++ {
		if got := p.Pick(KeyAll); got.Name == "v1" {
			t.Fatal("inactive relay was picked")
		}
	}
}
