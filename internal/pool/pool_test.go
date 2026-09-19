package pool

import (
	"errors"
	"net/url"
	"testing"
	"time"
)

// defaultMaxBody mirrors the resolved body limit for providers without an
// explicit entry (config.DefaultMaxBody; kept literal so the pool package
// stays independent of the config package).
const defaultMaxBody = 8 << 20

func testPool(t *testing.T, mutate func(*Input)) *Pool {
	t.Helper()
	in := &Input{
		FailureThreshold: 2,
		Cooldown:         1 * time.Second,
		Relays: []RelayInput{
			{ID: 1, Name: "v1", Provider: "vercel", URL: mustURL(t, "https://v1.example"), Active: true, Origin: OriginLegacy, MaxBody: 100},
			{ID: 2, Name: "v2", Provider: "vercel", URL: mustURL(t, "https://v2.example"), Active: true, Origin: OriginLegacy, MaxBody: 100},
			{ID: 3, Name: "c1", Provider: "cloudflare", URL: mustURL(t, "https://c1.example"), Active: true, Origin: OriginLegacy, MaxBody: defaultMaxBody},
		},
	}
	if mutate != nil {
		mutate(in)
	}
	p, err := New(*in)
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
	for range 6 {
		count[p.Pick(KeyAll).Name]++
	}
	for name, n := range count {
		if n != 2 {
			t.Fatalf("relay %s picked %d times, want 2", name, n)
		}
	}
}

// TestPickServesExactConfigOrder pins the README contract verbatim: with all
// relays healthy the served sequence is exactly the config order —
// deterministic, so consumers can reason about which relay serves next.
func TestPickServesExactConfigOrder(t *testing.T) {
	p := testPool(t, nil)
	for i, want := range []string{"v1", "v2", "c1", "v1", "v2", "c1"} {
		if got := p.Pick(KeyAll).Name; got != want {
			t.Fatalf("all-pick %d = %s, want %s (config order is the served order)", i+1, got, want)
		}
	}
	for i, want := range []string{"v1", "v2", "v1", "v2"} {
		if got := p.Pick("vercel").Name; got != want {
			t.Fatalf("vercel-pick %d = %s, want %s", i+1, got, want)
		}
	}
}

func TestPinProvider(t *testing.T) {
	p := testPool(t, nil)
	for range 4 {
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
	for range 10 {
		p.Pick("vercel")
	}
	count := map[string]int{}
	for range 6 {
		count[p.Pick(KeyAll).Name]++
	}
	for name, n := range count {
		if n != 2 {
			t.Fatalf("relay %s picked %d times on 'all' after pinned traffic, want 2", name, n)
		}
	}
}

func TestMaxBodyCarriesFromInput(t *testing.T) {
	p := testPool(t, nil)
	if got := relayByName(t, p, "v1").MaxBody; got != 100 {
		t.Fatalf("vercel relay MaxBody = %d, want 100 (resolved by the caller)", got)
	}
	if got := relayByName(t, p, "c1").MaxBody; got != defaultMaxBody {
		t.Fatalf("cloudflare relay MaxBody = %d, want the default %d", got, int64(defaultMaxBody))
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
	for i := range 10 {
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
	p := testPool(t, func(in *Input) {
		in.Relays[0].Active = false
	})
	for range 6 {
		if got := p.Pick(KeyAll); got.Name == "v1" {
			t.Fatal("inactive relay was picked")
		}
	}
}

func TestStatsRowsExposeOriginAndManaged(t *testing.T) {
	p := testPool(t, func(in *Input) {
		in.Relays[0].Origin = OriginManaged
		in.Relays[0].Token = "relay-secret"
	})
	rows := p.Stats()
	if rows[0].Origin != "managed" || !rows[0].Managed {
		t.Fatalf("managed row = %+v, want origin managed", rows[0])
	}
	if rows[1].Origin != "legacy" || rows[1].Managed {
		t.Fatalf("legacy row = %+v, want origin legacy, managed false", rows[1])
	}
	// The token must never surface in stats rows.
	if rows[0].Name != "v1" {
		t.Fatal("unexpected row order")
	}
	for _, row := range rows {
		if row.LastError == "relay-secret" {
			t.Fatal("token leaked into stats")
		}
	}
}
