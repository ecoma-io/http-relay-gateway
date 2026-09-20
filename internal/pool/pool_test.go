package pool

import (
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func mustPool(t *testing.T, in Input) *Pool {
	t.Helper()
	p, err := New(in)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	return p
}

func relayInput(t *testing.T, name, provider string) RelayInput {
	t.Helper()
	u, err := url.Parse("http://" + name + "." + provider + ".internal")
	if err != nil {
		t.Fatalf("parse relay url: %v", err)
	}
	return RelayInput{
		Name:     name,
		Provider: provider,
		URL:      u,
		Token:    "token-" + name,
		MaxBody:  ProviderMaxBody(provider),
	}
}

func TestPickRoundRobinFollowsInputOrder(t *testing.T) {
	p := mustPool(t, Input{
		FailureThreshold: 3,
		Cooldown:         time.Second,
		Relays: []RelayInput{
			relayInput(t, "alpha", "vercel"),
			relayInput(t, "beta", "cloudflare"),
			relayInput(t, "gamma", "deno"),
		},
	})
	var got []string
	for range 6 {
		r := p.Pick(KeyAll)
		if r == nil {
			t.Fatal("Pick returned nil with a healthy pool")
		}
		got = append(got, r.Name)
	}
	want := []string{"alpha", "beta", "gamma", "alpha", "beta", "gamma"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-robin sequence = %v, want the input order repeated: %v", got, want)
	}
}

func TestPickPerProviderCursors(t *testing.T) {
	relays := []RelayInput{
		relayInput(t, "alpha", "vercel"),
		relayInput(t, "beta", "vercel"),
		relayInput(t, "gamma", "cloudflare"),
	}
	p := mustPool(t, Input{FailureThreshold: 3, Cooldown: time.Second, Relays: relays})

	var gotAll, gotVercel, gotCloudflare []string
	for range 2 {
		gotAll = append(gotAll, p.Pick(KeyAll).Name)
	}
	for range 3 {
		gotVercel = append(gotVercel, p.Pick("vercel").Name)
	}
	for range 2 {
		gotAll = append(gotAll, p.Pick(KeyAll).Name)
	}
	for range 2 {
		gotCloudflare = append(gotCloudflare, p.Pick("cloudflare").Name)
	}

	// The all-key sequence runs over the whole input order, the vercel
	// sequence over its own two relays — the interleaved all picks above
	// must not have skewed it.
	wantAll := []string{"alpha", "beta", "gamma", "alpha"}
	wantVercel := []string{"alpha", "beta", "alpha"}
	wantCloudflare := []string{"gamma", "gamma"}
	if !reflect.DeepEqual(gotAll, wantAll) {
		t.Fatalf("all sequence = %v, want %v", gotAll, wantAll)
	}
	if !reflect.DeepEqual(gotVercel, wantVercel) {
		t.Fatalf("vercel sequence = %v, want %v (per-key cursor skewed by interleaving)", gotVercel, wantVercel)
	}
	if !reflect.DeepEqual(gotCloudflare, wantCloudflare) {
		t.Fatalf("cloudflare sequence = %v, want %v", gotCloudflare, wantCloudflare)
	}

	// The same pin on a dedicated pool yields the same sequence: pinning
	// traffic costs the unpinned rotation nothing, and vice versa.
	solo := mustPool(t, Input{FailureThreshold: 3, Cooldown: time.Second, Relays: relays})
	var pure []string
	for range 3 {
		pure = append(pure, solo.Pick("vercel").Name)
	}
	if !reflect.DeepEqual(pure, wantVercel) {
		t.Fatalf("isolated vercel sequence = %v, want %v", pure, wantVercel)
	}
}

func TestPassiveHealthStreakCooldownRecovery(t *testing.T) {
	p := mustPool(t, Input{
		FailureThreshold: 3,
		Cooldown:         5 * time.Millisecond,
		Relays:           []RelayInput{relayInput(t, "solo", "vercel")},
	})
	r := p.Pick(KeyAll)
	if r == nil {
		t.Fatal("Pick returned nil on a healthy single-relay pool")
	}
	boom := errors.New("dial relay: connection refused")

	// Below the threshold the relay stays in rotation.
	for range 2 {
		p.RecordFailure(r, boom)
	}
	row := p.Stats()[0]
	if !row.Healthy || row.Failures != 2 || row.LastError != boom.Error() {
		t.Fatalf("below-threshold failures must keep the relay healthy: %+v", row)
	}

	// A success clears the streak: interleaved failures must not accumulate
	// into a cooldown.
	p.RecordSuccess(r)
	p.RecordFailure(r, boom)
	if row := p.Stats()[0]; !row.Healthy {
		t.Fatalf("a success must reset the failure streak: %+v", row)
	}

	// The threshold-th consecutive failure puts the relay on cooldown.
	p.RecordFailure(r, boom)
	p.RecordFailure(r, boom)
	row = p.Stats()[0]
	if row.Healthy {
		t.Fatalf("third consecutive failure must cool the relay down: %+v", row)
	}
	if row.Requests != 0 || row.Failures != 5 || row.LastError != boom.Error() {
		t.Fatalf("counters drifted: %+v", row)
	}

	// Best effort beats a 503: the only candidate is still picked while down.
	r.Requests.Add(3)
	if got := p.Pick(KeyAll); got == nil {
		t.Fatal("Pick must stay best effort while every candidate is down")
	}
	if row := p.Stats()[0]; row.Requests != 3 {
		t.Fatalf("requests counter = %d, want 3", row.Requests)
	}

	// Half-open recovery: the relay returns once the cooldown has expired.
	time.Sleep(15 * time.Millisecond)
	row = p.Stats()[0]
	if !row.Healthy {
		t.Fatalf("relay must recover after the cooldown expires: %+v", row)
	}
	if got := p.Pick(KeyAll); got == nil || got.Name != "solo" {
		t.Fatalf("recovered relay must serve again, got %+v", got)
	}
}

func TestPickSkipsCooledDownRelay(t *testing.T) {
	p := mustPool(t, Input{
		FailureThreshold: 1,
		Cooldown:         5 * time.Millisecond,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel"), relayInput(t, "beta", "vercel")},
	})
	first, second := p.Pick(KeyAll), p.Pick(KeyAll)
	if first.Name != "alpha" || second.Name != "beta" {
		t.Fatalf("initial picks = %s, %s; want alpha, beta", first.Name, second.Name)
	}

	p.RecordFailure(second, errors.New("boom"))
	for range 4 {
		if got := p.Pick(KeyAll); got.Name != "alpha" {
			t.Fatalf("cooled-down relay must be skipped, got %s", got.Name)
		}
	}

	time.Sleep(15 * time.Millisecond) // the cooldown expires: half-open again
	sawBeta := false
	for range 4 {
		if p.Pick(KeyAll).Name == "beta" {
			sawBeta = true
			break
		}
	}
	if !sawBeta {
		t.Fatal("recovered relay must rejoin the rotation")
	}
}

func TestBestEffortWhenEveryRelayIsDown(t *testing.T) {
	p := mustPool(t, Input{
		FailureThreshold: 1,
		Cooldown:         time.Second,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel"), relayInput(t, "beta", "cloudflare")},
	})
	boom := errors.New("boom")
	for _, r := range []*Relay{p.Pick(KeyAll), p.Pick(KeyAll)} {
		p.RecordFailure(r, boom)
	}
	for _, row := range p.Stats() {
		if row.Healthy {
			t.Fatalf("%s should be on cooldown: %+v", row.Name, row)
		}
	}
	if got := p.Pick(KeyAll); got == nil {
		t.Fatal("Pick must return a relay even when the whole pool is down")
	}
	// Membership is passive-health independent: cooldowns never evict.
	if p.ReadyCount() != 2 {
		t.Fatalf("ReadyCount = %d after cooldowns, want 2", p.ReadyCount())
	}
}

func TestPickNilCases(t *testing.T) {
	empty := mustPool(t, Input{})
	if r := empty.Pick(KeyAll); r != nil {
		t.Fatalf("empty pool Pick = %+v, want nil", r)
	}
	if empty.ReadyCount() != 0 {
		t.Fatalf("empty pool ReadyCount = %d, want 0", empty.ReadyCount())
	}

	p := mustPool(t, Input{
		FailureThreshold: 3,
		Cooldown:         time.Second,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel")},
	})
	if r := p.Pick("deno"); r != nil {
		t.Fatalf("Pick on an unmatched provider = %+v, want nil", r)
	}
	if !p.HasProvider("vercel") {
		t.Fatal("HasProvider must find a serving provider")
	}
	if p.HasProvider("deno") {
		t.Fatal("HasProvider must reject a provider with no relays")
	}
}

func TestStatsRowsCarryNoSecrets(t *testing.T) {
	p := mustPool(t, Input{
		FailureThreshold: 3,
		Cooldown:         time.Second,
		Relays:           []RelayInput{relayInput(t, "alpha", "vercel")},
	})
	r := p.Pick(KeyAll)
	r.Requests.Add(4)
	p.RecordFailure(r, errors.New("connection refused"))

	raw, err := json.Marshal(p.Stats())
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("stats rows = %d, want 1", len(rows))
	}
	allowed := map[string]bool{
		"name": true, "provider": true, "healthy": true, "maxBody": true,
		"requests": true, "failures": true, "lastError": true,
	}
	for k := range rows[0] {
		if !allowed[k] {
			t.Fatalf("stats row carries unexpected key %q — a URL or token would leak here", k)
		}
	}
	row := rows[0]
	if row["name"] != "alpha" || row["provider"] != "vercel" {
		t.Fatalf("identity fields wrong: %v", row)
	}
	if row["healthy"] != true || row["failures"] != float64(1) || row["requests"] != float64(4) {
		t.Fatalf("health and counters wrong: %v", row)
	}
	if row["maxBody"] != float64(VercelMaxBody) {
		t.Fatalf("maxBody = %v, want %d", row["maxBody"], VercelMaxBody)
	}
	// The relay origin and token must not appear anywhere in the snapshot.
	if s := string(raw); strings.Contains(s, "token-alpha") || strings.Contains(s, ".internal") || strings.Contains(s, "http://") {
		t.Fatalf("stats expose secrets: %s", s)
	}
}

func TestProviderMaxBody(t *testing.T) {
	cases := []struct {
		provider string
		want     int64
	}{
		{"vercel", 4_500_000},
		{"cloudflare", 100 << 20},
		{"deno", 100 << 20},
		{"unknown", 4_500_000},
	}
	for _, tc := range cases {
		if got := ProviderMaxBody(tc.provider); got != tc.want {
			t.Fatalf("ProviderMaxBody(%q) = %d, want %d", tc.provider, got, tc.want)
		}
	}
}
