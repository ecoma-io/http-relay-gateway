package e2e_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// The manage tests replace the former YAML hot-reload tests: the pool now
// converges on database mutations through the admin API's apply loop, so
// each test drives a real mutation and asserts the swap happened.

func TestE2E_AdminCreateAppliesRelay(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	cf := NewEdgeSim(t, "cloudflare-1")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
	)

	if status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 before the create\nlogs:\n%s", status, g.Logs())
	}

	g.CreateRelay(t, RelaySeed{Name: "vercel-2", Provider: "vercel", URL: v2.URL})
	g.CreateRelay(t, RelaySeed{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL})
	g.WaitForRelays([]string{"vercel-1", "vercel-2", "cloudflare-1"}, applySettle)
	waitForLog(t, g, `"msg":"configuration reloaded"`, applySettle)

	// Pinned vercel now rotates through both vercel relays; the pool rebuilt
	// from scratch, so the pinned cursor restarts at the first relay.
	for i, want := range []*edgeSim{v1, v2, v1} {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != want.servedBody() {
			t.Fatalf("pinned request %d after create: status=%d body=%q, want 200 %q",
				i, status, body, want.servedBody())
		}
	}
	status, _, body := relayDo(t, g.Addr, "/", `{}`, nil)
	if status != http.StatusOK || body != v1.servedBody() {
		t.Fatalf("auto after create: status=%d body=%q, want 200 %q (rebuilt pool restarts the all-cursor)",
			status, body, v1.servedBody())
	}
}

func TestE2E_AdminDeleteShrinksPool(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		RelaySeed{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
	)

	// Advance the vercel cursor past vercel-2 so the shrink cannot hide a
	// stale-cursor bug (cursor 2 % len 1 = 0).
	for range 2 {
		if status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK {
			t.Fatalf("status = %d, want 200\nlogs:\n%s", status, g.Logs())
		}
	}
	if v2.hitCount() != 1 {
		t.Fatalf("pre-delete rotation did not reach vercel-2 (hits = %d)", v2.hitCount())
	}

	// Delete vercel-2's row: the relay id must be discovered from the API
	// list, exactly as the UI does it.
	id := findRelayID(t, g, "vercel-2")
	g.DeleteRelay(t, id)
	g.WaitForRelays([]string{"vercel-1"}, applySettle)

	// The survivor keeps serving; shrinking 2->1 does not break the pool.
	for range 5 {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != v1.servedBody() {
			t.Fatalf("status=%d body=%q, want 200 %q\nlogs:\n%s",
				status, body, v1.servedBody(), g.Logs())
		}
	}
	if v2.hitCount() != 1 {
		t.Fatalf("removed relay served %d more requests", v2.hitCount()-1)
	}
}

func TestE2E_AdminHotSwapsProviderLimit(t *testing.T) {
	v := NewEdgeSim(t, "vercel-1")
	cf := NewEdgeSim(t, "cloudflare-1")
	g := NewGatewayWithProviders(t,
		map[string]int64{"vercel": 8, "cloudflare": 100},
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: v.URL},
		RelaySeed{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	)

	big := "123456789012" // 12 bytes: over vercel's 8, under cloudflare's
	if status, _, _ := relayDo(t, g.Addr, "/vercel", big, nil); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 before the limit is raised", status)
	}

	g.PutProviders(t, map[string]int64{"vercel": 100, "cloudflare": 100})
	g.WaitForCondition(applySettle, "vercel maxBody == 100", func(st *StatsView) bool {
		row, ok := st.relay("vercel-1")
		return ok && row.MaxBody == 100
	})

	// The same body now passes through vercel itself.
	status, _, body := relayDo(t, g.Addr, "/vercel", big, nil)
	if status != http.StatusOK || body != v.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q after hot-swapped limit",
			status, body, v.servedBody())
	}
}

func TestE2E_AdminPatchDeactivatesRelay(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		RelaySeed{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
	)

	id := findRelayID(t, g, "vercel-2")
	code, body := g.PatchRelay(t, id, map[string]any{"active": false})
	if code != http.StatusOK {
		t.Fatalf("patch status = %d: %v", code, body)
	}
	// An inactive relay stays in /stats as an inactive row — observable but
	// never picked.
	g.WaitForCondition(applySettle, "vercel-2 inactive", func(st *StatsView) bool {
		row, ok := st.relay("vercel-2")
		return ok && !row.Active
	})

	for range 3 {
		status, _, got := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || got != v1.servedBody() {
			t.Fatalf("status=%d body=%q, want 200 %q", status, got, v1.servedBody())
		}
	}
	if v2.hitCount() != 0 {
		t.Fatal("deactivated relay served traffic")
	}

	// Re-activation rejoins the pool at the back of a rebuilt rotation.
	code, _ = g.PatchRelay(t, id, map[string]any{"active": true})
	if code != http.StatusOK {
		t.Fatalf("reactivate status = %d", code)
	}
	g.WaitForCondition(applySettle, "vercel-2 active again", func(st *StatsView) bool {
		row, ok := st.relay("vercel-2")
		return ok && row.Active
	})
	for _, want := range []*edgeSim{v1, v2} {
		status, _, got := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || got != want.servedBody() {
			t.Fatalf("status=%d body=%q, want 200 %q (rebuilt cursor restarts at vercel-1)",
				status, got, want.servedBody())
		}
	}
}

func TestE2E_AdminRejectionKeepsServing(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		RelaySeed{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
	)

	if status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 before the rejections\nlogs:\n%s", status, g.Logs())
	}

	// Reserved provider labels, malformed URLs, duplicate names, and invalid
	// header policies are all refused with a field-level 4xx; none of them
	// may perturb the serving pool.
	rejected := []struct {
		name  string
		body  map[string]any
		want  int
		field string
	}{
		{"reserved label", map[string]any{"name": "x", "provider": "all", "url": "https://x.example"}, http.StatusUnprocessableEntity, "provider"},
		{"bad url", map[string]any{"name": "x", "provider": "vercel", "url": "not-a-url"}, http.StatusUnprocessableEntity, "url"},
		{"duplicate name", map[string]any{"name": "vercel-1", "provider": "vercel", "url": "https://dup.example"}, http.StatusConflict, "name"},
		{"bad policy", map[string]any{"name": "x", "provider": "vercel", "url": "https://p.example", "headerPolicy": `{"strip":["connection"]}`}, http.StatusUnprocessableEntity, "headerPolicy"},
	}
	for _, tc := range rejected {
		code, headers, body := g.AdminDo(t, http.MethodPost, "/api/v1/relays", tc.body)
		if code != tc.want {
			t.Fatalf("%s: status = %d, want %d (%s)", tc.name, code, tc.want, body)
		}
		if headers.Get("Content-Type") != "application/json" {
			t.Fatalf("%s: error content type = %q, want application/json", tc.name, headers.Get("Content-Type"))
		}
		if got := jsonField(t, body, "field"); got != tc.field {
			t.Fatalf("%s: error field = %v, want %q", tc.name, got, tc.field)
		}
	}

	// The original generation is untouched: same relays, still serving, and
	// rotation works exactly as before.
	g.WaitForRelays([]string{"vercel-1", "vercel-2"}, applySettle)
	for i, want := range []*edgeSim{v2, v1, v2} {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != want.servedBody() {
			t.Fatalf("request %d after rejections: status=%d body=%q, want 200 %q",
				i, status, body, want.servedBody())
		}
	}
}

func TestE2E_SettingsChangeAppliesWithoutRestart(t *testing.T) {
	downed := NewEdgeSim(t, "vercel-dead")
	live := NewEdgeSim(t, "vercel-live")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-dead", Provider: "vercel", URL: downed.URL},
		RelaySeed{Name: "vercel-live", Provider: "vercel", URL: live.URL},
	)
	// Both verified and admitted; now the first relay dies. Before the
	// knob, a dead first pick failovers to the live relay.
	downed.shutdown()
	status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
	if status != http.StatusOK || body != live.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q via failover", status, body, live.servedBody())
	}
	live.mu.Lock()
	live.hits = 0
	live.mu.Unlock()

	// stream_threshold_bytes=8: the 12-byte request goes streaming, and
	// streaming never failovers — the dead pick must surface as a 502.
	// The apply log line repeats on every mutation, so count occurrences
	// rather than waiting for a first match (which earlier applies satisfy).
	before := strings.Count(g.Logs(), `"msg":"configuration reloaded"`)
	g.PatchSettings(t, map[string]any{"streamThresholdBytes": 8})
	waitForLogCount(t, g, `"msg":"configuration reloaded"`, before+1, applySettle)

	status, _, _ = relayDo(t, g.Addr, "/vercel", "123456789012", nil)
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (streaming requests never failover)", status)
	}
	if live.hitCount() != 0 {
		t.Fatal("streaming request failovered to the live relay")
	}
}

// findRelayID resolves a relay id by name through the authenticated API.
func findRelayID(t testing.TB, g *Gateway, name string) int64 {
	t.Helper()
	_, _, raw := g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	var rows []struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatalf("decode relay list: %v (%s)", err, raw)
	}
	for _, row := range rows {
		if row.Name == name {
			return row.ID
		}
	}
	t.Fatalf("relay %q not in list: %s", name, raw)
	return 0
}

// jsonField reads one top-level key from a raw JSON object body.
func jsonField(t testing.TB, raw string, key string) string {
	t.Helper()
	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode %q from %s: %v", key, raw, err)
	}
	got, _ := parsed[key].(string)
	return got
}
