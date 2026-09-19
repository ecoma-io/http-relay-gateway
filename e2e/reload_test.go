package e2e_test

import (
	"net/http"
	"testing"
)

func TestE2E_ReloadAddsRelay(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	cf := NewEdgeSim(t, "cloudflare-1")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
	})
	g := NewGateway(t, cfg)

	status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 before reload\nlogs:\n%s", status, g.Logs())
	}

	cfg.Relays = append(cfg.Relays,
		RelayConfig{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
		RelayConfig{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	)
	g.ReloadConfig(cfg, []string{"vercel-1", "vercel-2", "cloudflare-1"})
	waitForLog(t, g, `"msg":"configuration reloaded"`, reloadSettle)

	// Pinned vercel now rotates through both vercel relays; the pool rebuilt
	// from scratch, so the pinned cursor restarts at the first relay.
	for i, want := range []*edgeSim{v1, v2, v1} {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != want.servedBody() {
			t.Fatalf("pinned request %d after reload: status=%d body=%q, want 200 %q",
				i, status, body, want.servedBody())
		}
	}
	status, _, body := relayDo(t, g.Addr, "/", `{}`, nil)
	if status != http.StatusOK || body != v1.servedBody() {
		t.Fatalf("auto after reload: status=%d body=%q, want 200 %q (rebuilt pool restarts the all-cursor)",
			status, body, v1.servedBody())
	}
}

func TestE2E_ReloadRemovesRelay(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
	})
	g := NewGateway(t, cfg)

	// Advance the vercel cursor past vercel-2 so the shrink cannot hide a
	// stale-cursor bug (cursor 2 % len 1 = 0).
	for range 2 {
		if status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK {
			t.Fatalf("status = %d, want 200\nlogs:\n%s", status, g.Logs())
		}
	}
	if v2.hitCount() != 1 {
		t.Fatalf("pre-reload rotation did not reach vercel-2 (hits = %d)", v2.hitCount())
	}

	cfg.Relays = cfg.Relays[:1]
	g.ReloadConfig(cfg, []string{"vercel-1"})

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

func TestE2E_ReloadHotSwapsBodyLimit(t *testing.T) {
	v := NewEdgeSim(t, "vercel-1")
	cf := NewEdgeSim(t, "cloudflare-1")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v.URL},
		{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	})
	cfg.Providers = map[string]string{"vercel": "8", "cloudflare": "100"}
	g := NewGateway(t, cfg)

	big := "123456789012" // 12 bytes: over vercel's 8, under cloudflare's
	if status, _, _ := relayDo(t, g.Addr, "/vercel", big, nil); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 before the limit is raised", status)
	}

	cfg.Providers["vercel"] = "100"
	g.ReloadConfig(cfg, []string{"vercel-1", "cloudflare-1"})
	g.WaitForCondition(reloadSettle, "vercel maxBody == 100", func(st *StatsView) bool {
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

func TestE2E_InvalidConfigKeepsServing(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
	})
	g := NewGateway(t, cfg)

	if status, _, _ := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 before reload\nlogs:\n%s", status, g.Logs())
	}

	// Malformed YAML, an unknown key (removed settings must fail strict
	// decoding), an empty pool, and a reserved provider label all fail
	// validation; the last-known-good config keeps serving through every
	// rejection.
	badConfigs := map[string]string{
		"malformed yaml":  "log-level: [unclosed\nmax-retries: nope\n",
		"unknown key":     "api-keys: [x]\n" + renderConfig(cfg),
		"empty pool":      "relays: []\n",
		"reserved label":  "relays:\n  - provider: all\n    url: 'https://x.example'\n",
		"duplicate names": "relays:\n  - name: a\n    provider: vercel\n    url: 'https://a.example'\n  - name: a\n    provider: vercel\n    url: 'https://b.example'\n",
	}
	for name, raw := range badConfigs {
		g.ReloadRaw(raw)
		waitForLog(t, g, `"msg":"reload failed`, reloadSettle)
		t.Logf("rejected %s as expected", name)
	}

	// The original generation is untouched: same relays, still serving, and
	// rotation works exactly as before.
	st, err := g.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if names := relayNames(st); !equalStrings(names, []string{"vercel-1", "vercel-2"}) {
		t.Fatalf("relays after invalid reloads = %v, want unchanged", names)
	}
	for i, want := range []*edgeSim{v2, v1, v2} {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != want.servedBody() {
			t.Fatalf("request %d after invalid reloads: status=%d body=%q, want 200 %q",
				i, status, body, want.servedBody())
		}
	}
}
