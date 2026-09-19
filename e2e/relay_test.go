package e2e_test

import (
	"net/http"
	"testing"
)

func TestE2E_ForwardsRelaySpec(t *testing.T) {
	vercel := NewEdgeSim(t, "vercel-1")
	cf := NewEdgeSim(t, "cloudflare-1")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: vercel.URL},
		{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	}))

	status, headers, body := relayDo(t, g.Addr, "/vercel", `{"model":"x"}`, map[string]string{
		"Authorization": "Bearer provider-secret",
	})
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200\nlogs:\n%s", status, g.Logs())
	}
	if body != vercel.servedBody() {
		t.Fatalf("body = %q, want %q", body, vercel.servedBody())
	}
	if headers.Get("X-Sim-Relay") != "vercel-1" {
		t.Fatalf("X-Sim-Relay = %q, want vercel-1", headers.Get("X-Sim-Relay"))
	}

	// The edge sim must receive the relay spec verbatim, with the provider
	// pin stripped and the provider auth flowing through untouched.
	if got := vercel.header("X-Relay-Target"); got != "https://api.example.com" {
		t.Fatalf("upstream X-Relay-Target = %q", got)
	}
	if got := vercel.header("X-Relay-Path"); got != "/v1/messages?beta=true" {
		t.Fatalf("upstream X-Relay-Path = %q", got)
	}
	if got := vercel.header("Authorization"); got != "Bearer provider-secret" {
		t.Fatalf("upstream Authorization = %q", got)
	}
	if got := vercel.header("X-Relay-Provider"); got != "" {
		t.Fatalf("provider pin leaked upstream: %q", got)
	}
	if string(vercel.lastBody()) != `{"model":"x"}` {
		t.Fatalf("upstream body = %q", vercel.lastBody())
	}
	if cf.hitCount() != 0 {
		t.Fatal("pinned vercel request reached the cloudflare relay")
	}
}

func TestE2E_FailoverToLiveRelay(t *testing.T) {
	dead := deadRelayURL(t)
	live := NewEdgeSim(t, "vercel-live")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-dead", Provider: "vercel", URL: dead},
		{Name: "vercel-live", Provider: "vercel", URL: live.URL},
	}))

	status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 via failover\nlogs:\n%s", status, g.Logs())
	}
	if body != live.servedBody() {
		t.Fatalf("body = %q, want %q", body, live.servedBody())
	}

	st := g.WaitForCondition(reloadSettle, "dead relay recorded a failure", func(st *StatsView) bool {
		row, ok := st.relay("vercel-dead")
		return ok && row.Failures >= 1
	})
	if row, _ := st.relay("vercel-live"); row.Requests != 1 {
		t.Fatalf("live relay requests = %d, want 1", row.Requests)
	}
}

func TestE2E_ProviderPinAndRoundRobin(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	cf := NewEdgeSim(t, "cloudflare-1")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		{Name: "vercel-2", Provider: "vercel", URL: v2.URL},
		{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	}))

	// Pinned vercel traffic rotates the vercel cursor only: the round-robin
	// is strictly deterministic, so the served sequence must alternate.
	for i, want := range []*edgeSim{v1, v2, v1, v2} {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != want.servedBody() {
			t.Fatalf("pinned request %d: status=%d body=%q, want 200 %q",
				i, status, body, want.servedBody())
		}
	}
	if cf.hitCount() != 0 {
		t.Fatal("pinned vercel traffic leaked to cloudflare")
	}

	// Auto (no pin) rotates across every provider in config order.
	for i, want := range []*edgeSim{v1, v2, cf, v1, v2, cf} {
		status, _, body := relayDo(t, g.Addr, "/", `{}`, nil)
		if status != http.StatusOK || body != want.servedBody() {
			t.Fatalf("auto request %d: status=%d body=%q, want 200 %q",
				i, status, body, want.servedBody())
		}
	}

	// An explicit auto header behaves like no pin at all.
	status, _, body := relayDo(t, g.Addr, "/", `{}`, map[string]string{
		"X-Relay-Provider": "auto",
	})
	if status != http.StatusOK || body != v1.servedBody() {
		t.Fatalf("auto header: status=%d body=%q, want 200 %q (cursor continues at v1)",
			status, body, v1.servedBody())
	}
}

func TestE2E_BodyLimitSkipsToProvider(t *testing.T) {
	v := NewEdgeSim(t, "vercel-1")
	cf := NewEdgeSim(t, "cloudflare-1")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v.URL},
		{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	})
	cfg.Providers = map[string]string{"vercel": "8", "cloudflare": "100"}
	g := NewGateway(t, cfg)

	// 12 bytes: over vercel's 8-byte limit, under cloudflare's.
	big := "123456789012"

	status, _, _ := relayDo(t, g.Addr, "/vercel", big, nil)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("pinned vercel status = %d, want 413", status)
	}
	if v.hitCount() != 0 {
		t.Fatal("oversize body was forwarded to the rejecting provider")
	}

	status, _, body := relayDo(t, g.Addr, "/", big, nil)
	if status != http.StatusOK {
		t.Fatalf("auto status = %d, want 200 via cloudflare\nlogs:\n%s", status, g.Logs())
	}
	if body != cf.servedBody() {
		t.Fatalf("body = %q, want %q (skip target)", body, cf.servedBody())
	}
}

func TestE2E_BodyOverEveryLimitRejected(t *testing.T) {
	v := NewEdgeSim(t, "vercel-1")
	cf := NewEdgeSim(t, "cloudflare-1")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v.URL},
		{Name: "cloudflare-1", Provider: "cloudflare", URL: cf.URL},
	})
	cfg.Providers = map[string]string{"vercel": "4", "cloudflare": "4"}
	g := NewGateway(t, cfg)

	status, _, _ := relayDo(t, g.Addr, "/", "1234567890", nil)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 when no provider accepts the body", status)
	}
	if v.hitCount() != 0 || cf.hitCount() != 0 {
		t.Fatal("oversize body reached an edge relay")
	}
}

func TestE2E_UnknownProviderRejected(t *testing.T) {
	v := NewEdgeSim(t, "vercel-1")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v.URL},
	}))

	status, _, _ := relayDo(t, g.Addr, "/", `{}`, map[string]string{
		"X-Relay-Provider": "deno",
	})
	if status != http.StatusNotFound {
		t.Fatalf("header pin: status = %d, want 404", status)
	}
	status, _, _ = relayDo(t, g.Addr, "/deno", `{}`, nil)
	if status != http.StatusNotFound {
		t.Fatalf("path prefix: status = %d, want 404", status)
	}
}

func TestE2E_InactiveRelaySkipped(t *testing.T) {
	v1 := NewEdgeSim(t, "vercel-1")
	v2 := NewEdgeSim(t, "vercel-2")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v1.URL},
		{Name: "vercel-2", Provider: "vercel", URL: v2.URL, Active: activePtr(false)},
	}))

	for range 3 {
		status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
		if status != http.StatusOK || body != v1.servedBody() {
			t.Fatalf("status=%d body=%q, want 200 %q", status, body, v1.servedBody())
		}
	}
	if v2.hitCount() != 0 {
		t.Fatal("inactive relay served traffic")
	}
	st, err := g.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if row, ok := st.relay("vercel-2"); !ok || row.Active {
		t.Fatalf("inactive relay stats = %+v, want active=false", row)
	}
}

func TestE2E_HealthAndStatsEndpoints(t *testing.T) {
	v := NewEdgeSim(t, "vercel-1")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "vercel-1", Provider: "vercel", URL: v.URL},
	}))

	st, err := g.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Version == "" {
		t.Fatal("/stats version is empty")
	}
	if row, ok := st.relay("vercel-1"); !ok || !row.Healthy || row.MaxBody != 8<<20 {
		t.Fatalf("vercel-1 row = %+v, want healthy with default maxBody %d", row, int64(8<<20))
	}
}
