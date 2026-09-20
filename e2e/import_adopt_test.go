package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
)

// TestE2E_BulkImportAndAdopt walks the migration path for a relay list
// managed outside the gateway: a 17-line bulk import (one deliberately
// inactive), per-row rejection without failing the batch, then adoption of
// an imported relay onto a platform account — its URL comes back managed,
// tokenized, and serving through the worker sim.
func TestE2E_BulkImportAndAdopt(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	if code := g.Login(t, adminPassword); code != http.StatusOK {
		t.Fatalf("login = %d", code)
	}

	// The legacy list. Every URL is a refused local connection on purpose:
	// the strict readiness gate means none of them may ever be admitted,
	// and a closed listener fails the probe deterministically (no DNS
	// dependency in CI).
	items := make([]map[string]any, 0, 17)
	for i := 1; i <= 17; i++ {
		items = append(items, map[string]any{
			"name":     fmt.Sprintf("edge-%02d", i),
			"provider": "vercel",
			"url":      deadRelayURL(t),
			"active":   i != 17,
		})
	}
	code, _, body := g.AdminDo(t, http.MethodPost, "/api/v1/relays/import", map[string]any{"items": items})
	if code != http.StatusOK {
		t.Fatalf("import = %d: %s", code, body)
	}
	var reply struct {
		Imported int `json:"imported"`
		Rejected []struct {
			Name  string `json:"name"`
			Error string `json:"error"`
		} `json:"rejected"`
	}
	if err := json.Unmarshal([]byte(body), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Imported != 17 || len(reply.Rejected) != 0 {
		t.Fatalf("import = %d imported, rejected %v", reply.Imported, reply.Rejected)
	}

	// A batch with every rejection mode. Rows fail individually: the only
	// one that lands is the first "twin" — its duplicate is rejected
	// in-batch, the established name and the malformed rows are rejected
	// against their own validation.
	bad := []map[string]any{
		{"name": "edge-01", "provider": "vercel", "url": "https://dup.example"},
		{"name": "twin", "provider": "vercel", "url": deadRelayURL(t)},
		{"name": "twin", "provider": "vercel", "url": "https://twin-b.example"},
		{"name": "no-url", "provider": "vercel"},
		{"name": "bad-scheme", "provider": "vercel", "url": "ftp://x.example"},
		{"name": "reserved", "provider": "all", "url": "https://x.example"},
	}
	code, _, body = g.AdminDo(t, http.MethodPost, "/api/v1/relays/import", map[string]any{"items": bad})
	if code != http.StatusOK {
		t.Fatalf("bad import = %d: %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Imported != 1 || len(reply.Rejected) != 5 {
		t.Fatalf("bad import = %d imported, %d rejected (%v)", reply.Imported, len(reply.Rejected), reply.Rejected)
	}
	seen := map[string]bool{}
	for _, row := range reply.Rejected {
		if row.Error == "" {
			t.Fatalf("rejection without a reason: %+v", row)
		}
		seen[row.Name] = true
	}
	for _, want := range []string{"edge-01", "twin", "no-url", "bad-scheme", "reserved"} {
		if !seen[want] {
			t.Fatalf("expected a rejection named %q, got %v", want, reply.Rejected)
		}
	}

	// Strict gate over the whole pool: every imported URL is unreachable,
	// so none of them may ever earn admission. The rows land in the
	// registry lifecycle — with the failure reason — not in the traffic
	// pool, and /readyz stays 503 with zero ready relays.
	g.WaitForCondition(applySettle, "unreachable imports recorded in lifecycle", func(st *StatsView) bool {
		if st.Readiness.ReadyRelays != 0 {
			return false
		}
		row, ok := st.lifecycle("edge-01")
		return ok && row.State != "" && row.Reason != ""
	})
	st, err := g.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.relay("edge-01"); ok {
		t.Fatalf("unreachable imported relay entered the pool: %+v", st.Relays)
	}

	// Accounts and relay ids for the adoption leg.
	code, _, body = g.AdminDo(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "e2e-fake-vercel-platform-token",
	})
	if code != http.StatusCreated {
		t.Fatalf("account create = %d: %s", code, body)
	}
	var account struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &account); err != nil {
		t.Fatal(err)
	}
	code, _, body = g.AdminDo(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "cf-edge", "provider": "cloudflare", "url": deadRelayURL(t),
	})
	if code != http.StatusCreated {
		t.Fatalf("cloudflare relay create = %d: %s", code, body)
	}

	type relayRow struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Origin string `json:"origin"`
	}
	list := func() []relayRow {
		t.Helper()
		code, _, body := g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
		if code != http.StatusOK {
			t.Fatalf("relay list = %d: %s", code, body)
		}
		var rows []relayRow
		if err := json.Unmarshal([]byte(body), &rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	byName := func(name string) relayRow {
		t.Helper()
		for _, row := range list() {
			if row.Name == name {
				return row
			}
		}
		t.Fatalf("relay %q not found", name)
		return relayRow{}
	}

	edge := byName("edge-01")
	if edge.Origin != "legacy" {
		t.Fatalf("imported relay origin = %q, want legacy", edge.Origin)
	}

	// Adoption refuses an account whose platform is not the relay's provider.
	cf := byName("cf-edge")
	code, _, body = g.AdminDo(t, http.MethodPost, relayPath(cf.ID)+"/adopt", map[string]any{
		"accountId": account.ID,
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("mismatched adopt = %d: %s", code, body)
	}

	// Happy path: the imported relay becomes a managed deployment.
	code, _, body = g.AdminDo(t, http.MethodPost, relayPath(edge.ID)+"/adopt", map[string]any{
		"accountId": account.ID,
	})
	if code != http.StatusAccepted {
		t.Fatalf("adopt = %d: %s", code, body)
	}
	dep := waitForDeployment(t, g, edge.ID, "active", 30*time.Second)
	if dep.URL != sim.srv.URL {
		t.Fatalf("adopted deployment URL = %q, want the sim %q", dep.URL, sim.srv.URL)
	}

	g.WaitForCondition(applySettle, "adopted relay serving managed", func(st *StatsView) bool {
		row, ok := st.relay("edge-01")
		return ok && row.Origin == "managed" && row.Active && row.Healthy
	})

	// Traffic through the gateway reaches the adopted URL with the token.
	code, _, respBody := relayDo(t, g.Addr, "/", "{}", map[string]string{
		"X-Relay-Target": sim.srv.URL, "X-Relay-Path": "/up",
	})
	if code != http.StatusOK || !strings.Contains(respBody, `"served":true`) {
		t.Fatalf("relayed request = %d %q", code, respBody)
	}
	last, ok := sim.lastRequest()
	tokens := platform.deployedTokens()
	if !ok || last.AuthToken == "" || len(tokens) != 1 || last.AuthToken != tokens[0] {
		t.Fatalf("gateway did not adopt-authenticate (sim auth %q, tokens %d)", last.AuthToken, len(tokens))
	}

	// Nothing about the adoption leaked a token into the logs.
	logs := g.Logs()
	for _, token := range append(tokens, sim.tokens()...) {
		if strings.Contains(logs, token) {
			t.Fatalf("relay token %q… leaked into logs", token[:6])
		}
	}
}
