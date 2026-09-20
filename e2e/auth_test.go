package e2e_test

import (
	"net/http"
	"strings"
	"testing"
)

// TestE2E_AdminAuthFlow drives the whole credential lifecycle against the
// real process: locked before setup, unlocked by setup, locked again by
// logout, unlocked by login.
func TestE2E_AdminAuthFlow(t *testing.T) {
	sim := NewEdgeSim(t, "auth-sim")
	g := NewGateway(t)

	// A fresh client with no session is locked out of management.
	code, _, _ := g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("pre-setup relays status = %d, want 401", code)
	}

	g.Setup(t, adminPassword)
	g.CreateRelay(t, RelaySeed{Name: "auth-sim", Provider: "vercel", URL: sim.URL})

	// Logout clears the session server-side (cookie expiry), locking the
	// client out again.
	code, _, _ = g.AdminDo(t, http.MethodPost, "/api/v1/logout", nil)
	if code != http.StatusOK {
		t.Fatalf("logout status = %d, want 200", code)
	}
	code, _, _ = g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("post-logout relays status = %d, want 401", code)
	}

	// A wrong password never authenticates; the right one restores access.
	if code := g.Login(t, "definitely-wrong-1"); code != http.StatusUnauthorized {
		t.Fatalf("wrong-password login status = %d, want 401", code)
	}
	if code := g.Login(t, adminPassword); code != http.StatusOK {
		t.Fatalf("correct login status = %d, want 200", code)
	}
	code, _, _ = g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusOK {
		t.Fatalf("post-login relays status = %d, want 200", code)
	}

	// The second client never shares the first one's session.
	other, err := newAdminClient(g.AdminAddr)
	if err != nil {
		t.Fatal(err)
	}
	code, _, _ = other.do(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("independent client relays status = %d, want 401", code)
	}
}

// TestE2E_LoginRateLimit drives the limiter through the real listener: five
// wrong attempts from one address, then even the correct password is refused
// with a Retry-After until the window drains.
func TestE2E_LoginRateLimit(t *testing.T) {
	g := NewGateway(t)
	g.Setup(t, adminPassword)

	for i := range 5 {
		code, headers, _ := g.AdminDo(t, http.MethodPost, "/api/v1/login",
			map[string]string{"password": "wrong-password-" + string(rune('a'+i))})
		if code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status = %d, want 401", i+1, code)
		}
		if headers.Get("Retry-After") != "" {
			t.Fatalf("attempt %d set Retry-After on a plain 401", i+1)
		}
	}

	code, headers, _ := g.AdminDo(t, http.MethodPost, "/api/v1/login",
		map[string]string{"password": adminPassword})
	if code != http.StatusTooManyRequests {
		t.Fatalf("saturated correct login status = %d, want 429", code)
	}
	if headers.Get("Retry-After") == "" {
		t.Fatal("429 without Retry-After")
	}
}

// TestE2E_SecretsNeverLeakIntoObservability asserts the redaction contract on
// the real process: the setup password never appears in captured logs (the
// server must never log request bodies), and relay URLs never appear in
// /stats.
func TestE2E_SecretsNeverLeakIntoObservability(t *testing.T) {
	const secret = "hunter2-log-canary-9x7"
	sim := NewEdgeSim(t, "leak-sim")
	// Under the readiness gate a relay that never verifies never gets
	// admitted, so "dead" must be a live sim that is admitted and then
	// deleted — the delete path is what the leak assertions care about.
	leakDead := NewEdgeSim(t, "leak-dead")
	// Manual flow (not newGatewayWith) so the canary password is the one
	// this gateway actually installs.
	g := NewGateway(t)
	g.Setup(t, secret)
	g.CreateRelay(t, RelaySeed{Name: "leak-sim", Provider: "vercel", URL: sim.URL})
	g.CreateRelay(t, RelaySeed{Name: "leak-dead", Provider: "vercel", URL: leakDead.URL})
	g.WaitForRelays([]string{"leak-sim", "leak-dead"}, applySettle)

	// Debug logging first, so the data-plane traffic below is logged at the
	// chattiest level, then drive success, failover, and management
	// mutations — the paths most likely to log something if redaction ever
	// regresses. The request body carries the canary too: bodies must never
	// be logged either.
	g.PatchSettings(t, map[string]any{"logLevel": "debug"})
	_, _, _ = relayDo(t, g.Addr, "/vercel", `{"secret":"`+secret+`"}`, nil)
	_, _, _ = relayDo(t, g.Addr, "/leak-dead", `{}`, nil)
	id := findRelayID(t, g, "leak-dead")
	g.DeleteRelay(t, id)

	if logs := g.Logs(); strings.Contains(logs, secret) {
		t.Fatalf("setup password leaked into logs:\n%s", logs)
	}
	if raw := g.RawStats(t); strings.Contains(raw, sim.URL) || strings.Contains(raw, leakDead.URL) {
		t.Fatalf("relay URL leaked into /stats: %s", raw)
	}
}
