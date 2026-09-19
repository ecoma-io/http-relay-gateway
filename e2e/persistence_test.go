package e2e_test

import (
	"net/http"
	"testing"
)

// TestE2E_CreateThenRestartPersistence walks the storage contract: a relay
// created through the admin API lives in SQLite, and a restarted process
// reconstructs the identical pool from the database alone.
func TestE2E_CreateThenRestartPersistence(t *testing.T) {
	sim := NewEdgeSim(t, "persist-a")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "persist-a", Provider: "vercel", URL: sim.URL},
	)

	if status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK || body != sim.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q before restart\nlogs:\n%s",
			status, body, sim.servedBody(), g.Logs())
	}

	// No file carries the relay: the restarted process reads SQLite only.
	g.Restart()
	g.WaitForRelays([]string{"persist-a"}, applySettle)

	if status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK || body != sim.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q after restart\nlogs:\n%s",
			status, body, sim.servedBody(), g.Logs())
	}

	// The session survives the restart too: the JWT secret is database state,
	// so the pre-restart cookie still authorizes the admin API.
	code, _, _ := g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusOK {
		t.Fatalf("post-restart authenticated relays status = %d, want 200", code)
	}
}

// TestE2E_SetupOnceAcrossRestart pins the first-run flow's persistence: the
// setup completion is database state, so a restarted process is past setup
// and the original password still logs in.
func TestE2E_SetupOnceAcrossRestart(t *testing.T) {
	g := NewGateway(t)

	if !g.AdminStatus(t) {
		t.Fatal("fresh gateway reports setupRequired false")
	}
	g.Setup(t, "restart-setup-password-1")
	if g.AdminStatus(t) {
		t.Fatal("setupRequired still true after setup")
	}

	g.Restart()

	if g.AdminStatus(t) {
		t.Fatal("setupRequired true after restart")
	}
	// A fresh client (no cookie) must log in with the original password.
	fresh, err := newAdminClient(g.AdminAddr)
	if err != nil {
		t.Fatal(err)
	}
	g.admin = fresh
	if code := g.Login(t, "restart-setup-password-1"); code != http.StatusOK {
		t.Fatalf("post-restart login status = %d, want 200", code)
	}
	code, _, _ := g.AdminDo(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusOK {
		t.Fatalf("post-login relays status = %d, want 200", code)
	}

	// Setup stays once across restarts, and the stale client's second
	// attempt conflicts.
	code, _, body := g.AdminDo(t, http.MethodPost, "/api/v1/setup", map[string]string{
		"password": "another-password-12", "confirm": "another-password-12",
	})
	if code != http.StatusConflict {
		t.Fatalf("post-restart setup status = %d %s, want 409", code, body)
	}
}
