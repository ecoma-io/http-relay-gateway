package e2e_test

import (
	"net/http"
	"testing"
)

// TestE2E_FirstBootImportAndRestartPersistence walks the storage contract:
// an empty database adopts the legacy YAML on first boot, and after a
// restart with the YAML gone the same pool serves from the database alone.
func TestE2E_FirstBootImportAndRestartPersistence(t *testing.T) {
	sim := NewEdgeSim(t, "persist-a")
	g := NewGateway(t, defaultGatewayConfig([]RelayConfig{
		{Name: "persist-a", Provider: "vercel", URL: sim.URL},
	}))

	// First boot: the empty database imported the YAML wholesale.
	waitForLog(t, g, `"msg":"imported legacy config into database"`, reloadSettle)
	g.WaitForRelays([]string{"persist-a"}, reloadSettle)

	// The imported relay actually forwards: the generation was built from
	// database rows, not the YAML, before the first request.
	if status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK || body != sim.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q after import\nlogs:\n%s",
			status, body, sim.servedBody(), g.Logs())
	}

	// Deleting the YAML removes the only non-database state; the restarted
	// process must reconstruct the identical pool from SQLite.
	g.RemoveConfig()
	g.Restart()
	waitForLog(t, g, `"msg":"legacy config absent; serving from database"`, reloadSettle)
	g.WaitForRelays([]string{"persist-a"}, reloadSettle)

	if status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK || body != sim.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q after restart\nlogs:\n%s",
			status, body, sim.servedBody(), g.Logs())
	}
}

// TestE2E_RestartResyncsLegacyConfig pins the bridge's later-boot behavior:
// the YAML remains authoritative for its relays, so a restart with a changed
// file converges the database back to the file's content.
func TestE2E_RestartResyncsLegacyConfig(t *testing.T) {
	a := NewEdgeSim(t, "sync-a")
	b := NewEdgeSim(t, "sync-b")
	cfg := defaultGatewayConfig([]RelayConfig{
		{Name: "sync-a", Provider: "vercel", URL: a.URL},
	})
	g := NewGateway(t, cfg)
	waitForLog(t, g, `"msg":"imported legacy config into database"`, reloadSettle)

	// Simulate a fleet change made while the process was down: rewrite the
	// config before restarting, then confirm the database follows the file.
	cfg.Relays = []RelayConfig{
		{Name: "sync-b", Provider: "vercel", URL: b.URL},
	}
	g.writeConfig(cfg)
	g.Restart()

	g.WaitForRelays([]string{"sync-b"}, reloadSettle)
	if status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil); status != http.StatusOK || body != b.servedBody() {
		t.Fatalf("status=%d body=%q, want 200 %q after resync\nlogs:\n%s",
			status, body, b.servedBody(), g.Logs())
	}
}
