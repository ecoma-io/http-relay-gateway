package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// openTestStore opens a store on a fresh file in a per-test directory.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	return openStoreAt(t, filepath.Join(t.TempDir(), "gateway.db"))
}

func openStoreAt(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func syncTwo(s *Store) (int, error) {
	return s.SyncLegacy(
		RuntimeValues{LogLevel: "debug", MaxRetries: 4, FailureThreshold: 5, CooldownMs: 45000},
		[]ProviderRow{{Name: "vercel", MaxBody: 4_500_000}},
		[]LegacyRelay{
			{Name: "a", Provider: "vercel", URL: "https://a.example", Active: true},
			{Name: "b", Provider: "vercel", URL: "https://b.example", Active: false},
		},
	)
}

func TestMigrateFreshAndIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	s := openStoreAt(t, path)
	var version int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("schema version = %d, want 2 after fresh migrate", version)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening the same file must neither re-apply nor error.
	reopened := openStoreAt(t, path)
	if err := reopened.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("schema version = %d after reopen, want still 2", version)
	}
}

func TestSettingsDefaults(t *testing.T) {
	s := openTestStore(t)
	settings, err := s.Settings()
	if err != nil {
		t.Fatal(err)
	}
	if settings != (Settings{
		LogLevel:                 DefaultLogLevel,
		MaxRetries:               DefaultMaxRetries,
		FailureThreshold:         DefaultFailureThreshold,
		CooldownMs:               DefaultCooldownMs,
		StreamThresholdBytes:     DefaultStreamThresholdBytes,
		DialTimeoutMs:            DefaultDialTimeoutMs,
		ResponseHeaderTimeoutMs:  DefaultResponseHeaderTimeoutMs,
		ReconcileIntervalSeconds: DefaultReconcileIntervalSeconds,
	}) {
		t.Fatalf("fresh settings = %+v, want defaults", settings)
	}
}

func TestSyncLegacyInsertUpdateDelete(t *testing.T) {
	s := openTestStore(t)

	imported, err := syncTwo(s)
	if err != nil {
		t.Fatal(err)
	}
	if imported != 2 {
		t.Fatalf("first sync inserted %d relays, want 2", imported)
	}
	if empty, err := s.IsEmpty(); err != nil || empty {
		t.Fatalf("IsEmpty after sync = %v (err %v), want false", empty, err)
	}
	settings, err := s.Settings()
	if err != nil {
		t.Fatal(err)
	}
	if settings.LogLevel != "debug" || settings.MaxRetries != 4 || settings.FailureThreshold != 5 || settings.CooldownMs != 45000 {
		t.Fatalf("settings after sync = %+v", settings)
	}
	providers, err := s.Providers()
	if err != nil || len(providers) != 1 || providers[0].Name != "vercel" || providers[0].MaxBody != 4_500_000 {
		t.Fatalf("providers after sync = %+v (err %v)", providers, err)
	}

	// Second sync: a survives (id stable), b leaves the file (deleted),
	// c arrives (new id after a).
	next := []LegacyRelay{
		{Name: "a", Provider: "cloudflare", URL: "https://a2.example", Active: true},
		{Name: "c", Provider: "vercel", URL: "https://c.example", Active: true},
	}
	imported, err = s.SyncLegacy(RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000}, nil, next)
	if err != nil {
		t.Fatal(err)
	}
	if imported != 1 {
		t.Fatalf("second sync inserted %d relays, want 1", imported)
	}
	relays, err := s.Relays()
	if err != nil {
		t.Fatal(err)
	}
	if len(relays) != 2 {
		t.Fatalf("relays after second sync = %d rows, want 2", len(relays))
	}
	if relays[0].Name != "a" || relays[0].Provider != "cloudflare" || relays[0].URL != "https://a2.example" || !relays[0].Active {
		t.Fatalf("updated relay a = %+v", relays[0])
	}
	if relays[0].Origin != OriginLegacy || relays[0].ID >= relays[1].ID {
		t.Fatalf("relay order/origin wrong: %+v", relays)
	}
	if relays[1].Name != "c" {
		t.Fatalf("relay[1] = %q, want the new c", relays[1].Name)
	}
}

func TestSyncLegacyDeletesEverythingWhenFileHasNoRelays(t *testing.T) {
	s := openTestStore(t)
	if _, err := syncTwo(s); err != nil {
		t.Fatal(err)
	}
	imported, err := s.SyncLegacy(RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if imported != 0 {
		t.Fatalf("empty-file sync inserted %d, want 0", imported)
	}
	relays, err := s.Relays()
	if err != nil || len(relays) != 0 {
		t.Fatalf("relays after empty-file sync = %v (err %v), want none", relays, err)
	}
}

func TestSyncLegacyKeepsNonBridgeState(t *testing.T) {
	s := openTestStore(t)
	if _, err := syncTwo(s); err != nil {
		t.Fatal(err)
	}
	// Database-native state a YAML sync must never touch: a managed relay
	// and an admin-plane settings key.
	if err := insertManagedRelayForTest(s, "managed-1", "https://m.example", "tok-123"); err != nil {
		t.Fatal(err)
	}
	if err := setSettingDirect(s, "admin_password_hash", "bcrypt-digest"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SyncLegacy(RuntimeValues{LogLevel: "warn", MaxRetries: 1, FailureThreshold: 2, CooldownMs: 1000}, nil,
		[]LegacyRelay{{Name: "a", Provider: "vercel", URL: "https://a.example", Active: true}}); err != nil {
		t.Fatal(err)
	}

	relays, err := s.Relays()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]RelayRow{}
	for _, row := range relays {
		found[row.Name] = row
	}
	if row, ok := found["managed-1"]; !ok || row.Origin != OriginManaged || row.Deployment == nil {
		t.Fatalf("managed relay survived sync? %+v (all: %v)", row, relays)
	}
	if _, ok := found["b"]; ok {
		t.Fatal("legacy relay b left the YAML but survived the sync")
	}
	var hash string
	if err := s.db.QueryRow(`SELECT value FROM settings WHERE key = 'admin_password_hash'`).Scan(&hash); err != nil || hash != "bcrypt-digest" {
		t.Fatalf("admin setting survived sync? %q (err %v)", hash, err)
	}
}

func TestRelaysJoinsOnlyActiveDeployment(t *testing.T) {
	s := openTestStore(t)
	if err := insertManagedRelayForTest(s, "live", "https://db-url.example", "tok-live"); err != nil {
		t.Fatal(err)
	}
	// A second managed relay whose deployment is stuck deploying: the join
	// must skip it, so the pool never sees an unverified URL.
	if _, err := s.db.Exec(`
		INSERT INTO relays (name, provider, url, active, origin, created_at, updated_at)
		VALUES ('pending', 'vercel', 'https://relay-row.example', 1, 'managed', strftime('%s','now'), strftime('%s','now'))`); err != nil {
		t.Fatal(err)
	}
	var pendingID int64
	if err := s.db.QueryRow(`SELECT id FROM relays WHERE name = 'pending'`).Scan(&pendingID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`
		INSERT INTO deployments (relay_id, account_id, platform, url, auth_token, status, created_at, updated_at)
		VALUES (?, 1, 'vercel', 'https://unverified.example', 'tok-pending', 'deploying', strftime('%s','now'), strftime('%s','now'))`,
		pendingID); err != nil {
		t.Fatal(err)
	}

	relays, err := s.Relays()
	if err != nil {
		t.Fatal(err)
	}
	if len(relays) != 2 {
		t.Fatalf("relays = %d rows, want 2", len(relays))
	}
	if relays[0].Name != "live" || relays[0].Deployment == nil || relays[0].Deployment.URL != "https://db-url.example" {
		t.Fatalf("active relay row = %+v", relays[0])
	}
	if relays[0].InactiveDeployment {
		t.Fatalf("active deployment must not set InactiveDeployment: %+v", relays[0])
	}
	if relays[1].Name != "pending" || relays[1].Deployment != nil || !relays[1].InactiveDeployment {
		t.Fatalf("non-active relay must have no deployment pointer but the inactive flag: %+v", relays[1])
	}
}

// TestPausedDeploymentRoundTrip pins the v2 status end to end: a paused row
// satisfies the CHECK, comes back verbatim, and is findable by the revival
// scan's status query.
func TestPausedDeploymentRoundTrip(t *testing.T) {
	s := openTestStore(t)
	if err := insertManagedRelayForTest(s, "paused-1", "https://p.example", "tok-paused"); err != nil {
		t.Fatal(err)
	}
	var relayID int64
	if err := s.db.QueryRow(`SELECT id FROM relays WHERE name = 'paused-1'`).Scan(&relayID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if err := s.SetDeploymentStatus(relayID, DeployPaused, "relay answered HTTP 402, not a relay worker (DEPLOYMENT_DISABLED)", now); err != nil {
		t.Fatalf("SetDeploymentStatus paused: %v", err)
	}
	dep, err := s.Deployment(relayID)
	if err != nil || dep.Status != DeployPaused || dep.LastError == "" {
		t.Fatalf("paused row = %+v, %v", dep, err)
	}
	ids, err := s.DeploymentRelayIDsByStatus(DeployPaused)
	if err != nil || len(ids) != 1 || ids[0] != relayID {
		t.Fatalf("DeploymentRelayIDsByStatus(paused) = %v, %v", ids, err)
	}
	if ids, err := s.DeploymentRelayIDsByStatus(DeployActive); err != nil || len(ids) != 0 {
		t.Fatalf("DeploymentRelayIDsByStatus(active) = %v, %v; the relay just paused", ids, err)
	}
}

// TestRelayOriginAndNameConflicts pins the lifecycle encoding at the store
// seam: the origin derives from the account at create time (no account =
// legacy, adoptable; with one = managed, reconciler-owned), and duplicate
// names are refused on create and on rename alike.
func TestRelayOriginAndNameConflicts(t *testing.T) {
	s := openTestStore(t)

	legacyID, err := s.CreateRelay("edge", "vercel", "https://edge.example", true, nil, nil)
	if err != nil {
		t.Fatalf("CreateRelay legacy: %v", err)
	}
	row, err := s.Relay(legacyID)
	if err != nil || row.Origin != OriginLegacy || row.AccountID != nil {
		t.Fatalf("account-less create = %+v, %v; want born legacy", row, err)
	}

	accountID, err := s.CreateAccount("main", "vercel", "platform-token", "ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	managedID, err := s.CreateRelay("managed", "vercel", "https://m.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay managed: %v", err)
	}
	row, err = s.Relay(managedID)
	if err != nil || row.Origin != OriginManaged || row.AccountID == nil || *row.AccountID != accountID {
		t.Fatalf("managed create = %+v, %v; want born managed on the account", row, err)
	}

	// Duplicate names conflict on create and on rename onto another row.
	if _, err := s.CreateRelay("edge", "vercel", "https://x.example", true, nil, nil); !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("duplicate create = %v, want ErrDuplicateName", err)
	}
	taken := "edge"
	if err := s.UpdateRelay(managedID, RelayPatch{Name: &taken}); !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("renaming onto a taken name = %v, want ErrDuplicateName", err)
	}
	// Renaming a row to its own name is a no-op, not a conflict.
	same := "managed"
	if err := s.UpdateRelay(managedID, RelayPatch{Name: &same}); err != nil {
		t.Fatalf("self rename = %v", err)
	}

	// Unknown ids are ErrNoRelay on every mutation.
	url := "https://moved.example"
	if err := s.UpdateRelay(999, RelayPatch{URL: &url}); !errors.Is(err, ErrNoRelay) {
		t.Fatalf("update missing relay = %v, want ErrNoRelay", err)
	}
	if err := s.DeleteRelay(999); !errors.Is(err, ErrNoRelay) {
		t.Fatalf("delete missing relay = %v, want ErrNoRelay", err)
	}
}

func TestTokensIsolation(t *testing.T) {
	s := openTestStore(t)
	if err := insertManagedRelayForTest(s, "managed-1", "https://m.example", "tok-1234"); err != nil {
		t.Fatal(err)
	}
	var relayID int64
	if err := s.db.QueryRow(`SELECT id FROM relays WHERE name = 'managed-1'`).Scan(&relayID); err != nil {
		t.Fatal(err)
	}

	tok, err := s.Tokens().RelayAuthToken(relayID)
	if err != nil || tok != "tok-1234" {
		t.Fatalf("RelayAuthToken = %q (err %v), want tok-1234", tok, err)
	}
	if _, err := s.Tokens().RelayAuthToken(relayID + 100); err != ErrNotFound {
		t.Fatalf("unknown relay token err = %v, want ErrNotFound", err)
	}
	if _, err := s.Tokens().PlatformToken(999); err != ErrNotFound {
		t.Fatalf("unknown account token err = %v, want ErrNotFound", err)
	}
	tok, err = s.Tokens().PlatformToken(1)
	if err != nil || tok != "platform-token" {
		t.Fatalf("PlatformToken(1) = %q (err %v), want platform-token", tok, err)
	}
}

func TestForeignKeysEnforced(t *testing.T) {
	s := openTestStore(t)
	_, err := s.db.Exec(`
		INSERT INTO relays (name, provider, url, active, origin, account_id, created_at, updated_at)
		VALUES ('orphan', 'vercel', 'https://x.example', 1, 'managed', 999, strftime('%s','now'), strftime('%s','now'))`)
	if err == nil {
		t.Fatal("insert with unknown account_id must violate the foreign key")
	}
}

func TestStatePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	s := openStoreAt(t, path)
	if _, err := syncTwo(s); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStoreAt(t, path)
	relays, err := reopened.Relays()
	if err != nil {
		t.Fatal(err)
	}
	if len(relays) != 2 || relays[0].Name != "a" || relays[1].Name != "b" {
		t.Fatalf("relays after reopen = %+v, want a then b", relays)
	}
	settings, err := reopened.Settings()
	if err != nil || settings.MaxRetries != 4 {
		t.Fatalf("settings after reopen = %+v (err %v)", settings, err)
	}
}

func TestChangesCoalesce(t *testing.T) {
	s := openTestStore(t)
	// Three rapid mutations may leave at most one pending wakeup.
	for range 3 {
		if _, err := s.SyncLegacy(RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000}, nil,
			[]LegacyRelay{{Name: "a", Provider: "vercel", URL: "https://a.example", Active: true}}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-s.Changes():
	default:
		t.Fatal("no pending change after mutations")
	}
	select {
	case <-s.Changes():
		t.Fatal("more than one pending change — coalescing broken")
	default:
	}
}

// insertManagedRelayForTest wires a managed relay with an account and an
// active deployment in one go, the raw-SQL equivalent of a completed deploy.
func insertManagedRelayForTest(s *Store, name, url, token string) error {
	if _, err := s.db.Exec(`
		INSERT INTO platform_accounts (id, name, platform, token, created_at, updated_at)
		VALUES (1, 'acct', 'vercel', 'platform-token', strftime('%s','now'), strftime('%s','now'))`); err != nil {
		return err
	}
	if _, err := s.db.Exec(`
		INSERT INTO relays (name, provider, url, active, origin, account_id, created_at, updated_at)
		VALUES (?, 'vercel', ?, 1, 'managed', 1, strftime('%s','now'), strftime('%s','now'))`, name, url); err != nil {
		return err
	}
	var relayID int64
	if err := s.db.QueryRow(`SELECT id FROM relays WHERE name = ?`, name).Scan(&relayID); err != nil {
		return err
	}
	_, err := s.db.Exec(`
		INSERT INTO deployments (relay_id, account_id, platform, url, version, auth_token, status, created_at, updated_at)
		VALUES (?, 1, 'vercel', ?, 'v1', ?, 'active', strftime('%s','now'), strftime('%s','now'))`,
		relayID, url, token)
	return err
}

// setSettingDirect writes a settings key bypassing the bridge, standing in
// for admin-plane state that does not exist yet.
func setSettingDirect(s *Store, key, value string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := setSetting(tx, key, value); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
