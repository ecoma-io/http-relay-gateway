package store

import (
	"errors"
	"testing"
)

func TestAccountCRUD(t *testing.T) {
	s := openTestStore(t)

	id, err := s.CreateAccount("main", "vercel", "v-token-1234", "user-1", 100)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if _, err := s.CreateAccount("main", "deno", "d", "", 0); !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("duplicate name = %v", err)
	}
	if _, err := s.CreateAccount("other", "nomad", "d", "", 0); !errors.Is(err, ErrBadPlatform) {
		t.Fatalf("bad platform = %v", err)
	}

	// The token is readable only through Tokens.
	token, err := s.Tokens().PlatformToken(id)
	if err != nil || token != "v-token-1234" {
		t.Fatalf("PlatformToken = %q, %v", token, err)
	}
	accounts, err := s.Accounts()
	if err != nil || len(accounts) != 1 {
		t.Fatalf("Accounts = %+v, %v", accounts, err)
	}
	if accounts[0].Token != "" {
		t.Fatal("Accounts leaked the token")
	}

	renamed := "renamed"
	if err := s.UpdateAccount(id, AccountPatch{Name: &renamed}); err != nil {
		t.Fatalf("UpdateAccount: %v", err)
	}
	if err := s.MarkAccountVerified(id, "canonical-ref", 200); err != nil {
		t.Fatalf("MarkAccountVerified: %v", err)
	}
	row, err := s.Account(id)
	if err != nil {
		t.Fatalf("Account: %v", err)
	}
	if row.Name != "renamed" || row.AccountRef != "canonical-ref" || row.VerifiedAt != 200 {
		t.Fatalf("row = %+v", row)
	}
	if _, err := s.Account(999); !errors.Is(err, ErrNoAccount) {
		t.Fatalf("missing account = %v", err)
	}
}

func TestDeleteAccountGuardsReferences(t *testing.T) {
	s := openTestStore(t)
	accountID, err := s.CreateAccount("main", "vercel", "t", "ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	relayID, err := s.CreateRelay("managed-one", "vercel", "https://r.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay: %v", err)
	}
	if err := s.UpsertDeployment(DeploymentRow{RelayID: relayID, AccountID: accountID, Platform: "vercel", URL: "https://r.example", AuthToken: "tok", Status: DeployActive}); err != nil {
		t.Fatalf("UpsertDeployment: %v", err)
	}

	if err := s.DeleteAccount(accountID, false); !errors.Is(err, ErrAccountInUse) {
		t.Fatalf("guarded delete = %v", err)
	}
	// The guarded delete changed nothing.
	if _, err := s.Account(accountID); err != nil {
		t.Fatalf("account vanished: %v", err)
	}

	if err := s.DeleteAccount(accountID, true); err != nil {
		t.Fatalf("forced delete: %v", err)
	}
	if _, err := s.Account(accountID); !errors.Is(err, ErrNoAccount) {
		t.Fatalf("account after force = %v", err)
	}
	// The relay survives, detached and deploymentless (the deployment
	// cascaded away with its account reference).
	row, err := s.Relay(relayID)
	if err != nil {
		t.Fatalf("relay after force: %v", err)
	}
	if row.AccountID != nil || row.Deployment != nil {
		t.Fatalf("relay = %+v", row)
	}
}

func TestDeploymentUpsertAndStatus(t *testing.T) {
	s := openTestStore(t)
	accountID, err := s.CreateAccount("main", "deno", "t", "ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	relayID, err := s.CreateRelay("managed-one", "deno", "https://own.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay: %v", err)
	}

	// Before a deployment the managed relay serves its own URL, tokenless.
	row, err := s.Relay(relayID)
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if row.Deployment != nil || row.URL != "https://own.example" {
		t.Fatalf("pre-deploy row = %+v", row)
	}

	first := DeploymentRow{
		RelayID: relayID, AccountID: accountID, Platform: "deno", Project: "managed-one",
		ExternalID: "dep_1", URL: "https://managed-one.deno.dev", Version: "1",
		AuthToken: "token-aaaa", Status: DeployActive, LastCheckedAt: 10, DeployedAt: 10,
	}
	if err := s.UpsertDeployment(first); err != nil {
		t.Fatalf("insert deployment: %v", err)
	}

	// A redeploy overwrites the single row instead of adding a second.
	second := first
	second.ExternalID, second.AuthToken, second.Version = "dep_2", "token-bbbb", "2"
	if err := s.UpsertDeployment(second); err != nil {
		t.Fatalf("redeploy: %v", err)
	}
	deployments, err := s.db.Query(`SELECT COUNT(*) FROM deployments`)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	var count int
	if !deployments.Next() {
		t.Fatal("no count row")
	}
	if err := deployments.Scan(&count); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := deployments.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if count != 1 {
		t.Fatalf("deployment rows = %d", count)
	}

	// Draining to stale removes the row from the serving join.
	if err := s.SetDeploymentStatus(relayID, DeployStale, "gateway version 2, relay reports 1", 20); err != nil {
		t.Fatalf("SetDeploymentStatus: %v", err)
	}
	row, err = s.Relay(relayID)
	if err != nil {
		t.Fatalf("Relay after stale: %v", err)
	}
	if row.Deployment != nil {
		t.Fatalf("stale deployment still joins: %+v", row.Deployment)
	}
	got, err := s.Deployment(relayID)
	if err != nil || got.Status != DeployStale || got.LastError == "" || got.AuthToken != "token-bbbb" {
		t.Fatalf("Deployment = %+v, %v", got, err)
	}

	// Reactivating puts it back.
	if err := s.SetDeploymentStatus(relayID, DeployActive, "", 30); err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	if row, err = s.Relay(relayID); err != nil || row.Deployment == nil {
		t.Fatalf("reactivated row = %+v, %v", row, err)
	}
}

func TestAdoptRelayAtomic(t *testing.T) {
	s := openTestStore(t)
	// A legacy relay arrives without an account, exactly as first-boot
	// import plants it.
	if _, err := s.SyncLegacy(
		RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000},
		nil,
		[]LegacyRelay{{Name: "imported", Provider: "vercel", URL: "https://public.example", Active: true}},
	); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	rows, err := s.Relays()
	if err != nil || len(rows) != 1 {
		t.Fatalf("Relays = %+v, %v", rows, err)
	}
	legacyID := rows[0].ID
	if rows[0].Origin != OriginLegacy || rows[0].AccountID != nil {
		t.Fatalf("seeded relay = %+v", rows[0])
	}
	accountID, err := s.CreateAccount("main", "vercel", "t", "ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	deployment := DeploymentRow{
		RelayID: legacyID, AccountID: accountID, Platform: "vercel", Project: "imported",
		URL: "https://imported.vercel.app", Version: "1", AuthToken: "adopt-tok",
		Status: DeployActive, LastCheckedAt: 5, DeployedAt: 5,
	}
	if err := s.AdoptRelay(legacyID, accountID, deployment); err != nil {
		t.Fatalf("AdoptRelay: %v", err)
	}
	row, err := s.Relay(legacyID)
	if err != nil {
		t.Fatalf("Relay: %v", err)
	}
	if row.Origin != OriginManaged || row.AccountID == nil || row.Deployment == nil ||
		row.Deployment.AuthToken != "adopt-tok" || row.Deployment.URL != "https://imported.vercel.app" {
		t.Fatalf("adopted row = %+v", row)
	}

	if err := s.AdoptRelay(999, accountID, deployment); !errors.Is(err, ErrNoRelay) {
		t.Fatalf("missing relay = %v", err)
	}
	if err := s.AdoptRelay(legacyID, 999, deployment); !errors.Is(err, ErrNoAccount) {
		t.Fatalf("missing account = %v", err)
	}
}

func TestPlatformCounts(t *testing.T) {
	s := openTestStore(t)
	accountID, err := s.CreateAccount("main", "vercel", "t", "ref", 1)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	relayID, err := s.CreateRelay("one", "vercel", "https://one.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay: %v", err)
	}
	if err := s.UpsertDeployment(DeploymentRow{RelayID: relayID, AccountID: accountID, Platform: "vercel", URL: "https://one.example", AuthToken: "t", Status: DeployActive}); err != nil {
		t.Fatalf("UpsertDeployment: %v", err)
	}
	relay2, err := s.CreateRelay("two", "vercel", "https://two.example", true, &accountID, nil)
	if err != nil {
		t.Fatalf("CreateRelay: %v", err)
	}
	if err := s.UpsertDeployment(DeploymentRow{RelayID: relay2, AccountID: accountID, Platform: "vercel", URL: "https://two.example", AuthToken: "t", Status: DeployStale}); err != nil {
		t.Fatalf("UpsertDeployment: %v", err)
	}
	counts, err := s.PlatformCounts()
	if err != nil {
		t.Fatalf("PlatformCounts: %v", err)
	}
	if counts[DeployActive] != 1 || counts[DeployStale] != 1 {
		t.Fatalf("counts = %v", counts)
	}
}
