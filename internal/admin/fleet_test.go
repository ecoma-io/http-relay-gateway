package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/admin"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/store"
)

// fakeFleet records what the admin plane enqueues.
type fakeFleet struct {
	mu       sync.Mutex
	checks   int
	recs     int
	redeploy []int64
	adopts   [][2]int64
}

func (f *fakeFleet) CheckAll() { f.mu.Lock(); f.checks++; f.mu.Unlock() }

func (f *fakeFleet) Reconcile() { f.mu.Lock(); f.recs++; f.mu.Unlock() }

func (f *fakeFleet) Redeploy(id int64) {
	f.mu.Lock()
	f.redeploy = append(f.redeploy, id)
	f.mu.Unlock()
}

func (f *fakeFleet) Adopt(relayID, accountID int64) {
	f.mu.Lock()
	f.adopts = append(f.adopts, [2]int64{relayID, accountID})
	f.mu.Unlock()
}

func (f *fakeFleet) counts() (checks, reconciles int, redeploys []int64, adopts [][2]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checks, f.recs, f.redeploy, f.adopts
}

// fakeDeployClient answers Verify and records Delete calls.
type fakeDeployClient struct {
	mu        sync.Mutex
	verifyErr error
	deleted   []string
	platform  string
}

func (c *fakeDeployClient) Platform() string { return c.platform }

func (c *fakeDeployClient) Verify(_ context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verifyErr != nil {
		return "", c.verifyErr
	}
	return "acc-ref", nil
}

func (c *fakeDeployClient) Deploy(_ context.Context, _ deploy.Spec) (deploy.Result, error) {
	return deploy.Result{}, errors.New("not used by the admin plane")
}

func (c *fakeDeployClient) Delete(_ context.Context, project string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, project)
	return nil
}

func (c *fakeDeployClient) deletions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleted
}

type fakeDeployFactory struct{ client *fakeDeployClient }

func (f fakeDeployFactory) For(platform, _, _ string) (deploy.Client, error) {
	c := f.client
	if c.platform == "" {
		c.platform = platform
	}
	return c, nil
}

// newFleetTestServer builds the admin server with the reconciler and the
// deployer factory wired, plus a logged-in API client.
func newFleetTestServer(t *testing.T) (*fakeFleet, *fakeDeployClient, *store.Store, *client) {
	t.Helper()
	db, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	fleet := &fakeFleet{}
	deployer := &fakeDeployClient{}
	srv := httptest.NewServer(admin.New(db, "test", logging.Nop(), http.NotFoundHandler(),
		admin.WithFleet(fleet, fakeDeployFactory{client: deployer})))
	t.Cleanup(srv.Close)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	c := &client{base: srv.URL, http: &http.Client{Timeout: 10 * time.Second, Jar: jar}}
	c.setup(t, testPassword)
	return fleet, deployer, db, c
}

func accountPath(id int64) string {
	return "/api/v1/accounts/" + strconv.FormatInt(id, 10)
}

func TestAccountLifecycle(t *testing.T) {
	fleet, deployer, _, c := newFleetTestServer(t)

	code, _, body := c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "vercel-platform-token-abcdef",
	})
	if code != http.StatusCreated {
		t.Fatalf("create status = %d: %v", code, body)
	}
	if body["tokenLast4"] != "cdef" {
		t.Fatalf("tokenLast4 = %v", body["tokenLast4"])
	}
	if _, leaked := body["token"]; leaked {
		t.Fatal("response leaked the token")
	}
	accountID := int64(body["id"].(float64))

	// Duplicate names and unknown platforms are rejected.
	code, _, _ = c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "another-token-value-12345",
	})
	if code != http.StatusConflict {
		t.Fatalf("dup name status = %d", code)
	}
	code, _, _ = c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "other", "platform": "nomad", "token": "another-token-value-12345",
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("bad platform status = %d", code)
	}

	// A managed relay can be born against the account.
	code, _, body = c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "web", "provider": "vercel", "url": "https://web.example", "accountId": accountID,
	})
	if code != http.StatusCreated || body["origin"] != "managed" {
		t.Fatalf("managed create = %d %v", code, body)
	}
	relayID := int64(body["id"].(float64))

	// The account is in use: plain delete refuses, force detaches.
	code, _, _ = c.do(t, http.MethodDelete, accountPath(accountID), nil)
	if code != http.StatusConflict {
		t.Fatalf("in-use delete status = %d", code)
	}
	code, _, _ = c.do(t, http.MethodPatch, accountPath(accountID),
		map[string]string{"token": "rotated-platform-token-123456"})
	if code != http.StatusOK {
		t.Fatalf("token rotation status = %d", code)
	}
	code, _, body = c.do(t, http.MethodPost, accountPath(accountID)+"/verify", nil)
	if code != http.StatusOK || body["verifiedAt"].(float64) == 0 {
		t.Fatalf("verify = %d %v", code, body)
	}
	code, _, _ = c.do(t, http.MethodDelete, accountPath(accountID)+"?force=true", nil)
	if code != http.StatusOK {
		t.Fatalf("force delete status = %d", code)
	}

	// The detached relay survives, still managed, deploymentless.
	code, _, body = c.do(t, http.MethodGet, relayPath(relayID), nil)
	if code != http.StatusOK || body["origin"] != "managed" || body["accountId"] != nil {
		t.Fatalf("detached relay = %d %v", code, body)
	}

	// A fresh account can re-parent the detached relay: adoption is the
	// recovery path for a managed row whose account was force-deleted.
	code, _, body = c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "second", "platform": "vercel", "token": "vercel-platform-token-123456",
	})
	if code != http.StatusCreated {
		t.Fatalf("second account = %d: %v", code, body)
	}
	secondID := int64(body["id"].(float64))
	code, _, _ = c.do(t, http.MethodPost, relayPath(relayID)+"/adopt", map[string]any{"accountId": secondID})
	if code != http.StatusAccepted {
		t.Fatalf("adopt detached relay = %d", code)
	}
	if adopts := func() [][2]int64 {
		_, _, _, a := fleet.counts()
		return a
	}(); len(adopts) != 1 || adopts[0] != [2]int64{relayID, secondID} {
		t.Fatalf("adopts recorded = %v", adopts)
	}

	if deletions := deployer.deletions(); len(deletions) != 0 {
		t.Fatalf("unexpected platform deletes: %v", deletions)
	}
}

func TestAccountCreateRejectsBadCredentials(t *testing.T) {
	_, deployer, _, c := newFleetTestServer(t)

	deployer.verifyErr = deploy.ErrCredentials
	code, _, body := c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "bad-token-value-000000",
	})
	if code != http.StatusUnprocessableEntity || body["field"] != "token" {
		t.Fatalf("bad credentials = %d %v", code, body)
	}

	deployer.verifyErr = errors.New("connection refused")
	code, _, _ = c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "bad-token-value-000000",
	})
	if code != http.StatusBadGateway {
		t.Fatalf("platform outage status = %d", code)
	}

	// Neither attempt stored anything.
	raw := strings.TrimSpace(c.getRaw(t, "/api/v1/accounts"))
	if raw != "[]" && raw != "null" {
		t.Fatalf("accounts were stored despite failures: %s", raw)
	}
}

func TestFleetActions(t *testing.T) {
	fleet, _, db, c := newFleetTestServer(t)

	code, _, body := c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "vercel-platform-token-abcdef",
	})
	if code != http.StatusCreated {
		t.Fatalf("account create = %d", code)
	}
	accountID := int64(body["id"].(float64))
	code, _, body = c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "web", "provider": "vercel", "url": "https://web.example", "accountId": accountID,
	})
	if code != http.StatusCreated {
		t.Fatalf("relay create = %d", code)
	}
	relayID := int64(body["id"].(float64))

	code, _, body = c.do(t, http.MethodPost, relayPath(relayID)+"/redeploy", nil)
	if code != http.StatusAccepted || body["queued"] != "redeploy" {
		t.Fatalf("redeploy = %d %v", code, body)
	}
	code, _, _ = c.do(t, http.MethodPost, "/api/v1/fleet/check", nil)
	if code != http.StatusAccepted {
		t.Fatalf("fleet check = %d", code)
	}
	code, _, _ = c.do(t, http.MethodPost, "/api/v1/fleet/reconcile", nil)
	if code != http.StatusAccepted {
		t.Fatalf("fleet reconcile = %d", code)
	}
	checks, reconciles, redeploys, _ := fleet.counts()
	if checks != 1 || reconciles != 1 || len(redeploys) != 1 || redeploys[0] != relayID {
		t.Fatalf("fleet calls: checks=%d reconciles=%d redeploys=%v", checks, reconciles, redeploys)
	}

	code, _, _ = c.do(t, http.MethodPost, relayPath(999)+"/redeploy", nil)
	if code != http.StatusNotFound {
		t.Fatalf("redeploy missing relay = %d", code)
	}

	// Adopt validations: a managed relay conflicts, a platform mismatch and
	// a missing account are refused.
	code, _, _ = c.do(t, http.MethodPost, relayPath(relayID)+"/adopt", map[string]any{"accountId": accountID})
	if code != http.StatusConflict {
		t.Fatalf("adopt managed relay = %d", code)
	}
	if _, err := db.SyncLegacy(
		store.RuntimeValues{LogLevel: "info", MaxRetries: 2, FailureThreshold: 3, CooldownMs: 30000},
		nil,
		[]store.LegacyRelay{{Name: "imported", Provider: "deno", URL: "https://old.example", Active: true}},
	); err != nil {
		t.Fatalf("SyncLegacy: %v", err)
	}
	importedID := int64(0)
	var listed []map[string]any
	if err := json.Unmarshal([]byte(c.getRaw(t, "/api/v1/relays")), &listed); err != nil {
		t.Fatalf("decode relay list: %v", err)
	}
	for _, row := range listed {
		if row["name"] == "imported" {
			importedID = int64(row["id"].(float64))
		}
	}
	if importedID == 0 {
		t.Fatal("legacy relay missing from list")
	}
	code, _, _ = c.do(t, http.MethodPost, relayPath(importedID)+"/adopt", map[string]any{"accountId": accountID})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("platform-mismatch adopt = %d", code)
	}
	code, _, _ = c.do(t, http.MethodPost, relayPath(importedID)+"/adopt", map[string]any{"accountId": 999})
	if code != http.StatusNotFound {
		t.Fatalf("adopt missing account = %d", code)
	}

	// Fleet version reports the worker generation.
	code, _, body = c.do(t, http.MethodGet, "/api/v1/fleet/version", nil)
	if code != http.StatusOK || body["workerVersion"] != deploy.RelayVersion {
		t.Fatalf("fleet version = %d %v", code, body)
	}
}

func TestDeleteRelayRemote(t *testing.T) {
	_, deployer, db, c := newFleetTestServer(t)

	code, _, body := c.do(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "vercel-platform-token-abcdef",
	})
	if code != http.StatusCreated {
		t.Fatalf("account create = %d", code)
	}
	accountID := int64(body["id"].(float64))
	code, _, body = c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "web", "provider": "vercel", "url": "https://web.example", "accountId": accountID,
	})
	if code != http.StatusCreated {
		t.Fatalf("relay create = %d", code)
	}
	relayID := int64(body["id"].(float64))
	if err := db.UpsertDeployment(store.DeploymentRow{
		RelayID: relayID, AccountID: accountID, Platform: "vercel", Project: "web",
		URL: "https://web.vercel.app", Version: deploy.RelayVersion, AuthToken: "relay-token-0000",
		Status: store.DeployActive,
	}); err != nil {
		t.Fatalf("UpsertDeployment: %v", err)
	}

	code, _, _ = c.do(t, http.MethodDelete, relayPath(relayID)+"?deleteRemote=true", nil)
	if code != http.StatusOK {
		t.Fatalf("remote delete status = %d", code)
	}
	if deletions := deployer.deletions(); len(deletions) != 1 || deletions[0] != "web" {
		t.Fatalf("platform deletions = %v", deletions)
	}
	code, _, _ = c.do(t, http.MethodGet, relayPath(relayID), nil)
	if code != http.StatusNotFound {
		t.Fatalf("relay after delete = %d", code)
	}
}
