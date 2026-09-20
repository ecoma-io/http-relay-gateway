package e2e_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
)

func relayPath(id int64) string {
	return "/api/v1/relays/" + strconv.FormatInt(id, 10)
}

// The managed lifecycle drives the real binary against two in-process
// fakes: a platform API (the Vercel slice the deployer speaks) and a
// worker sim that enforces the relay token exactly like the embedded
// worker does. The gateway subprocess trusts the sim's TLS certificate
// through SSL_CERT_FILE, so the https:// stable URLs the platform client
// constructs resolve to a live local server.

// workerSim is a stand-in deployed relay: mutable version + token config
// (the platform "writes" it on every deploy), token-enforced relay path,
// an unauthenticated version endpoint, and a paused mode in which the
// platform answers every path with its suspension page instead of the
// worker.
type workerSim struct {
	srv *httptest.Server

	mu       sync.Mutex
	token    string
	version  string
	paused   bool
	requests []simRequest
}

type simRequest struct {
	Path      string
	AuthToken string
	Target    string
	Body      string
}

func newWorkerSim(t *testing.T, token, version string) *workerSim {
	t.Helper()
	sim := &workerSim{token: token, version: version}
	mux := http.NewServeMux()
	mux.HandleFunc("/__relay/version", func(w http.ResponseWriter, _ *http.Request) {
		sim.mu.Lock()
		defer sim.mu.Unlock()
		if sim.paused {
			sim.writePausePage(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(`{"version":"` + sim.version + `"}`))
	})
	mux.HandleFunc("/__relay/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		// The embedded worker reconstructs the upstream path from
		// X-Relay-Path (default "/") against the target's origin; the
		// request path the gateway used is irrelevant.
		relayPath := r.Header.Get("X-Relay-Path")
		if relayPath == "" {
			relayPath = "/"
		}
		sim.mu.Lock()
		auth := r.Header.Get("X-Relay-Token")
		expected := sim.token
		sim.requests = append(sim.requests, simRequest{
			Path: relayPath, AuthToken: auth, Target: r.Header.Get("X-Relay-Target"), Body: string(raw),
		})
		paused := sim.paused
		sim.mu.Unlock()
		if paused {
			sim.writePausePage(w)
			return
		}
		if expected == "" || auth != expected {
			// A locked relay is indistinguishable from an empty one.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "sim")
		_, _ = w.Write([]byte(`{"served":true,"path":"` + relayPath + `"}`))
	})
	sim.srv = httptest.NewTLSServer(mux)
	t.Cleanup(sim.srv.Close)
	return sim
}

func (s *workerSim) setConfig(token, version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token, s.version = token, version
}

// setPaused flips the platform suspension: while paused, the sim answers
// every path — version endpoint included — with the suspension page a
// quota-exhausted Vercel deployment really serves, never the worker.
func (s *workerSim) setPaused(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = paused
}

// writePausePage answers like a real suspended Vercel deployment: HTTP 402,
// the x-vercel-error marker, and the platform's page — on any path.
func (s *workerSim) writePausePage(w http.ResponseWriter) {
	w.Header().Set("X-Vercel-Error", "DEPLOYMENT_DISABLED")
	w.WriteHeader(http.StatusPaymentRequired)
	_, _ = w.Write([]byte("Payment required\n\nDEPLOYMENT_DISABLED\n\n"))
}

// tokens returns every X-Relay-Token value the sim has ever seen — exactly
// the strings that must never show up in the gateway's logs.
func (s *workerSim) tokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make([]string, 0, len(s.requests))
	for _, r := range s.requests {
		if r.AuthToken != "" {
			seen = append(seen, r.AuthToken)
		}
	}
	return seen
}

func (s *workerSim) lastRequest() (simRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return simRequest{}, false
	}
	return s.requests[len(s.requests)-1], true
}

// trustMaterial writes the sim's self-signed certificate as a PEM root file
// (for the gateway subprocess's SSL_CERT_FILE) and returns the bytes.
func trustMaterial(t *testing.T, sim *workerSim) (string, *x509.CertPool) {
	t.Helper()
	der := sim.srv.Certificate().Raw
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	path := filepath.Join(t.TempDir(), "sim-root.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("sim certificate not parseable")
	}
	return path, pool
}

// fakePlatform speaks the Vercel API slice the deployer uses, pointing every
// deployment's stable alias at the worker sim.
type fakePlatform struct {
	srv *httptest.Server
	sim *workerSim

	mu              sync.Mutex
	tokens          []string
	projects        []string
	deletedProjects []string
	verifyCalls     int
}

func newFakePlatform(t *testing.T, sim *workerSim) *fakePlatform {
	t.Helper()
	f := &fakePlatform{sim: sim}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/user", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		f.verifyCalls++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user":{"username":"e2e-user"}}`))
	})
	mux.HandleFunc("/v13/deployments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Name string `json:"name"`
			Env  []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"env"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
		token, version := "", ""
		for _, entry := range body.Env {
			switch entry.Key {
			case "RELAY_AUTH_TOKEN":
				token = entry.Value
			case "RELAY_VERSION":
				version = entry.Value
			}
		}
		if token == "" || version != deploy.RelayVersion {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad deploy payload"}`))
			return
		}
		f.mu.Lock()
		f.projects = append(f.projects, body.Name)
		f.tokens = append(f.tokens, token)
		f.mu.Unlock()
		// The platform runs the worker with the deploy-time environment;
		// the sim starts answering before the deployer's live check.
		f.sim.setConfig(token, version)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"dpl_e2e","url":"per-deploy","alias":["` +
			strings.TrimPrefix(sim.srv.URL, "https://") + `"],"readyState":"READY"}`))
	})
	mux.HandleFunc("/v9/projects/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		f.mu.Lock()
		f.deletedProjects = append(f.deletedProjects, strings.TrimPrefix(r.URL.Path, "/v9/projects/"))
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePlatform) deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletedProjects...)
}

func (f *fakePlatform) deployedTokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.tokens...)
}

func TestE2E_ManagedLifecycle(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, simRoots := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	if code := g.Login(t, adminPassword); code != http.StatusOK {
		t.Fatalf("login = %d", code)
	}

	// Account: the platform verifies the credential before it is stored.
	code, _, body := g.AdminDo(t, http.MethodPost, "/api/v1/accounts", map[string]string{
		"name": "main", "platform": "vercel", "token": "e2e-fake-vercel-platform-token",
	})
	if code != http.StatusCreated {
		t.Fatalf("account create = %d: %s\nlogs:\n%s", code, body, g.Logs())
	}
	var account struct {
		ID         int64  `json:"id"`
		TokenLast4 string `json:"tokenLast4"`
	}
	if err := json.Unmarshal([]byte(body), &account); err != nil {
		t.Fatal(err)
	}

	// Managed relay: born against the account, no deployment yet.
	code, _, body = g.AdminDo(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "managed-one", "provider": "vercel",
		"url": "https://placeholder.example", "accountId": account.ID,
	})
	if code != http.StatusCreated {
		t.Fatalf("relay create = %d: %s", code, body)
	}
	var relay struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &relay); err != nil {
		t.Fatal(err)
	}

	// Redeploy: the reconciler deploys the embedded worker and the relay
	// turns active only once its worker answers the gateway's version.
	code, _, body = g.AdminDo(t, http.MethodPost, relayPath(relay.ID)+"/redeploy", nil)
	if code != http.StatusAccepted {
		t.Fatalf("redeploy = %d: %s", code, body)
	}
	dep := waitForDeployment(t, g, relay.ID, "active", 30*time.Second)
	if dep.URL != sim.srv.URL {
		t.Fatalf("deployment URL = %q, want the sim %q", dep.URL, sim.srv.URL)
	}
	if len(dep.TokenLast4) != 4 {
		t.Fatalf("tokenLast4 = %q", dep.TokenLast4)
	}
	if dep.Version != deploy.RelayVersion {
		t.Fatalf("deployment version = %q", dep.Version)
	}

	// The pool serves through the deployment, authenticated.
	g.WaitForCondition(applySettle, "managed relay active in pool", func(st *StatsView) bool {
		row, ok := st.relay("managed-one")
		return ok && row.Active && row.Healthy && row.Origin == "managed"
	})
	code, _, respBody := relayDo(t, g.Addr, "/", "{}", map[string]string{
		"X-Relay-Target": sim.srv.URL, "X-Relay-Path": "/up",
	})
	if code != http.StatusOK || !strings.Contains(respBody, `"served":true`) {
		t.Fatalf("relayed request = %d %q", code, respBody)
	}
	last, ok := sim.lastRequest()
	if !ok || last.Path != "/up" || last.Target != sim.srv.URL {
		t.Fatalf("sim saw %+v", last)
	}
	tokens := platform.deployedTokens()
	if last.AuthToken == "" || len(tokens) != 1 || last.AuthToken != tokens[0] {
		t.Fatalf("gateway did not send the deployment token (sim auth %q)", last.AuthToken)
	}

	// A scanner hitting the relay URL without the token finds nothing.
	scan := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: simRoots}, DisableKeepAlives: true},
		Timeout:   5 * time.Second,
	}
	res, err := scan.Get(sim.srv.URL + "/steal")
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("tokenless scan status = %d, want 404", res.StatusCode)
	}

	// Drift: the worker stops reporting the gateway's generation; a
	// passive check marks it stale and pulls it from the pool.
	sim.setConfig(tokens[0], "drift-9")
	code, _, _ = g.AdminDo(t, http.MethodPost, "/api/v1/fleet/check", nil)
	if code != http.StatusAccepted {
		t.Fatalf("fleet check = %d", code)
	}
	stale := waitForDeployment(t, g, relay.ID, "stale", 30*time.Second)
	if !strings.Contains(stale.LastError, "version") {
		t.Fatalf("stale lastError = %q", stale.LastError)
	}

	// Reconcile redeploys onto a fresh token and reactivates the relay.
	code, _, _ = g.AdminDo(t, http.MethodPost, "/api/v1/fleet/reconcile", nil)
	if code != http.StatusAccepted {
		t.Fatalf("fleet reconcile = %d", code)
	}
	dep = waitForDeployment(t, g, relay.ID, "active", 30*time.Second)
	if dep.Version != deploy.RelayVersion {
		t.Fatalf("redeployed version = %q", dep.Version)
	}
	code, _, respBody = relayDo(t, g.Addr, "/", "{}", map[string]string{
		"X-Relay-Target": sim.srv.URL, "X-Relay-Path": "/after-redeploy",
	})
	if code != http.StatusOK || !strings.Contains(respBody, `"served":true`) {
		t.Fatalf("post-redeploy relay = %d %q", code, respBody)
	}
	last, _ = sim.lastRequest()
	tokens = platform.deployedTokens()
	if len(tokens) != 2 || last.AuthToken != tokens[1] || tokens[0] == tokens[1] {
		t.Fatalf("token rotation wrong: first %q… last auth %q, tokens=%d",
			tokens[0][:6], last.AuthToken[:6], len(tokens))
	}

	// A restart rebuilds the serving pool from the database alone — and the
	// deployment join must survive it: the relay serves again through the
	// same deployment with the rotated token, no redeploy needed.
	g.Restart()
	g.WaitForCondition(applySettle, "managed relay active after restart", func(st *StatsView) bool {
		row, ok := st.relay("managed-one")
		return ok && row.Active && row.Healthy && row.Origin == "managed"
	})
	code, _, respBody = relayDo(t, g.Addr, "/", "{}", map[string]string{
		"X-Relay-Target": sim.srv.URL, "X-Relay-Path": "/after-restart",
	})
	if code != http.StatusOK || !strings.Contains(respBody, `"served":true`) {
		t.Fatalf("post-restart relay = %d %q", code, respBody)
	}
	last, _ = sim.lastRequest()
	tokens = platform.deployedTokens()
	if last.AuthToken != tokens[1] {
		t.Fatalf("post-restart auth %q…, want the persisted rotated token %q…",
			last.AuthToken[:6], tokens[1][:6])
	}

	// Remote delete removes the platform project and the relay row.
	code, _, body = g.AdminDo(t, http.MethodDelete, relayPath(relay.ID)+"?deleteRemote=true", nil)
	if code != http.StatusOK {
		t.Fatalf("remote delete = %d: %s", code, body)
	}
	deleted := platform.deleted()
	if len(deleted) != 1 || deleted[0] != "managed-one" {
		t.Fatalf("platform deletions = %v", deleted)
	}
	code, _, _ = g.AdminDo(t, http.MethodGet, relayPath(relay.ID), nil)
	if code != http.StatusNotFound {
		t.Fatalf("relay after delete = %d", code)
	}
	g.WaitForCondition(applySettle, "relay gone from pool", func(st *StatsView) bool {
		_, ok := st.relay("managed-one")
		return !ok
	})

	// No relay token ever reached the logs — neither the deploy-time secrets
	// nor anything the worker sim actually observed on the wire.
	logs := g.Logs()
	for _, token := range append(platform.deployedTokens(), sim.tokens()...) {
		if strings.Contains(logs, token) {
			t.Fatalf("relay token %q… leaked into logs", token[:6])
		}
	}
}

// TestE2E_PlatformPauseWaitsForRevival drives the suspension lifecycle free
// tiers produce: the platform starts answering its pause page instead of the
// worker, the gateway classifies the deployment paused — never redeploying,
// because no deploy can lift a platform suspension — and pulls the relay
// from the pool so no client ever sees the pause page. The always-on
// revival scan then returns it to active serving when the platform lifts
// the suspension, on the same deployment, token untouched.
func TestE2E_PlatformPauseWaitsForRevival(t *testing.T) {
	sim := newWorkerSim(t, "", "uninitialized")
	platform := newFakePlatform(t, sim)
	certFile, _ := trustMaterial(t, sim)
	g := NewGatewayWithEnv(t,
		deploy.VercelAPIBaseEnv+"="+platform.srv.URL,
		"RELAY_REVIVE_SCAN_INTERVAL=200ms",
		"SSL_CERT_FILE="+certFile,
	)
	g.Setup(t, adminPassword)
	if code := g.Login(t, adminPassword); code != http.StatusOK {
		t.Fatalf("login = %d", code)
	}

	code, _, body := g.AdminDo(t, http.MethodPost, "/api/v1/accounts", map[string]string{
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
		"name": "paused-one", "provider": "vercel",
		"url": "https://placeholder.example", "accountId": account.ID,
	})
	if code != http.StatusCreated {
		t.Fatalf("relay create = %d: %s", code, body)
	}
	var relay struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &relay); err != nil {
		t.Fatal(err)
	}
	code, _, _ = g.AdminDo(t, http.MethodPost, relayPath(relay.ID)+"/redeploy", nil)
	if code != http.StatusAccepted {
		t.Fatalf("redeploy = %d", code)
	}
	waitForDeployment(t, g, relay.ID, "active", 30*time.Second)
	tokens := platform.deployedTokens()
	if len(tokens) != 1 {
		t.Fatalf("setup deploys = %d, want 1", len(tokens))
	}

	// The platform suspends the deployment; every path answers its page.
	sim.setPaused(true)
	code, _, _ = g.AdminDo(t, http.MethodPost, "/api/v1/fleet/check", nil)
	if code != http.StatusAccepted {
		t.Fatalf("fleet check = %d", code)
	}
	paused := waitForDeployment(t, g, relay.ID, "paused", 30*time.Second)
	if !strings.Contains(paused.LastError, "DEPLOYMENT_DISABLED") {
		t.Fatalf("paused lastError = %q, want the platform's marker", paused.LastError)
	}
	// The classification is relay-side, not upstream-side: the probe only
	// ever touched the relay's own version endpoint, and it never deployed.
	if got := len(platform.deployedTokens()); got != 1 {
		t.Fatalf("deploys after pause = %d, want 1 — a pause is not redeployed", got)
	}

	// The relay left the pool: whatever the gateway answers with, the client
	// must never see the platform's pause page through it.
	g.WaitForCondition(applySettle, "paused relay out of pool", func(st *StatsView) bool {
		_, ok := st.relay("paused-one")
		return !ok
	})
	code, _, body = relayDo(t, g.Addr, "/", "{}", map[string]string{
		"X-Relay-Target": "https://anywhere.example",
	})
	if code < 400 {
		t.Fatalf("request with everything paused = %d %q, want an error", code, body)
	}
	if strings.Contains(body, "DEPLOYMENT_DISABLED") || strings.Contains(body, "Payment required") {
		t.Fatalf("the platform's pause page leaked to a client: %d %q", code, body)
	}

	// The platform lifts the suspension; the same worker answers again and
	// the scanner revives the deployment with a clean error.
	sim.setPaused(false)
	dep := waitForDeployment(t, g, relay.ID, "active", 30*time.Second)
	if dep.LastError != "" {
		t.Fatalf("revived deployment carries an error: %+v", dep)
	}
	g.WaitForCondition(applySettle, "revived relay back in pool", func(st *StatsView) bool {
		row, ok := st.relay("paused-one")
		return ok && row.Active && row.Healthy
	})
	code, _, body = relayDo(t, g.Addr, "/", "{}", map[string]string{
		"X-Relay-Target": sim.srv.URL, "X-Relay-Path": "/revived",
	})
	if code != http.StatusOK || !strings.Contains(body, `"served":true`) {
		t.Fatalf("post-revival relay = %d %q", code, body)
	}
	last, _ := sim.lastRequest()
	if last.AuthToken != tokens[0] {
		t.Fatalf("revival rotated the token: auth %q…, want the original %q…",
			last.AuthToken[:6], tokens[0][:6])
	}
	if got := len(platform.deployedTokens()); got != 1 {
		t.Fatalf("deploys after revival = %d, want 1 — revival is not a redeploy", got)
	}

	logs := g.Logs()
	for _, token := range append(platform.deployedTokens(), sim.tokens()...) {
		if strings.Contains(logs, token) {
			t.Fatalf("relay token %q… leaked into logs", token[:6])
		}
	}
}

// deploymentView is the slice of the relay API the lifecycle test reads.
type deploymentView struct {
	Status     string `json:"status"`
	URL        string `json:"url"`
	Version    string `json:"version"`
	TokenLast4 string `json:"tokenLast4"`
	LastError  string `json:"lastError"`
}

func waitForDeployment(t testing.TB, g *Gateway, relayID int64, status string, timeout time.Duration) deploymentView {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		code, _, body := g.AdminDo(t, http.MethodGet, relayPath(relayID), nil)
		if code == http.StatusOK {
			var row struct {
				Deployment *deploymentView `json:"deployment"`
			}
			if err := json.Unmarshal([]byte(body), &row); err != nil {
				t.Fatalf("decode relay: %v (%s)", err, body)
			}
			if row.Deployment != nil && row.Deployment.Status == status {
				return *row.Deployment
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("deployment never reached %q within %s; last body %s", status, timeout, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
