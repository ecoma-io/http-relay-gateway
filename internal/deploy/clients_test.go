package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// routeAll sends every outgoing request — whatever host a constructed URL
// names — to the fake platform, keeping the original Host so handlers can
// assert on the URL the client built.
type routeAll struct{ base string }

func (r routeAll) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = "http"
	u.Host = strings.TrimPrefix(r.base, "http://")
	clone.URL = &u
	return http.DefaultTransport.RoundTrip(clone)
}

func routedClient(base string) *http.Client {
	return &http.Client{Transport: routeAll{base}}
}

// fakePlatform implements the tiny slice of each platform API the clients
// call, plus the worker version endpoint awaitLive probes.
type fakePlatform struct {
	t   *testing.T
	mu  sync.Mutex
	srv *httptest.Server

	// version is what the live worker reports from /__relay/version.
	version string
	// readyState / deployStatus answer the platform pollers.
	readyState   string
	deployStatus string
	// createStatus, when non-zero, fails the next create call.
	createStatus int

	// recorded observations
	polls          int
	deploys        int
	lastEnv        map[string]string
	lastBindings   map[string]string
	lastModuleSize int
	lastMeta       map[string]string
	deletedProject string
	createdProject string
	createAuth     string
}

// seenCreateAuth returns the Authorization header the most recent create
// call carried (the version probes carry none — that is the point).
func (f *fakePlatform) seenCreateAuth() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createAuth
}

func newFakePlatform(t *testing.T) *fakePlatform {
	f := &fakePlatform{
		t:            t,
		version:      RelayVersion,
		readyState:   "BUILDING",
		deployStatus: "pending",
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePlatform) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasSuffix(r.URL.Path, "/__relay/version") {
		// Version probes are unauthenticated by design; only record the
		// credential headers of the platform API calls.
		f.createAuth = r.Header.Get("Authorization")
	}
	switch {
	case r.URL.Path == "/__relay/version":
		f.json(w, http.StatusOK, map[string]string{"version": f.version})

	// --- vercel ---
	case r.URL.Path == "/v2/user" && r.Method == http.MethodGet:
		f.json(w, http.StatusOK, map[string]any{"user": map[string]string{"username": "fake-user"}})
	case r.URL.Path == "/v13/deployments" && r.Method == http.MethodPost:
		if f.createStatus != 0 {
			f.json(w, f.createStatus, map[string]any{"error": map[string]string{"message": "nope"}})
			return
		}
		var body struct {
			Name   string `json:"name"`
			Target string `json:"target"`
			Files  []struct {
				File string `json:"file"`
				Data string `json:"data"`
			} `json:"files"`
			Env []map[string]any `json:"env"`
		}
		f.decode(r, &body)
		f.deploys++
		f.lastEnv = map[string]string{}
		for _, entry := range body.Env {
			f.lastEnv[entry["key"].(string)] = entry["value"].(string)
		}
		if len(body.Files) != 1 || body.Files[0].File != "api/relay.js" || body.Files[0].Data == "" {
			f.t.Errorf("vercel create: unexpected files payload %+v", body.Files)
		}
		f.json(w, http.StatusOK, map[string]any{
			"id":         fmt.Sprintf("dpl_test_%d", f.deploys),
			"url":        "per-deploy-abc.vercel.sh",
			"alias":      []string{"my-relay.fake-team.vercel.app"},
			"readyState": f.readyState,
		})
	case strings.HasPrefix(r.URL.Path, "/v13/deployments/") && r.Method == http.MethodGet:
		// A BUILDING deployment finishes after the first poll.
		if f.readyState == "BUILDING" {
			f.polls++
			if f.polls >= 1 {
				f.readyState = "READY"
			}
		}
		f.json(w, http.StatusOK, map[string]any{"readyState": f.readyState})
	case strings.HasPrefix(r.URL.Path, "/v9/projects/") && r.Method == http.MethodDelete:
		f.deletedProject = strings.TrimPrefix(r.URL.Path, "/v9/projects/")
		f.json(w, http.StatusOK, map[string]any{"ok": true})

	// --- cloudflare ---
	case r.URL.Path == "/accounts/acc_123" && r.Method == http.MethodGet:
		f.json(w, http.StatusOK, map[string]any{"success": true, "result": map[string]string{"id": "acc_123"}})
	case strings.HasPrefix(r.URL.Path, "/accounts/acc_123/workers/scripts/") && r.Method == http.MethodPut:
		if f.createStatus != 0 {
			f.json(w, f.createStatus, map[string]any{"success": false, "errors": []map[string]any{{"message": "nope"}}})
			return
		}
		f.deploys++
		f.lastBindings = map[string]string{}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			f.t.Errorf("cloudflare deploy: multipart: %v", err)
			f.json(w, http.StatusBadRequest, map[string]string{})
			return
		}
		var meta struct {
			Bindings []struct {
				Type string `json:"type"`
				Name string `json:"name"`
				Text string `json:"text"`
			} `json:"bindings"`
		}
		if err := json.Unmarshal([]byte(r.FormValue("metadata")), &meta); err != nil {
			f.t.Errorf("cloudflare deploy: metadata: %v", err)
		}
		for _, b := range meta.Bindings {
			if b.Type != "plain_text" {
				f.t.Errorf("cloudflare deploy: binding type %q", b.Type)
			}
			f.lastBindings[b.Name] = b.Text
		}
		file, _, err := r.FormFile("relay.js")
		if err != nil {
			f.t.Errorf("cloudflare deploy: module part: %v", err)
		} else {
			n, _ := io.ReadAll(file)
			f.lastModuleSize = len(n)
			_ = file.Close()
		}
		f.json(w, http.StatusOK, map[string]any{"success": true, "result": map[string]string{}})
	case strings.HasSuffix(r.URL.Path, "/subdomain") && r.Method == http.MethodPost:
		f.json(w, http.StatusOK, map[string]any{"success": true, "result": map[string]bool{"enabled": true}})
	case strings.HasSuffix(r.URL.Path, "/workers/subdomain") && r.Method == http.MethodGet:
		f.json(w, http.StatusOK, map[string]any{"success": true, "result": map[string]string{"subdomain": "fake-team"}})
	case strings.HasPrefix(r.URL.Path, "/accounts/acc_123/workers/scripts/") && r.Method == http.MethodDelete:
		f.deletedProject = strings.TrimPrefix(r.URL.Path, "/accounts/acc_123/workers/scripts/")
		f.json(w, http.StatusOK, map[string]any{"success": true})

	// --- deno ---
	case r.URL.Path == "/v1/user" && r.Method == http.MethodGet:
		f.json(w, http.StatusOK, map[string]string{"id": "user-9"})
	case r.URL.Path == "/v1/projects" && r.Method == http.MethodPost:
		if f.createStatus != 0 {
			w.WriteHeader(f.createStatus)
			return
		}
		f.createdProject = "decoded-below"
		f.json(w, http.StatusOK, map[string]string{"id": "proj_1"})
	case strings.HasPrefix(r.URL.Path, "/v1/projects/") && !strings.Contains(r.URL.Path, "/deployments") && r.Method == http.MethodGet:
		// Project lookup: missing until created.
		if f.createdProject == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.json(w, http.StatusOK, map[string]string{"id": "proj_1"})
	case strings.HasSuffix(r.URL.Path, "/deployments") && r.Method == http.MethodPost:
		if f.createStatus != 0 {
			w.WriteHeader(f.createStatus)
			return
		}
		f.deploys++
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			f.t.Errorf("deno deploy: multipart: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var meta struct {
			EnvVars       map[string]string `json:"envVars"`
			EntryPointURL string            `json:"entryPointUrl"`
		}
		if err := json.Unmarshal([]byte(r.FormValue("meta")), &meta); err != nil {
			f.t.Errorf("deno deploy: meta: %v", err)
		}
		f.lastMeta = meta.EnvVars
		file, _, err := r.FormFile("file")
		if err != nil {
			f.t.Errorf("deno deploy: file part: %v", err)
		} else {
			n, _ := io.ReadAll(file)
			f.lastModuleSize = len(n)
			_ = file.Close()
		}
		f.json(w, http.StatusOK, map[string]string{"id": fmt.Sprintf("dep_%d", f.deploys), "status": f.deployStatus})
	case strings.HasPrefix(r.URL.Path, "/v1/deployments/") && r.Method == http.MethodGet:
		// A pending deployment finishes after the first poll.
		if f.deployStatus == "pending" {
			f.polls++
			if f.polls >= 1 {
				f.deployStatus = "success"
			}
		}
		f.json(w, http.StatusOK, map[string]string{"status": f.deployStatus})
	case strings.HasPrefix(r.URL.Path, "/v1/projects/") && r.Method == http.MethodDelete:
		f.deletedProject = strings.TrimPrefix(r.URL.Path, "/v1/projects/")
		f.json(w, http.StatusOK, map[string]string{})
	default:
		f.t.Errorf("unexpected platform call %s %s", r.Method, r.URL.Path)
		f.json(w, http.StatusNotFound, map[string]string{})
	}
}

func (f *fakePlatform) json(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *fakePlatform) decode(r *http.Request, into any) {
	raw, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(raw, into); err != nil {
		f.t.Errorf("decode %s: %v", r.URL.Path, err)
	}
}

func (f *fakePlatform) snapshot() (env, bindings, meta map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastEnv, f.lastBindings, f.lastMeta
}

func TestVercelDeployHappyPath(t *testing.T) {
	f := newFakePlatform(t)
	c := newVercelClient(f.srv.URL, "vercel-token", "", routedClient(f.srv.URL))
	ref, err := c.Verify(context.Background())
	if err != nil || ref != "fake-user" {
		t.Fatalf("Verify = %q, %v", ref, err)
	}
	res, err := c.Deploy(context.Background(), Spec{Project: "my-relay", Source: "// worker", Version: RelayVersion, Token: "relay-tok"})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if res.ExternalID != "dpl_test_1" || res.Project != "my-relay" {
		t.Fatalf("Result = %+v", res)
	}
	if res.URL != "https://my-relay.fake-team.vercel.app" {
		t.Fatalf("stable URL = %q", res.URL)
	}
	env, _, _ := f.snapshot()
	if env["RELAY_VERSION"] != RelayVersion || env["RELAY_AUTH_TOKEN"] != "relay-tok" {
		t.Fatalf("deployment env = %v", env)
	}
	if !strings.Contains(f.seenCreateAuth(), "vercel-token") {
		t.Fatalf("create auth header = %q", f.seenCreateAuth())
	}
}

func TestVercelDeployErrorMapping(t *testing.T) {
	f := newFakePlatform(t)
	c := newVercelClient(f.srv.URL, "t", "", routedClient(f.srv.URL))

	f.createStatus = http.StatusForbidden
	if _, err := c.Deploy(context.Background(), Spec{Project: "p", Source: "// worker", Version: "1", Token: "k"}); err == nil ||
		!strings.Contains(err.Error(), "status 403") {
		t.Fatalf("create failure error = %v", err)
	}

	f.createStatus = 0
	f.readyState = "ERROR"
	if _, err := c.Deploy(context.Background(), Spec{Project: "p", Source: "// worker", Version: "1", Token: "k"}); err == nil ||
		!strings.Contains(err.Error(), "deployment error") {
		t.Fatalf("readyState ERROR error = %v", err)
	}

	// Live check: the worker answers the wrong version — not READY, an error.
	f.readyState = "READY"
	f.version = "stale"
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err := c.Deploy(ctx, Spec{Project: "p", Source: "// worker", Version: RelayVersion, Token: "k"})
	if err == nil || !strings.Contains(err.Error(), "relay not live") {
		t.Fatalf("wrong live version error = %v", err)
	}
}

func TestVercelDeleteToleratesMissing(t *testing.T) {
	f := newFakePlatform(t)
	c := newVercelClient(f.srv.URL, "t", "", routedClient(f.srv.URL))
	if err := c.Delete(context.Background(), "my-relay"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if f.deletedProject != "my-relay" {
		t.Fatalf("deleted project = %q", f.deletedProject)
	}
}

func TestCloudflareDeployHappyPath(t *testing.T) {
	f := newFakePlatform(t)
	c := newCloudflareClient(f.srv.URL, "cf-token", "acc_123", routedClient(f.srv.URL))
	ref, err := c.Verify(context.Background())
	if err != nil || ref != "acc_123" {
		t.Fatalf("Verify = %q, %v", ref, err)
	}
	res, err := c.Deploy(context.Background(), Spec{Project: "my-relay", Source: "// worker", Version: RelayVersion, Token: "relay-tok"})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if res.URL != "https://my-relay.fake-team.workers.dev" {
		t.Fatalf("URL = %q", res.URL)
	}
	_, bindings, _ := f.snapshot()
	if bindings["RELAY_VERSION"] != RelayVersion || bindings["RELAY_AUTH_TOKEN"] != "relay-tok" {
		t.Fatalf("bindings = %v", bindings)
	}
	if f.lastModuleSize == 0 {
		t.Fatal("module part empty")
	}
}

func TestCloudflareCredentialsError(t *testing.T) {
	f := newFakePlatform(t)
	f.createStatus = 0
	// Make Verify hit a 403: no handler answers /accounts/other — the fake
	// 404s unexpected calls, so use a distinct account id and a status hint.
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	c := newCloudflareClient(f.srv.URL, "t", "acc_123", routedClient(f.srv.URL))
	_, err := c.Verify(context.Background())
	if !errors.Is(err, ErrCredentials) {
		t.Fatalf("Verify error = %v, want ErrCredentials", err)
	}
}

// TestVercelAndDenoVerifyRejectBadCredentials pins credential-mapping parity:
// every platform's Verify must turn a 401/403 into ErrCredentials, so the
// admin plane answers a typo'd token with the 422 field error, not a 502
// platform outage.
func TestVercelAndDenoVerifyRejectBadCredentials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"unauthorized", http.StatusUnauthorized},
		{"forbidden", http.StatusForbidden},
	} {
		t.Run("vercel/"+tc.name, func(t *testing.T) {
			f := newFakePlatform(t)
			f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			c := newVercelClient(f.srv.URL, "t", "", routedClient(f.srv.URL))
			if _, err := c.Verify(context.Background()); !errors.Is(err, ErrCredentials) {
				t.Fatalf("Verify error = %v, want ErrCredentials", err)
			}
		})
		t.Run("deno/"+tc.name, func(t *testing.T) {
			f := newFakePlatform(t)
			f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			c := newDenoClient(f.srv.URL, "t", "ref", routedClient(f.srv.URL))
			if _, err := c.Verify(context.Background()); !errors.Is(err, ErrCredentials) {
				t.Fatalf("Verify error = %v, want ErrCredentials", err)
			}
		})
	}
}

func TestCloudflareDeleteToleratesMissing(t *testing.T) {
	f := newFakePlatform(t)
	c := newCloudflareClient(f.srv.URL, "t", "acc_123", routedClient(f.srv.URL))
	if err := c.Delete(context.Background(), "gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestDenoDeployHappyPath(t *testing.T) {
	f := newFakePlatform(t)
	c := newDenoClient(f.srv.URL, "deno-token", "acc-ref", routedClient(f.srv.URL))
	ref, err := c.Verify(context.Background())
	if err != nil || ref != "acc-ref" {
		t.Fatalf("Verify = %q, %v", ref, err)
	}
	res, err := c.Deploy(context.Background(), Spec{Project: "my-relay", Source: "// worker", Version: RelayVersion, Token: "relay-tok"})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if res.URL != "https://my-relay.deno.dev" || res.ExternalID != "dep_1" {
		t.Fatalf("Result = %+v", res)
	}
	_, _, meta := f.snapshot()
	if meta["RELAY_VERSION"] != RelayVersion || meta["RELAY_AUTH_TOKEN"] != "relay-tok" {
		t.Fatalf("envVars = %v", meta)
	}
	if f.lastModuleSize == 0 {
		t.Fatal("file part empty")
	}
}

func TestDenoDeployErrorMapping(t *testing.T) {
	f := newFakePlatform(t)
	c := newDenoClient(f.srv.URL, "t", "ref", routedClient(f.srv.URL))

	f.createStatus = http.StatusUnauthorized
	if _, err := c.Deploy(context.Background(), Spec{Project: "p", Source: "// worker", Version: "1", Token: "k"}); err == nil {
		t.Fatal("expected create failure")
	}

	f.createStatus = 0
	f.deployStatus = "failed"
	if _, err := c.Deploy(context.Background(), Spec{Project: "p", Source: "// worker", Version: "1", Token: "k"}); err == nil ||
		!strings.Contains(err.Error(), "failed") {
		t.Fatalf("deployment failed error = %v", err)
	}

	// Success on the platform but the live worker never confirms.
	f.deployStatus = "success"
	f.version = "stale"
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := c.Deploy(ctx, Spec{Project: "p", Version: RelayVersion, Token: "k"}); err == nil ||
		!strings.Contains(err.Error(), "relay not live") {
		t.Fatalf("wrong live version error = %v", err)
	}
}

func TestFactoryPlatforms(t *testing.T) {
	t.Setenv(VercelAPIBaseEnv, "http://127.0.0.1:1")
	t.Setenv(CloudflareAPIBaseEnv, "http://127.0.0.1:2")
	t.Setenv(DenoAPIBaseEnv, "http://127.0.0.1:3")
	factory := NewFactory(nil)
	for _, platform := range []string{PlatformVercel, PlatformCloudflare, PlatformDeno} {
		client, err := factory.For(platform, "tok", "ref")
		if err != nil {
			t.Fatalf("For(%q): %v", platform, err)
		}
		if client.Platform() != platform {
			t.Fatalf("For(%q) returned %q", platform, client.Platform())
		}
	}
	if _, err := factory.For("nomad", "tok", "ref"); err == nil {
		t.Fatal("unknown platform accepted")
	}
}

func TestTokenLengthAndUniqueness(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		token, err := Token()
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		if len(token) != 43 {
			t.Fatalf("token length = %d", len(token))
		}
		if seen[token] {
			t.Fatal("token repeated")
		}
		seen[token] = true
	}
}
