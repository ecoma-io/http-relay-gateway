package deploy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- fake platform API ---

// fakeCall is one recorded platform API request.
type fakeCall struct {
	Method        string
	Path          string
	RawQuery      string
	Authorization string
	ContentType   string
	Body          []byte
}

// fakeAPI records every request before routing it, so assertions see the
// wire exactly as the platform would have. gate lets a test resume control
// at the call that precedes the (unroutable) stable-URL live check.
type fakeAPI struct {
	mu    sync.Mutex
	calls []fakeCall
	route func(f *fakeAPI, w http.ResponseWriter, r *http.Request)
	gate  chan struct{}
	URL   string
	// active counts handlers mid-flight, so a test that cancels the deploy
	// context can first wait until every in-flight request has completed —
	// otherwise the cancellation races the poll response and the client
	// reports "context canceled" instead of reaching awaitLive.
	active sync.WaitGroup
}

func newFakeAPI(t *testing.T, route func(f *fakeAPI, w http.ResponseWriter, r *http.Request)) *fakeAPI {
	t.Helper()
	f := &fakeAPI{route: route, gate: make(chan struct{}, 1)}
	srv := httptest.NewServer(f)
	f.URL = srv.URL
	t.Cleanup(srv.Close)
	return f
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.active.Add(1)
	defer f.active.Done()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusInternalServerError)
		return
	}
	_ = r.Body.Close()
	f.mu.Lock()
	f.calls = append(f.calls, fakeCall{
		Method:        r.Method,
		Path:          r.URL.Path,
		RawQuery:      r.URL.RawQuery,
		Authorization: r.Header.Get("Authorization"),
		ContentType:   r.Header.Get("Content-Type"),
		Body:          body,
	})
	f.mu.Unlock()
	f.route(f, w, r)
}

func (f *fakeAPI) recorded() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

func (f *fakeAPI) signal() {
	select {
	case f.gate <- struct{}{}:
	default:
	}
}

// callFor finds the one recorded call for (method, path) or fails the test.
func (f *fakeAPI) callFor(t *testing.T, method, path string) fakeCall {
	t.Helper()
	var found []fakeCall
	for _, c := range f.recorded() {
		if c.Method == method && c.Path == path {
			found = append(found, c)
		}
	}
	switch len(found) {
	case 1:
		return found[0]
	case 0:
		t.Fatalf("no recorded %s %s among %+v", method, path, f.recorded())
	default:
		t.Fatalf("%d recorded %s %s calls, want 1", len(found), method, path)
	}
	return fakeCall{}
}

func requireBearer(t *testing.T, call fakeCall, token string) {
	t.Helper()
	if want := "Bearer " + token; call.Authorization != want {
		t.Errorf("%s %s Authorization = %q, want %q", call.Method, call.Path, call.Authorization, want)
	}
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// factoryAt points the given base-override env at the fake and builds a
// production factory over a transport with no proxy path — fully
// independent of the ambient environment and of net/http's proxy cache.
func factoryAt(t *testing.T, baseEnv, baseURL string) Factory {
	t.Helper()
	t.Setenv(baseEnv, baseURL)
	return NewFactory(&http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{},
	})
}

// --- Deploy plumbing: Deploy ends in awaitLive, which polls the relay's
// real public URL — unreachable from a test. The fakes therefore signal on
// the last call before the live check; the test cancels the context and
// Deploy unwinds immediately without ever dialing the platform domain. ---

type deployOutcome struct {
	result Result
	err    error
}

func awaitOutcome(t *testing.T, done <-chan deployOutcome) deployOutcome {
	t.Helper()
	select {
	case out := <-done:
		return out
	case <-time.After(10 * time.Second):
		t.Fatal("Deploy did not return within 10s")
		return deployOutcome{}
	}
}

// runDeployUntilGate starts Deploy, waits for the fake to signal the marked
// call, lets that request complete, cancels the context and returns the
// outcome — unwinding awaitLive the moment it starts without cutting short
// any in-flight platform call.
func runDeployUntilGate(t *testing.T, ctx context.Context, cancel context.CancelFunc, f *fakeAPI, client Client, spec Spec) deployOutcome {
	t.Helper()
	done := make(chan deployOutcome, 1)
	go func() {
		result, err := client.Deploy(ctx, spec)
		done <- deployOutcome{result: result, err: err}
	}()
	<-f.gate
	f.active.Wait()
	cancel()
	return awaitOutcome(t, done)
}

// multipartForm parses a recorded multipart body using its content-type
// boundary.
func multipartForm(t *testing.T, call fakeCall) (*multipart.Form, error) {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(call.ContentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		t.Fatalf("%s %s content type = %q (%v), want multipart", call.Method, call.Path, call.ContentType, err)
	}
	return multipart.NewReader(bytes.NewReader(call.Body), params["boundary"]).ReadForm(1 << 20)
}

func fileContent(t *testing.T, form *multipart.Form, field string) string {
	t.Helper()
	headers := form.File[field]
	if len(headers) != 1 {
		t.Fatalf("multipart field %q has %d file parts, want 1", field, len(headers))
	}
	fh, err := headers[0].Open()
	if err != nil {
		t.Fatalf("open %q part: %v", field, err)
	}
	defer func() { _ = fh.Close() }()
	raw, err := io.ReadAll(fh)
	if err != nil {
		t.Fatalf("read %q part: %v", field, err)
	}
	return string(raw)
}

// --- Vercel ---

func TestVercelDiscoverMatrix(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		wantExist bool
		wantErr   error
	}{
		{name: "existing project", status: http.StatusOK, wantExist: true},
		{name: "missing project", status: http.StatusNotFound},
		{name: "unauthorized", status: http.StatusUnauthorized, wantErr: ErrCredentials},
		{name: "forbidden", status: http.StatusForbidden, wantErr: ErrCredentials},
		{name: "server error", status: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, `{"name":"web-relay"}`)
			})
			client, err := factoryAt(t, VercelAPIBaseEnv, api.URL).For(PlatformVercel, Credential{Token: "tok"})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			if client.Platform() != PlatformVercel {
				t.Errorf("Platform() = %q, want vercel", client.Platform())
			}
			discovery, err := client.Discover(context.Background(), "web-relay")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Discover error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil && tc.status >= 500 {
				return // any error is fine for a server failure, as long as it is not success
			}
			if err != nil {
				t.Fatalf("Discover: %v", err)
			}
			if discovery.Exists != tc.wantExist {
				t.Errorf("Exists = %v, want %v", discovery.Exists, tc.wantExist)
			}
			if tc.wantExist && discovery.URL != "https://web-relay.vercel.app" {
				t.Errorf("URL = %q, want the stable vercel domain", discovery.URL)
			}
		})
	}
}

func TestVercelDiscoverPinsTeamScope(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, `{}`)
	})
	pinned, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok", Team: "team_42"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if _, err := pinned.Discover(context.Background(), "web-relay"); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	call := f.callFor(t, http.MethodGet, "/v9/projects/web-relay")
	requireBearer(t, call, "tok")
	if call.RawQuery != "teamId=team_42" {
		t.Errorf("query = %q, want teamId=team_42", call.RawQuery)
	}

	// Without a pin the credential's own scope is used — no teamId.
	unpinned, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if _, err := unpinned.Discover(context.Background(), "web-relay"); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	calls := f.recorded()
	last := calls[len(calls)-1]
	requireBearer(t, last, "tok")
	if last.RawQuery != "" {
		t.Errorf("unpinned query = %q, want empty", last.RawQuery)
	}
}

func TestVercelDeployRecordsProductionDeployment(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v13/deployments" {
			writeJSON(w, http.StatusOK, `{"id":"dpl_1","readyState":"READY"}`)
			f.signal()
			return
		}
		writeJSON(w, http.StatusNotFound, `{"error":"unexpected"}`)
	})
	client, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	spec := Spec{Project: "web-relay", Source: "// relay worker", Version: "1.0.0", Token: "relay-key"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := runDeployUntilGate(t, ctx, cancel, f, client, spec)

	// The cancellation races the final response read, so the error text
	// varies (poll cut short vs live-check unwind); Deploy must never report
	// success against a fake that cannot go live.
	if out.err == nil {
		t.Fatal("Deploy = nil error; a deployment can never go live against a fake")
	}

	call := f.callFor(t, http.MethodPost, "/v13/deployments")
	requireBearer(t, call, "tok")
	var payload struct {
		Name   string `json:"name"`
		Target string `json:"target"`
		Files  []struct {
			File     string `json:"file"`
			Data     string `json:"data"`
			Encoding string `json:"encoding"`
		} `json:"files"`
		ProjectSettings      map[string]any `json:"projectSettings"`
		DeploymentProtection map[string]any `json:"deploymentProtection"`
		Env                  []struct {
			Key    string   `json:"key"`
			Value  string   `json:"value"`
			Target []string `json:"target"`
		} `json:"env"`
	}
	if err := json.Unmarshal(call.Body, &payload); err != nil {
		t.Fatalf("decode deploy payload: %v", err)
	}
	if payload.Name != "web-relay" || payload.Target != "production" {
		t.Errorf("payload name/target = %q/%q, want web-relay/production", payload.Name, payload.Target)
	}

	// Two files: the worker and the vercel.json that routes the project
	// root to it. The rewrite is pinned byte for byte — it is what makes
	// every root probe and relayed request reach the worker (#21).
	if len(payload.Files) != 2 {
		t.Fatalf("files = %+v, want the worker and vercel.json", payload.Files)
	}
	uploaded := map[string]string{}
	for _, fl := range payload.Files {
		if fl.Encoding != "base64" {
			t.Errorf("file %q encoding = %q, want base64", fl.File, fl.Encoding)
		}
		decoded, err := base64.StdEncoding.DecodeString(fl.Data)
		if err != nil {
			t.Errorf("file %q data: %v", fl.File, err)
			continue
		}
		uploaded[fl.File] = string(decoded)
	}
	if uploaded["api/relay.js"] != spec.Source {
		t.Errorf("api/relay.js = %q, want the worker source", uploaded["api/relay.js"])
	}
	const wantRewrite = `{"rewrites":[{"source":"/(.*)","destination":"/api/relay"}]}`
	if uploaded["vercel.json"] != wantRewrite {
		t.Errorf("vercel.json = %q, want the catch-all rewrite %q", uploaded["vercel.json"], wantRewrite)
	}

	if v, ok := payload.ProjectSettings["framework"]; !ok || v != nil {
		t.Errorf("projectSettings.framework = %v (present %v), want explicit null", v, ok)
	}
	if v, ok := payload.DeploymentProtection["ssoProtection"]; !ok || v != nil {
		t.Errorf("deploymentProtection.ssoProtection = %v (present %v), want explicit null (no SSO wall)", v, ok)
	}
	env := map[string]struct {
		value  string
		target []string
	}{}
	for _, e := range payload.Env {
		env[e.Key] = struct {
			value  string
			target []string
		}{e.Value, e.Target}
	}
	if e, ok := env["RELAY_VERSION"]; !ok || e.value != "1.0.0" || len(e.target) != 1 || e.target[0] != "production" {
		t.Errorf("RELAY_VERSION env = %+v, want 1.0.0 on production", e)
	}
	if e, ok := env["RELAY_AUTH_TOKEN"]; !ok || e.value != "relay-key" || len(e.target) != 1 || e.target[0] != "production" {
		t.Errorf("RELAY_AUTH_TOKEN env = %+v, want the relay key on production", e)
	}
}

func TestVercelDeployPollsUntilReady(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v13/deployments":
			writeJSON(w, http.StatusOK, `{"id":"dpl_1","readyState":"BUILDING"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v13/deployments/dpl_1":
			writeJSON(w, http.StatusOK, `{"id":"dpl_1","readyState":"READY"}`)
			f.signal()
		default:
			writeJSON(w, http.StatusNotFound, `{"error":"unexpected"}`)
		}
	})
	client, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok", Team: "team_42"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	spec := Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "relay-key"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := runDeployUntilGate(t, ctx, cancel, f, client, spec)
	// The cancellation races the poll response read, so either the live
	// check unwound or the poll itself was cut short — Deploy must never
	// report success either way; the recorded poll below proves the loop ran.
	if out.err == nil {
		t.Fatal("Deploy = nil error; a deployment can never go live against a fake")
	}
	call := f.callFor(t, http.MethodGet, "/v13/deployments/dpl_1")
	if call.RawQuery != "teamId=team_42" {
		t.Errorf("poll query = %q, want teamId=team_42", call.RawQuery)
	}
}

func TestVercelDeployFailsOnErroredDeployment(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v13/deployments":
			writeJSON(w, http.StatusOK, `{"id":"dpl_1","readyState":"BUILDING"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v13/deployments/dpl_1":
			writeJSON(w, http.StatusOK, `{"id":"dpl_1","readyState":"ERROR"}`)
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	if err == nil || !strings.Contains(err.Error(), "deployment error") {
		t.Fatalf("Deploy error = %v, want the errored-deployment failure", err)
	}
}

func TestVercelDeployPostFailure(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusInternalServerError, `boom`)
	})
	client, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	if err == nil {
		t.Fatal("Deploy = nil error on a 500 create answer")
	}
}

func TestVercelDeployAnswerWithoutID(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, `{"readyState":"READY"}`)
	})
	client, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	if err == nil || !strings.Contains(err.Error(), "without deployment id") {
		t.Fatalf("Deploy error = %v, want the missing-id failure", err)
	}
}

func TestVercelDeleteMatrix(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr error
	}{
		{name: "deleted", status: http.StatusNoContent},
		{name: "already gone", status: http.StatusNotFound},
		{name: "unauthorized", status: http.StatusUnauthorized, wantErr: ErrCredentials},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, `{}`)
			})
			client, err := factoryAt(t, VercelAPIBaseEnv, f.URL).For(PlatformVercel, Credential{Token: "tok"})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			err = client.Delete(context.Background(), "web-relay")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Delete error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete: %v", err)
			}
			call := f.callFor(t, http.MethodDelete, "/v9/projects/web-relay")
			requireBearer(t, call, "tok")
		})
	}
}

// --- Cloudflare ---

const cfSubdomainJSON = `{"success":true,"result":{"subdomain":"example"},"errors":[]}`

func TestCloudflareDiscoverResolvesSingleAccount(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/accounts":
			writeJSON(w, http.StatusOK, `{"success":true,"result":[{"id":"acc_1"}],"errors":[]}`)
		case "/accounts/acc_1/workers/scripts/web-relay":
			writeJSON(w, http.StatusOK, `{"success":true,"result":{"id":"web-relay"},"errors":[]}`)
		case "/accounts/acc_1/workers/subdomain":
			writeJSON(w, http.StatusOK, cfSubdomainJSON)
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if client.Platform() != PlatformCloudflare {
		t.Errorf("Platform() = %q, want cloudflare", client.Platform())
	}
	discovery, err := client.Discover(context.Background(), "web-relay")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !discovery.Exists || discovery.URL != "https://web-relay.example.workers.dev" {
		t.Errorf("discovery = %+v, want the existing script with its workers.dev URL", discovery)
	}
	// The account is resolved per call (Discover and the subdomain read each
	// resolve it), so /accounts may repeat — every hit must carry the token.
	for _, call := range f.recorded() {
		if call.Path == "/accounts" {
			requireBearer(t, call, "tok")
		}
	}
	requireBearer(t, f.callFor(t, http.MethodGet, "/accounts/acc_1/workers/scripts/web-relay"), "tok")
}

func TestCloudflareDiscoverPinnedAccountSkipsResolution(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/accounts":
			t.Error("/accounts must not be called when the account is pinned")
		case "/accounts/acc_pin/workers/scripts/web-relay":
			writeJSON(w, http.StatusOK, `{"success":true,"result":{},"errors":[]}`)
		case "/accounts/acc_pin/workers/subdomain":
			writeJSON(w, http.StatusOK, cfSubdomainJSON)
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok", Account: "acc_pin"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	discovery, err := client.Discover(context.Background(), "web-relay")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !discovery.Exists {
		t.Errorf("discovery = %+v, want the pinned-account script", discovery)
	}
}

func TestCloudflareDiscoverAccountResolution(t *testing.T) {
	cases := []struct {
		name     string
		accounts string
		wantErr  error
	}{
		{name: "zero accounts", accounts: `[]`, wantErr: ErrCredentials},
		{name: "several accounts", accounts: `[{"id":"a"},{"id":"b"}]`, wantErr: ErrAmbiguousScope},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK,
					`{"success":true,"result":`+tc.accounts+`,"errors":[]}`)
			})
			client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok"})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			if _, err := client.Discover(context.Background(), "web-relay"); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Discover error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCloudflareDiscoverMissingScript(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{
			name:   "http 404",
			status: http.StatusNotFound,
			body:   `{"success":false,"result":null,"errors":[]}`,
		},
		{
			name:   "envelope 7003 inside a 200",
			status: http.StatusOK,
			body:   `{"success":false,"result":null,"errors":[{"code":7003,"message":"no route"}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/accounts":
					writeJSON(w, http.StatusOK, `{"success":true,"result":[{"id":"acc_1"}],"errors":[]}`)
				case "/accounts/acc_1/workers/scripts/web-relay":
					writeJSON(w, tc.status, tc.body)
				default:
					writeJSON(w, http.StatusNotFound, `{}`)
				}
			})
			client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok"})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			discovery, err := client.Discover(context.Background(), "web-relay")
			if err != nil {
				t.Fatalf("Discover: %v", err)
			}
			if discovery.Exists {
				t.Errorf("discovery = %+v, want a missing script", discovery)
			}
		})
	}
}

func TestCloudflareDiscoverCredentialRejection(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusUnauthorized, `{"success":false,"errors":[{"code":10000,"message":"bad token"}]}`)
	})
	client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if _, err := client.Discover(context.Background(), "web-relay"); !errors.Is(err, ErrCredentials) {
		t.Fatalf("Discover error = %v, want ErrCredentials", err)
	}
}

func TestCloudflareDeployUploadsScriptAndEnablesSubdomain(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/accounts/acc_pin/workers/scripts/web-relay":
			writeJSON(w, http.StatusOK, `{"success":true,"result":{},"errors":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/accounts/acc_pin/workers/scripts/web-relay/subdomain":
			writeJSON(w, http.StatusOK, `{"success":true,"result":{},"errors":[]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/accounts/acc_pin/workers/subdomain":
			writeJSON(w, http.StatusOK, cfSubdomainJSON)
			f.signal()
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok", Account: "acc_pin"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	spec := Spec{Project: "web-relay", Source: "// worker", Version: "2.0.0", Token: "relay-key"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := runDeployUntilGate(t, ctx, cancel, f, client, spec)
	// The cancellation races the final response read, so the error text
	// varies; Deploy must never report success against a fake that cannot
	// go live.
	if out.err == nil {
		t.Fatal("Deploy = nil error; a deployment can never go live against a fake")
	}

	put := f.callFor(t, http.MethodPut, "/accounts/acc_pin/workers/scripts/web-relay")
	requireBearer(t, put, "tok")
	form, err := multipartForm(t, put)
	if err != nil {
		t.Fatalf("parse multipart body: %v", err)
	}
	var meta struct {
		MainModule        string `json:"main_module"`
		CompatibilityDate string `json:"compatibility_date"`
		Bindings          []struct {
			Type string `json:"type"`
			Name string `json:"name"`
			Text string `json:"text"`
		} `json:"bindings"`
	}
	if err := json.Unmarshal([]byte(form.Value["metadata"][0]), &meta); err != nil {
		t.Fatalf("decode metadata: %v (%q)", err, form.Value["metadata"])
	}
	if meta.MainModule != "relay.js" {
		t.Errorf("main_module = %q, want relay.js", meta.MainModule)
	}
	bindings := map[string]string{}
	for _, b := range meta.Bindings {
		if b.Type != "plain_text" {
			t.Errorf("binding %s type = %q, want plain_text", b.Name, b.Type)
		}
		bindings[b.Name] = b.Text
	}
	if bindings["RELAY_VERSION"] != "2.0.0" || bindings["RELAY_AUTH_TOKEN"] != "relay-key" {
		t.Errorf("bindings = %v, want the version and relay key", bindings)
	}
	if got := fileContent(t, form, "relay.js"); got != spec.Source {
		t.Errorf("relay.js part = %q, want the worker source", got)
	}

	enable := f.callFor(t, http.MethodPost, "/accounts/acc_pin/workers/scripts/web-relay/subdomain")
	if string(enable.Body) != `{"enabled":true}` {
		t.Errorf("subdomain enable body = %q, want {\"enabled\":true}", enable.Body)
	}
	f.callFor(t, http.MethodGet, "/accounts/acc_pin/workers/subdomain")
}

func TestCloudflareDeployEnvelopeError(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, `{"success":false,"result":null,"errors":[{"code":10021,"message":"validation failed"}]}`)
	})
	client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok", Account: "acc_pin"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	if err == nil || !strings.Contains(err.Error(), "validation failed") {
		t.Fatalf("Deploy error = %v, want the envelope message", err)
	}
}

func TestCloudflareDeployMissingSubdomain(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			writeJSON(w, http.StatusOK, `{"success":true,"result":{},"errors":[]}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/subdomain"):
			writeJSON(w, http.StatusOK, `{"success":true,"result":{},"errors":[]}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/subdomain"):
			writeJSON(w, http.StatusOK, `{"success":true,"result":{"subdomain":""},"errors":[]}`)
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok", Account: "acc_pin"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	if err == nil || !strings.Contains(err.Error(), "no workers.dev subdomain") {
		t.Fatalf("Deploy error = %v, want the missing-subdomain failure", err)
	}
}

func TestCloudflareDeleteMatrix(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr error
	}{
		{name: "deleted", status: http.StatusOK},
		{name: "already gone", status: http.StatusNotFound},
		{name: "unauthorized", status: http.StatusUnauthorized, wantErr: ErrCredentials},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, `{"success":true,"result":{},"errors":[]}`)
			})
			client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok", Account: "acc_pin"})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			err = client.Delete(context.Background(), "web-relay")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Delete error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete: %v", err)
			}
			f.callFor(t, http.MethodDelete, "/accounts/acc_pin/workers/scripts/web-relay")
		})
	}
}

// A 200 envelope reporting failure is a failed delete — the registry must
// not purge the relay while the script lives on. The envelope's not-found
// code is the desired end state, exactly as in Discover.
func TestCloudflareDeleteEnvelopeMatrix(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		wantOK bool
	}{
		{
			name:   "success envelope",
			status: http.StatusOK,
			body:   `{"success":true,"result":null,"errors":[]}`,
			wantOK: true,
		},
		{
			name:   "http 404",
			status: http.StatusNotFound,
			body:   `{"success":false,"result":null,"errors":[]}`,
			wantOK: true,
		},
		{
			name:   "envelope 7003 inside a 200",
			status: http.StatusOK,
			body:   `{"success":false,"result":null,"errors":[{"code":7003,"message":"no route"}]}`,
			wantOK: true,
		},
		{
			name:   "envelope failure inside a 200",
			status: http.StatusOK,
			body:   `{"success":false,"result":null,"errors":[{"code":10143,"message":"script is in use"}]}`,
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, tc.body)
			})
			client, err := factoryAt(t, CloudflareAPIBaseEnv, f.URL).For(PlatformCloudflare, Credential{Token: "tok", Account: "acc_pin"})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			err = client.Delete(context.Background(), "web-relay")
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Delete = %v, want the already-deleted end state", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "script is in use") {
				t.Fatalf("Delete = %v, want the envelope's failure to surface", err)
			}
		})
	}
}

// --- Deno ---
//
// The fakes speak the published Deploy API wire (api.deno.com/v1): projects
// live under /organizations/{organizationId}/projects and are addressed by
// UUID everywhere else; deployments are JSON bodies on
// /projects/{projectId}/deployments, polled at /deployments/{deploymentId}.

const (
	denoTestOrgID     = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	denoTestProjectID = "b1eef7d2-9c0b-4ef8-bb6d-6bb9bd380a11"
)

// denoProjectsPath is the org-scoped project list route.
func denoProjectsPath() string {
	return "/v1/organizations/" + denoTestOrgID + "/projects"
}

func TestDenoDiscoverMatrix(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		wantExist bool
		wantErr   error
	}{
		{
			name:      "project in list",
			status:    http.StatusOK,
			body:      `[{"id":"` + denoTestProjectID + `","name":"web-relay"}]`,
			wantExist: true,
		},
		{name: "empty list", status: http.StatusOK, body: `[]`},
		{
			name:   "other projects only",
			status: http.StatusOK,
			body:   `[{"id":"11111111-1111-4111-8111-111111111111","name":"other-relay"}]`,
		},
		{name: "unauthorized", status: http.StatusUnauthorized, wantErr: ErrCredentials},
		{name: "forbidden", status: http.StatusForbidden, wantErr: ErrCredentials},
		{name: "server error", status: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, tc.body)
			})
			client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno,
				Credential{Token: "tok", Organization: denoTestOrgID})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			if client.Platform() != PlatformDeno {
				t.Errorf("Platform() = %q, want deno", client.Platform())
			}
			discovery, err := client.Discover(context.Background(), "web-relay")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Discover error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if tc.status >= 500 {
				if err == nil {
					t.Fatal("Discover = nil error on a server failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("Discover: %v", err)
			}
			if discovery.Exists != tc.wantExist {
				t.Errorf("Exists = %v, want %v", discovery.Exists, tc.wantExist)
			}
			if tc.wantExist && discovery.URL != "https://web-relay.deno.dev" {
				t.Errorf("URL = %q, want the stable deno.dev domain", discovery.URL)
			}
			call := f.callFor(t, http.MethodGet, denoProjectsPath())
			requireBearer(t, call, "tok")
			if call.RawQuery != "q=web-relay&page=1&limit=100" {
				t.Errorf("list query = %q, want the spec's q name filter with paged full pages", call.RawQuery)
			}
		})
	}
}

func TestDenoDiscoverWalksPagesUntilMatch(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") != "1" {
			writeJSON(w, http.StatusOK, `[{"id":"`+denoTestProjectID+`","name":"web-relay"}]`)
			return
		}
		// A full first page of strangers — a server that ignores the q
		// filter: the walk must continue and the match stay client-side.
		var b strings.Builder
		b.WriteByte('[')
		for i := 0; i < 100; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"id":"11111111-1111-4111-8111-%012d","name":"filler-%d"}`, i, i)
		}
		b.WriteByte(']')
		writeJSON(w, http.StatusOK, b.String())
	})
	client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno,
		Credential{Token: "tok", Organization: denoTestOrgID})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	discovery, err := client.Discover(context.Background(), "web-relay")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !discovery.Exists || discovery.URL != "https://web-relay.deno.dev" {
		t.Errorf("discovery = %+v, want the project found past the first page", discovery)
	}
	var listCalls int
	for _, call := range f.recorded() {
		if call.Method != http.MethodGet || call.Path != denoProjectsPath() {
			t.Errorf("unexpected call %s %s", call.Method, call.Path)
			continue
		}
		listCalls++
		requireBearer(t, call, "tok")
		if call.RawQuery != "q=web-relay&page=1&limit=100" && call.RawQuery != "q=web-relay&page=2&limit=100" {
			t.Errorf("list query = %q, want the q filter on every paged request", call.RawQuery)
		}
	}
	if listCalls != 2 {
		t.Errorf("list calls = %d, want 2 (the walk continues past a full page)", listCalls)
	}
}

func TestDenoRequiresOrganizationPin(t *testing.T) {
	// The API has no route that resolves the organization from a token, so
	// every deno operation refuses up front rather than guess a scope.
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		t.Errorf("no platform call may happen without the organization pin, got %s %s", r.Method, r.URL.Path)
		writeJSON(w, http.StatusNotFound, `{}`)
	})
	client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno, Credential{Token: "tok"})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	check := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s = nil error; the organization pin is mandatory", what)
		}
		if !errors.Is(err, ErrAmbiguousScope) {
			t.Errorf("%s error = %v, want it to classify as ErrAmbiguousScope", what, err)
		}
		if !strings.Contains(err.Error(), "organization") {
			t.Errorf("%s error = %v, want it to name the missing organization pin", what, err)
		}
	}
	_, err = client.Discover(context.Background(), "web-relay")
	check("Discover", err)
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	check("Deploy", err)
	check("Delete", client.Delete(context.Background(), "web-relay"))
	if calls := f.recorded(); len(calls) != 0 {
		t.Errorf("platform calls = %d, want 0", len(calls))
	}
}

func TestDenoDeployCreatesProjectThenDeploys(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == denoProjectsPath():
			writeJSON(w, http.StatusOK, `[]`) // the project does not exist yet
		case r.Method == http.MethodPost && r.URL.Path == denoProjectsPath():
			writeJSON(w, http.StatusOK,
				`{"id":"`+denoTestProjectID+`","name":"web-relay","description":"",`+
					`"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/projects/"+denoTestProjectID+"/deployments":
			writeJSON(w, http.StatusOK,
				`{"id":"dep_1","projectId":"`+denoTestProjectID+`","status":"success"}`)
			f.signal() // the last call before the live check
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno,
		Credential{Token: "tok", Organization: denoTestOrgID})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	spec := Spec{Project: "web-relay", Source: "// worker", Version: "3.0.0", Token: "relay-key"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := runDeployUntilGate(t, ctx, cancel, f, client, spec)
	// The cancellation races the final response read, so the error text
	// varies; Deploy must never report success against a fake that cannot
	// go live.
	if out.err == nil {
		t.Fatal("Deploy = nil error; a deployment can never go live against a fake")
	}

	create := f.callFor(t, http.MethodPost, denoProjectsPath())
	requireBearer(t, create, "tok")
	if want := `{"name":"web-relay"}`; string(create.Body) != want {
		t.Errorf("create-project body = %s, want %s (the schema allows no other field)", create.Body, want)
	}

	deployCall := f.callFor(t, http.MethodPost, "/v1/projects/"+denoTestProjectID+"/deployments")
	requireBearer(t, deployCall, "tok")
	if !strings.HasPrefix(deployCall.ContentType, "application/json") {
		t.Errorf("deploy content type = %q, want application/json", deployCall.ContentType)
	}
	// CreateDeploymentRequest is additionalProperties:false on the platform:
	// assert the exact field set, not just the values.
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(deployCall.Body, &payload); err != nil {
		t.Fatalf("decode deploy payload: %v", err)
	}
	wantFields := map[string]bool{
		"entryPointUrl": false, "assets": false, "envVars": false, "domains": false,
	}
	for field := range payload {
		if _, known := wantFields[field]; !known {
			t.Errorf("deploy payload carries %q, which the published schema does not define", field)
			continue
		}
		wantFields[field] = true
	}
	for field, seen := range wantFields {
		if !seen {
			t.Errorf("deploy payload is missing %q", field)
		}
	}
	if got := string(payload["entryPointUrl"]); got != `"relay.js"` {
		t.Errorf("entryPointUrl = %s, want \"relay.js\"", got)
	}
	if got := string(payload["domains"]); got != `["{project.name}.deno.dev"]` {
		t.Errorf("domains = %s, want the stable {project.name}.deno.dev attachment", got)
	}
	var envVars map[string]string
	if err := json.Unmarshal(payload["envVars"], &envVars); err != nil {
		t.Fatalf("decode envVars: %v", err)
	}
	wantEnv := map[string]string{"RELAY_VERSION": "3.0.0", "RELAY_AUTH_TOKEN": "relay-key"}
	if len(envVars) != len(wantEnv) {
		t.Errorf("envVars = %v, want exactly %v", envVars, wantEnv)
	}
	for k, v := range wantEnv {
		if envVars[k] != v {
			t.Errorf("envVars[%s] = %q, want %q", k, envVars[k], v)
		}
	}
	var assets map[string]struct {
		Kind     string `json:"kind"`
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := json.Unmarshal(payload["assets"], &assets); err != nil {
		t.Fatalf("decode assets: %v", err)
	}
	if len(assets) != 1 {
		t.Fatalf("assets = %v, want exactly the entry module", assets)
	}
	asset, ok := assets["relay.js"]
	if !ok {
		t.Fatalf("assets = %v, want a relay.js entry", assets)
	}
	if asset.Kind != "file" || asset.Encoding != "base64" {
		t.Errorf("relay.js asset = %+v, want a base64 file asset", asset)
	}
	decoded, err := base64.StdEncoding.DecodeString(asset.Content)
	if err != nil || string(decoded) != spec.Source {
		t.Errorf("relay.js content = %q (%v), want the worker source", decoded, err)
	}
}

func TestDenoDeployExistingProjectSkipsCreate(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == denoProjectsPath():
			writeJSON(w, http.StatusOK, `[{"id":"`+denoTestProjectID+`","name":"web-relay"}]`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/projects/"+denoTestProjectID+"/deployments":
			writeJSON(w, http.StatusOK, `{"id":"dep_1","status":"success"}`)
			f.signal() // the last call before the live check
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno,
		Credential{Token: "tok", Organization: denoTestOrgID})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := runDeployUntilGate(t, ctx, cancel, f, client, Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	// The cancellation races the final response read, so the error text
	// varies; Deploy must never report success against a fake that cannot
	// go live.
	if out.err == nil {
		t.Fatal("Deploy = nil error; a deployment can never go live against a fake")
	}
	for _, call := range f.recorded() {
		if call.Method == http.MethodPost && call.Path == denoProjectsPath() {
			t.Error("existing project must not be recreated")
		}
	}
}

func TestDenoDeployFailsOnFailedDeployment(t *testing.T) {
	f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == denoProjectsPath():
			writeJSON(w, http.StatusOK, `[{"id":"`+denoTestProjectID+`","name":"web-relay"}]`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/projects/"+denoTestProjectID+"/deployments":
			writeJSON(w, http.StatusOK, `{"id":"dep_1","status":"pending"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/deployments/dep_1":
			writeJSON(w, http.StatusOK, `{"id":"dep_1","status":"failed"}`)
		default:
			writeJSON(w, http.StatusNotFound, `{}`)
		}
	})
	client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno,
		Credential{Token: "tok", Organization: denoTestOrgID})
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	_, err = client.Deploy(context.Background(), Spec{Project: "web-relay", Source: "src", Version: "1.0.0", Token: "k"})
	if err == nil || !strings.Contains(err.Error(), "deployment dep_1 failed") {
		t.Fatalf("Deploy error = %v, want the failed-deployment failure", err)
	}
}

func TestDenoDeleteMatrix(t *testing.T) {
	cases := []struct {
		name    string
		inList  bool
		status  int // the by-id delete answer; 0 = no delete call expected
		wantErr error
	}{
		{name: "deleted", inList: true, status: http.StatusNoContent},
		{name: "delete answered 404", inList: true, status: http.StatusNotFound},
		{name: "absent from the org already", inList: false},
		{name: "unauthorized", inList: true, status: http.StatusUnauthorized, wantErr: ErrCredentials},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			listBody := `[]`
			if tc.inList {
				listBody = `[{"id":"` + denoTestProjectID + `","name":"web-relay"}]`
			}
			f := newFakeAPI(t, func(f *fakeAPI, w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == denoProjectsPath():
					writeJSON(w, http.StatusOK, listBody)
				case r.Method == http.MethodDelete && r.URL.Path == "/v1/projects/"+denoTestProjectID:
					writeJSON(w, tc.status, `{}`)
				default:
					writeJSON(w, http.StatusNotFound, `{}`)
				}
			})
			client, err := factoryAt(t, DenoAPIBaseEnv, f.URL).For(PlatformDeno,
				Credential{Token: "tok", Organization: denoTestOrgID})
			if err != nil {
				t.Fatalf("For: %v", err)
			}
			err = client.Delete(context.Background(), "web-relay")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Delete error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if !tc.inList {
				// A name absent from the pinned organization is the only
				// "already gone" the client may conclude — and it must not
				// fire a delete at the platform to learn that.
				if calls := f.recorded(); len(calls) != 1 {
					t.Fatalf("calls = %d (%+v), want only the list walk", len(calls), calls)
				}
				return
			}
			call := f.callFor(t, http.MethodDelete, "/v1/projects/"+denoTestProjectID)
			requireBearer(t, call, "tok")
		})
	}
}

// --- awaitLive ---

func TestAwaitLiveSucceedsWhenVersionMatches(t *testing.T) {
	srv := versionServer(t, http.StatusOK, versionJSON("1.0.0"), nil)
	if err := awaitLive(context.Background(), srv.URL, "1.0.0", 5*time.Second); err != nil {
		t.Fatalf("awaitLive: %v", err)
	}
}

func TestAwaitLiveReportsVersionMismatch(t *testing.T) {
	srv := versionServer(t, http.StatusOK, versionJSON("0.9.0"), nil)
	err := awaitLive(context.Background(), srv.URL, "1.0.0", 100*time.Millisecond)
	if err == nil {
		t.Fatal("awaitLive = nil error on a version mismatch")
	}
	if !strings.Contains(err.Error(), "relay answers version") || !strings.Contains(err.Error(), "not live after") {
		t.Errorf("awaitLive error = %v, want the mismatch and deadline report", err)
	}
}

func TestAwaitLiveReportsNonWorkerAnswer(t *testing.T) {
	srv := versionServer(t, http.StatusOK, "<html>pause page</html>", nil)
	err := awaitLive(context.Background(), srv.URL, "1.0.0", 100*time.Millisecond)
	if err == nil {
		t.Fatal("awaitLive = nil error for a non-worker answer")
	}
	if !strings.Contains(err.Error(), "not a relay worker") {
		t.Errorf("awaitLive error = %v, want the non-worker classification", err)
	}
}

func TestAwaitLiveWaitsForTheNewWorker(t *testing.T) {
	var probeCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probeCount.Add(1) == 1 {
			// First probe: the old worker is still answering.
			writeJSON(w, http.StatusOK, versionJSON("0.9.0"))
			return
		}
		writeJSON(w, http.StatusOK, versionJSON("1.0.0"))
	}))
	t.Cleanup(srv.Close)
	if err := awaitLive(context.Background(), srv.URL, "1.0.0", 5*time.Second); err != nil {
		t.Fatalf("awaitLive: %v", err)
	}
}
