package e2e

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeEdge is one in-process platform: it speaks the exact REST shapes the
// gateway's vercel/cloudflare/deno clients use AND hosts every project's
// worker sim on the same origin. The gateway derives each relay's stable
// URL as <fake base>/<project slug>, so a relay's ingress, version probe
// and self-forward all land here, muxed by the first path segment.
type fakeEdge struct {
	t    *testing.T
	base string
	srv  *http.Server
	ln   net.Listener

	client *http.Client // shared by the sims for their inner forwarding fetch

	mu       sync.Mutex
	require  map[string]string       // provider -> required bearer token ("" accepts any)
	projects map[string]*fakeProject // slug -> state
	calls    []fakeCall
	closed   bool
}

type fakeCall struct {
	At       time.Time
	Provider string
	Kind     string // discover | deploy | accounts | subdomain | delete
	Project  string
	Query    url.Values
	Auth     string
}

// deployRecord is one completed deployment as the fake observed it.
type deployRecord struct {
	Version string
	Token   string
	Source  string
	At      time.Time
}

type fakeProject struct {
	provider   string
	exists     bool
	sim        *simState
	deploys    []deployRecord
	deletes    []time.Time
	deleteDone time.Time // when the last remote delete fully completed

	gate     chan struct{} // non-nil: the next deploy blocks until closed
	deleteOp chan struct{} // non-nil: the next delete blocks until closed
}

func newFakeEdge(t *testing.T) *fakeEdge {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake platform listen: %v", err)
	}
	f := &fakeEdge{
		t:        t,
		base:     "http://" + ln.Addr().String(),
		ln:       ln,
		require:  map[string]string{},
		projects: map[string]*fakeProject{},
		client:   &http.Client{Timeout: 30 * time.Second},
	}
	f.srv = &http.Server{Handler: f, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = f.srv.Serve(ln) }()
	t.Cleanup(f.Close)
	return f
}

func (f *fakeEdge) Close() {
	f.mu.Lock()
	was := f.closed
	f.closed = true
	f.mu.Unlock()
	if !was {
		_ = f.srv.Close()
	}
}

// ServeHTTP muxes platform API routes and worker-sim routes on one origin:
// the API paths are all under known prefixes; everything else addresses a
// project sim by its slug (/<slug>/...).
func (f *fakeEdge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/__relay/version" || path == "/__relay/healthz":
		// The self-forward upstream: a worker sim forwarding a relay-spec
		// request back to its own origin lands here, just like the real
		// worker's unauthenticated version route.
		writeJSON(w, http.StatusOK, map[string]string{"version": "self-forward-upstream"})
		return
	case strings.HasPrefix(path, "/v9/projects/"), strings.HasPrefix(path, "/v13/deployments"):
		f.serveVercel(w, r)
	case path == "/accounts" || strings.HasPrefix(path, "/accounts/"):
		f.serveCloudflare(w, r)
	case strings.HasPrefix(path, "/v1/"):
		f.serveDeno(w, r)
	default:
		slug := strings.TrimPrefix(path, "/")
		if i := strings.IndexByte(slug, '/'); i >= 0 {
			slug = slug[:i]
		}
		if slug == "" {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no route"})
			return
		}
		f.mu.Lock()
		p := f.projects[slug]
		f.mu.Unlock()
		if p == nil || p.sim == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no route"})
			return
		}
		p.sim.serveHTTP(w, r, f)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *fakeEdge) authorized(w http.ResponseWriter, r *http.Request, provider string) bool {
	f.mu.Lock()
	want := f.require[provider]
	f.mu.Unlock()
	if want == "" || r.Header.Get("Authorization") == "Bearer "+want {
		return true
	}
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "AUTHENTICATION_ERROR"})
	return false
}

func (f *fakeEdge) record(provider, kind, project string, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{
		At: time.Now(), Provider: provider, Kind: kind, Project: project,
		Query: r.URL.Query(), Auth: r.Header.Get("Authorization"),
	})
}

// --- test-facing observation ---

func (f *fakeEdge) callsFor(provider, kind string) []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeCall
	for _, c := range f.calls {
		if c.Provider == provider && c.Kind == kind {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeEdge) project(slug string) *fakeProject {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.projects[slug]
}

// addProject pre-registers a project as existing on its platform, with a
// fresh sim in the given initial mode (the caller then sets the deployed
// version/token or the mode).
func (f *fakeEdge) addProject(provider, slug, mode string) *fakeProject {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := &fakeProject{provider: provider, exists: true, sim: newSim(slug, mode)}
	f.projects[slug] = p
	return p
}

func (f *fakeEdge) setToken(provider, token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.require[provider] = token
}

func (f *fakeEdge) deploysOf(slug string) []deployRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.projects[slug]
	if p == nil {
		return nil
	}
	return append([]deployRecord(nil), p.deploys...)
}

func (f *fakeEdge) deployCount(slug string) int { return len(f.deploysOf(slug)) }

func (f *fakeEdge) deleteCount(slug string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.projects[slug]
	if p == nil {
		return 0
	}
	return len(p.deletes)
}

// deleteDoneAt reports when the project's last remote delete fully
// completed (its answer on the wire), which is the earliest instant a
// replacement deploy is allowed to exist.
func (f *fakeEdge) deleteDoneAt(slug string) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.projects[slug]
	if p == nil || p.deleteDone.IsZero() {
		return time.Time{}, false
	}
	return p.deleteDone, true
}

// armDeployGate blocks the project's NEXT deploy inside the fake until the
// returned release is called: the test observes the gateway's behavior
// while a deploy is in flight. Arming before the gateway's first deploy is
// the deterministic form — the stub it creates here is what the deploy
// then lands on.
func (f *fakeEdge) armDeployGate(slug string) (release func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.projects[slug]
	if !ok {
		p = &fakeProject{sim: newSim(slug, simOK)}
		f.projects[slug] = p
	}
	gate := make(chan struct{})
	p.gate = gate
	return func() { close(gate) }
}

// armDeleteGate does the same for the project's next remote delete.
func (f *fakeEdge) armDeleteGate(slug string) (release func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.projects[slug]
	if !ok {
		p = &fakeProject{sim: newSim(slug, simOK)}
		f.projects[slug] = p
	}
	gate := make(chan struct{})
	p.deleteOp = gate
	return func() { close(gate) }
}

// secretMarkers are the credentials this fake has ever required: none of
// them may ever surface in the gateway's logs, /stats or error bodies.
func (f *fakeEdge) secretMarkers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, tok := range f.require {
		if tok != "" {
			out = append(out, tok)
		}
	}
	for _, p := range f.projects {
		for _, d := range p.deploys {
			out = append(out, d.Token)
		}
	}
	return out
}

// urlMarkers are the origins the gateway must never log or expose.
func (f *fakeEdge) urlMarkers() []string {
	return []string{f.base, strings.TrimPrefix(f.base, "http://")}
}

func (f *fakeEdge) markerLabel(marker string) string {
	if strings.Contains(f.base, marker) || strings.Contains(marker, "127.0.0.1") {
		return "relay/fake URL"
	}
	return "provider credential"
}

// --- vercel API ---

type vercelPayload struct {
	Name  string `json:"name"`
	Files []struct {
		Data string `json:"data"`
	} `json:"files"`
	Env []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"env"`
}

func (f *fakeEdge) serveVercel(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v9/projects/"):
		slug := strings.Trim(strings.TrimPrefix(path, "/v9/projects/"), "/")
		if !f.authorized(w, r, "vercel") {
			return
		}
		f.record("vercel", "discover", slug, r)
		if p := f.lookup("vercel", slug); p != nil && p.exists {
			writeJSON(w, http.StatusOK, map[string]any{"id": "prj-" + slug, "name": slug})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "NOT_FOUND"})

	case r.Method == http.MethodPost && path == "/v13/deployments":
		if !f.authorized(w, r, "vercel") {
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 32<<20))
		var payload vercelPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "BAD_REQUEST"})
			return
		}
		var version, token, source string
		for _, e := range payload.Env {
			switch e.Key {
			case "RELAY_VERSION":
				version = e.Value
			case "RELAY_AUTH_TOKEN":
				token = e.Value
			}
		}
		if len(payload.Files) > 0 {
			if decoded, err := base64.StdEncoding.DecodeString(payload.Files[0].Data); err == nil {
				source = string(decoded)
			}
		}
		f.record("vercel", "deploy", payload.Name, r)
		p := f.waitGate("vercel", payload.Name)
		f.completeDeploy(p, version, token, source)
		writeJSON(w, http.StatusOK, map[string]any{"id": "dpl-" + slugHash(payload.Name), "readyState": "READY"})

	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/v9/projects/"):
		slug := strings.Trim(strings.TrimPrefix(path, "/v9/projects/"), "/")
		if !f.authorized(w, r, "vercel") {
			return
		}
		f.record("vercel", "delete", slug, r)
		f.completeDelete("vercel", slug)
		writeJSON(w, http.StatusOK, map[string]any{})

	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no vercel route"})
	}
}

// --- cloudflare API ---

type cfBindings struct {
	Bindings []struct {
		Type string `json:"type"`
		Name string `json:"name"`
		Text string `json:"text"`
	} `json:"bindings"`
}

func (f *fakeEdge) serveCloudflare(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // accounts/{a}/workers/scripts/{slug}[...]
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/accounts":
		if !f.authorized(w, r, "cloudflare") {
			return
		}
		f.record("cloudflare", "accounts", "", r)
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true, "result": []map[string]string{{"id": "acct-e2e"}},
		})

	case r.Method == http.MethodGet && len(parts) == 5 && parts[2] == "workers" && parts[3] == "scripts":
		slug := parts[4]
		if !f.authorized(w, r, "cloudflare") {
			return
		}
		f.record("cloudflare", "discover", slug, r)
		p := f.lookup("cloudflare", slug)
		if p != nil && p.exists {
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]string{"id": slug}})
			return
		}
		// Cloudflare reports a missing script as success=false + code 7003
		// inside HTTP 200 — the shape the client normalizes.
		writeJSON(w, http.StatusOK, map[string]any{
			"success": false,
			"errors":  []map[string]any{{"code": 7003, "message": "Workers script not found"}},
		})

	case r.Method == http.MethodGet && len(parts) == 4 && parts[2] == "workers" && parts[3] == "subdomain":
		if !f.authorized(w, r, "cloudflare") {
			return
		}
		f.record("cloudflare", "subdomain", "", r)
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true, "result": map[string]string{"subdomain": "e2e"},
		})

	case r.Method == http.MethodPut && len(parts) == 5:
		slug := parts[4]
		if !f.authorized(w, r, "cloudflare") {
			return
		}
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "multipart"})
			return
		}
		var meta cfBindings
		if err := json.Unmarshal([]byte(r.FormValue("metadata")), &meta); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "metadata"})
			return
		}
		var version, token, source string
		for _, b := range meta.Bindings {
			switch b.Name {
			case "RELAY_VERSION":
				version = b.Text
			case "RELAY_AUTH_TOKEN":
				token = b.Text
			}
		}
		if file, _, err := r.FormFile("relay.js"); err == nil {
			raw, _ := io.ReadAll(file)
			_ = file.Close()
			source = string(raw)
		}
		f.record("cloudflare", "deploy", slug, r)
		p := f.waitGate("cloudflare", slug)
		f.completeDeploy(p, version, token, source)
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]string{}})

	case r.Method == http.MethodPost && len(parts) == 6 && parts[5] == "subdomain":
		if !f.authorized(w, r, "cloudflare") {
			return
		}
		f.record("cloudflare", "enable-subdomain", parts[4], r)
		writeJSON(w, http.StatusOK, map[string]any{"success": true})

	case r.Method == http.MethodDelete && len(parts) == 5:
		slug := parts[4]
		if !f.authorized(w, r, "cloudflare") {
			return
		}
		f.record("cloudflare", "delete", slug, r)
		f.completeDelete("cloudflare", slug)
		writeJSON(w, http.StatusOK, map[string]any{"success": true})

	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no cloudflare route"})
	}
}

// --- deno API ---
//
// The published Deploy API wire (api.deno.com/v1): the project collection is
// organization-scoped, projects are addressed by UUID everywhere else, and
// deployments are JSON bodies. The by-name routes the previous client
// invented (/v1/projects/{name}) are deliberately unhandled: the fake 404s
// them exactly like the real platform, which is the regression lock for the
// client rewrite.

// e2eDenoOrgID is the organization every e2e deno relay pins. The fake
// serves this org only; any other id answers 404 like the real API.
const e2eDenoOrgID = "e17a0e6b-7ba7-4a6e-9dbd-1f9a83d4db0a"

// denoProjectID derives the deterministic project UUID for a slug — the
// platform addresses projects by UUID, never by name.
func denoProjectID(slug string) string {
	sum := sha256.Sum256([]byte("deno-project:" + slug))
	src := []byte(hex.EncodeToString(sum[:16]))
	src[12] = '4' // UUID version nibble
	src[16] = '8' // RFC 4122 variant nibble
	return fmt.Sprintf("%s-%s-%s-%s-%s", src[0:8], src[8:12], src[12:16], src[16:20], src[20:32])
}

// denoDeployPayload is the spec's CreateDeploymentRequest as the fake reads
// it: the worker as one inline asset plus the deployment envVars.
type denoDeployPayload struct {
	EntryPointUrl string `json:"entryPointUrl"`
	Assets        map[string]struct {
		Kind     string `json:"kind"`
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	} `json:"assets"`
	EnvVars map[string]string `json:"envVars"`
	Domains []string          `json:"domains"`
}

// denoProjectJSON renders the spec's Project (all its required fields).
func denoProjectJSON(slug string) map[string]any {
	return map[string]any{
		"id":          denoProjectID(slug),
		"name":        slug,
		"description": "",
		"createdAt":   "2026-01-01T00:00:00Z",
		"updatedAt":   "2026-01-01T00:00:00Z",
	}
}

func (f *fakeEdge) serveDeno(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // v1/...
	switch {
	// GET list / POST create: /v1/organizations/{organizationId}/projects
	case len(parts) == 4 && parts[0] == "v1" && parts[1] == "organizations" && parts[3] == "projects":
		if parts[2] != e2eDenoOrgID {
			writeJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "message": "organization not found"})
			return
		}
		switch r.Method {
		case http.MethodGet:
			if !f.authorized(w, r, "deno") {
				return
			}
			f.record("deno", "discover", "", r)
			f.mu.Lock()
			var slugs []string
			for slug, p := range f.projects {
				if p.provider == "deno" && p.exists {
					slugs = append(slugs, slug)
				}
			}
			f.mu.Unlock()
			sort.Strings(slugs)
			page, limit := 1, 20 // the spec's documented defaults
			if v, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && v >= 1 {
				page = v
			}
			if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v >= 1 && v <= 100 {
				limit = v
			}
			start := (page - 1) * limit
			items := []map[string]any{}
			if start < len(slugs) {
				end := start + limit
				if end > len(slugs) {
					end = len(slugs)
				}
				for _, slug := range slugs[start:end] {
					items = append(items, denoProjectJSON(slug))
				}
			}
			writeJSON(w, http.StatusOK, items)

		case http.MethodPost:
			if !f.authorized(w, r, "deno") {
				return
			}
			var body struct {
				Name string `json:"name"`
			}
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
			if body.Name == "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request", "message": "name is required"})
				return
			}
			f.record("deno", "create-project", body.Name, r)
			f.mu.Lock()
			p, ok := f.projects[body.Name]
			if !ok {
				p = &fakeProject{sim: newSim(body.Name, simOK)}
				f.projects[body.Name] = p
			}
			p.provider = "deno"
			p.exists = true // gates and sims on a pre-armed stub stay untouched
			f.mu.Unlock()
			writeJSON(w, http.StatusOK, denoProjectJSON(body.Name))

		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no deno route"})
		}

	// POST /v1/projects/{projectId}/deployments — JSON body, UUID addressing.
	case len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "deployments":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no deno route"})
			return
		}
		if !f.authorized(w, r, "deno") {
			return
		}
		var payload denoDeployPayload
		if err := json.NewDecoder(io.LimitReader(r.Body, 32<<20)).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request", "message": "json body required"})
			return
		}
		asset, ok := payload.Assets[payload.EntryPointUrl]
		if payload.EntryPointUrl != "relay.js" || !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request", "message": "entryPointUrl must name a shipped asset"})
			return
		}
		source := asset.Content
		if asset.Encoding == "base64" {
			if decoded, err := base64.StdEncoding.DecodeString(asset.Content); err == nil {
				source = string(decoded)
			}
		}
		slug, ok := f.denoSlugFor(parts[2])
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "message": "project not found"})
			return
		}
		f.record("deno", "deploy", slug, r)
		p := f.waitGate("deno", slug)
		f.completeDeploy(p, payload.EnvVars["RELAY_VERSION"], payload.EnvVars["RELAY_AUTH_TOKEN"], source)
		writeJSON(w, http.StatusOK, map[string]any{
			"id":        "dep-" + slugHash(slug),
			"projectId": parts[2],
			"status":    "success",
			"databases": map[string]any{},
			"createdAt": "2026-01-01T00:00:00Z",
			"updatedAt": "2026-01-01T00:00:00Z",
		})

	// GET /v1/deployments/{deploymentId} — the client's status poll.
	case len(parts) == 3 && parts[0] == "v1" && parts[1] == "deployments":
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no deno route"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": parts[2], "status": "success"})

	// DELETE /v1/projects/{projectId} — by UUID only.
	case len(parts) == 3 && parts[0] == "v1" && parts[1] == "projects":
		if r.Method != http.MethodDelete {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no deno route"})
			return
		}
		if !f.authorized(w, r, "deno") {
			return
		}
		slug, ok := f.denoSlugFor(parts[2])
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "message": "project not found"})
			return
		}
		f.record("deno", "delete", slug, r)
		f.completeDelete("deno", slug)
		w.WriteHeader(http.StatusOK) // the spec's delete answers 200 with no body

	default:
		// The old client's by-name routes land here: /v1/projects/{name} and
		// /v1/projects (create) do not exist on the platform, so they must
		// not exist here either.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no deno route"})
	}
}

// denoSlugFor resolves a project UUID back to its slug among the fake's deno
// (or not-yet-typed, gate-armed) projects.
func (f *fakeEdge) denoSlugFor(projectID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for slug, p := range f.projects {
		if (p.provider == "deno" || p.provider == "") && denoProjectID(slug) == projectID {
			return slug, true
		}
	}
	return "", false
}

// --- shared deploy/delete plumbing ---

func (f *fakeEdge) lookup(provider, slug string) *fakeProject {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.projects[slug]
}

// waitGate marks the project existing (a deploy creates it), blocks while a
// test-armed gate is held, and returns the project.
func (f *fakeEdge) waitGate(provider, slug string) *fakeProject {
	f.mu.Lock()
	p, ok := f.projects[slug]
	if !ok {
		p = &fakeProject{provider: provider, sim: newSim(slug, simOK)}
		f.projects[slug] = p
	}
	gate := p.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-time.After(60 * time.Second):
		}
	}
	return p
}

// completeDeploy finishes a deployment: the project now exists and its sim
// serves the just-deployed version/token — exactly what a platform switching
// the production URL to the new worker means.
func (f *fakeEdge) completeDeploy(p *fakeProject, version, token, source string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p.exists = true
	p.deploys = append(p.deploys, deployRecord{Version: version, Token: token, Source: source, At: time.Now()})
	p.sim.setDeployed(version, token)
}

func (f *fakeEdge) completeDelete(provider, slug string) {
	f.mu.Lock()
	p := f.projects[slug]
	if p == nil {
		p = &fakeProject{provider: provider}
		f.projects[slug] = p
	}
	gate := p.deleteOp
	p.exists = false
	p.deletes = append(p.deletes, time.Now())
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-time.After(60 * time.Second):
		}
	}
	f.mu.Lock()
	p.deleteDone = time.Now()
	f.mu.Unlock()
}

// slugHash keeps deployment ids unique per project without carrying names.
func slugHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return base64.RawURLEncoding.EncodeToString(sum[:6])
}

// --- worker sim ---

// Sim modes. Platform-level modes (down, suspended, not-worker) answer for
// every path — the worker never runs. Worker-level modes keep the
// unauthenticated version route answering and change ingress behavior only.
const (
	simOK              = "ok"
	simDown            = "down"            // TCP-level refusal: connection accepted then closed
	simSuspended402    = "suspended402"    // platform quota page: HTTP 402
	simSuspendedMarker = "suspendedMarker" // platform page: X-Vercel-Error DEPLOYMENT_DISABLED
	simNotWorker       = "notWorker"       // someone else's app on the URL
	simStale           = "staleVersion"    // worker answers an old version
	simProbe500        = "probeFailed"     // worker forwards, upstream leg fails (its 502)
)

// hopByHop mirrors the connection-scoped header set the worker strips.
var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true,
	"Proxy-Authorization": true, "Te": true, "Trailer": true,
	"Transfer-Encoding": true, "Upgrade": true,
}

// simState is one deployed relay worker, mirroring the embedded worker's
// contract (core.js): unauthenticated /__relay/version and /__relay/healthz,
// a 404 (never 403) for a missing or wrong X-Relay-Token, X-Relay-Target
// required with X-Relay-Path pinned to the target origin, end-to-end headers
// forwarded verbatim, streaming bodies, upstream failure answered 502.
type simState struct {
	slug string

	mu        sync.Mutex
	version   string // deployed worker version reported by /__relay/version
	token     string // deployed relay key required on ingress
	holdToken string // non-empty: ingress requires this instead (relay-key drift)
	mode      string
	slow      map[string]chan struct{} // X-Marker value -> release channel
	ingress   []ingressRecord
}

type ingressRecord struct {
	At        time.Time
	Method    string
	Path      string // effective path, the project prefix stripped
	Marker    string
	Target    string
	RelayPath string
	TokenOK   bool
}

func newSim(slug, mode string) *simState {
	return &simState{slug: slug, mode: mode, slow: map[string]chan struct{}{}}
}

func (s *simState) setDeployed(version, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version, s.token, s.mode, s.holdToken = version, token, simOK, ""
}

func (s *simState) setMode(mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
}

// blockMarker makes the next ingress carrying X-Marker hang inside the sim
// until release — an in-flight request the gateway must drain before a
// replacement deploys.
func (s *simState) blockMarker(marker string) (release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan struct{})
	s.slow[marker] = ch
	return func() { close(ch) }
}

func (s *simState) markers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, rec := range s.ingress {
		if rec.Marker != "" {
			out = append(out, rec.Marker)
		}
	}
	return out
}

func (s *simState) markerSeen(marker string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.ingress {
		if rec.Marker == marker {
			return true
		}
	}
	return false
}

// serveHTTP runs the sim. The request path is /<slug>/... on the shared
// fake origin; the worker behaves as if its origin were the project's own,
// so the prefix is stripped before the worker logic runs.
func (s *simState) serveHTTP(w http.ResponseWriter, r *http.Request, f *fakeEdge) {
	s.mu.Lock()
	mode, version, token, hold := s.mode, s.version, s.token, s.holdToken
	var release chan struct{}
	if marker := r.Header.Get("X-Marker"); marker != "" {
		release = s.slow[marker]
	}
	s.mu.Unlock()

	eff := strings.TrimPrefix(r.URL.Path, "/"+s.slug)

	// Platform-level answers: the worker is not running at all.
	switch mode {
	case simDown:
		// Accept the TCP connection and close it without answering — a
		// transport error for the probe, never an HTTP verdict. (Hijack so
		// the server does not write a 200 for us.)
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		time.Sleep(45 * time.Second)
		return
	case simSuspended402:
		writeJSON(w, http.StatusPaymentRequired, map[string]string{"error": "QUOTA_EXHAUSTED"})
		return
	case simSuspendedMarker:
		w.Header().Set("X-Vercel-Error", "DEPLOYMENT_DISABLED")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("this deployment is disabled"))
		return
	case simNotWorker:
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>someone else's app</html>"))
		return
	}

	// Unauthenticated worker routes (no-store in the real worker; the
	// gateway only reads them straight through).
	if r.Method == http.MethodGet && eff == "/__relay/version" {
		if mode == simStale {
			version = "0.0.0-stale"
		}
		writeJSON(w, http.StatusOK, map[string]string{"version": version})
		return
	}
	if r.Method == http.MethodGet && eff == "/__relay/healthz" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	// Relay-key gate: 404, never 403 — a scanned relay is indistinguishable
	// from an empty worker.
	expected := token
	if hold != "" {
		expected = hold
	}
	provided := r.Header.Get("X-Relay-Token")
	tokenOK := expected != "" && provided == expected

	s.mu.Lock()
	s.ingress = append(s.ingress, ingressRecord{
		At: time.Now(), Method: r.Method, Path: eff,
		Marker: r.Header.Get("X-Marker"),
		Target: r.Header.Get("X-Relay-Target"), RelayPath: r.Header.Get("X-Relay-Path"),
		TokenOK: tokenOK,
	})
	s.mu.Unlock()

	if !tokenOK {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if mode == simProbe500 {
		// The worker is current and its key accepted, but its own upstream
		// fetch fails — exactly the shape its 502 answer has.
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream fetch failed"})
		return
	}

	if release != nil {
		select {
		case <-release:
		case <-time.After(45 * time.Second):
		}
	}

	s.forward(w, r)
}

// forward mirrors the worker's relay leg: validate the target, pin the path
// to the target origin, strip relay-spec and hop-by-hop headers, stream the
// body up and the response back; an upstream transport failure is a 502.
func (s *simState) forward(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Relay-Target")
	tu, err := url.Parse(target)
	if err != nil || tu.Host == "" || (tu.Scheme != "http" && tu.Scheme != "https") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "x-relay-target must be an absolute http(s) URL"})
		return
	}
	relayPath := r.Header.Get("X-Relay-Path")
	if relayPath == "" {
		relayPath = "/"
	}
	pu, err := url.Parse(relayPath)
	if err != nil || pu.Host != "" || strings.HasPrefix(pu.Path, "//") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "x-relay-path must stay on the target origin"})
		return
	}
	up := tu.ResolveReference(pu)
	if up.Host != tu.Host || up.Scheme != tu.Scheme {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "x-relay-path must stay on the target origin"})
		return
	}

	var body io.Reader
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		body = r.Body
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, up.String(), body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream fetch failed"})
		return
	}
	for k, vv := range r.Header {
		ck := http.CanonicalHeaderKey(k)
		if hopByHop[ck] || ck == "Host" || ck == "Content-Length" ||
			ck == "X-Relay-Target" || ck == "X-Relay-Path" ||
			ck == "X-Relay-Provider" || ck == "X-Relay-Token" {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	resp, err := sharedSimClient.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream fetch failed"})
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for k, vv := range resp.Header {
		if hopByHop[http.CanonicalHeaderKey(k)] || http.CanonicalHeaderKey(k) == "Content-Length" {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// sharedSimClient carries the sims' inner forwarding fetches.
var sharedSimClient = &http.Client{Timeout: 30 * time.Second}

// --- echo upstream ---

type echoRecord struct {
	At       time.Time
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Body     string
	BodyLen  int
}

type echoServer struct {
	base string
	srv  *http.Server
	ln   net.Listener

	mu   sync.Mutex
	reqs []echoRecord
}

func newEcho(t *testing.T) *echoServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	e := &echoServer{base: "http://" + ln.Addr().String(), ln: ln}
	e.srv = &http.Server{Handler: http.HandlerFunc(e.handle), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = e.srv.Serve(ln) }()
	t.Cleanup(func() { _ = e.srv.Close() })
	return e
}

func (e *echoServer) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 256<<20))
	_ = r.Body.Close()
	rec := echoRecord{
		At: time.Now(), Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery,
		Header: r.Header.Clone(), BodyLen: len(raw),
	}
	if len(raw) <= 64<<10 {
		rec.Body = string(raw)
	}
	e.mu.Lock()
	e.reqs = append(e.reqs, rec)
	e.mu.Unlock()

	w.Header().Set("X-Echo", "e2e")
	writeJSON(w, http.StatusOK, map[string]any{
		"method": rec.Method,
		"path":   rec.Path,
		"query":  rec.RawQuery,
		// Echo only the headers a test may assert on: the full set is
		// recorded server-side; the body stays small and deterministic.
		"headers": map[string]string{
			"x-custom":         r.Header.Get("X-Custom"),
			"authorization":    r.Header.Get("Authorization"),
			"user-agent":       r.Header.Get("User-Agent"),
			"x-relay-token":    r.Header.Get("X-Relay-Token"),
			"x-relay-provider": r.Header.Get("X-Relay-Provider"),
			"x-relay-target":   r.Header.Get("X-Relay-Target"),
			"x-relay-path":     r.Header.Get("X-Relay-Path"),
			"x-forwarded-for":  r.Header.Get("X-Forwarded-For"),
			"x-marker":         r.Header.Get("X-Marker"),
		},
		"body":    rec.Body,
		"bodyLen": rec.BodyLen,
	})
}

func (e *echoServer) requests() []echoRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]echoRecord(nil), e.reqs...)
}

func (e *echoServer) lastRequest() (echoRecord, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.reqs) == 0 {
		return echoRecord{}, false
	}
	return e.reqs[len(e.reqs)-1], true
}

// parsedEcho decodes one echo answer body.
type parsedEcho struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   string            `json:"query"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	BodyLen int               `json:"bodyLen"`
}
