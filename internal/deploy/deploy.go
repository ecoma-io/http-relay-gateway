// Package deploy owns the managed-relay lifecycle: the embedded worker
// source, the per-platform clients that turn it into a live relay, and the
// version/token contract between the gateway and its fleet.
//
// Version and token travel with the deployment, never inside the source: the
// worker files are static, and each platform receives RELAY_VERSION and
// RELAY_AUTH_TOKEN as deploy-time environment, so a live worker can only
// report the pair its deployer injected.
package deploy

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RelayVersion is the generation of the embedded worker source. It rides
// every deployment as RELAY_VERSION and every live worker reports it from
// GET /__relay/version; the reconciler compares the two to detect drift — a
// gateway upgrade or downgrade leaves the whole fleet stale until it
// redeploys. Bump it whenever any file under internal/deploy/workers changes
// behavior.
const RelayVersion = "1"

// Platform names, matching the store's account/deployment platforms.
const (
	PlatformVercel     = "vercel"
	PlatformCloudflare = "cloudflare"
	PlatformDeno       = "deno"
)

// Token returns a new relay auth token: 32 crypto/rand bytes, base64url
// without padding — 43 URL-safe characters that survive headers, URLs and
// database columns unchanged.
func Token() (string, error) {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

// maxProjectName is the project-name length every platform accepts; the
// shortest stick wins so one slug works everywhere.
const maxProjectName = 40

// ProjectName turns a relay name into a platform-safe project slug:
// lowercase letters, digits and single dashes, at most maxProjectName
// characters. It is deterministic so every redeploy of a relay lands on the
// same project; name collisions between relays are impossible because relay
// names are unique.
func ProjectName(name string) string {
	var b strings.Builder
	prevDash := true // swallows leading dashes
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		case !prevDash:
			b.WriteByte('-')
			prevDash = true
		}
	}
	slug := strings.TrimRight(b.String(), "-")
	if len(slug) > maxProjectName {
		slug = strings.TrimRight(slug[:maxProjectName], "-")
	}
	if slug == "" {
		slug = "relay"
	}
	return slug
}

// Spec is one deployment to create or replace. Source is the assembled
// worker (workers.Assemble); Version and Token ride as platform environment.
type Spec struct {
	Project string
	Source  string
	Version string
	Token   string
}

// Result reports a finished deployment. URL must be the relay's stable
// public URL — identical across redeploys — because the pool keeps serving
// it until the next verified deploy replaces it.
type Result struct {
	Project    string
	ExternalID string
	URL        string
}

// Client deploys and removes the worker on one platform with one account's
// credentials. Implementations must be idempotent: Deploy creates the
// project when missing, replaces its code, and returns only once the new
// worker is live; Delete reaches the same end state when the worker is
// already gone.
type Client interface {
	Platform() string
	// Verify checks the credentials against the platform and returns the
	// canonical account reference to store (username or account id).
	Verify(ctx context.Context) (string, error)
	Deploy(ctx context.Context, spec Spec) (Result, error)
	Delete(ctx context.Context, project string) error
}

// Factory builds a client for one account's credentials. The admin plane
// uses it for credential verification and remote deletes, the reconciler for
// deploys.
type Factory interface {
	For(platform, token, accountRef string) (Client, error)
}

// Environment variable names that override platform API base URLs. They are
// test-only knobs: the e2e suite points them at in-process fakes so the
// black-box tests never touch a real platform. They never appear in the
// database or the API.
const (
	VercelAPIBaseEnv     = "VERCEL_API_BASE"
	CloudflareAPIBaseEnv = "CLOUDFLARE_API_BASE"
	DenoAPIBaseEnv       = "DENO_API_BASE"
)

// probeTimeout bounds one version probe; a slower relay is unreachable, not
// mismatched.
const probeTimeout = 5 * time.Second

// ProbeAnswer is what answered a version probe. A relay worker answers with
// its deployed version — Version non-empty, Status 200. Any other HTTP
// answer (a platform pause page, a deleted deployment, a stranger's app on
// the URL) is NOT a relay worker: Version empty, plus the status and the
// best-effort platform marker so the admin can see why.
type ProbeAnswer struct {
	Version string
	Status  int
	Marker  string
}

// NotWorkerErr describes a non-worker HTTP answer as an error, so probe
// verdicts flow through the same sanitize-and-record path as transport
// failures. Nil for a worker answer.
func (a ProbeAnswer) NotWorkerErr() error {
	if a.Version != "" {
		return nil
	}
	if a.Marker != "" {
		return fmt.Errorf("relay answered HTTP %d, not a relay worker (%s)", a.Status, a.Marker)
	}
	return fmt.Errorf("relay answered HTTP %d, not a relay worker", a.Status)
}

// Probe asks a live relay for its deployed version via GET
// <base>/__relay/version. Only a transport failure — timeout, TLS, refused
// connection — is an error; every HTTP answer comes back classified, because
// the probe is the one place that may conclude "the platform answered but
// our worker is not behind this URL" (a pause) as opposed to "the relay
// could not be reached at all". A pause must wait for revival; an
// unreachable relay must never be redeployed on a hunch either — the
// distinction between the two is this function's job, not the caller's.
func Probe(ctx context.Context, client *http.Client, base string) (ProbeAnswer, error) {
	u, err := url.JoinPath(base, "__relay/version")
	if err != nil {
		return ProbeAnswer{}, fmt.Errorf("probe URL: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
	if err != nil {
		return ProbeAnswer{}, fmt.Errorf("probe request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return ProbeAnswer{}, fmt.Errorf("probe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	answer := ProbeAnswer{
		Status: resp.StatusCode,
		Marker: strings.TrimSpace(resp.Header.Get("X-Vercel-Error")),
	}
	if resp.StatusCode != http.StatusOK {
		return answer, nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return ProbeAnswer{}, fmt.Errorf("probe: read answer: %w", err)
	}
	var parsed struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed.Version == "" {
		return answer, nil
	}
	answer.Version = parsed.Version
	return answer, nil
}

// forwardProbeTimeout bounds one end-to-end forwarding probe. The probe
// costs the relay two fetch legs — ingress, then the forwarding fetch back
// into its own version path — so it gets twice the version probe's budget.
const forwardProbeTimeout = 10 * time.Second

// forwardProbePath is the relay-spec path every forwarding probe targets:
// the worker's unauthenticated version route, which exists since the first
// worker generation and answers a deterministic JSON body.
const forwardProbePath = "/__relay/version"

// ForwardAnswer is what answered a forwarding probe.
type ForwardAnswer struct {
	Status int
	// JSON reports a 200 whose body parsed as a JSON object — the shape the
	// worker's version route answers after a completed forwarding round
	// trip, and the shape the e2e worker sims answer for the same request.
	JSON bool
}

// ForwardProbe drives one real relay-spec request through the relay itself:
// the relay's own origin rides in X-Relay-Target, the version path in
// X-Relay-Path, and the deployment's token (when non-empty) in
// X-Relay-Token. The worker's normal ingress runs — token check included —
// and then performs a genuine forwarding fetch back into its own version
// route, so a passing probe is evidence that ingress, authentication and
// the forwarding fetch all work, not merely that the URL answers. The
// inner request hits the version route ahead of the token check, which the
// worker answers unauthenticated — exactly what makes the loop close
// without a second protocol.
//
// A transport failure is an error; every HTTP answer comes back for the
// caller to classify: 404 is the worker's wrong-token answer (an
// unauthenticated request must be indistinguishable from an empty worker),
// 502 its upstream-fetch-failed answer, and a 200 with a JSON object body a
// completed round trip. token may be empty for relays that predate managed
// tokens (legacy rows): such a probe proves only that the relay executes a
// forwarding fetch, and any answer below 500 counts.
func ForwardProbe(ctx context.Context, client *http.Client, base, token string) (ForwardAnswer, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return ForwardAnswer{}, fmt.Errorf("forward probe URL: %w", err)
	}
	// The target is the relay's own origin — the path travels separately in
	// X-Relay-Path and must never widen the target beyond its origin.
	target := (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
	cctx, cancel := context.WithTimeout(ctx, forwardProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, base, nil)
	if err != nil {
		return ForwardAnswer{}, fmt.Errorf("forward probe request: %w", err)
	}
	req.Header.Set("X-Relay-Target", target)
	req.Header.Set("X-Relay-Path", forwardProbePath)
	if token != "" {
		req.Header.Set("X-Relay-Token", token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return ForwardAnswer{}, fmt.Errorf("forward probe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	answer := ForwardAnswer{Status: resp.StatusCode}
	if resp.StatusCode != http.StatusOK {
		return answer, nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return ForwardAnswer{}, fmt.Errorf("forward probe: read answer: %w", err)
	}
	var parsed map[string]any
	answer.JSON = json.Unmarshal(raw, &parsed) == nil
	return answer, nil
}
