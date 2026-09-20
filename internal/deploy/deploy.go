// Package deploy owns the relay fleet's platform half: the hard-coded
// provider set, the per-provider clients that discover, deploy and remove
// relay workers, and the version/key contract between the gateway and its
// fleet.
//
// Identity is (provider, name) — never a URL, a database row or a deployment
// id. From identity plus the provider credential alone, every client can
// discover the platform project and its stable URL, decide reuse versus
// redeploy, and replace the worker in place. No persisted deployment state
// exists anywhere: a restart re-derives everything from the desired
// configuration plus secrets.
//
// Version and relay key travel with the deployment, never inside the
// source: the worker files are static, and each platform receives
// RELAY_VERSION and RELAY_AUTH_TOKEN as deploy-time environment, so a live
// worker can only report the pair its deployer injected.
package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"http-relay-gateway/internal/relayversion"
)

// RelayVersion is the generation of the embedded worker source: the
// release-managed artifact from internal/relayversion, not a hand-edited
// constant. It rides every deployment as RELAY_VERSION and every live worker
// reports it from GET /__relay/version; the reconciler compares the two to
// detect drift — a gateway upgrade or downgrade leaves the whole fleet stale
// until it redeploys.
var RelayVersion = relayversion.Version

// The hard-coded provider set. There is no runtime provider registry: a
// relay's provider is one of these three, validated at config load.
const (
	PlatformVercel     = "vercel"
	PlatformCloudflare = "cloudflare"
	PlatformDeno       = "deno"
)

// Platforms returns the hard-coded provider set in stable order.
func Platforms() []string {
	return []string{PlatformVercel, PlatformCloudflare, PlatformDeno}
}

// RelayKey is one relay's absolute identity: (provider, name). Reconcile,
// single-flight, incarnation checks, metrics and pool membership all key on
// this — never on a URL, a deployment id or a platform row.
type RelayKey struct {
	Provider string
	Name     string
}

func (k RelayKey) String() string { return k.Provider + "/" + k.Name }

// Credential is the provider management credential plus the optional scope
// pins a provider needs to resolve (provider, name) to exactly one project.
type Credential struct {
	// Token is the provider management credential.
	Token string
	// Team pins the Vercel team scope; empty means the token's own user
	// scope. Required in config when a token reaches several teams —
	// discovery never guesses.
	Team string
	// Account pins the Cloudflare account; empty means "resolve from the
	// token", which succeeds only when the token sees exactly one account.
	Account string
}

// SameScope reports whether two credentials address the same platform
// scope — same pin (or same absence of one). A scope change under a stable
// relay identity moves the identity to a different part of the platform
// account; the deployment the old scope hosted is unreachable from the
// desired state from then on.
func (c Credential) SameScope(other Credential) bool {
	return c.Team == other.Team && c.Account == other.Account
}

// Discovery reports what provider discovery found for one project name.
type Discovery struct {
	// Exists reports whether the project is already on the platform.
	Exists bool
	// URL is the project's stable production URL — identical across
	// redeploys — filled whenever the project exists.
	URL string
}

// Spec is one deployment to create or replace. Source is the assembled
// worker (workers.Assemble); Version and Token ride as platform environment.
type Spec struct {
	Project string
	Source  string
	Version string
	Token   string
}

// Result reports a finished deployment. URL is the relay's stable public
// URL — derived from the project name deterministically, so it is identical
// across redeploys and identical to what Discover resolves.
type Result struct {
	Project    string
	ExternalID string
	URL        string
}

// Client discovers, deploys and removes the worker on one platform with one
// credential. Implementations must be idempotent: Deploy creates the
// project when missing and replaces its code otherwise, returning only once
// the new worker is live; Delete reaches the same end state when the worker
// is already gone.
type Client interface {
	Platform() string
	// Discover locates the platform project this identity maps to. A
	// missing project is a Discovery{Exists: false}, not an error; any other
	// failure (credentials, API, ambiguous scope) is.
	Discover(ctx context.Context, project string) (Discovery, error)
	Deploy(ctx context.Context, spec Spec) (Result, error)
	Delete(ctx context.Context, project string) error
}

// Factory builds a client for one credential. The reconciler uses it for
// every platform call.
type Factory interface {
	For(platform string, cred Credential) (Client, error)
}

// ErrCredentials reports a platform rejecting the credential: the token is
// wrong or expired, which is configuration data, not a gateway fault.
var ErrCredentials = errors.New("platform rejected credentials")

// ErrAmbiguousScope reports a credential that can reach several scopes
// where the identity does not pin one: discovery refuses to guess which
// project (or account) is meant, because silently deploying into the wrong
// scope is unrecoverable.
var ErrAmbiguousScope = errors.New("credential reaches several scopes; pin the scope in the relay config")

// maxProjectName is the project-name length every platform accepts; the
// shortest stick wins so one slug works everywhere.
const maxProjectName = 40

// ProjectName turns a relay name into a platform-safe project slug:
// lowercase letters, digits and single dashes, at most maxProjectName
// characters. It is deterministic so every deploy of a relay — and its
// discovery — lands on the same project.
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

// relayProbeTransport is the transport every readiness probe and live-check
// uses. Proxy is deliberately nil: probes must model the production data
// plane, which never routes through ambient proxies — a probe that succeeded
// via HTTP_PROXY while production fails direct would make readiness lie.
// Platform API calls (management plane) keep their own default transport.
func relayProbeTransport() *http.Transport {
	return &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        32,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
}

// ProbeClient returns an HTTP client for probing relay workers: the data
// plane's no-proxy transport with one overall timeout.
func ProbeClient(timeout time.Duration) *http.Client {
	return &http.Client{Transport: relayProbeTransport(), Timeout: timeout}
}

// Environment variable names that override platform API base URLs. They are
// test-only knobs: the e2e suite points them at in-process fakes so the
// black-box tests never touch a real platform. They never appear in logs.
const (
	VercelAPIBaseEnv     = "VERCEL_API_BASE"
	CloudflareAPIBaseEnv = "CLOUDFLARE_API_BASE"
	DenoAPIBaseEnv       = "DENO_API_BASE"
)

// Environment variable names that override the stable relay URL each client
// derives from the project name. Like the API base overrides they are
// test-only knobs: in-process worker sims answer for the platform, and the
// suite has no way to route <project>.vercel.app and friends to them.
// Production leaves them unset and every relay keeps its real platform
// origin. They never appear in logs.
const (
	VercelURLBaseEnv     = "VERCEL_URL_BASE"
	CloudflareURLBaseEnv = "CLOUDFLARE_URL_BASE"
	DenoURLBaseEnv       = "DENO_URL_BASE"
)

// probeTimeout bounds one version probe; a slower relay is unreachable, not
// mismatched.
const probeTimeout = 5 * time.Second

// ProbeAnswer is what answered a version probe. A relay worker answers with
// its deployed version — Version non-empty, Status 200. Any other HTTP
// answer (a platform pause page, a deleted deployment, a stranger's app on
// the URL) is NOT a relay worker: Version empty, plus the status and the
// best-effort platform marker so /stats can show why.
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

// Suspended reports whether the answer carries a platform suspension
// signature — the quota-exhausted pages free tiers serve instead of the
// worker (Vercel: HTTP 402, x-vercel-error DEPLOYMENT_DISABLED). Only these
// classify as paused: any other non-worker answer is unreachable or missing
// and must be treated on its own merits, never as a suspension to wait out.
func (a ProbeAnswer) Suspended() bool {
	if a.Marker != "" && strings.Contains(a.Marker, "DEPLOYMENT_DISABLED") {
		return true
	}
	return a.Status == http.StatusPaymentRequired
}

// Probe asks a live relay for its deployed version via GET
// <base>/__relay/version. Only a transport failure — timeout, TLS, refused
// connection — is an error; every HTTP answer comes back classified, because
// the probe is the one place that may conclude "the platform answered but
// our worker is not behind this URL" as opposed to "the relay could not be
// reached at all". A suspension must wait for revival; an unreachable relay
// must never be redeployed on a hunch either — the distinction between the
// two is this function's job, not the caller's.
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
// the worker's unauthenticated version route, which answers a deterministic
// JSON body — a controlled upstream the relay itself serves, so the probe
// never depends on random Internet weather.
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
// X-Relay-Path, and the relay key in X-Relay-Token. The worker's normal
// ingress runs — relay-key check included — and then performs a genuine
// forwarding fetch back into its own version route, so a passing probe is
// evidence that ingress, authentication and the forwarding fetch all work,
// not merely that the URL answers. The inner request hits the version route
// ahead of the key check, which the worker answers unauthenticated — exactly
// what makes the loop close deterministically: the controlled upstream is
// the relay itself, so readiness never depends on a third-party site being
// up. (The documented limitation: a probe proves the relay forwards, not
// that any particular upstream target is reachable from it.)
//
// A transport failure is an error; every HTTP answer comes back for the
// caller to classify: 404 is the worker's wrong-key answer (an
// unauthenticated request must be indistinguishable from an empty worker),
// 502 its upstream-fetch-failed answer, and a 200 with a JSON object body a
// completed round trip.
func ForwardProbe(ctx context.Context, client *http.Client, base, key string) (ForwardAnswer, error) {
	u, err := url.Parse(base)
	if err != nil {
		return ForwardAnswer{}, fmt.Errorf("forward probe URL: %w", err)
	}
	if u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return ForwardAnswer{}, fmt.Errorf("forward probe URL %q: missing host or scheme", base)
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
	if key != "" {
		req.Header.Set("X-Relay-Token", key)
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
