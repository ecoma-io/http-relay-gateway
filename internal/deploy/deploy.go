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

// ProbeVersion asks a live relay for its deployed version via
// GET <base>/__relay/version. Any transport failure — timeout, TLS, 5xx,
// unparseable body — is an error; callers treat that as unreachable and must
// never conclude version mismatch from it.
func ProbeVersion(ctx context.Context, client *http.Client, base string) (string, error) {
	u, err := url.JoinPath(base, "__relay/version")
	if err != nil {
		return "", fmt.Errorf("probe URL: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
	if err != nil {
		return "", fmt.Errorf("probe request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("probe: status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("probe: read answer: %w", err)
	}
	var parsed struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed.Version == "" {
		return "", fmt.Errorf("probe: unreadable version answer")
	}
	return parsed.Version, nil
}
