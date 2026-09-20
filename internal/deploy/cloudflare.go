package deploy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Cloudflare Workers client. One script per relay on the account, uploaded
// as an ES module with the version and relay key riding as plain_text
// bindings. The stable URL is the script's workers.dev subdomain host;
// enabling it is part of every deploy.
//
// Scope: the account id the script lives under. A pinned Account is used
// verbatim; an unpinned one is resolved from the credential, which succeeds
// only when the token sees exactly one account — with several visible,
// discovery returns ErrAmbiguousScope rather than silently picking.
const (
	cloudflareDefaultBase = "https://api.cloudflare.com/client/v4"
	cloudflareLiveTimeout = 30 * time.Second
	cloudflareWorkerFile  = "relay.js"
	// The date the worker binds to: pins the runtime's breaking-change
	// window. Bump deliberately, with a RelayVersion bump.
	cloudflareCompatDate = "2026-01-01"
)

type cloudflareClient struct {
	base string
	cred Credential
	http *http.Client
}

// newCloudflareClient builds the client; base empty selects the real API.
func newCloudflareClient(base string, cred Credential, hc *http.Client) *cloudflareClient {
	if base == "" {
		base = cloudflareDefaultBase
	}
	return &cloudflareClient{base: strings.TrimRight(base, "/"), cred: cred, http: hc}
}

func (c *cloudflareClient) Platform() string { return PlatformCloudflare }

// cloudflareStableURL is the script's workers.dev host. The test-only
// CLOUDFLARE_URL_BASE override replaces the origin so the e2e suite can
// answer for the platform.
func cloudflareStableURL(project, subdomain string) string {
	if base := strings.TrimRight(os.Getenv(CloudflareURLBaseEnv), "/"); base != "" {
		return base + "/" + project
	}
	return "https://" + project + "." + subdomain + ".workers.dev"
}

// cloudflareAnswer is the envelope every Cloudflare API answer wraps.
type cloudflareAnswer struct {
	Success bool            `json:"success"`
	Result  json.RawMessage `json:"result"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

func (a cloudflareAnswer) err() error {
	if a.Success {
		return nil
	}
	msg := "unknown error"
	if len(a.Errors) > 0 && a.Errors[0].Message != "" {
		msg = a.Errors[0].Message
	}
	// Deliberately a plain error: credential rejections arrive as real
	// HTTP 401/403 statuses (caught by credentialsRejected on the
	// platformError), while envelope errors inside a 200 are semantic.
	return fmt.Errorf("cloudflare: %s", msg)
}

// resolveAccount returns the account id the relay's script lives under: the
// config pin when present, otherwise the credential's own scope — which
// must be exactly one account.
func (c *cloudflareClient) resolveAccount(ctx context.Context) (string, error) {
	if c.cred.Account != "" {
		return c.cred.Account, nil
	}
	var answer struct {
		cloudflareAnswer
		Result []struct {
			ID string `json:"id"`
		} `json:"result"`
	}
	err := platformCall(ctx, c.http, http.MethodGet, c.base+"/accounts", c.cred.Token, "", nil, &answer)
	if err != nil {
		if credentialsRejected(err) {
			return "", fmt.Errorf("cloudflare resolve account: %w", ErrCredentials)
		}
		return "", fmt.Errorf("cloudflare resolve account: %w", err)
	}
	if err := answer.err(); err != nil {
		return "", fmt.Errorf("cloudflare resolve account: %w", err)
	}
	switch len(answer.Result) {
	case 1:
		return answer.Result[0].ID, nil
	case 0:
		return "", fmt.Errorf("cloudflare resolve account: %w (token sees none)", ErrCredentials)
	default:
		return "", fmt.Errorf("cloudflare resolve account: %w (%d accounts; set account: in the relay config)",
			ErrAmbiguousScope, len(answer.Result))
	}
}

// Discover resolves the script's existence and stable URL. An unpinned
// account resolves through the credential; ErrAmbiguousScope is a hard
// stop, never a guess.
func (c *cloudflareClient) Discover(ctx context.Context, project string) (Discovery, error) {
	account, err := c.resolveAccount(ctx)
	if err != nil {
		return Discovery{}, err
	}
	var answer cloudflareAnswer
	err = platformCall(ctx, c.http, http.MethodGet,
		c.base+"/accounts/"+url.PathEscape(account)+"/workers/scripts/"+url.PathEscape(project),
		c.cred.Token, "", nil, &answer)
	switch {
	case err == nil:
	case notFound(err):
		return Discovery{Exists: false}, nil
	case credentialsRejected(err):
		return Discovery{}, fmt.Errorf("cloudflare discover: %w", ErrCredentials)
	default:
		return Discovery{}, fmt.Errorf("cloudflare discover: %w", err)
	}
	if !answer.Success {
		// Cloudflare reports a missing script as success=false with a
		// not-found error code inside a 200 — normalize before classifying.
		if isCloudflareNotFound(answer) {
			return Discovery{Exists: false}, nil
		}
		return Discovery{}, fmt.Errorf("cloudflare discover: %w", answer.err())
	}
	subdomain, err := c.workersDevSubdomain(ctx, account)
	if err != nil {
		return Discovery{}, err
	}
	return Discovery{Exists: true, URL: cloudflareStableURL(project, subdomain)}, nil
}

// isCloudflareNotFound reports the envelope form of "no such script".
func isCloudflareNotFound(a cloudflareAnswer) bool {
	for _, e := range a.Errors {
		// 7003: no route for the request (missing script/path).
		if e.Code == 7003 {
			return true
		}
	}
	return false
}

// Deploy uploads the script, enables its workers.dev subdomain, resolves
// that subdomain, and waits until the worker answers its version endpoint.
func (c *cloudflareClient) Deploy(ctx context.Context, spec Spec) (Result, error) {
	account, err := c.resolveAccount(ctx)
	if err != nil {
		return Result{}, err
	}
	metadata, err := json.Marshal(map[string]any{
		"main_module":        cloudflareWorkerFile,
		"compatibility_date": cloudflareCompatDate,
		"bindings": []map[string]string{
			{"type": "plain_text", "name": "RELAY_VERSION", "text": spec.Version},
			{"type": "plain_text", "name": "RELAY_AUTH_TOKEN", "text": spec.Token},
		},
	})
	if err != nil {
		return Result{}, err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	metaPart, err := mw.CreateFormField("metadata")
	if err != nil {
		return Result{}, err
	}
	if _, err := metaPart.Write(metadata); err != nil {
		return Result{}, err
	}
	modulePart, err := mw.CreateFormFile(cloudflareWorkerFile, cloudflareWorkerFile)
	if err != nil {
		return Result{}, err
	}
	if _, err := modulePart.Write([]byte(spec.Source)); err != nil {
		return Result{}, err
	}
	if err := mw.Close(); err != nil {
		return Result{}, err
	}

	script := url.PathEscape(spec.Project)
	var answer cloudflareAnswer
	err = platformCall(ctx, c.http, http.MethodPut,
		c.base+"/accounts/"+url.PathEscape(account)+"/workers/scripts/"+script,
		c.cred.Token, mw.FormDataContentType(), &buf, &answer)
	if err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: %w", err)
	}
	if err := answer.err(); err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: %w", err)
	}

	// The workers.dev route answers only when enabled; re-enabling an
	// enabled route is a no-op.
	var enableAnswer cloudflareAnswer
	err = platformCall(ctx, c.http, http.MethodPost,
		c.base+"/accounts/"+url.PathEscape(account)+"/workers/scripts/"+script+"/subdomain",
		c.cred.Token, "application/json", strings.NewReader(`{"enabled":true}`), &enableAnswer)
	if err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: enable subdomain: %w", err)
	}
	if err := enableAnswer.err(); err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: enable subdomain: %w", err)
	}

	subdomain, err := c.workersDevSubdomain(ctx, account)
	if err != nil {
		return Result{}, err
	}
	result := Result{
		Project:    spec.Project,
		ExternalID: spec.Project,
		URL:        cloudflareStableURL(spec.Project, subdomain),
	}
	if err := awaitLive(ctx, result.URL, spec.Version, cloudflareLiveTimeout); err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: %w", err)
	}
	return result, nil
}

// workersDevSubdomain resolves the account's workers.dev subdomain — it is
// per account and must be fetched, never guessed. The account arrives
// resolved: both callers resolved it for their first platform call already,
// and resolving twice would double the /accounts traffic per operation.
func (c *cloudflareClient) workersDevSubdomain(ctx context.Context, account string) (string, error) {
	var answer struct {
		cloudflareAnswer
		Result struct {
			Subdomain string `json:"subdomain"`
		} `json:"result"`
	}
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/accounts/"+url.PathEscape(account)+"/workers/subdomain",
		c.cred.Token, "", nil, &answer)
	if err != nil {
		return "", fmt.Errorf("cloudflare deploy: read subdomain: %w", err)
	}
	if err := answer.err(); err != nil {
		return "", fmt.Errorf("cloudflare deploy: read subdomain: %w", err)
	}
	if answer.Result.Subdomain == "" {
		return "", fmt.Errorf("cloudflare deploy: account has no workers.dev subdomain")
	}
	return answer.Result.Subdomain, nil
}

// Delete removes the script; a missing script is already the desired end
// state.
func (c *cloudflareClient) Delete(ctx context.Context, project string) error {
	account, err := c.resolveAccount(ctx)
	if err != nil {
		return err
	}
	err = platformCall(ctx, c.http, http.MethodDelete,
		c.base+"/accounts/"+url.PathEscape(account)+"/workers/scripts/"+url.PathEscape(project),
		c.cred.Token, "", nil, nil)
	switch {
	case err == nil, notFound(err):
		return nil
	case credentialsRejected(err):
		return fmt.Errorf("cloudflare delete: %w", ErrCredentials)
	default:
		return fmt.Errorf("cloudflare delete: %w", err)
	}
}
