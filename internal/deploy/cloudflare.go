package deploy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Cloudflare Workers client. One script per relay on the account, uploaded
// as an ES module with the version and token riding as plain_text bindings.
// The stable URL is the script's workers.dev subdomain host; enabling it is
// part of every deploy.
const (
	cloudflareDefaultBase = "https://api.cloudflare.com/client/v4"
	cloudflareLiveTimeout = 30 * time.Second
	cloudflareWorkerFile  = "relay.js"
	// The date the worker binds to: pins the runtime's breaking-change
	// window. Bump deliberately, with a RelayVersion bump.
	cloudflareCompatDate = "2026-01-01"
)

type cloudflareClient struct {
	base       string
	token      string
	accountRef string
	http       *http.Client
}

// newCloudflareClient builds the client; base empty selects the real API.
// accountRef is the Cloudflare account id the script belongs to.
func newCloudflareClient(base, token, accountRef string, hc *http.Client) *cloudflareClient {
	if base == "" {
		base = cloudflareDefaultBase
	}
	return &cloudflareClient{
		base:       strings.TrimRight(base, "/"),
		token:      token,
		accountRef: accountRef,
		http:       hc,
	}
}

func (c *cloudflareClient) Platform() string { return PlatformCloudflare }

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
	return fmt.Errorf("cloudflare: %s", msg)
}

// Verify checks the token against the account: the token must be valid and
// allowed to read the account the relay will deploy to.
func (c *cloudflareClient) Verify(ctx context.Context) (string, error) {
	if c.accountRef == "" {
		return "", fmt.Errorf("cloudflare verify: account id is required")
	}
	var answer cloudflareAnswer
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/accounts/"+url.PathEscape(c.accountRef), c.token, "", nil, &answer)
	if err != nil {
		if strings.Contains(err.Error(), "status 403") || strings.Contains(err.Error(), "status 401") {
			return "", fmt.Errorf("cloudflare verify: %w", ErrCredentials)
		}
		return "", fmt.Errorf("cloudflare verify: %w", err)
	}
	if err := answer.err(); err != nil {
		return "", fmt.Errorf("cloudflare verify: %w", err)
	}
	return c.accountRef, nil
}

// Deploy uploads the script, enables its workers.dev subdomain, resolves
// that subdomain, and waits until the worker answers its version endpoint.
func (c *cloudflareClient) Deploy(ctx context.Context, spec Spec) (Result, error) {
	if c.accountRef == "" {
		return Result{}, fmt.Errorf("cloudflare deploy: account id is required")
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
		c.base+"/accounts/"+url.PathEscape(c.accountRef)+"/workers/scripts/"+script,
		c.token, mw.FormDataContentType(), &buf, &answer)
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
		c.base+"/accounts/"+url.PathEscape(c.accountRef)+"/workers/scripts/"+script+"/subdomain",
		c.token, "application/json", strings.NewReader(`{"enabled":true}`), &enableAnswer)
	if err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: enable subdomain: %w", err)
	}
	if err := enableAnswer.err(); err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: enable subdomain: %w", err)
	}

	subdomain, err := c.workersDevSubdomain(ctx)
	if err != nil {
		return Result{}, err
	}
	result := Result{
		Project:    spec.Project,
		ExternalID: spec.Project,
		URL:        fmt.Sprintf("https://%s.%s.workers.dev", spec.Project, subdomain),
	}
	if err := awaitLive(ctx, c.http, result.URL, spec.Version, cloudflareLiveTimeout); err != nil {
		return Result{}, fmt.Errorf("cloudflare deploy: %w", err)
	}
	return result, nil
}

// workersDevSubdomain resolves the account's workers.dev subdomain — it is
// per account and must be fetched, never guessed.
func (c *cloudflareClient) workersDevSubdomain(ctx context.Context) (string, error) {
	var answer struct {
		cloudflareAnswer
		Result struct {
			Subdomain string `json:"subdomain"`
		} `json:"result"`
	}
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/accounts/"+url.PathEscape(c.accountRef)+"/workers/subdomain",
		c.token, "", nil, &answer)
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
	err := platformCall(ctx, c.http, http.MethodDelete,
		c.base+"/accounts/"+url.PathEscape(c.accountRef)+"/workers/scripts/"+url.PathEscape(project),
		c.token, "", nil, nil)
	if err != nil && strings.Contains(err.Error(), "status 404") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cloudflare delete: %w", err)
	}
	return nil
}
