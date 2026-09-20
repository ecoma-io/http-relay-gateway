package deploy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Vercel client. One project per relay, deployed from one inline file
// (api/relay.js) with the version and relay key riding as deployment env.
// The stable URL is the project's default production domain
// (<project>.vercel.app) — derived deterministically, identical across
// redeploys, and never the per-deployment URL, which changes on every push.
//
// Scope: without a Team pin the credential's own user scope is used; with a
// Team pin every call carries teamId. Discovery and deploy always resolve
// the same scope, so (provider, name) maps to exactly one project.
const (
	vercelDefaultBase  = "https://api.vercel.com"
	vercelReadyTimeout = 2 * time.Minute
	vercelLiveTimeout  = 30 * time.Second
	vercelWorkerPath   = "api/relay.js"
	vercelStatusPoll   = time.Second
)

type vercelClient struct {
	base string
	cred Credential
	http *http.Client
}

// newVercelClient builds the client; base empty selects the real API.
func newVercelClient(base string, cred Credential, hc *http.Client) *vercelClient {
	if base == "" {
		base = vercelDefaultBase
	}
	return &vercelClient{base: strings.TrimRight(base, "/"), cred: cred, http: hc}
}

func (c *vercelClient) Platform() string { return PlatformVercel }

// stableURL is the project's default production domain. Vercel serves every
// project at <name>.vercel.app unless the operator deleted that domain; the
// slug charset (lowercase letters, digits, dashes) needs no domain-encoding.
func vercelStableURL(project string) string {
	return "https://" + project + ".vercel.app"
}

// teamQuery appends the team scope when pinned.
func (c *vercelClient) teamQuery() string {
	if c.cred.Team == "" {
		return ""
	}
	return "?teamId=" + url.QueryEscape(c.cred.Team)
}

// Discover resolves the project's existence and stable URL in the
// credential's scope.
func (c *vercelClient) Discover(ctx context.Context, project string) (Discovery, error) {
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/v9/projects/"+url.PathEscape(project)+c.teamQuery(), c.cred.Token, "", nil, nil)
	switch {
	case err == nil:
		return Discovery{Exists: true, URL: vercelStableURL(project)}, nil
	case notFound(err):
		return Discovery{Exists: false}, nil
	case credentialsRejected(err):
		return Discovery{}, fmt.Errorf("vercel discover: %w", ErrCredentials)
	default:
		return Discovery{}, fmt.Errorf("vercel discover: %w", err)
	}
}

type vercelFile struct {
	File     string `json:"file"`
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
}

type vercelDeployment struct {
	ID         string `json:"id"`
	ReadyState string `json:"readyState"`
}

// Deploy uploads the worker as a production deployment of the project (the
// project is created on first deploy), waits for READY, then waits until the
// new worker actually answers its version endpoint.
func (c *vercelClient) Deploy(ctx context.Context, spec Spec) (Result, error) {
	payload := map[string]any{
		"name":   spec.Project,
		"target": "production",
		"files": []vercelFile{{
			File:     vercelWorkerPath,
			Data:     base64.StdEncoding.EncodeToString([]byte(spec.Source)),
			Encoding: "base64",
		}},
		// No framework: the worker is a bare edge function, not an app.
		"projectSettings": map[string]any{"framework": nil},
		// A public relay must never sit behind the SSO wall.
		"deploymentProtection": map[string]any{"ssoProtection": nil},
		"env": []map[string]any{
			{"key": "RELAY_VERSION", "value": spec.Version, "target": []string{"production"}},
			{"key": "RELAY_AUTH_TOKEN", "value": spec.Token, "target": []string{"production"}},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Result{}, err
	}
	var created vercelDeployment
	err = platformCall(ctx, c.http, http.MethodPost, c.base+"/v13/deployments"+c.teamQuery(), c.cred.Token,
		"application/json", strings.NewReader(string(body)), &created)
	if err != nil {
		return Result{}, fmt.Errorf("vercel deploy: %w", err)
	}
	if created.ID == "" {
		return Result{}, fmt.Errorf("vercel deploy: answer without deployment id")
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, vercelReadyTimeout)
	defer cancel()
	ticker := time.NewTicker(vercelStatusPoll)
	defer ticker.Stop()
	for created.ReadyState != "READY" {
		switch created.ReadyState {
		case "ERROR", "CANCELED":
			return Result{}, fmt.Errorf("vercel deploy: deployment %s", strings.ToLower(created.ReadyState))
		}
		select {
		case <-deadlineCtx.Done():
			return Result{}, fmt.Errorf("vercel deploy: not ready after %s (state %s)",
				vercelReadyTimeout, created.ReadyState)
		case <-ticker.C:
		}
		var poll vercelDeployment
		err := platformCall(deadlineCtx, c.http, http.MethodGet,
			c.base+"/v13/deployments/"+url.PathEscape(created.ID)+c.teamQuery(), c.cred.Token, "", nil, &poll)
		if err != nil {
			return Result{}, fmt.Errorf("vercel deploy: poll: %w", err)
		}
		created.ReadyState = poll.ReadyState
	}
	result := Result{
		Project:    spec.Project,
		ExternalID: created.ID,
		URL:        vercelStableURL(spec.Project),
	}
	if err := awaitLive(ctx, result.URL, spec.Version, vercelLiveTimeout); err != nil {
		return Result{}, fmt.Errorf("vercel deploy: %w", err)
	}
	return result, nil
}

// Delete removes the project; a missing project is already the desired end
// state.
func (c *vercelClient) Delete(ctx context.Context, project string) error {
	err := platformCall(ctx, c.http, http.MethodDelete,
		c.base+"/v9/projects/"+url.PathEscape(project)+c.teamQuery(), c.cred.Token, "", nil, nil)
	switch {
	case err == nil, notFound(err):
		return nil
	case credentialsRejected(err):
		return fmt.Errorf("vercel delete: %w", ErrCredentials)
	default:
		return fmt.Errorf("vercel delete: %w", err)
	}
}
