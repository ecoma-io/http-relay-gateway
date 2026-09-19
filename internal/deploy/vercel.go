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
// (api/relay.js) with the version and token riding as deployment env. The
// stable URL is the project's production alias — never the per-deployment
// URL, which changes on every redeploy.
const (
	vercelDefaultBase  = "https://api.vercel.com"
	vercelReadyTimeout = 2 * time.Minute
	vercelLiveTimeout  = 30 * time.Second
	vercelWorkerPath   = "api/relay.js"
	vercelStatusPoll   = time.Second
)

type vercelClient struct {
	base  string
	token string
	http  *http.Client
}

// newVercelClient builds the client; base empty selects the real API.
func newVercelClient(base, token, _ string, hc *http.Client) *vercelClient {
	if base == "" {
		base = vercelDefaultBase
	}
	return &vercelClient{base: strings.TrimRight(base, "/"), token: token, http: hc}
}

func (c *vercelClient) Platform() string { return PlatformVercel }

// Verify resolves the account's username from the token.
func (c *vercelClient) Verify(ctx context.Context) (string, error) {
	var answer struct {
		User struct {
			Username string `json:"username"`
		} `json:"user"`
	}
	err := platformCall(ctx, c.http, http.MethodGet, c.base+"/v2/user", c.token, "", nil, &answer)
	if err != nil {
		// 401/403 is the platform rejecting the token — client data, not a
		// gateway fault; the admin plane turns it into a field error instead
		// of reporting a platform outage.
		if strings.Contains(err.Error(), "status 401") || strings.Contains(err.Error(), "status 403") {
			return "", fmt.Errorf("vercel verify: %w", ErrCredentials)
		}
		return "", fmt.Errorf("vercel verify: %w", err)
	}
	if answer.User.Username == "" {
		return "", fmt.Errorf("vercel verify: answer without username")
	}
	return answer.User.Username, nil
}

type vercelFile struct {
	File     string `json:"file"`
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
}

type vercelDeployment struct {
	ID         string   `json:"id"`
	URL        string   `json:"url"`
	Alias      []string `json:"alias"`
	ReadyState string   `json:"readyState"`
}

// stableURL prefers the production alias — stable across redeploys — over
// the per-deployment URL the creation answer leads with.
func (d vercelDeployment) stableURL(project string) string {
	for _, alias := range d.Alias {
		if alias != "" {
			return "https://" + alias
		}
	}
	if d.URL != "" {
		return "https://" + d.URL
	}
	return "https://" + project + ".vercel.app"
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
	err = platformCall(ctx, c.http, http.MethodPost, c.base+"/v13/deployments", c.token,
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
			c.base+"/v13/deployments/"+url.PathEscape(created.ID), c.token, "", nil, &poll)
		if err != nil {
			return Result{}, fmt.Errorf("vercel deploy: poll: %w", err)
		}
		created.ReadyState = poll.ReadyState
	}
	result := Result{
		Project:    spec.Project,
		ExternalID: created.ID,
		URL:        created.stableURL(spec.Project),
	}
	if err := awaitLive(ctx, c.http, result.URL, spec.Version, vercelLiveTimeout); err != nil {
		return Result{}, fmt.Errorf("vercel deploy: %w", err)
	}
	return result, nil
}

// Delete removes the project; a missing project is already the desired end
// state.
func (c *vercelClient) Delete(ctx context.Context, project string) error {
	err := platformCall(ctx, c.http, http.MethodDelete,
		c.base+"/v9/projects/"+url.PathEscape(project), c.token, "", nil, nil)
	if err != nil && strings.Contains(err.Error(), "status 404") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("vercel delete: %w", err)
	}
	return nil
}
