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

// Deno Deploy client. One project per relay; each deploy is a new
// deployment of the single-module worker with the version and token riding
// as deployment envVars. The stable URL is the project's *.deno.dev domain.
const (
	denoDefaultBase = "https://api.deno.com"
	denoLiveTimeout = 90 * time.Second
	denoStatusPoll  = time.Second
	denoWorkerFile  = "relay.js"
)

type denoClient struct {
	base       string
	token      string
	accountRef string
	http       *http.Client
}

// newDenoClient builds the client; base empty selects the real API.
func newDenoClient(base, token, accountRef string, hc *http.Client) *denoClient {
	if base == "" {
		base = denoDefaultBase
	}
	return &denoClient{
		base:       strings.TrimRight(base, "/"),
		token:      token,
		accountRef: accountRef,
		http:       hc,
	}
}

func (c *denoClient) Platform() string { return PlatformDeno }

// Verify checks the token against the platform's user endpoint. The stored
// account reference stays whatever the operator supplied (or the platform's
// user id when nothing was supplied).
func (c *denoClient) Verify(ctx context.Context) (string, error) {
	var user struct {
		ID string `json:"id"`
	}
	err := platformCall(ctx, c.http, http.MethodGet, c.base+"/v1/user", c.token, "", nil, &user)
	if err != nil {
		if strings.Contains(err.Error(), "status 401") || strings.Contains(err.Error(), "status 403") {
			return "", fmt.Errorf("deno verify: %w", ErrCredentials)
		}
		return "", fmt.Errorf("deno verify: %w", err)
	}
	if c.accountRef != "" {
		return c.accountRef, nil
	}
	if user.ID == "" {
		return "", fmt.Errorf("deno verify: answer without user id")
	}
	return user.ID, nil
}

// Deploy ensures the project exists, creates a production deployment of the
// worker, waits for the platform to report success, then waits until the new
// worker answers its version endpoint.
func (c *denoClient) Deploy(ctx context.Context, spec Spec) (Result, error) {
	if err := c.ensureProject(ctx, spec.Project); err != nil {
		return Result{}, err
	}

	meta, err := json.Marshal(map[string]any{
		"entryPointUrl": denoWorkerFile,
		"production":    true,
		"envVars": map[string]string{
			"RELAY_VERSION":    spec.Version,
			"RELAY_AUTH_TOKEN": spec.Token,
		},
	})
	if err != nil {
		return Result{}, err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	metaPart, err := mw.CreateFormField("meta")
	if err != nil {
		return Result{}, err
	}
	if _, err := metaPart.Write(meta); err != nil {
		return Result{}, err
	}
	filePart, err := mw.CreateFormFile("file", denoWorkerFile)
	if err != nil {
		return Result{}, err
	}
	if _, err := filePart.Write([]byte(spec.Source)); err != nil {
		return Result{}, err
	}
	if err := mw.Close(); err != nil {
		return Result{}, err
	}

	var created struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	err = platformCall(ctx, c.http, http.MethodPost,
		c.base+"/v1/projects/"+url.PathEscape(spec.Project)+"/deployments",
		c.token, mw.FormDataContentType(), &buf, &created)
	if err != nil {
		return Result{}, fmt.Errorf("deno deploy: %w", err)
	}
	if created.ID == "" {
		return Result{}, fmt.Errorf("deno deploy: answer without deployment id")
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, denoLiveTimeout)
	defer cancel()
	ticker := time.NewTicker(denoStatusPoll)
	defer ticker.Stop()
	for created.Status != "success" {
		switch created.Status {
		case "failed", "canceled":
			return Result{}, fmt.Errorf("deno deploy: deployment %s", created.Status)
		}
		select {
		case <-deadlineCtx.Done():
			return Result{}, fmt.Errorf("deno deploy: not ready after %s (state %s)",
				denoLiveTimeout, created.Status)
		case <-ticker.C:
		}
		var poll struct {
			Status string `json:"status"`
		}
		err := platformCall(deadlineCtx, c.http, http.MethodGet,
			c.base+"/v1/deployments/"+url.PathEscape(created.ID), c.token, "", nil, &poll)
		if err != nil {
			return Result{}, fmt.Errorf("deno deploy: poll: %w", err)
		}
		created.Status = poll.Status
	}
	result := Result{
		Project:    spec.Project,
		ExternalID: created.ID,
		URL:        "https://" + spec.Project + ".deno.dev",
	}
	if err := awaitLive(ctx, c.http, result.URL, spec.Version, denoLiveTimeout); err != nil {
		return Result{}, fmt.Errorf("deno deploy: %w", err)
	}
	return result, nil
}

// ensureProject creates the project when missing; an existing one is left
// untouched (a redeploy replaces its deployment, not the project).
func (c *denoClient) ensureProject(ctx context.Context, name string) error {
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/v1/projects/"+url.PathEscape(name), c.token, "", nil, nil)
	if err == nil {
		return nil
	}
	if !strings.Contains(err.Error(), "status 404") {
		return fmt.Errorf("deno deploy: read project: %w", err)
	}
	err = platformCall(ctx, c.http, http.MethodPost, c.base+"/v1/projects", c.token,
		"application/json", strings.NewReader(fmt.Sprintf(`{"name":%q}`, name)), nil)
	if err != nil {
		return fmt.Errorf("deno deploy: create project: %w", err)
	}
	return nil
}

// Delete removes the project (and with it every deployment); a missing
// project is already the desired end state.
func (c *denoClient) Delete(ctx context.Context, project string) error {
	err := platformCall(ctx, c.http, http.MethodDelete,
		c.base+"/v1/projects/"+url.PathEscape(project), c.token, "", nil, nil)
	if err != nil && strings.Contains(err.Error(), "status 404") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deno delete: %w", err)
	}
	return nil
}
