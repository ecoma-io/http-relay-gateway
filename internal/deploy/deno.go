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

// Deno Deploy client. One project per relay; each deploy is a new
// deployment of the single-module worker with the version and relay key
// riding as deployment envVars. The stable URL is the project's *.deno.dev
// domain.
//
// Scope: Deno Deploy projects live in the credential's user scope — the
// user the token belongs to. Project names are unique within that scope,
// and both discovery and deploy resolve the same one, so (provider, name)
// maps to exactly one project. Organization scopes are deliberately not
// supported: a token that must manage org projects needs explicit org
// support added here first, not a guess.
const (
	denoDefaultBase = "https://api.deno.com"
	denoLiveTimeout = 90 * time.Second
	denoStatusPoll  = time.Second
	denoWorkerFile  = "relay.js"
)

type denoClient struct {
	base string
	cred Credential
	http *http.Client
}

// newDenoClient builds the client; base empty selects the real API.
func newDenoClient(base string, cred Credential, hc *http.Client) *denoClient {
	if base == "" {
		base = denoDefaultBase
	}
	return &denoClient{base: strings.TrimRight(base, "/"), cred: cred, http: hc}
}

func (c *denoClient) Platform() string { return PlatformDeno }

// denoStableURL is the project's *.deno.dev domain. The test-only
// DENO_URL_BASE override replaces the origin so the e2e suite can answer
// for the platform.
func denoStableURL(project string) string {
	if base := strings.TrimRight(os.Getenv(DenoURLBaseEnv), "/"); base != "" {
		return base + "/" + project
	}
	return "https://" + project + ".deno.dev"
}

// Discover resolves the project's existence and stable URL in the
// credential's user scope.
func (c *denoClient) Discover(ctx context.Context, project string) (Discovery, error) {
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/v1/projects/"+url.PathEscape(project), c.cred.Token, "", nil, nil)
	switch {
	case err == nil:
		return Discovery{Exists: true, URL: denoStableURL(project)}, nil
	case notFound(err):
		return Discovery{Exists: false}, nil
	case credentialsRejected(err):
		return Discovery{}, fmt.Errorf("deno discover: %w", ErrCredentials)
	default:
		return Discovery{}, fmt.Errorf("deno discover: %w", err)
	}
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
		c.cred.Token, mw.FormDataContentType(), &buf, &created)
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
			c.base+"/v1/deployments/"+url.PathEscape(created.ID), c.cred.Token, "", nil, &poll)
		if err != nil {
			return Result{}, fmt.Errorf("deno deploy: poll: %w", err)
		}
		created.Status = poll.Status
	}
	result := Result{
		Project:    spec.Project,
		ExternalID: created.ID,
		URL:        denoStableURL(spec.Project),
	}
	if err := awaitLive(ctx, result.URL, spec.Version, denoLiveTimeout); err != nil {
		return Result{}, fmt.Errorf("deno deploy: %w", err)
	}
	return result, nil
}

// ensureProject creates the project when missing; an existing one is left
// untouched (a redeploy replaces its deployment, not the project).
func (c *denoClient) ensureProject(ctx context.Context, name string) error {
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/v1/projects/"+url.PathEscape(name), c.cred.Token, "", nil, nil)
	switch {
	case err == nil:
		return nil
	case notFound(err):
	case credentialsRejected(err):
		return fmt.Errorf("deno deploy: read project: %w", ErrCredentials)
	default:
		return fmt.Errorf("deno deploy: read project: %w", err)
	}
	err = platformCall(ctx, c.http, http.MethodPost, c.base+"/v1/projects", c.cred.Token,
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
		c.base+"/v1/projects/"+url.PathEscape(project), c.cred.Token, "", nil, nil)
	switch {
	case err == nil, notFound(err):
		return nil
	case credentialsRejected(err):
		return fmt.Errorf("deno delete: %w", ErrCredentials)
	default:
		return fmt.Errorf("deno delete: %w", err)
	}
}
