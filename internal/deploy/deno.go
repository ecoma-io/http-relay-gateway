package deploy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Deno Deploy client, written against the published API
// (api.deno.com/v1/openapi.json): projects live under
// /organizations/{organizationId}/projects (GET list / POST create) and are
// addressed by UUID everywhere else; deployments are
// POST /projects/{projectId}/deployments with a JSON body (assets inline,
// envVars), polled at GET /deployments/{deploymentId}.
//
// Scope: the API has no route that resolves the organization from a token —
// /organizations/{organizationId} is by-id only — so the credential must pin
// the organization (Credential.Organization). Within that scope project names
// are unique, discovery matches the exact ProjectName slug, and (provider,
// name) maps to exactly one project: the gateway never touches a project it
// did not find under its own name in its own organization.
const (
	denoDefaultBase = "https://api.deno.com"
	denoLiveTimeout = 90 * time.Second
	denoStatusPoll  = time.Second
	denoWorkerFile  = "relay.js"
	// denoStableDomain is the wildcard deno.dev template from the spec's
	// AttachableDomain: the project's stable URL across deployments, as
	// opposed to the per-deployment default {name}-{id}.deno.dev.
	denoStableDomain = "{project.name}.deno.dev"
	// The org project list is paginated (page/limit, limit ≤ 100). The walk
	// requests full pages until one comes back short; denoListPageMax bounds
	// the walk against a pathological server that never runs out of pages.
	denoListLimit   = 100
	denoListPageMax = 100
)

// errDenoOrganization reports a deno credential without its organization pin.
// It wraps ErrAmbiguousScope so the reconciler classifies it as operator
// configuration data (never healed by a redeploy).
var errDenoOrganization = fmt.Errorf(
	"deno: organization is required — the Deploy API addresses projects by "+
		"organization id and offers no route to resolve it from the token; "+
		"pin organization on the deno relay: %w", ErrAmbiguousScope)

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

// The wire shapes, exactly as the published schema defines them. Every
// request schema has additionalProperties: false — no field may be sent that
// the spec does not name, and none of its optional fields is set unless the
// client means it.

// denoProject is the spec's Project: identity only — the serving domain is
// not part of the project resource.
type denoProject struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// denoCreateProject is the spec's CreateProjectRequest with the name the
// relay slug deterministic; description stays unset (null → empty string).
type denoCreateProject struct {
	Name string `json:"name"`
}

// denoAsset is the spec's Asset with the File variant: inline content.
type denoAsset struct {
	Kind     string `json:"kind"` // "file"
	Content  string `json:"content"`
	Encoding string `json:"encoding"` // "utf-8" | "base64"
}

// denoCreateDeployment is the spec's CreateDeploymentRequest: the three
// required fields (entryPointUrl, assets, envVars) plus the stable-domain
// attachment, without which the deployment would only answer on its
// per-deployment {name}-{id}.deno.dev URL.
type denoCreateDeployment struct {
	EntryPointUrl string               `json:"entryPointUrl"`
	Assets        map[string]denoAsset `json:"assets"`
	EnvVars       map[string]string    `json:"envVars"`
	Domains       []string             `json:"domains"`
}

// denoDeployment is the spec's Deployment as far as the client reads it:
// identity plus the status enum (failed | pending | success).
type denoDeployment struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// Discover resolves the project in the pinned organization by walking the
// org's project list for an exact name match.
func (c *denoClient) Discover(ctx context.Context, project string) (Discovery, error) {
	if c.cred.Organization == "" {
		return Discovery{}, fmt.Errorf("deno discover: %w", errDenoOrganization)
	}
	found, err := c.findProject(ctx, project)
	if err != nil {
		switch {
		case credentialsRejected(err):
			return Discovery{}, fmt.Errorf("deno discover: %w", ErrCredentials)
		default:
			return Discovery{}, fmt.Errorf("deno discover: %w", err)
		}
	}
	if found == nil {
		return Discovery{Exists: false}, nil
	}
	return Discovery{Exists: true, URL: denoStableURL(found.Name)}, nil
}

// findProject walks the organization's project list for an exact name match,
// returning nil when the org holds no such project. The walk is the scope
// pin: only a project found under the relay's own slug in the pinned
// organization is ever addressed again.
func (c *denoClient) findProject(ctx context.Context, name string) (*denoProject, error) {
	for page := 1; page <= denoListPageMax; page++ {
		listURL := fmt.Sprintf("%s/v1/organizations/%s/projects?page=%d&limit=%d",
			c.base, url.PathEscape(c.cred.Organization), page, denoListLimit)
		var projects []denoProject
		err := platformCall(ctx, c.http, http.MethodGet, listURL, c.cred.Token, "", nil, &projects)
		if err != nil {
			return nil, err
		}
		for i := range projects {
			if projects[i].Name == name {
				return &projects[i], nil
			}
		}
		if len(projects) < denoListLimit {
			return nil, nil // short page: the list is exhausted
		}
	}
	return nil, fmt.Errorf("project list did not settle within %d pages", denoListPageMax)
}

// createProject creates the project in the pinned organization and returns
// the platform's view of it (the UUID every later call addresses).
func (c *denoClient) createProject(ctx context.Context, name string) (denoProject, error) {
	body, err := json.Marshal(denoCreateProject{Name: name})
	if err != nil {
		return denoProject{}, err
	}
	var created denoProject
	err = platformCall(ctx, c.http, http.MethodPost,
		c.base+"/v1/organizations/"+url.PathEscape(c.cred.Organization)+"/projects",
		c.cred.Token, "application/json", bytes.NewReader(body), &created)
	if err != nil {
		return denoProject{}, err
	}
	if created.ID == "" {
		return denoProject{}, errors.New("answer without project id")
	}
	return created, nil
}

// Deploy ensures the project exists in the pinned organization, creates a
// deployment of the worker with the stable {name}.deno.dev domain attached,
// waits for the platform's terminal status, then waits until the new worker
// answers its version endpoint.
func (c *denoClient) Deploy(ctx context.Context, spec Spec) (Result, error) {
	if c.cred.Organization == "" {
		return Result{}, fmt.Errorf("deno deploy: %w", errDenoOrganization)
	}
	project, err := c.findProject(ctx, spec.Project)
	if err != nil {
		if credentialsRejected(err) {
			return Result{}, fmt.Errorf("deno deploy: %w", ErrCredentials)
		}
		return Result{}, fmt.Errorf("deno deploy: resolve project: %w", err)
	}
	if project == nil {
		created, err := c.createProject(ctx, spec.Project)
		if err != nil {
			if credentialsRejected(err) {
				return Result{}, fmt.Errorf("deno deploy: create project: %w", ErrCredentials)
			}
			return Result{}, fmt.Errorf("deno deploy: create project: %w", err)
		}
		project = &created
	}

	// The single-module worker ships as one base64 asset; version and relay
	// key ride as deployment envVars, never inside the source.
	request := denoCreateDeployment{
		EntryPointUrl: denoWorkerFile,
		Assets: map[string]denoAsset{
			denoWorkerFile: {
				Kind:     "file",
				Content:  base64.StdEncoding.EncodeToString([]byte(spec.Source)),
				Encoding: "base64",
			},
		},
		EnvVars: map[string]string{
			"RELAY_VERSION":    spec.Version,
			"RELAY_AUTH_TOKEN": spec.Token,
		},
		Domains: []string{denoStableDomain},
	}
	body, err := json.Marshal(request)
	if err != nil {
		return Result{}, err
	}
	var created denoDeployment
	err = platformCall(ctx, c.http, http.MethodPost,
		c.base+"/v1/projects/"+url.PathEscape(project.ID)+"/deployments",
		c.cred.Token, "application/json", bytes.NewReader(body), &created)
	if err != nil {
		return Result{}, fmt.Errorf("deno deploy: %w", err)
	}
	if created.ID == "" {
		return Result{}, errors.New("deno deploy: answer without deployment id")
	}

	if err := c.awaitDeployment(ctx, created); err != nil {
		return Result{}, fmt.Errorf("deno deploy: %w", err)
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

// awaitDeployment polls GET /deployments/{deploymentId} until the spec's
// terminal statuses: "success" returns, "failed" errors, anything else keeps
// polling inside the live-timeout budget (the enum currently has exactly
// failed | pending | success).
func (c *denoClient) awaitDeployment(ctx context.Context, created denoDeployment) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, denoLiveTimeout)
	defer cancel()
	ticker := time.NewTicker(denoStatusPoll)
	defer ticker.Stop()
	current := created
	for {
		switch current.Status {
		case "success":
			return nil
		case "failed":
			return fmt.Errorf("deployment %s failed", current.ID)
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("not ready after %s (state %s)", denoLiveTimeout, current.Status)
		case <-ticker.C:
		}
		var poll denoDeployment
		err := platformCall(deadlineCtx, c.http, http.MethodGet,
			c.base+"/v1/deployments/"+url.PathEscape(current.ID), c.cred.Token, "", nil, &poll)
		if err != nil {
			return fmt.Errorf("poll: %w", err)
		}
		if poll.ID == "" {
			poll.ID = current.ID
		}
		current = poll
	}
}

// Delete removes the project by UUID (and with it every deployment). The
// project is resolved through the same organization-scoped list walk as
// discovery: a name absent from the pinned organization means the remote is
// already gone — success — and only a 404 on the by-id delete is the same
// verdict; the gateway never deletes by name.
func (c *denoClient) Delete(ctx context.Context, project string) error {
	if c.cred.Organization == "" {
		return fmt.Errorf("deno delete: %w", errDenoOrganization)
	}
	found, err := c.findProject(ctx, project)
	if err != nil {
		switch {
		case credentialsRejected(err):
			return fmt.Errorf("deno delete: %w", ErrCredentials)
		default:
			return fmt.Errorf("deno delete: %w", err)
		}
	}
	if found == nil {
		return nil // not in the pinned organization: already the desired end state
	}
	err = platformCall(ctx, c.http, http.MethodDelete,
		c.base+"/v1/projects/"+url.PathEscape(found.ID), c.cred.Token, "", nil, nil)
	switch {
	case err == nil, notFound(err):
		return nil
	case credentialsRejected(err):
		return fmt.Errorf("deno delete: %w", ErrCredentials)
	default:
		return fmt.Errorf("deno delete: %w", err)
	}
}
