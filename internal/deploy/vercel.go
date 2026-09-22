package deploy

import (
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

// Vercel client. One project per relay, deployed from two inline files
// (api/relay.js + vercel.json) with the version and relay key riding as
// project environment variables, upserted through the Project Env API
// before each deployment — POST /v13/deployments carries no env of its
// own. The stable URL is the project's default production domain
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
	vercelConfigPath   = "vercel.json"
	vercelStatusPoll   = time.Second
)

// vercelConfig routes every request to the worker. Files under api/ serve
// only at /api/* on Vercel, while every gateway probe and every relayed
// request targets the project root — without this rewrite the worker is
// unreachable (#21). The catch-all is Vercel's documented SPA fallback
// shape, and rewrites run only after the filesystem check, so /api/relay
// itself still resolves to the function and the rewrite never loops.
const vercelConfig = `{"rewrites":[{"source":"/(.*)","destination":"/api/relay"}]}`

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
// The test-only VERCEL_URL_BASE override replaces the origin so the e2e
// suite can answer for the platform.
func vercelStableURL(project string) string {
	if base := strings.TrimRight(os.Getenv(VercelURLBaseEnv), "/"); base != "" {
		return base + "/" + project
	}
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

// Deploy uploads the worker as a production deployment of the project,
// waits for READY, then waits until the new worker actually answers its
// version endpoint. The version and relay key reach the worker as project
// environment variables set before the deployment is created — the
// documented mechanism, since the v13 deployment request has no env fields
// of its own (#22).
func (c *vercelClient) Deploy(ctx context.Context, spec Spec) (Result, error) {
	if err := c.ensureEnv(ctx, spec.Project, spec.Version, spec.Token); err != nil {
		return Result{}, err
	}
	payload := map[string]any{
		"name":   spec.Project,
		"target": "production",
		"files": []vercelFile{
			{
				File:     vercelWorkerPath,
				Data:     base64.StdEncoding.EncodeToString([]byte(spec.Source)),
				Encoding: "base64",
			},
			{
				File:     vercelConfigPath,
				Data:     base64.StdEncoding.EncodeToString([]byte(vercelConfig)),
				Encoding: "base64",
			},
		},
		// No framework: the worker is a bare edge function, not an app.
		"projectSettings": map[string]any{"framework": nil},
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

// vercelEnvVar is one entry of a project's environment variable list. The
// value never appears in list answers (type encrypted answers redacted), and
// the gateway never asks for it back.
type vercelEnvVar struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// vercelEnvList is the answer of GET /v10/projects/{name}/env.
type vercelEnvList struct {
	Envs []vercelEnvVar `json:"envs"`
}

// ensureEnv makes RELAY_VERSION and RELAY_AUTH_TOKEN — the exact pair the
// worker's Vercel entry reads from process.env — carry the deployed values
// on the production target. A relay project owns exactly these two
// variables, so the list always fits one page. The values ride request
// bodies only and never appear in an error or a log.
func (c *vercelClient) ensureEnv(ctx context.Context, project, version, token string) error {
	for _, v := range []struct{ key, value string }{
		{"RELAY_VERSION", version},
		{"RELAY_AUTH_TOKEN", token},
	} {
		err := c.upsertEnv(ctx, project, v.key, v.value)
		switch {
		case err == nil:
		case notFound(err):
			// A 404 from the env list is a project that does not exist
			// yet. The first deployment would create it, but the
			// variables must be in place before that deployment runs,
			// so create the empty project and set the variable on it.
			if cerr := c.createProject(ctx, project); cerr != nil {
				return cerr
			}
			if err = c.upsertEnv(ctx, project, v.key, v.value); err != nil {
				return err
			}
		default:
			return err
		}
	}
	return nil
}

// upsertEnv sets one project environment variable to value on the production
// target — idempotent by key, so a redeploy with a rotated key or a new
// version lands the new value before the next deployment reads it.
func (c *vercelClient) upsertEnv(ctx context.Context, project, key, value string) error {
	ids, err := c.envVarIDs(ctx, project, key)
	if err != nil {
		return err
	}
	if len(ids) > 0 {
		return c.updateEnv(ctx, project, ids, key, value)
	}
	createErr := c.createEnv(ctx, project, key, value)
	if createErr == nil {
		return nil
	}
	if !conflictAnswer(createErr) {
		return createErr
	}
	// 403/409 on create is the documented already-exists answer: a variable
	// created between the list and this call. Adopt it and update instead.
	// When the re-list still finds nothing the answer was a permission
	// refusal, and the create error stands.
	ids, err = c.envVarIDs(ctx, project, key)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return createErr
	}
	return c.updateEnv(ctx, project, ids, key, value)
}

// envVarIDs returns the ids of the project's environment variables carrying
// key.
func (c *vercelClient) envVarIDs(ctx context.Context, project, key string) ([]string, error) {
	var list vercelEnvList
	err := platformCall(ctx, c.http, http.MethodGet,
		c.base+"/v10/projects/"+url.PathEscape(project)+"/env"+c.teamQuery(),
		c.cred.Token, "", nil, &list)
	if err != nil {
		return nil, fmt.Errorf("vercel env %s: %w", key, err)
	}
	var ids []string
	for _, e := range list.Envs {
		if e.Key == key && e.ID != "" {
			ids = append(ids, e.ID)
		}
	}
	return ids, nil
}

// createEnv creates one project environment variable: encrypted, scoped to
// the production target the deployments run on.
func (c *vercelClient) createEnv(ctx context.Context, project, key, value string) error {
	body, err := json.Marshal(vercelEnvBody{Key: key, Value: value, Type: "encrypted", Target: []string{"production"}})
	if err != nil {
		return err
	}
	var answer struct {
		Failed []struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"failed"`
	}
	u := c.base + "/v10/projects/" + url.PathEscape(project) + "/env" + c.teamQuery()
	if err := platformCall(ctx, c.http, http.MethodPost, u, c.cred.Token,
		"application/json", strings.NewReader(string(body)), &answer); err != nil {
		return fmt.Errorf("vercel env %s: %w", key, err)
	}
	if len(answer.Failed) > 0 {
		return fmt.Errorf("vercel env %s: %s", key, answer.Failed[0].Error.Message)
	}
	return nil
}

// updateEnv rewrites every existing entry of the key to the new value,
// pinned to the production target.
func (c *vercelClient) updateEnv(ctx context.Context, project string, ids []string, key, value string) error {
	body, err := json.Marshal(vercelEnvBody{Value: value, Type: "encrypted", Target: []string{"production"}})
	if err != nil {
		return err
	}
	for _, id := range ids {
		u := c.base + "/v9/projects/" + url.PathEscape(project) + "/env/" + url.PathEscape(id) + c.teamQuery()
		if err := platformCall(ctx, c.http, http.MethodPatch, u, c.cred.Token,
			"application/json", strings.NewReader(string(body)), nil); err != nil {
			return fmt.Errorf("vercel env %s: %w", key, err)
		}
	}
	return nil
}

// vercelEnvBody is the request body of both env calls. Key is empty on
// update — the entry is addressed by id.
type vercelEnvBody struct {
	Key    string   `json:"key,omitempty"`
	Value  string   `json:"value"`
	Type   string   `json:"type"`
	Target []string `json:"target"`
}

// createProject creates the empty project a first deploy needs before its
// environment variables can be set. An existing project is the desired end
// state — the 409 conflict counts as success.
func (c *vercelClient) createProject(ctx context.Context, project string) error {
	body, err := json.Marshal(map[string]any{"name": project, "framework": nil})
	if err != nil {
		return err
	}
	u := c.base + "/v11/projects" + c.teamQuery()
	err = platformCall(ctx, c.http, http.MethodPost, u, c.cred.Token,
		"application/json", strings.NewReader(string(body)), nil)
	switch {
	case err == nil, conflictAnswer(err):
		return nil
	case credentialsRejected(err):
		return fmt.Errorf("vercel create project: %w", ErrCredentials)
	default:
		return fmt.Errorf("vercel create project: %w", err)
	}
}

// conflictAnswer reports a platform answer meaning "already exists" for a
// create call: Vercel answers 403 for a duplicated env variable and 409 for
// a duplicated project.
func conflictAnswer(err error) bool {
	var pe *platformError
	return errors.As(err, &pe) && (pe.status == http.StatusForbidden || pe.status == http.StatusConflict)
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
