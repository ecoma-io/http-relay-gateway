package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// platformHTTPClient is the client the production factory hands every
// platform client; each call carries its own context deadline.
func platformHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute}
}

// platformCall executes one authenticated platform API request and decodes a
// JSON answer. A non-2xx status becomes an error carrying a bounded snippet
// of the body: platforms answer with their own messages, never with the
// credentials that were sent.
func platformCall(ctx context.Context, hc *http.Client, method, url, token, contentType string, body io.Reader, into any) error {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("read answer: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, errorSnippet(raw))
	}
	if into == nil {
		return nil
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("decode answer: %w", err)
	}
	return nil
}

// errorSnippet keeps the platform's message readable without letting a
// multi-kilobyte HTML error page (or anything else) flood the log.
func errorSnippet(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	const max = 200
	if len(text) > max {
		text = text[:max] + "…"
	}
	if text == "" {
		return "empty answer"
	}
	return text
}

// ErrCredentials reports a platform rejecting the account's token: the
// stored credential is wrong or expired, which is client data, not a
// gateway fault.
var ErrCredentials = errors.New("platform rejected credentials")

// envFactory is the production Factory: each client points at its real
// platform API unless the test-only base-override environment redirects it.
type envFactory struct {
	client *http.Client
}

// NewFactory builds the production Factory.
func NewFactory(hc *http.Client) Factory {
	if hc == nil {
		hc = platformHTTPClient()
	}
	return envFactory{client: hc}
}

// For builds the client for one account's credentials. The base-override
// environment variables are read per call so tests may point them at fakes
// before the first use.
func (f envFactory) For(platform, token, accountRef string) (Client, error) {
	switch platform {
	case PlatformVercel:
		return newVercelClient(os.Getenv(VercelAPIBaseEnv), token, accountRef, f.client), nil
	case PlatformCloudflare:
		return newCloudflareClient(os.Getenv(CloudflareAPIBaseEnv), token, accountRef, f.client), nil
	case PlatformDeno:
		return newDenoClient(os.Getenv(DenoAPIBaseEnv), token, accountRef, f.client), nil
	default:
		return nil, fmt.Errorf("unknown platform %q", platform)
	}
}

// awaitLive polls the deployed relay's version endpoint until it reports the
// just-deployed version — Deploy returns only once the new worker is
// actually answering. deadline bounds the wait (platform cold starts vary).
func awaitLive(ctx context.Context, hc *http.Client, url, version string, deadline time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		answer, err := Probe(deadlineCtx, hc, url)
		switch {
		case err != nil:
			lastErr = err
		case answer.Version == version:
			return nil
		case answer.Version == "":
			lastErr = answer.NotWorkerErr()
		default:
			lastErr = fmt.Errorf("relay answers version %q, deployed %q", answer.Version, version)
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("relay not live after %s: %w", deadline, lastErr)
		case <-ticker.C:
		}
	}
}
