package admin_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"http-relay-gateway/internal/admin"
	"http-relay-gateway/internal/admin/auth"
	"http-relay-gateway/internal/logging"
	"http-relay-gateway/internal/store"
)

// newTestServer builds an admin server over a fresh temporary database. The
// SPA is a stub that 404s (most tests only touch /api paths).
func newTestServer(t *testing.T) (*httptest.Server, *client) {
	t.Helper()
	db, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	spa := http.NotFoundHandler()
	srv := httptest.NewServer(admin.New(db, "test", logging.Nop(), spa))
	t.Cleanup(srv.Close)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return srv, &client{base: srv.URL, http: &http.Client{Timeout: 10 * time.Second, Jar: jar}}
}

// client is a JSON API client with a cookie jar (sessions persist across
// calls, like a browser).
type client struct {
	base string
	http *http.Client
}

func (c *client) do(t *testing.T, method, path string, body any) (int, http.Header, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed)
	return res.StatusCode, res.Header, parsed
}

func (c *client) getRaw(t *testing.T, path string) string {
	t.Helper()
	res, err := c.http.Get(c.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	return string(raw)
}

func (c *client) setup(t *testing.T, password string) {
	t.Helper()
	code, _, body := c.do(t, http.MethodPost, "/api/v1/setup", map[string]string{
		"password": password,
		"confirm":  password,
	})
	if code != http.StatusOK {
		t.Fatalf("setup status = %d: %v", code, body)
	}
}

const testPassword = "test-password-123"

// loginFailLimit is the limiter threshold the rate-limit test drives to.
const loginFailLimit = 5

func TestSetupFlowOnce(t *testing.T) {
	_, c := newTestServer(t)

	// Before setup: setup required, management calls unauthorized.
	code, _, body := c.do(t, http.MethodGet, "/api/v1/status", nil)
	if code != http.StatusOK || body["setupRequired"] != true {
		t.Fatalf("pre-setup status = %d %v, want setupRequired true", code, body)
	}
	code, _, _ = c.do(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated relays status = %d, want 401", code)
	}

	c.setup(t, testPassword)

	code, _, body = c.do(t, http.MethodGet, "/api/v1/status", nil)
	if code != http.StatusOK || body["setupRequired"] != false {
		t.Fatalf("post-setup status = %d %v, want setupRequired false", code, body)
	}
	// The session cookie issued at setup authorizes management calls.
	code, _, _ = c.do(t, http.MethodGet, "/api/v1/relays", nil)
	if code != http.StatusOK {
		t.Fatalf("authenticated relays status = %d", code)
	}

	// Setup is once: a second attempt conflicts even with valid input.
	code, _, body = c.do(t, http.MethodPost, "/api/v1/setup", map[string]string{
		"password": testPassword, "confirm": testPassword,
	})
	if code != http.StatusConflict {
		t.Fatalf("second setup status = %d %v, want 409", code, body)
	}
}

func TestSetupValidation(t *testing.T) {
	_, c := newTestServer(t)
	code, _, body := c.do(t, http.MethodPost, "/api/v1/setup", map[string]string{
		"password": "short", "confirm": "short",
	})
	if code != http.StatusUnprocessableEntity || body["field"] != "password" {
		t.Fatalf("short password = %d %v, want 422 field password", code, body)
	}
	code, _, body = c.do(t, http.MethodPost, "/api/v1/setup", map[string]string{
		"password": testPassword, "confirm": "different-password",
	})
	if code != http.StatusUnprocessableEntity || body["field"] != "confirm" {
		t.Fatalf("mismatched confirm = %d %v, want 422 field confirm", code, body)
	}
}

func TestLoginAndRateLimit(t *testing.T) {
	_, c := newTestServer(t)
	c.setup(t, testPassword)

	// loginFailLimit wrong attempts, then even the correct password is
	// refused until the window drains.
	for i := range loginFailLimit {
		code, headers, body := c.do(t, http.MethodPost, "/api/v1/login", map[string]string{"password": "wrong"})
		if code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status = %d %v, want 401", i+1, code, body)
		}
		if headers.Get("Retry-After") != "" {
			t.Fatalf("attempt %d set Retry-After on a plain 401", i+1)
		}
	}
	code, headers, _ := c.do(t, http.MethodPost, "/api/v1/login", map[string]string{"password": testPassword})
	if code != http.StatusTooManyRequests {
		t.Fatalf("saturated login status = %d, want 429", code)
	}
	if headers.Get("Retry-After") == "" {
		t.Fatal("429 without Retry-After")
	}
}

func TestLoginBeforeSetupConflicts(t *testing.T) {
	_, c := newTestServer(t)
	code, _, body := c.do(t, http.MethodPost, "/api/v1/login", map[string]string{"password": "whatever123"})
	if code != http.StatusConflict {
		t.Fatalf("pre-setup login status = %d %v, want 409", code, body)
	}
}

func TestRelayCRUD(t *testing.T) {
	_, c := newTestServer(t)
	c.setup(t, testPassword)

	code, _, created := c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "edge-1", "provider": "Vercel", "url": "https://edge.example.com/api",
	})
	if code != http.StatusCreated {
		t.Fatalf("create status = %d: %v", code, created)
	}
	id := int64(created["id"].(float64))
	if created["origin"] != "managed" {
		t.Fatalf("origin = %v, want managed", created["origin"])
	}
	if created["provider"] != "vercel" {
		t.Fatalf("provider not normalized: %v", created["provider"])
	}

	raw := c.getRaw(t, "/api/v1/relays")
	if !strings.Contains(raw, `"name":"edge-1"`) || !strings.Contains(raw, `"origin":"managed"`) {
		t.Fatalf("relay list wrong: %s", raw)
	}

	// Duplicate names conflict.
	code, _, _ = c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "edge-1", "provider": "vercel", "url": "https://other.example.com",
	})
	if code != http.StatusConflict {
		t.Fatalf("duplicate name status = %d, want 409", code)
	}

	// Reserved providers and bad URLs are field errors.
	code, _, body := c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "x", "provider": "healthz", "url": "https://ok.example.com",
	})
	if code != http.StatusUnprocessableEntity || body["field"] != "provider" {
		t.Fatalf("reserved provider = %d %v, want 422 field provider", code, body)
	}
	code, _, body = c.do(t, http.MethodPost, "/api/v1/relays", map[string]any{
		"name": "x", "provider": "vercel", "url": "not-a-url",
	})
	if code != http.StatusUnprocessableEntity || body["field"] != "url" {
		t.Fatalf("bad url = %d %v, want 422 field url", code, body)
	}

	// Patch: deactivate.
	code, _, patched := c.do(t, http.MethodPatch, relayPath(id), map[string]any{"active": false})
	if code != http.StatusOK || patched["active"] != false {
		t.Fatalf("patch status = %d %v, want active false", code, patched)
	}

	// Header policy: valid applies, denied names are refused, "" clears.
	code, _, patched = c.do(t, http.MethodPatch, relayPath(id), map[string]any{
		"headerPolicy": `{"strip":["x-internal-trace"],"set":{"x-relayed-by":["gw"]}}`,
	})
	if code != http.StatusOK || patched["headerPolicy"] == nil {
		t.Fatalf("policy patch = %d %v", code, patched)
	}
	code, _, body = c.do(t, http.MethodPatch, relayPath(id), map[string]any{
		"headerPolicy": `{"strip":["connection"]}`,
	})
	if code != http.StatusUnprocessableEntity || body["field"] != "headerPolicy" {
		t.Fatalf("denied policy = %d %v, want 422 field headerPolicy", code, body)
	}
	code, _, patched = c.do(t, http.MethodPatch, relayPath(id), map[string]any{"headerPolicy": ""})
	if code != http.StatusOK || patched["headerPolicy"] != nil {
		t.Fatalf("policy clear = %d %v, want headerPolicy nil", code, patched)
	}

	// Delete: gone afterwards; the remote flag is refused pre-deployers.
	code, _, _ = c.do(t, http.MethodDelete, relayPath(id)+"?deleteRemote=true", nil)
	if code != http.StatusNotImplemented {
		t.Fatalf("deleteRemote status = %d, want 501 before deployers exist", code)
	}
	code, _, _ = c.do(t, http.MethodDelete, relayPath(id), nil)
	if code != http.StatusOK {
		t.Fatalf("delete status = %d", code)
	}
	code, _, _ = c.do(t, http.MethodGet, relayPath(id), nil)
	if code != http.StatusNotFound {
		t.Fatalf("get deleted status = %d, want 404", code)
	}
}

func relayPath(id int64) string {
	return "/api/v1/relays/" + strconv.FormatInt(id, 10)
}

func TestProvidersPutGet(t *testing.T) {
	_, c := newTestServer(t)
	c.setup(t, testPassword)

	put := []map[string]any{
		{"name": "vercel", "maxBody": 4500000},
		{"name": "cloudflare", "maxBody": 100000000, "headerPolicy": `{"strip":["x-internal"]}`},
	}
	code, _, body := c.do(t, http.MethodPut, "/api/v1/providers", put)
	if code != http.StatusOK {
		t.Fatalf("put providers = %d %v", code, body)
	}
	raw := c.getRaw(t, "/api/v1/providers")
	if !strings.Contains(raw, `"name":"vercel"`) || !strings.Contains(raw, `"maxBody":4500000`) {
		t.Fatalf("providers list wrong: %s", raw)
	}

	code, _, body = c.do(t, http.MethodPut, "/api/v1/providers", []map[string]any{
		{"name": "a", "maxBody": 1}, {"name": "a", "maxBody": 2},
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("duplicate provider = %d %v, want 422", code, body)
	}
	code, _, body = c.do(t, http.MethodPut, "/api/v1/providers", []map[string]any{
		{"name": "vercel", "maxBody": 0},
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("zero maxBody = %d %v, want 422", code, body)
	}
}

func TestSettingsPatch(t *testing.T) {
	_, c := newTestServer(t)
	c.setup(t, testPassword)

	code, _, settings := c.do(t, http.MethodGet, "/api/v1/settings", nil)
	if code != http.StatusOK || settings["logLevel"] != "info" || settings["maxRetries"] != float64(2) {
		t.Fatalf("default settings = %d %v", code, settings)
	}

	code, _, _ = c.do(t, http.MethodPatch, "/api/v1/settings", map[string]any{
		"logLevel": "debug", "maxRetries": 5, "streamThresholdBytes": 65536,
	})
	if code != http.StatusOK {
		t.Fatal("patch settings failed")
	}
	_, _, settings = c.do(t, http.MethodGet, "/api/v1/settings", nil)
	if settings["logLevel"] != "debug" || settings["maxRetries"] != float64(5) ||
		settings["streamThresholdBytes"] != float64(65536) {
		t.Fatalf("patched settings = %v", settings)
	}

	// Unknown keys (including admin credential keys!) are refused.
	code, _, _ = c.do(t, http.MethodPatch, "/api/v1/settings", map[string]any{
		"adminPasswordHash": "x",
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatal("admin key accepted by the settings API")
	}
	// Out-of-domain values are refused.
	code, _, _ = c.do(t, http.MethodPatch, "/api/v1/settings", map[string]any{
		"failureThreshold": 0,
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatal("failureThreshold 0 accepted")
	}
}

func TestUnknownAPIPathIsNotSPA(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	spaCalls := 0
	spa := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		spaCalls++
		_, _ = w.Write([]byte("spa-index"))
	})
	srv := httptest.NewServer(admin.New(db, "test", logging.Nop(), spa))
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/api/v2/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusNotFound || strings.Contains(string(raw), "spa-index") {
		t.Fatalf("unknown API path = %d %q, want a 404 that never hits the SPA", res.StatusCode, raw)
	}
	if spaCalls != 0 {
		t.Fatal("SPA served an /api path")
	}
}

func TestSessionCookieAttributes(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	srv := httptest.NewServer(admin.New(db, "test", logging.Nop(), http.NotFoundHandler(), admin.WithSecureCookie()))
	t.Cleanup(srv.Close)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar, Timeout: 10 * time.Second}
	payload := fmt.Sprintf(`{"password":%q,"confirm":%q}`, testPassword, testPassword)
	res, err := client.Post(srv.URL+"/api/v1/setup", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	cookies := res.Cookies()
	if len(cookies) != 1 || cookies[0].Name != auth.CookieName {
		t.Fatalf("session cookies = %+v", cookies)
	}
	ck := cookies[0]
	if !ck.HttpOnly || !ck.Secure || ck.SameSite != http.SameSiteStrictMode || ck.Path != "/" {
		t.Fatalf("cookie attributes wrong: %+v", ck)
	}
}
