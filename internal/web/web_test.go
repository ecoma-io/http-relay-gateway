package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	h, err := Handler()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func get(t *testing.T, ts *httptest.Server, path string) (*http.Response, string) {
	t.Helper()
	res, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, string(body)
}

func TestServesIndexAtRoot(t *testing.T) {
	ts := newServer(t)
	res, body := get(t, ts, "/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "<!doctype html>") {
		t.Fatalf("root body is not the SPA entry: %q", body)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("index Cache-Control = %q, want no-store", cc)
	}
}

func TestHistoryFallbackServesIndex(t *testing.T) {
	ts := newServer(t)
	res, body := get(t, ts, "/relays")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (deep links resolve client-side)", res.StatusCode)
	}
	if !strings.Contains(body, "<!doctype html>") {
		t.Fatalf("fallback body is not the SPA entry: %q", body)
	}
}

func TestMissingAssetIs404(t *testing.T) {
	ts := newServer(t)
	res, _ := get(t, ts, "/assets/nope-abcdef.js")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for a missing extension path", res.StatusCode)
	}
}
