package e2e_test

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestE2E_AdminServesSPA pins the admin plane's SPA contract: the index is
// served at / and at deep links (history-mode routing), extension misses
// 404, assets are immutable, and index is never cached — a redeployed UI
// must be picked up by a refresh.
func TestE2E_AdminServesSPA(t *testing.T) {
	g := NewGateway(t)

	get := func(path string) (int, http.Header, string) {
		t.Helper()
		res, err := http.Get("http://" + g.AdminAddr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		raw, _ := io.ReadAll(res.Body)
		return res.StatusCode, res.Header, string(raw)
	}

	// / and a deep link both serve the SPA entry with no-store.
	for _, path := range []string{"/", "/relays", "/settings"} {
		code, headers, body := get(path)
		if code != http.StatusOK || !strings.Contains(body, "<!doctype html") {
			t.Fatalf("GET %s = %d %q, want the SPA index", path, code, body)
		}
		if cache := headers.Get("Cache-Control"); cache != "no-store" {
			t.Fatalf("GET %s Cache-Control = %q, want no-store", path, cache)
		}
	}

	// An extension-bearing miss is a plain 404, never the index.
	code, _, body := get("/nonexistent.js")
	if code != http.StatusNotFound || strings.Contains(body, "<!doctype html") {
		t.Fatalf("GET /nonexistent.js = %d %q, want a bare 404", code, body)
	}

	// When the real bundle is embedded (make web), its content-hashed assets
	// must be immutable-cached. The committed stub has no asset tags, so the
	// assertion applies only when one is present.
	res, err := http.Get("http://" + g.AdminAddr + "/")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if m := assetTag.FindStringSubmatch(string(raw)); m != nil {
		code, headers, _ := get("/assets/" + m[1])
		if code != http.StatusOK {
			t.Fatalf("asset status = %d, want 200", code)
		}
		if cc := headers.Get("Cache-Control"); !strings.Contains(cc, "immutable") {
			t.Fatalf("asset Cache-Control = %q, want immutable", cc)
		}
	}
}

// assetTag matches the script tag of a real Vite bundle ("/assets/<name>").
var assetTag = regexp.MustCompile(`src="/assets/([^"]+)"`)

// TestE2E_UnknownAPIPathIsJSON is the admin-plane sibling of the SPA rules:
// no /api path may ever fall through to the SPA handler.
func TestE2E_UnknownAPIPathIsJSON(t *testing.T) {
	g := NewGateway(t)

	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Get("http://" + g.AdminAddr + "/api/v2/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", res.StatusCode)
	}
	if strings.Contains(string(raw), "<!doctype html") {
		t.Fatalf("API miss served the SPA: %q", raw)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q, want application/json", ct)
	}
}
