package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// assertNoLeaks fails the test when any credential or relay/fake URL
// marker appears in the given response bodies.
func assertNoLeaks(t *testing.T, label string, g *gatewayProc, fake *fakeEdge, blobs ...[]byte) {
	t.Helper()
	markers := append([]string{g.key}, fake.secretMarkers()...)
	markers = append(markers, fake.urlMarkers()...)
	for _, blob := range blobs {
		for _, m := range markers {
			if m != "" && bytes.Contains(blob, []byte(m)) {
				t.Fatalf("%s: body leaks a %d-byte secret/URL marker", label, len(m))
			}
		}
	}
}

// TestE2E_StatsNeverLeaksAndBodyLimits: /stats carries names, providers,
// health and counters — never relay URLs or credentials; the per-provider
// body caps gate the buffered path (413 only when no accepting provider
// remains, otherwise the body skips to one that accepts it).
func TestE2E_StatsNeverLeaksAndBodyLimits(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("leak")
	ct := "cf-tok-" + randSuffix(12)
	fake.setToken("vercel", vt)
	fake.setToken("cloudflare", ct)

	g := startGateway(t, fake, gwOptions{
		key: key,
		config: configYAML(
			relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT")),
			relayBlock("c-rel", "cloudflare", tokenEnvLine("E2E_CT")),
		),
		extraEnv: map[string]string{"E2E_VT": vt, "E2E_CT": ct},
	})
	g.waitForStats(t, 20*time.Second, "both relays to admit", func(d *statsDoc) bool {
		return d.Readiness.ReadyRelays == 2
	})

	// /stats: only the documented fields, and no secrets or URLs anywhere.
	resp, raw := g.do(t, http.MethodGet, "/stats", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/stats = %d", resp.StatusCode)
	}
	assertNoLeaks(t, "/stats", g, fake, raw)
	var generic struct {
		Relays    []map[string]any `json:"relays"`
		Lifecycle []map[string]any `json:"lifecycle"`
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("/stats decode: %v", err)
	}
	if len(generic.Relays) != 2 || len(generic.Lifecycle) != 2 {
		t.Fatalf("/stats = %d relays, %d lifecycle rows, want 2/2",
			len(generic.Relays), len(generic.Lifecycle))
	}
	relayKeys := map[string]bool{
		"name": true, "provider": true, "healthy": true,
		"maxBody": true, "requests": true, "failures": true,
	}
	for i, row := range generic.Relays {
		for k := range row {
			if !relayKeys[k] {
				t.Fatalf("/stats relays[%d] carries undocumented key %q", i, k)
			}
		}
		for _, must := range []string{"name", "provider", "healthy", "maxBody"} {
			if _, ok := row[must]; !ok {
				t.Fatalf("/stats relays[%d] misses %q", i, must)
			}
		}
	}
	lifecycleKeys := map[string]bool{
		"name": true, "provider": true, "state": true, "reason": true, "generation": true,
	}
	for i, row := range generic.Lifecycle {
		for k := range row {
			if !lifecycleKeys[k] {
				t.Fatalf("/stats lifecycle[%d] carries undocumented key %q", i, k)
			}
		}
	}

	big := bytes.Repeat([]byte("x"), 4_600_000) // above the vercel cap, below cloudflare's

	// Pinned to the 4.5MB provider: nothing else may take it — 413, and
	// the error body stays clean.
	resp, body := g.do(t, http.MethodPost, "/vercel", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/upload",
	}, big)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("4.6MB pinned to vercel = %d, want 413", resp.StatusCode)
	}
	assertNoLeaks(t, "413 body", g, fake, body)
	if n := len(echo.requests()); n != 0 {
		t.Fatalf("echo saw %d requests after the 413, want 0", n)
	}

	// Unpinned: the body skips to a provider that accepts it.
	resp, body = g.do(t, http.MethodPost, "/", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/upload", "X-Marker": "big1",
	}, big)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("4.6MB unpinned = %d (%s), want 200 via a provider that accepts it", resp.StatusCode, body[:0])
	}
	assertNoLeaks(t, "relayed 4.6MB body", g, fake, body)
	if !fake.project("c-rel").sim.markerSeen("big1") {
		t.Fatal("the oversized body was not served by the cloudflare relay")
	}
	rec, ok := echo.lastRequest()
	if !ok || rec.BodyLen != 4_600_000 {
		t.Fatalf("echo body length = %d (ok=%v), want 4600000 intact", rec.BodyLen, ok)
	}

	// Pinned to the accepting provider: the same body relays fine.
	resp, body = g.do(t, http.MethodPost, "/cloudflare", map[string]string{
		"X-Relay-Target": echo.base, "X-Relay-Path": "/upload",
	}, big)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("4.6MB pinned to cloudflare = %d, want 200", resp.StatusCode)
	}
	assertNoLeaks(t, "pinned 4.6MB body", g, fake, body)
}

// rawRequest speaks one hand-built HTTP/1.1 request against addr and
// returns the whole wire answer (the request sets Connection: close).
func rawRequest(t *testing.T, addr, raw string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if _, err := io.WriteString(conn, raw); err != nil {
		t.Fatalf("write request: %v", err)
	}
	out, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	_ = conn.Close()
	return string(out)
}

func statusLine(wire string) string {
	line, _, _ := strings.Cut(wire, "\r\n")
	return line
}

// TestE2E_ProxyInbound: an absolute-form request target (forward-proxy
// inbound) routes through the relay with the target/path derived from the
// URL — client-supplied relay headers are overwritten, never smuggled;
// CONNECT is a documented 501; a proxy-form control path relays instead of
// being shadowed.
func TestE2E_ProxyInbound(t *testing.T) {
	fake := newFakeEdge(t)
	echo := newEcho(t)
	key, vt := freshIdentity("px")
	fake.setToken("vercel", vt)

	g := startGateway(t, fake, gwOptions{
		config:   configYAML(relayBlock("v-rel", "vercel", tokenEnvLine("E2E_VT"))),
		key:      key,
		extraEnv: map[string]string{"E2E_VT": vt},
	})
	g.waitForReady(t, 20*time.Second)

	gwAddr := strings.TrimPrefix(g.base, "http://")
	echoAuth := strings.TrimPrefix(echo.base, "http://")

	// Absolute-form target with poison relay headers: the derived
	// target/path win over the caller's, while the pin stays header-only
	// (a valid pin is honored; here it routes to the only relay).
	wire := rawRequest(t, gwAddr, "GET "+echo.base+"/data?x=1 HTTP/1.1\r\n"+
		"Host: "+echoAuth+"\r\n"+
		"X-Relay-Target: http://127.0.0.1:9\r\n"+
		"X-Relay-Path: /evil\r\n"+
		"X-Relay-Provider: vercel\r\n"+
		"X-Marker: px1\r\n"+
		"Connection: close\r\n\r\n")
	if code := statusLine(wire); !strings.Contains(code, " 200 ") {
		t.Fatalf("proxy-form request answered %q, want 200\ngateway log:\n%s", code, g.logDump())
	}
	var forwarded *echoRecord
	waitFor(t, 10*time.Second, "the echo to record the proxied request", func() bool {
		for i, rec := range echo.requests() {
			if rec.Header.Get("X-Marker") == "px1" {
				forwarded = &echo.requests()[i]
				return true
			}
		}
		return false
	})
	if forwarded.Path != "/data" || forwarded.RawQuery != "x=1" {
		t.Fatalf("echo saw %s?%s, want /data?x=1", forwarded.Path, forwarded.RawQuery)
	}
	for name, want := range map[string]string{
		"x-relay-target":   "", // derived from the absolute-form URL
		"x-relay-path":     "",
		"x-relay-provider": "", // pin stays header-only; overwritten too
		"x-relay-token":    "", // the gateway's own key never reaches upstream
		"x-forwarded-for":  "", // nothing identifying the caller, ever
	} {
		if got := forwarded.Header.Get(name); got != want {
			t.Fatalf("proxied upstream header %s = %q, want stripped/absent", name, got)
		}
	}

	// CONNECT: edge relays carry no raw TCP tunnels — a documented 501.
	wire = rawRequest(t, gwAddr, "CONNECT example.com:443 HTTP/1.1\r\n"+
		"Host: example.com:443\r\nConnection: close\r\n\r\n")
	if code := statusLine(wire); !strings.Contains(code, " 501 ") {
		t.Fatalf("CONNECT answered %q, want 501", code)
	}

	// A proxy-form request for a control path relays; it is never shadowed.
	wire = rawRequest(t, gwAddr, "GET "+echo.base+"/healthz HTTP/1.1\r\n"+
		"Host: "+echoAuth+"\r\nConnection: close\r\n\r\n")
	if code := statusLine(wire); !strings.Contains(code, " 200 ") {
		t.Fatalf("proxy-form /healthz answered %q, want 200 (relayed)", code)
	}
	var shadow parsedEcho
	// The relayed answer arrives chunked; take the JSON object out of the
	// chunk framing.
	start := strings.Index(wire, "{")
	end := strings.LastIndex(wire, "}")
	if start < 0 || end < start {
		t.Fatalf("proxied /healthz carried no JSON body\nwire: %s", wire)
	}
	if err := json.Unmarshal([]byte(wire[start:end+1]), &shadow); err != nil {
		t.Fatalf("proxied /healthz body: %v\nwire: %s", err, wire)
	}
	if shadow.Path != "/healthz" {
		t.Fatalf("proxy-form /healthz landed on %q, want the relayed upstream", shadow.Path)
	}

	// The origin-form control endpoint still answers directly.
	resp, body := g.do(t, http.MethodGet, "/healthz", nil, nil)
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("origin-form /healthz = %d %q, want 200 ok", resp.StatusCode, body)
	}
}
