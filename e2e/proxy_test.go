package e2e_test

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// rawRequestTo writes one hand-rolled HTTP/1.1 request to the gateway and
// returns the full response text. The net/http client always speaks
// origin-form, so proxy-form inbound (absolute-form target, CONNECT) needs
// a raw connection.
func rawRequestTo(t *testing.T, addr, req string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(raw)
}

func respFirstLine(t *testing.T, resp string) string {
	t.Helper()
	if i := strings.IndexByte(resp, '\n'); i >= 0 {
		return resp[:i]
	}
	return resp
}

func TestE2E_ProxyFormRelaysAbsoluteTarget(t *testing.T) {
	vercel := NewEdgeSim(t, "vercel-1")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: vercel.URL},
	)

	resp := rawRequestTo(t, g.Addr, "GET http://api.example.com/v1/messages?beta=true HTTP/1.1\r\n"+
		"Host: api.example.com\r\n"+
		"X-Relay-Provider: vercel\r\n"+
		"Connection: close\r\n\r\n")

	if line := respFirstLine(t, resp); !strings.Contains(line, " 200 ") {
		t.Fatalf("status = %q, want 200\nlogs:\n%s", line, g.Logs())
	}
	if !strings.Contains(resp, vercel.servedBody()) {
		t.Fatalf("body missing sim response:\n%s", resp)
	}
	if got := vercel.header("X-Relay-Target"); got != "http://api.example.com" {
		t.Fatalf("upstream X-Relay-Target = %q", got)
	}
	if got := vercel.header("X-Relay-Path"); got != "/v1/messages?beta=true" {
		t.Fatalf("upstream X-Relay-Path = %q", got)
	}
}

func TestE2E_ProxyFormOverwritesSmuggledSpecHeaders(t *testing.T) {
	vercel := NewEdgeSim(t, "vercel-1")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: vercel.URL},
	)

	resp := rawRequestTo(t, g.Addr, "GET http://api.example.com/v1 HTTP/1.1\r\n"+
		"Host: api.example.com\r\n"+
		"X-Relay-Provider: vercel\r\n"+
		"X-Relay-Target: http://evil.example\r\n"+
		"X-Relay-Path: /evil\r\n"+
		"Connection: close\r\n\r\n")

	if line := respFirstLine(t, resp); !strings.Contains(line, " 200 ") {
		t.Fatalf("status = %q, want 200\nlogs:\n%s", line, g.Logs())
	}
	if got := vercel.header("X-Relay-Target"); got != "http://api.example.com" {
		t.Fatalf("smuggled target rode upstream: %q", got)
	}
	if got := vercel.header("X-Relay-Path"); got != "/v1" {
		t.Fatalf("smuggled path rode upstream: %q", got)
	}
}

func TestE2E_ProxyFormUnknownPathIsNotAPin(t *testing.T) {
	vercel := NewEdgeSim(t, "vercel-1")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: vercel.URL},
	)

	// No pin header, and "nosuch" is not a provider: spec mode would 404,
	// proxy mode must round-robin the target's path like any other.
	resp := rawRequestTo(t, g.Addr, "GET http://api.example.com/nosuch/x HTTP/1.1\r\n"+
		"Host: api.example.com\r\n"+
		"Connection: close\r\n\r\n")

	if line := respFirstLine(t, resp); !strings.Contains(line, " 200 ") {
		t.Fatalf("status = %q, want 200\nlogs:\n%s", line, g.Logs())
	}
	if got := vercel.header("X-Relay-Path"); got != "/nosuch/x" {
		t.Fatalf("upstream X-Relay-Path = %q", got)
	}
}

func TestE2E_ConnectRejectedWithoutTunnel(t *testing.T) {
	vercel := NewEdgeSim(t, "vercel-1")
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "vercel-1", Provider: "vercel", URL: vercel.URL},
	)

	resp := rawRequestTo(t, g.Addr, "CONNECT api.example.com:443 HTTP/1.1\r\n"+
		"Host: api.example.com:443\r\n"+
		"Connection: close\r\n\r\n")

	if line := respFirstLine(t, resp); !strings.Contains(line, " 501 ") {
		t.Fatalf("status = %q, want 501\nlogs:\n%s", line, g.Logs())
	}
	if vercel.hitCount() != 0 {
		t.Fatal("CONNECT reached the relay")
	}
}
