package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// lockedBuffer is a goroutine-safe sink for gateway process output.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// adminPassword is the setup password every e2e gateway installs.
const adminPassword = "e2e-admin-password-123"

// applySettle bounds one database-mutation-to-pool-swap cycle: the applier
// reacts to the store's change signal in-process, so this is pure headroom
// for slow machines.
const applySettle = 5 * time.Second

// StatsView is the decoded /stats body.
type StatsView struct {
	Version string      `json:"version"`
	Relays  []RelayView `json:"relays"`
}

// RelayView is one per-relay row from /stats.
type RelayView struct {
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Origin    string `json:"origin"`
	Managed   bool   `json:"managed"`
	Active    bool   `json:"active"`
	Healthy   bool   `json:"healthy"`
	MaxBody   int64  `json:"maxBody"`
	Requests  int64  `json:"requests"`
	Failures  int64  `json:"failures"`
	LastError string `json:"lastError,omitempty"`
}

func (s *StatsView) relay(name string) (RelayView, bool) {
	for _, r := range s.Relays {
		if r.Name == name {
			return r, true
		}
	}
	return RelayView{}, false
}

func relayNames(st *StatsView) []string {
	out := make([]string, 0, len(st.Relays))
	for _, r := range st.Relays {
		out = append(out, r.Name)
	}
	return out
}

// Gateway is one real gateway subprocess with its own data file and ports.
type Gateway struct {
	t         testing.TB
	dir       string
	dataFile  string
	cmd       *exec.Cmd
	output    *lockedBuffer
	admin     *adminClient
	adminPass string

	Addr      string
	AdminAddr string
}

// handedOut records every loopback address freeAddr returned in this process.
// The OS can hand back a just-closed ephemeral port immediately, which would
// collide two listeners of one test run; the registry makes every allocation
// unique for the whole run.
var (
	handedOutMu sync.Mutex
	handedOut   = map[string]bool{}
)

func freeAddr(t testing.TB) string {
	t.Helper()
	handedOutMu.Lock()
	defer handedOutMu.Unlock()
	for {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		if !handedOut[addr] {
			handedOut[addr] = true
			return addr
		}
	}
}

// deadRelayURL returns a relay URL that refuses connections (a closed
// listener) — the e2e stand-in for a downed edge deployment.
func deadRelayURL(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "http://" + addr
}

// RelaySeed is one relay a test creates through the admin API.
type RelaySeed struct {
	Name     string
	Provider string
	URL      string
	Active   *bool
}

func activePtr(v bool) *bool { return &v }

// NewGateway starts a bare gateway (no relays, no setup yet) and waits for
// both listeners.
func NewGateway(t testing.TB) *Gateway {
	return newGateway(t, nil)
}

// NewGatewayWithEnv is NewGateway with extra bootstrap environment entries
// (for example "SHUTDOWN_GRACE=1s") appended to the standard set.
func NewGatewayWithEnv(t testing.TB, extraEnv ...string) *Gateway {
	return newGateway(t, extraEnv)
}

// NewGatewayWithRelays performs the whole first-run flow — setup, then relay
// creation through the admin API — and waits for the pool to reflect it.
// Relay order is creation order, which is the pool's round-robin order.
func NewGatewayWithRelays(t testing.TB, seeds ...RelaySeed) *Gateway {
	return newGatewayWith(t, nil, nil, seeds)
}

// NewGatewayWithProviders is NewGatewayWithRelays with provider rows (name ->
// max body bytes) written first, so relay generation resolves their limits.
func NewGatewayWithProviders(t testing.TB, providers map[string]int64, seeds ...RelaySeed) *Gateway {
	return newGatewayWith(t, providers, nil, seeds)
}

func newGatewayWith(t testing.TB, providers map[string]int64, extraEnv []string, seeds []RelaySeed) *Gateway {
	t.Helper()
	g := newGateway(t, extraEnv)
	g.Setup(t, adminPassword)
	if len(providers) > 0 {
		g.PutProviders(t, providers)
	}
	names := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		g.CreateRelay(t, seed)
		names = append(names, seed.Name)
	}
	if len(names) > 0 {
		g.WaitForRelays(names, applySettle)
	}
	return g
}

func newGateway(t testing.TB, extraEnv []string) *Gateway {
	t.Helper()
	if testBinaryPath == "" {
		t.Skip("e2e binary not built (short mode?)")
	}
	dir := t.TempDir()
	adminAddr := freeAddr(t)
	admin, err := newAdminClient(adminAddr)
	if err != nil {
		t.Fatal(err)
	}
	g := &Gateway{
		t:         t,
		dir:       dir,
		dataFile:  "gateway.db",
		output:    &lockedBuffer{},
		Addr:      freeAddr(t),
		AdminAddr: adminAddr,
		admin:     admin,
		adminPass: adminPassword,
	}
	g.start(extraEnv)
	t.Cleanup(g.stop)
	g.waitHealthy(10 * time.Second)
	g.waitAdminReady(10 * time.Second)
	return g
}

// start launches the gateway subprocess with this instance's paths and ports.
func (g *Gateway) start(extraEnv []string) {
	g.t.Helper()
	cmd := exec.Command(testBinaryPath)
	cmd.Dir = g.dir
	cmd.Env = append([]string{
		"LISTEN_ADDR=" + g.Addr,
		"ADMIN_ADDR=" + g.AdminAddr,
		"DATA_FILE=" + g.dataFile,
		"PATH=" + os.Getenv("PATH"),
	}, extraEnv...)
	cmd.Stdout = g.output
	cmd.Stderr = g.output
	if err := cmd.Start(); err != nil {
		g.t.Fatalf("start gateway: %v", err)
	}
	g.cmd = cmd
}

// Restart stops the process and starts a fresh one with the same data file
// and ports — the harness stand-in for a container restart.
func (g *Gateway) Restart() {
	g.t.Helper()
	g.stop()
	g.start(nil)
	g.waitHealthy(10 * time.Second)
	g.waitAdminReady(10 * time.Second)
}

func (g *Gateway) waitHealthy(timeout time.Duration) {
	g.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if g.cmd.ProcessState != nil && g.cmd.ProcessState.Exited() {
			g.t.Fatalf("gateway exited during startup; output:\n%s", g.output.String())
		}
		resp, err := http.Get("http://" + g.Addr + "/healthz")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && string(body) == "ok\n" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	g.t.Fatalf("gateway never became healthy; output:\n%s", g.output.String())
}

// waitAdminReady polls the admin plane until it answers — the data listener
// binds first, so health alone does not prove the admin API is up.
func (g *Gateway) waitAdminReady(timeout time.Duration) {
	g.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if g.cmd.ProcessState != nil && g.cmd.ProcessState.Exited() {
			g.t.Fatalf("gateway exited during startup; output:\n%s", g.output.String())
		}
		resp, err := http.Get("http://" + g.AdminAddr + "/api/v1/ping")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	g.t.Fatalf("admin plane never became ready; output:\n%s", g.output.String())
}

// WaitForRelays polls /stats until the relay list reports exactly want in order.
func (g *Gateway) WaitForRelays(want []string, timeout time.Duration) {
	g.t.Helper()
	g.WaitForCondition(timeout, fmt.Sprintf("relays == %v", want), func(st *StatsView) bool {
		return equalStrings(relayNames(st), want)
	})
}

// WaitForCondition polls until cond holds on the live /stats.
func (g *Gateway) WaitForCondition(timeout time.Duration, what string, cond func(*StatsView) bool) *StatsView {
	g.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := g.Stats()
		if err == nil && cond(st) {
			return st
		}
		time.Sleep(50 * time.Millisecond)
	}
	st, _ := g.Stats()
	g.t.Fatalf("condition %q never held; stats=%+v\nlogs:\n%s", what, st, g.output.String())
	return nil
}

func waitForLog(t testing.TB, g *Gateway, substr string, timeout time.Duration) {
	t.Helper()
	waitForLogCount(t, g, substr, 1, timeout)
}

// waitForLogCount waits until substr has appeared at least want times.
// Substrings that repeat every apply (like "configuration reloaded") need a
// count, not a membership check, or the wait passes on an older line.
func waitForLogCount(t testing.TB, g *Gateway, substr string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Count(g.Logs(), substr) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("logs never contained %q %d times\nlogs:\n%s", substr, want, g.output.String())
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Stats fetches and decodes /stats.
func (g *Gateway) Stats() (*StatsView, error) {
	resp, err := http.Get("http://" + g.Addr + "/stats")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var st StatsView
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil, err
	}
	return &st, nil
}

// RawStats returns the /stats body verbatim, for redaction assertions.
func (g *Gateway) RawStats(t testing.TB) string {
	t.Helper()
	resp, err := http.Get("http://" + g.Addr + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

// Logs returns captured gateway stdout/stderr.
func (g *Gateway) Logs() string { return g.output.String() }

func (g *Gateway) stop() {
	if g.cmd == nil || (g.cmd.ProcessState != nil && g.cmd.ProcessState.Exited()) {
		return
	}
	_ = g.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = g.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = g.cmd.Process.Kill()
		<-done
	}
}

// --- admin API client ---

// adminClient talks to the admin API with a cookie jar, like a browser.
type adminClient struct {
	base string
	http *http.Client
}

func newAdminClient(base string) (*adminClient, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &adminClient{base: "http://" + base, http: &http.Client{Jar: jar, Timeout: 15 * time.Second}}, nil
}

// do sends one JSON request and returns status, headers and the raw body.
func (a *adminClient) do(t testing.TB, method, path string, body any) (int, http.Header, string) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, a.base+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := a.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, res.Header, string(raw)
}

// AdminDo is the escape hatch for tests asserting on raw API behavior.
func (g *Gateway) AdminDo(t testing.TB, method, path string, body any) (int, http.Header, string) {
	g.t.Helper()
	return g.admin.do(t, method, path, body)
}

// Setup performs first-run setup and fails on any non-200.
func (g *Gateway) Setup(t testing.TB, password string) {
	t.Helper()
	code, _, body := g.admin.do(t, http.MethodPost, "/api/v1/setup", map[string]string{
		"password": password, "confirm": password,
	})
	if code != http.StatusOK {
		t.Fatalf("setup status = %d: %s\nlogs:\n%s", code, body, g.Logs())
	}
}

// Login attempts a login and returns the status code.
func (g *Gateway) Login(t testing.TB, password string) int {
	t.Helper()
	code, _, _ := g.admin.do(t, http.MethodPost, "/api/v1/login", map[string]string{"password": password})
	return code
}

// CreateRelay creates one relay through the API and fails on non-201.
func (g *Gateway) CreateRelay(t testing.TB, seed RelaySeed) int64 {
	t.Helper()
	fields := map[string]any{
		"name": seed.Name, "provider": seed.Provider, "url": seed.URL,
	}
	if seed.Active != nil {
		fields["active"] = *seed.Active
	}
	code, _, body := g.admin.do(t, http.MethodPost, "/api/v1/relays", fields)
	if code != http.StatusCreated {
		t.Fatalf("create relay %s status = %d: %s\nlogs:\n%s", seed.Name, code, body, g.Logs())
	}
	var created struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("decode created relay: %v (%s)", err, body)
	}
	return created.ID
}

// PatchRelay patches one relay and returns status plus the decoded body.
func (g *Gateway) PatchRelay(t testing.TB, id int64, fields map[string]any) (int, map[string]any) {
	t.Helper()
	code, _, raw := g.admin.do(t, http.MethodPatch, "/api/v1/relays/"+strconv.FormatInt(id, 10), fields)
	var parsed map[string]any
	_ = json.Unmarshal([]byte(raw), &parsed)
	return code, parsed
}

// DeleteRelay deletes one relay and fails on non-200.
func (g *Gateway) DeleteRelay(t testing.TB, id int64) {
	t.Helper()
	code, _, body := g.admin.do(t, http.MethodDelete, "/api/v1/relays/"+strconv.FormatInt(id, 10), nil)
	if code != http.StatusOK {
		t.Fatalf("delete relay %d status = %d: %s", id, code, body)
	}
}

// PutProviders replaces the provider set (name -> max body bytes).
func (g *Gateway) PutProviders(t testing.TB, providers map[string]int64) {
	t.Helper()
	rows := make([]map[string]any, 0, len(providers))
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	// Sorted for determinism in failures.
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	for _, name := range names {
		rows = append(rows, map[string]any{"name": name, "maxBody": providers[name]})
	}
	code, _, body := g.admin.do(t, http.MethodPut, "/api/v1/providers", rows)
	if code != http.StatusOK {
		t.Fatalf("put providers status = %d: %s", code, body)
	}
}

// PatchSettings patches runtime settings and fails on non-200.
func (g *Gateway) PatchSettings(t testing.TB, fields map[string]any) {
	t.Helper()
	code, _, body := g.admin.do(t, http.MethodPatch, "/api/v1/settings", fields)
	if code != http.StatusOK {
		t.Fatalf("patch settings status = %d: %s", code, body)
	}
}

// AdminStatus returns setupRequired from the public status endpoint.
func (g *Gateway) AdminStatus(t testing.TB) bool {
	t.Helper()
	code, _, raw := g.admin.do(t, http.MethodGet, "/api/v1/status", nil)
	if code != http.StatusOK {
		t.Fatalf("admin status = %d: %s", code, raw)
	}
	var parsed struct {
		SetupRequired bool `json:"setupRequired"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode admin status: %v (%s)", err, raw)
	}
	return parsed.SetupRequired
}

// edgeSim is one fake edge relay (the Vercel/Deno/Cloudflare deployment the
// gateway forwards into). It records the last request and answers with its
// own name, so assertions can tell which relay served.
type edgeSim struct {
	name string
	URL  string

	mu      sync.Mutex
	headers http.Header
	body    []byte
	hits    int
}

func (s *edgeSim) header(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headers == nil {
		return ""
	}
	return s.headers.Get(name)
}

func (s *edgeSim) lastBody() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.body...)
}

func (s *edgeSim) hitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits
}

func (s *edgeSim) servedBody() string { return s.name + "\n" }

// NewEdgeSim starts one fake edge relay. The response body is the sim's name
// so tests can identify which relay answered.
func NewEdgeSim(t testing.TB, name string) *edgeSim {
	t.Helper()
	s := &edgeSim{name: name}
	srv := httptestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.headers = r.Header.Clone()
		s.body = raw
		s.hits++
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Sim-Relay", name)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(s.servedBody()))
	}))
	s.URL = srv.URL
	return s
}

func httptestServer(t testing.TB, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// relayDo sends one spec-conformant relay request to the gateway: the origin
// travels in X-Relay-Target, the real path+query
// in X-Relay-Path, everything else is pass-through. Keep-alives are disabled
// so one call is one gateway request.
func relayDo(t testing.TB, addr, path, body string, headers map[string]string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relay-Target", "https://api.example.com")
	req.Header.Set("X-Relay-Path", "/v1/messages?beta=true")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   15 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST http://%s%s: %v", addr, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(raw)
}
