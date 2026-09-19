package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

// RelayConfig is one relay entry in the generated gateway config. A nil
// Active omits the key (the gateway defaults to true).
type RelayConfig struct {
	Name     string
	Provider string
	URL      string
	Active   *bool
}

// GatewayConfig is the full runtime YAML written for one gateway instance.
// Providers maps a provider label to its raw max-body value ("" renders a
// bare providers entry, which applies the gateway default).
type GatewayConfig struct {
	LogLevel         string
	MaxRetries       int
	FailureThreshold int // 0 omits the key; the gateway defaults to 3
	Cooldown         string
	Providers        map[string]string
	Relays           []RelayConfig
}

func defaultGatewayConfig(relays []RelayConfig) GatewayConfig {
	return GatewayConfig{
		LogLevel:   "info",
		MaxRetries: 2,
		Cooldown:   "30s",
		Relays:     relays,
	}
}

func activePtr(v bool) *bool { return &v }

// StatsView is the decoded /stats body.
type StatsView struct {
	Version string      `json:"version"`
	Relays  []RelayView `json:"relays"`
}

// RelayView is one per-relay row from /stats.
type RelayView struct {
	Name      string `json:"name"`
	Provider  string `json:"provider"`
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

// Gateway is one real gateway subprocess with its own config file and port.
type Gateway struct {
	t          testing.TB
	dir        string
	configPath string
	cmd        *exec.Cmd
	output     *lockedBuffer

	Addr string
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

func yamlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func renderConfig(cfg GatewayConfig) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "log-level: %s\n", cfg.LogLevel)
	fmt.Fprintf(&sb, "max-retries: %d\n", cfg.MaxRetries)
	if cfg.FailureThreshold > 0 {
		fmt.Fprintf(&sb, "failure-threshold: %d\n", cfg.FailureThreshold)
	}
	if cfg.Cooldown != "" {
		fmt.Fprintf(&sb, "cooldown: %s\n", cfg.Cooldown)
	}
	if len(cfg.Providers) > 0 {
		sb.WriteString("providers:\n")
		names := make([]string, 0, len(cfg.Providers))
		for name := range cfg.Providers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if raw := cfg.Providers[name]; raw == "" {
				fmt.Fprintf(&sb, "  %s: {}\n", name)
			} else {
				fmt.Fprintf(&sb, "  %s:\n    max-body: %s\n", name, raw)
			}
		}
	}
	sb.WriteString("relays:\n")
	for _, r := range cfg.Relays {
		fmt.Fprintf(&sb, "  - provider: %s\n", r.Provider)
		if r.Name != "" {
			fmt.Fprintf(&sb, "    name: %s\n", r.Name)
		}
		fmt.Fprintf(&sb, "    url: %s\n", yamlQuote(r.URL))
		if r.Active != nil {
			fmt.Fprintf(&sb, "    active: %v\n", *r.Active)
		}
	}
	return sb.String()
}

// NewGateway writes cfg to a temp config file, starts the real binary, and
// waits for /healthz. Every instance owns its port so runs stay collision-free.
func NewGateway(t testing.TB, cfg GatewayConfig) *Gateway {
	return newGateway(t, cfg, nil)
}

// NewGatewayWithEnv is NewGateway with extra bootstrap environment entries
// (for example "SHUTDOWN_GRACE=1s") appended to the standard set.
func NewGatewayWithEnv(t testing.TB, cfg GatewayConfig, extraEnv ...string) *Gateway {
	return newGateway(t, cfg, extraEnv)
}

func newGateway(t testing.TB, cfg GatewayConfig, extraEnv []string) *Gateway {
	t.Helper()
	if testBinaryPath == "" {
		t.Skip("e2e binary not built (short mode?)")
	}
	dir := t.TempDir()
	g := &Gateway{
		t:          t,
		dir:        dir,
		configPath: filepath.Join(dir, "config.yaml"),
		output:     &lockedBuffer{},
		Addr:       freeAddr(t),
	}
	g.writeConfig(cfg)
	cmd := exec.Command(testBinaryPath)
	cmd.Dir = dir
	cmd.Env = append([]string{
		"CONFIG_FILE=" + g.configPath,
		"LISTEN_ADDR=" + g.Addr,
		"PATH=" + os.Getenv("PATH"),
	}, extraEnv...)
	cmd.Stdout = g.output
	cmd.Stderr = g.output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	g.cmd = cmd
	t.Cleanup(g.stop)
	g.waitHealthy(10 * time.Second)
	return g
}

// writeConfig atomically replaces the config file (temp + rename) so the
// gateway's poller never reads a partial write.
func (g *Gateway) writeConfig(cfg GatewayConfig) {
	g.t.Helper()
	install(g.t, g.dir, g.configPath, renderConfig(cfg))
}

// WriteRaw replaces the config with literal content for invalid-config tests.
func (g *Gateway) WriteRaw(content string) {
	g.t.Helper()
	install(g.t, g.dir, g.configPath, content)
}

func install(t testing.TB, dir, configPath, content string) {
	t.Helper()
	tmp := filepath.Join(dir, "config.yaml.tmp")
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.Rename(tmp, configPath); err != nil {
		t.Fatalf("install config: %v", err)
	}
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

// reloadSettle bounds one poll cycle: the gateway's 1s content-hash poll
// interval plus reload work, with comfortable headroom for slow machines.
const reloadSettle = 6 * time.Second

// ReloadConfig rewrites the config file (atomic tmp+rename, modeling an editor
// or bind-mount update) and relies on the gateway's content-hash config poller
// to apply it. It fails when /stats does not report exactly wantRelays within
// one poll cycle.
func (g *Gateway) ReloadConfig(cfg GatewayConfig, wantRelays []string) {
	g.t.Helper()
	g.writeConfig(cfg)
	g.WaitForRelays(wantRelays, reloadSettle)
}

// ReloadRaw installs literal content and returns without waiting: callers
// assert either that the pool stays unchanged (invalid input) or that a warn
// line appeared in the logs.
func (g *Gateway) ReloadRaw(content string) {
	g.t.Helper()
	g.WriteRaw(content)
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
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(g.Logs(), substr) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("logs never contained %q\nlogs:\n%s", substr, g.output.String())
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
