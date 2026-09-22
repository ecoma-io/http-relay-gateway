// Package e2e is the black-box desired-state matrix: every test drives the
// real http-relay-gateway binary as a subprocess — HTTP only, plus the
// process's structured stdout — against in-process fakes that speak the
// platforms' REST shapes and worker sims that answer for the deployed
// relays. Nothing from internal/ is imported or linked: the binary under
// test is the only production code in the loop.
//
// Run: go test ./e2e/ -count=1 (skip with -short).
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// binPath is the gateway binary TestMain builds once for the whole run.
var binPath string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Fprintln(os.Stderr, "e2e: skipping matrix in -short mode")
		os.Exit(0)
	}
	dir, err := os.MkdirTemp("", "e2e-gateway-bin")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: temp dir: %v\n", err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "http-relay-gateway")
	build := exec.Command("go", "build", "-ldflags", "-X main.version=e2e-build",
		"-o", binPath, "../cmd/http-relay-gateway")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: build gateway: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// --- gateway process ---

// gatewayProc is one running gateway subprocess and everything the tests
// need to observe it: the listener URL, its desired-state directory, the
// relay key it booted with, and a rolling copy of its stdout+stderr.
type gatewayProc struct {
	t    *testing.T
	base string
	dir  string
	key  string

	cmd     *exec.Cmd
	exited  chan error
	stopMu  sync.Mutex
	stopped bool

	logMu  sync.Mutex
	logBuf bytes.Buffer
}

// gwOptions shapes one gateway run.
type gwOptions struct {
	// config is the desired-state YAML; empty starts with the file absent
	// (the legal empty-fleet boot test 1 exercises).
	config string
	// key is the relay key; it reaches the process through
	// RELAY_AUTH_TOKEN, or RELAY_AUTH_TOKEN_FILE when keyFile is set.
	key     string
	keyFile bool
	// extraEnv rides on top of a scrubbed copy of the test environment.
	extraEnv map[string]string
}

// startGateway builds, envures and launches one gateway against fake, and
// registers cleanup (stop + log scan) on the test.
func startGateway(t *testing.T, fake *fakeEdge, o gwOptions) *gatewayProc {
	t.Helper()
	port := freePort(t)
	g := &gatewayProc{
		t:    t,
		base: "http://127.0.0.1:" + port,
		dir:  t.TempDir(),
		key:  o.key,
	}
	cfgPath := filepath.Join(g.dir, "config.yaml")
	if o.config != "" {
		g.writeConfig(o.config)
	}

	env := scrubbedEnv()
	set := func(k, v string) { env = append(env, k+"="+v) }
	set("LISTEN_ADDR", "127.0.0.1:"+port)
	set("CONFIG_FILE", cfgPath)
	set("SHUTDOWN_GRACE", "5s")
	for _, k := range []string{
		"VERCEL_API_BASE", "CLOUDFLARE_API_BASE", "DENO_API_BASE",
		"VERCEL_URL_BASE", "CLOUDFLARE_URL_BASE", "DENO_URL_BASE",
	} {
		set(k, fake.base)
	}
	if o.keyFile {
		keyPath := filepath.Join(g.dir, "relay-key")
		if err := os.WriteFile(keyPath, []byte(o.key+"\n"), 0o600); err != nil {
			t.Fatalf("write relay key file: %v", err)
		}
		set("RELAY_AUTH_TOKEN_FILE", keyPath)
	} else {
		set("RELAY_AUTH_TOKEN", o.key)
	}
	for k, v := range o.extraEnv {
		set(k, v)
	}

	g.cmd = exec.Command(binPath)
	g.cmd.Env = env
	stdout, err := g.cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderr, err := g.cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := g.cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	g.exited = make(chan error, 1)
	go func() { g.exited <- g.cmd.Wait() }()
	go g.tail(stdout)
	go g.tail(stderr)

	g.awaitServing(10 * time.Second)
	t.Cleanup(func() {
		_ = g.stop(10 * time.Second)
		g.scanLog(t, fake)
	})
	return g
}

// scrubbedEnv is the test environment without proxies and without anything
// the gateway would read, so a developer's ambient VERCEL_* or RELAY_*
// variables can never leak into a run.
func scrubbedEnv() []string {
	out := make([]string, 0, len(os.Environ()))
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		upper := strings.ToUpper(key)
		switch {
		case strings.Contains(upper, "PROXY"):
			continue
		case strings.HasPrefix(upper, "RELAY_"),
			strings.HasPrefix(upper, "VERCEL_"),
			strings.HasPrefix(upper, "CLOUDFLARE_"),
			strings.HasPrefix(upper, "DENO_"),
			upper == "LISTEN_ADDR", upper == "CONFIG_FILE", upper == "SHUTDOWN_GRACE":
			continue
		}
		out = append(out, kv)
	}
	return out
}

func (g *gatewayProc) tail(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		g.logMu.Lock()
		if g.logBuf.Len() > 8<<20 {
			g.logBuf.Reset()
		}
		g.logBuf.WriteString(sc.Text())
		g.logBuf.WriteByte('\n')
		g.logMu.Unlock()
	}
}

func (g *gatewayProc) logDump() string {
	g.logMu.Lock()
	defer g.logMu.Unlock()
	return g.logBuf.String()
}

// awaitServing waits for /healthz to answer, failing early (with the
// process log) when the process dies instead.
func (g *gatewayProc) awaitServing(timeout time.Duration) {
	g.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-g.exited:
			g.t.Fatalf("gateway exited during startup: %v\nlog:\n%s", err, g.logDump())
		default:
		}
		resp, err := http.Get(g.base + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	g.t.Fatalf("gateway did not start serving within %s\nlog:\n%s", timeout, g.logDump())
}

// stop terminates the process (SIGTERM, then SIGKILL past timeout) and
// returns its exit status: nil only for a clean exit. Idempotent — the
// second call returns immediately; the first caller already observed the
// exit status.
func (g *gatewayProc) stop(timeout time.Duration) error {
	g.stopMu.Lock()
	if g.stopped {
		g.stopMu.Unlock()
		return nil
	}
	g.stopped = true
	g.stopMu.Unlock()
	_ = g.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-g.exited:
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 0 {
			return nil
		}
		return err
	case <-time.After(timeout):
		_ = g.cmd.Process.Kill()
		return <-g.exited
	}
}

// exitOK asserts a clean SIGTERM exit within timeout.
func (g *gatewayProc) exitOK(timeout time.Duration) {
	g.t.Helper()
	if err := g.stop(timeout); err != nil {
		g.t.Fatalf("gateway did not exit cleanly: %v\nlog:\n%s", err, g.logDump())
	}
}

// scanLog fails the test when a secret or a relay/fake URL appears anywhere
// in the process log — including the transport-failure paths, whose Go
// *url.Error strings the sanitizer strips down to `Get "…": cause`.
func (g *gatewayProc) scanLog(t *testing.T, fake *fakeEdge) {
	t.Helper()
	log := g.logDump()
	markers := []string{g.key}
	markers = append(markers, fake.secretMarkers()...)
	markers = append(markers, fake.urlMarkers()...)
	for _, m := range markers {
		if m == "" {
			continue
		}
		if strings.Contains(log, m) {
			for _, line := range strings.Split(log, "\n") {
				if strings.Contains(line, m) {
					t.Errorf("gateway log leaks %q: %s", leakLabel(m, g.key, fake), line)
					break
				}
			}
		}
	}
}

// leakLabel names a leaked marker without printing the secret itself.
func leakLabel(marker, key string, fake *fakeEdge) string {
	switch marker {
	case key:
		return "relay key"
	default:
		return fake.markerLabel(marker)
	}
}

// --- HTTP helpers ---

var relayClient = &http.Client{Timeout: 30 * time.Second}

// do issues one request against the gateway and returns it with the body
// fully read.
func (g *gatewayProc) do(t *testing.T, method, path string, headers map[string]string, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, g.base+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := relayClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	return resp, raw
}

// --- typed /stats ---

type statsDoc struct {
	Version      string          `json:"version"`
	RelayVersion string          `json:"relayVersion"`
	Relays       []relayStatsRow `json:"relays"`
	Readiness    struct {
		Ready       bool `json:"ready"`
		ReadyRelays int  `json:"readyRelays"`
	} `json:"readiness"`
	Lifecycle []lifecycleRow `json:"lifecycle"`
}

// relayStatsRow is one relay's passive-health row: the serving counters and
// the cooldown view (/stats never carries the relay URL or a credential).
type relayStatsRow struct {
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Healthy   bool   `json:"healthy"`
	MaxBody   int64  `json:"maxBody"`
	Requests  int64  `json:"requests"`
	Failures  int64  `json:"failures"`
	LastError string `json:"lastError,omitempty"`
}

type lifecycleRow struct {
	Name       string `json:"name"`
	Provider   string `json:"provider"`
	State      string `json:"state"`
	Reason     string `json:"reason,omitempty"`
	Generation uint64 `json:"generation"`
}

// relayRow returns one relay's passive-health row from a snapshot.
func relayRow(d *statsDoc, provider, name string) (relayStatsRow, bool) {
	for _, row := range d.Relays {
		if row.Provider == provider && row.Name == name {
			return row, true
		}
	}
	return relayStatsRow{}, false
}

func (g *gatewayProc) stats(t *testing.T) *statsDoc {
	t.Helper()
	resp, raw := g.do(t, http.MethodGet, "/stats", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/stats: status %d body %s", resp.StatusCode, raw)
	}
	d := &statsDoc{}
	if err := json.Unmarshal(raw, d); err != nil {
		t.Fatalf("/stats: decode %s: %v", raw, err)
	}
	return d
}

func lifecycleOf(d *statsDoc, provider, name string) (lifecycleRow, bool) {
	for _, row := range d.Lifecycle {
		if row.Provider == provider && row.Name == name {
			return row, true
		}
	}
	return lifecycleRow{}, false
}

// --- waiting ---

func waitFor(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// waitForReady waits until /readyz answers 200.
func (g *gatewayProc) waitForReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, "/readyz to answer 200", func() bool {
		resp, raw := g.do(t, http.MethodGet, "/readyz", nil, nil)
		return resp.StatusCode == http.StatusOK && strings.Contains(string(raw), `"ready":true`)
	})
}

// waitForZeroReady waits until /readyz answers 503 with an empty pool.
func (g *gatewayProc) waitForZeroReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, "/readyz to answer 503", func() bool {
		resp, _ := g.do(t, http.MethodGet, "/readyz", nil, nil)
		return resp.StatusCode == http.StatusServiceUnavailable
	})
}

// waitForStats polls /stats until pred holds, returning the matching
// snapshot; on timeout it fails with the last snapshot and the process log.
func (g *gatewayProc) waitForStats(t *testing.T, timeout time.Duration, what string, pred func(*statsDoc) bool) *statsDoc {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *statsDoc
	for time.Now().Before(deadline) {
		last = g.stats(t)
		if pred(last) {
			return last
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s\nlast /stats: %+v\ngateway log:\n%s",
		timeout, what, last, g.logDump())
	return nil
}

// waitForLifecycle waits for one relay's lifecycle row to reach the given
// state (and reason, when non-empty).
func (g *gatewayProc) waitForLifecycle(t *testing.T, provider, name, state, reason string, timeout time.Duration) lifecycleRow {
	t.Helper()
	label := fmt.Sprintf("relay %s/%s to reach state %s", provider, name, state)
	if reason != "" {
		label += fmt.Sprintf(" (reason %s)", reason)
	}
	doc := g.waitForStats(t, timeout, label, func(d *statsDoc) bool {
		row, ok := lifecycleOf(d, provider, name)
		return ok && row.State == state && (reason == "" || row.Reason == reason)
	})
	row, _ := lifecycleOf(doc, provider, name)
	return row
}

// --- config and secrets ---

// writeConfig replaces the desired-state file atomically (the poller reads
// on a one-second tick, rename keeps every read whole).
func (g *gatewayProc) writeConfig(content string) {
	g.t.Helper()
	tmp := filepath.Join(g.dir, fmt.Sprintf("config-%d.yaml", time.Now().UnixNano()))
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		g.t.Fatalf("write config temp: %v", err)
	}
	if err := os.Rename(tmp, filepath.Join(g.dir, "config.yaml")); err != nil {
		g.t.Fatalf("rename config: %v", err)
	}
}

// configYAML assembles a desired-state file: fast test cadences plus the
// given relay blocks.
func configYAML(relays ...string) string {
	return renderConfig(harnessSettings(nil, nil), relays)
}

// configYAMLOverrides assembles a desired-state file from the harness's
// fast cadences with individual settings overridden — the failover and
// passive-health tests need shapes the shared defaults cannot express (a
// quiet verify tick so the verified layer stays out of the way, a
// one-strike failure threshold, streaming switched on). An override that
// names no harness setting fails the test: a typo must not silently run
// the scenario on the default cadence.
func configYAMLOverrides(t *testing.T, overrides map[string]string, relays ...string) string {
	t.Helper()
	return renderConfig(harnessSettings(t, overrides), relays)
}

// harnessSettings is the shared settings block, with overrides applied.
func harnessSettings(t *testing.T, overrides map[string]string) [][2]string {
	settings := [][2]string{
		{"log_level", "debug"},
		{"failure_threshold", "2"},
		{"cooldown", "2s"},
		{"max_retries", "1"},
		{"stream_threshold_bytes", "0"},
		{"response_header_timeout", "0"},
		{"verify_interval", "1s"},
		{"revive_scan_interval", "2s"},
		{"verify_backoff_base", "200ms"},
		{"verify_backoff_max", "1s"},
		{"verify_recover_max", "300ms"},
		{"verify_demote_after", "2"},
	}
	for key, value := range overrides {
		known := false
		for i := range settings {
			if settings[i][0] == key {
				settings[i][1] = value
				known = true
				break
			}
		}
		if !known {
			if t == nil {
				panic("configYAML override " + key + " is not a harness setting")
			}
			t.Fatalf("config override %q is not a harness setting", key)
		}
	}
	return settings
}

// renderConfig writes the settings block plus the given relay blocks.
func renderConfig(settings [][2]string, relays []string) string {
	var b strings.Builder
	b.WriteString("settings:\n")
	for _, kv := range settings {
		fmt.Fprintf(&b, "  %s: %s\n", kv[0], kv[1])
	}
	if len(relays) > 0 {
		b.WriteString("relays:\n")
		for _, r := range relays {
			b.WriteString(r)
		}
	}
	return b.String()
}

// relayBlock renders one relay entry; secret is the indented token or
// token_file line(s). Deno relays carry the organization pin the Deploy API
// requires — it has no route that resolves the organization from a token.
func relayBlock(name, provider, secret string) string {
	org := ""
	if provider == "deno" {
		org = "    organization: " + e2eDenoOrgID + "\n"
	}
	return fmt.Sprintf("  - name: %s\n    provider: %s\n%s%s", name, provider, secret, org)
}

// tokenEnvLine is a ${VAR} credential reference resolved from the process
// environment at load time.
func tokenEnvLine(varName string) string { return "    token: ${" + varName + "}\n" }

// tokenFileLine points the credential at a secret file.
func tokenFileLine(path string) string { return "    token_file: " + path + "\n" }

// --- small utilities ---

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return strconv.Itoa(port)
}

// randSuffix keeps per-test credentials and marker strings unique, so a
// leak in one run can never be masked by another run's values.
func randSuffix(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.IntN(len(alphabet))]
	}
	return string(b)
}

// runSubcommand executes one gateway subcommand (healthcheck,
// readinesscheck) against a running gatewayProc — the same process image,
// pointed at the running listener by LISTEN_ADDR alone — and returns its
// exit code with the combined output.
func runSubcommand(t *testing.T, g *gatewayProc, arg string) (int, string) {
	t.Helper()
	cmd := exec.Command(binPath, arg)
	env := scrubbedEnv()
	env = append(env, "LISTEN_ADDR="+strings.TrimPrefix(g.base, "http://"))
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run %q: %v (%s)", arg, err, out)
	}
	return code, string(out)
}
