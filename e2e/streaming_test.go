package e2e_test

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"
)

// streamingSim is an SSE stand-in that writes and flushes its first chunk,
// then parks until the test releases it. The park is the whole point: the
// second chunk does not exist until the client has already had the chance
// to observe the first, so any gateway-side buffering turns into a timeout
// here instead of a silent pass.
type streamingSim struct {
	srv     *httptest.Server
	URL     string
	release chan struct{}
}

func newStreamingSim(t *testing.T) *streamingSim {
	t.Helper()
	s := &streamingSim{release: make(chan struct{})}
	s.srv = httptestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: one\n\n"))
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-s.release
		_, _ = w.Write([]byte("data: two\n\n"))
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	}))
	s.URL = s.srv.URL
	return s
}

// openRelayStream starts one relay request and returns the open response so
// a test can read the body incrementally (relayDo would drain it).
func openRelayStream(t *testing.T, addr string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/vercel", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relay-Target", "https://api.example.com")
	req.Header.Set("X-Relay-Path", "/v1/messages?beta=true")
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   30 * time.Second,
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("open relay stream: %v", err)
	}
	return res
}

// scanLines streams a response body into a channel, one line at a time, so
// tests can wait for a specific chunk with a timeout instead of blocking.
func scanLines(t *testing.T, body io.Reader) <-chan string {
	t.Helper()
	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	return lines
}

// waitLine reads streamed lines until want arrives; SSE event separators
// (blank lines) are skipped, and a closed stream before the match is fatal.
func waitLine(t *testing.T, lines <-chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed before %q arrived", want)
			}
			if line == want {
				return
			}
		case <-deadline:
			t.Fatalf("%q never arrived within %s", want, timeout)
		}
	}
}

// TestE2E_ResponseFlushesPerChunk pins the streaming contract end to end:
// the first SSE chunk must reach the client while the relay is still holding
// the second one back. A gateway that buffers the whole response before
// forwarding would deliver nothing until the release and time out here.
func TestE2E_ResponseFlushesPerChunk(t *testing.T) {
	sim := newStreamingSim(t)
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "stream", Provider: "vercel", URL: sim.URL},
	)

	res := openRelayStream(t, g.Addr)
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	lines := scanLines(t, res.Body)

	waitLine(t, lines, "data: one", 10*time.Second)

	// The first chunk is already client-visible; now the relay finishes.
	close(sim.release)
	waitLine(t, lines, "data: two", 10*time.Second)
}

// TestE2E_ShutdownDrainsInFlightStream pins the shutdown contract: SIGTERM
// with a response half-delivered must drain the in-flight stream to the
// client within the grace budget — the chunk the relay releases after the
// signal still has to arrive, not be cut at the first byte boundary.
func TestE2E_ShutdownDrainsInFlightStream(t *testing.T) {
	sim := newStreamingSim(t)
	g := newGatewayWith(t, nil, []string{"SHUTDOWN_GRACE=10s"},
		[]RelaySeed{{Name: "stream", Provider: "vercel", URL: sim.URL}},
	)

	res := openRelayStream(t, g.Addr)
	defer func() { _ = res.Body.Close() }()
	lines := scanLines(t, res.Body)

	waitLine(t, lines, "data: one", 10*time.Second)

	// Signal shutdown mid-stream, then let the relay finish: the draining
	// server must keep proxying this response until the relay closes.
	if err := g.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal SIGTERM: %v", err)
	}
	close(sim.release)

	// The post-signal chunk must still arrive — waitLine treats a stream
	// closed before the match as the failure it is.
	waitLine(t, lines, "data: two", 10*time.Second)
	g.stop() // reap the process; it exits on its own once the drain completes
}

// TestE2E_AllRelaysDownIsBestEffort502 pins the exhausted-pool answer: with
// every relay dead the gateway still attempts the candidates (best effort,
// never an instant 503) and only then answers 502 with the sanitized error.
func TestE2E_AllRelaysDownIsBestEffort502(t *testing.T) {
	g := NewGatewayWithRelays(t,
		RelaySeed{Name: "down-1", Provider: "vercel", URL: deadRelayURL(t)},
		RelaySeed{Name: "down-2", Provider: "vercel", URL: deadRelayURL(t)},
	)

	status, _, body := relayDo(t, g.Addr, "/vercel", `{}`, nil)
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 with every relay down\nlogs:\n%s", status, g.Logs())
	}
	if !strings.Contains(body, "all attempts failed") {
		t.Fatalf("body = %q, want the all-attempts error", body)
	}
	// Both candidates were genuinely attempted and counted, not skipped.
	g.WaitForCondition(applySettle, "both dead relays recorded failures", func(st *StatsView) bool {
		r1, ok1 := st.relay("down-1")
		r2, ok2 := st.relay("down-2")
		return ok1 && ok2 && r1.Failures >= 1 && r2.Failures >= 1
	})
}
