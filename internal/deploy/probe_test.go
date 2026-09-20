package deploy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// versionServer is a stand-in relay worker: it answers every path with the
// given status, body and headers, so the same server doubles as a platform
// API fake. Path assertions belong in fakeAPI, not here.
func versionServer(t *testing.T, status int, body string, header map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func versionJSON(v string) string { return `{"version":"` + v + `"}` }

func TestProbeWorkerAnswer(t *testing.T) {
	srv := versionServer(t, http.StatusOK, versionJSON("1.2.3"), nil)
	answer, err := Probe(context.Background(), ProbeClient(time.Second), srv.URL)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if answer.Version != "1.2.3" || answer.Status != http.StatusOK {
		t.Errorf("answer = %+v, want version 1.2.3 status 200", answer)
	}
	if err := answer.NotWorkerErr(); err != nil {
		t.Errorf("NotWorkerErr = %v, want nil for a worker answer", err)
	}
	if answer.Suspended() {
		t.Error("Suspended() = true for a healthy worker answer")
	}
}

func TestProbeNonWorkerAnswers(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{name: "html page instead of the worker", status: http.StatusOK, body: "<html>someone else</html>"},
		{name: "empty 200 body", status: http.StatusOK, body: ""},
		{name: "json without a version", status: http.StatusOK, body: `{"other":1}`},
		{name: "platform error page", status: http.StatusServiceUnavailable, body: "unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := versionServer(t, tc.status, tc.body, nil)
			answer, err := Probe(context.Background(), ProbeClient(time.Second), srv.URL)
			if err != nil {
				t.Fatalf("Probe: %v — an HTTP answer is classified, never an error", err)
			}
			if answer.Version != "" {
				t.Errorf("Version = %q, want empty for a non-worker answer", answer.Version)
			}
			if answer.Status != tc.status {
				t.Errorf("Status = %d, want %d", answer.Status, tc.status)
			}
			notWorker := answer.NotWorkerErr()
			if notWorker == nil {
				t.Fatal("NotWorkerErr = nil, want an error describing the non-worker answer")
			}
			if !strings.Contains(notWorker.Error(), "not a relay worker") {
				t.Errorf("NotWorkerErr = %q, want it to name the non-worker answer", notWorker)
			}
		})
	}
}

func TestProbeTransportFailureIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // closed: the connection is refused, the transport-failure case
	if _, err := Probe(context.Background(), ProbeClient(time.Second), srv.URL); err == nil {
		t.Fatal("Probe on a dead relay = nil error, want a transport failure")
	}
}

func TestProbeSuspensionSignatures(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		header    map[string]string
		suspended bool
	}{
		{
			name:      "vercel deployment-disabled marker",
			status:    http.StatusForbidden,
			header:    map[string]string{"X-Vercel-Error": "DEPLOYMENT_DISABLED"},
			suspended: true,
		},
		{
			name:      "payment required",
			status:    http.StatusPaymentRequired,
			suspended: true,
		},
		{
			name:      "other vercel markers are not suspensions",
			status:    http.StatusNotFound,
			header:    map[string]string{"X-Vercel-Error": "NOT_FOUND"},
			suspended: false,
		},
		{
			name:      "plain 503 is not a suspension",
			status:    http.StatusServiceUnavailable,
			suspended: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := versionServer(t, tc.status, "body", tc.header)
			answer, err := Probe(context.Background(), ProbeClient(time.Second), srv.URL)
			if err != nil {
				t.Fatalf("Probe: %v", err)
			}
			if got := answer.Suspended(); got != tc.suspended {
				t.Errorf("Suspended() = %v (answer %+v), want %v", got, answer, tc.suspended)
			}
		})
	}
}

// fakeRelay answers a relay-spec request the way the worker sims do, and
// asserts the wire shape ForwardProbe must produce.
func fakeRelay(t *testing.T, key string, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Relay-Target"); got != "http://"+r.Host {
			t.Errorf("X-Relay-Target = %q, want the relay origin %q", got, "http://"+r.Host)
		}
		if got := r.Header.Get("X-Relay-Path"); got != "/__relay/version" {
			t.Errorf("X-Relay-Path = %q, want /__relay/version", got)
		}
		if got := r.Header.Get("X-Relay-Token"); key == "" && got != "" {
			t.Errorf("X-Relay-Token = %q with an empty relay key, want the header absent", got)
		} else if key != "" && got != key {
			t.Errorf("X-Relay-Token = %q, want the relay key %q", got, key)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestForwardProbeCompletedRoundTrip(t *testing.T) {
	srv := fakeRelay(t, "relay-key", http.StatusOK, versionJSON("9.9.9"))
	answer, err := ForwardProbe(context.Background(), ProbeClient(time.Second), srv.URL, "relay-key")
	if err != nil {
		t.Fatalf("ForwardProbe: %v", err)
	}
	if answer.Status != http.StatusOK || !answer.JSON {
		t.Errorf("answer = %+v, want status 200 JSON", answer)
	}
}

func TestForwardProbeClassifiesHTTPAnswers(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{name: "wrong relay key", status: http.StatusNotFound, body: ""},
		{name: "upstream fetch failed", status: http.StatusBadGateway, body: ""},
		{name: "200 with a non-JSON body", status: http.StatusOK, body: "plain text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakeRelay(t, "", tc.status, tc.body)
			answer, err := ForwardProbe(context.Background(), ProbeClient(time.Second), srv.URL, "")
			if err != nil {
				t.Fatalf("ForwardProbe: %v — HTTP answers are classified, never errors", err)
			}
			if answer.Status != tc.status {
				t.Errorf("Status = %d, want %d", answer.Status, tc.status)
			}
			if answer.JSON {
				t.Errorf("JSON = true for %q, want false", tc.body)
			}
		})
	}
}

func TestForwardProbeRejectsBadTargets(t *testing.T) {
	cases := []struct {
		name string
		base string
	}{
		{name: "relative URL without a host", base: "not-a-url"},
		{name: "unsupported scheme", base: "ftp://relay.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ForwardProbe(context.Background(), ProbeClient(time.Second), tc.base, ""); err == nil {
				t.Fatalf("ForwardProbe(%q) = nil error, want a rejection", tc.base)
			}
		})
	}
}

func TestForwardProbeTransportFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	if _, err := ForwardProbe(context.Background(), ProbeClient(time.Second), srv.URL, ""); err == nil {
		t.Fatal("ForwardProbe on a dead relay = nil error, want a transport failure")
	}
}

func TestProbeClientTransportHasNoProxy(t *testing.T) {
	client := ProbeClient(time.Second)
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("ProbeClient transport = %T, want *http.Transport", client.Transport)
	}
	if tr.Proxy != nil {
		t.Error("ProbeClient transport Proxy != nil — probes must never ride ambient proxies")
	}
}

// recordingProxy is an httptest reverse proxy that counts every request it
// sees. It stands in for an ambient HTTP_PROXY.
func recordingProxy(t *testing.T, target *url.URL) (proxyURL string, hits *atomic.Int64) {
	t.Helper()
	hits = &atomic.Int64{}
	proxy := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, hits
}

// proxyFollows probes the target with a proxy-honoring client, then with
// ProbeClient, and asserts only the first ever touched the proxy. The
// proxy-honoring client is built with an explicit Proxy function rather than
// from the environment on purpose: net/http caches the proxy environment on
// first use process-wide, and stdlib's own loopback exclusion would make an
// env-based assertion silently vacuous for httptest targets.
func proxyFollows(t *testing.T, target string, hits *atomic.Int64, proxyURL *url.URL) {
	t.Helper()
	honoring := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: func(*http.Request) (*url.URL, error) { return proxyURL, nil },
		},
	}

	// Ambient proxies carry platform management traffic — a client wired for
	// one must be seen by the recorder.
	if _, err := Probe(context.Background(), honoring, target); err != nil {
		t.Fatalf("Probe via the proxy-honoring client: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("proxy saw %d requests after the honoring client, want 1", got)
	}

	// The probe transport must never touch the proxy, whatever the ambient
	// configuration says.
	answer, err := Probe(context.Background(), ProbeClient(5*time.Second), target)
	if err != nil {
		t.Fatalf("Probe via ProbeClient: %v", err)
	}
	if answer.Version == "" {
		t.Errorf("Probe via ProbeClient answered %+v, want the worker version", answer)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("proxy saw %d requests after ProbeClient ran, want still 1 — the probe went direct", got)
	}

	// ForwardProbe rides the same probe transport.
	if _, err := ForwardProbe(context.Background(), ProbeClient(5*time.Second), target, ""); err != nil {
		t.Fatalf("ForwardProbe via ProbeClient: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("proxy saw %d requests after ForwardProbe ran, want still 1", got)
	}
}

func TestProbeTransportIgnoresAmbientProxy(t *testing.T) {
	target := versionServer(t, http.StatusOK, versionJSON("1.0.0"), nil)
	proxyURL, hits := recordingProxy(t, mustParseURL(t, target.URL))

	// The ambient configuration is fully pointed at the recorder; the probe
	// transport's Proxy being nil must make it irrelevant.
	t.Setenv("HTTP_PROXY", proxyURL)
	t.Setenv("HTTPS_PROXY", proxyURL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	t.Setenv("ALL_PROXY", "")
	t.Setenv("all_proxy", "")

	proxy, err := url.Parse(proxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	proxyFollows(t, target.URL, hits, proxy)
}

func TestFactoryUsesClientForPlatformCalls(t *testing.T) {
	api := versionServer(t, http.StatusOK, "{}", nil) // answers any path the fake platform needs
	proxyURL, hits := recordingProxy(t, mustParseURL(t, api.URL))

	t.Setenv(VercelAPIBaseEnv, api.URL)
	// The management plane is the opposite contract of the probes: whatever
	// client the operator handed the factory is used verbatim, proxies
	// included.
	honoring := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: func(*http.Request) (*url.URL, error) { return mustParseURL(t, proxyURL), nil },
		},
	}
	client, err := NewFactory(honoring).For(PlatformVercel, Credential{Token: "t"})
	if err != nil {
		t.Fatalf("Factory.For: %v", err)
	}
	discovery, err := client.Discover(context.Background(), "web-relay")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !discovery.Exists || discovery.URL != "https://web-relay.vercel.app" {
		t.Errorf("discovery = %+v, want the existing project with its stable URL", discovery)
	}
	if got := hits.Load(); got < 1 {
		t.Errorf("proxy saw %d requests, want the platform call routed through it", got)
	}
}

func TestFactoryUnknownPlatform(t *testing.T) {
	if _, err := NewFactory(nil).For("flyio", Credential{}); err == nil {
		t.Fatal("Factory.For(flyio) = nil error, want unknown-platform rejection")
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}
