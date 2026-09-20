package deploy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestProbeClassification pins the three-way verdict a probe can produce:
// a relay worker's version JSON, a non-worker HTTP answer (a platform pause
// page — 402 + x-vercel-error DEPLOYMENT_DISABLED, the shape a real
// quota-paused Vercel deployment answers — or anything else that is not our
// worker), and a transport failure. The reconciler's pause/stale/unreachable
// branches key on exactly these classes, so the probe must not blur them.
func TestProbeClassification(t *testing.T) {
	for _, tc := range []struct {
		name          string
		handler       http.HandlerFunc
		wantVersion   string
		wantStatus    int
		wantMarker    string
		wantNotWorker bool
	}{
		{
			name: "worker answers its version",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"version":"7"}`))
			},
			wantVersion: "7",
			wantStatus:  http.StatusOK,
		},
		{
			name: "platform pause page with marker",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Vercel-Error", "DEPLOYMENT_DISABLED")
				w.WriteHeader(http.StatusPaymentRequired)
				_, _ = w.Write([]byte("Payment required\n\nDEPLOYMENT_DISABLED\n"))
			},
			wantStatus:    http.StatusPaymentRequired,
			wantMarker:    "DEPLOYMENT_DISABLED",
			wantNotWorker: true,
		},
		{
			name: "non-worker answer without a marker",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
			},
			wantStatus:    http.StatusForbidden,
			wantNotWorker: true,
		},
		{
			name: "200 that is not worker JSON",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("<html>someone else's app</html>"))
			},
			wantStatus:    http.StatusOK,
			wantNotWorker: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			t.Cleanup(srv.Close)

			answer, err := Probe(context.Background(), srv.Client(), srv.URL)
			if err != nil {
				t.Fatalf("Probe: %v", err)
			}
			if answer.Version != tc.wantVersion || answer.Status != tc.wantStatus || answer.Marker != tc.wantMarker {
				t.Fatalf("answer = %+v, want version %q status %d marker %q",
					answer, tc.wantVersion, tc.wantStatus, tc.wantMarker)
			}
			if notWorker := answer.NotWorkerErr(); (notWorker != nil) != tc.wantNotWorker {
				t.Fatalf("NotWorkerErr = %v, want notWorker %v", notWorker, tc.wantNotWorker)
			}
		})
	}

	// A transport failure is an error, never a classification: the probe
	// must not guess between "paused" and "unreachable" without an answer.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + closed.Addr().String()
	_ = closed.Close()
	if _, err := Probe(context.Background(), &http.Client{}, url); err == nil {
		t.Fatal("probe of a refusing endpoint must be a transport error")
	}
}

// TestForwardProbeRoundTrip pins the forwarding probe's wire shape: the
// relay must receive its own origin as X-Relay-Target, the version path as
// X-Relay-Path, and the deployment token in X-Relay-Token — the same
// request a real client's production request produces, only pointed at the
// relay itself. Only a 200 whose body is a JSON object counts as a
// completed round trip.
func TestForwardProbeRoundTrip(t *testing.T) {
	var got struct {
		target, path, token string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.target = r.Header.Get("X-Relay-Target")
		got.path = r.Header.Get("X-Relay-Path")
		got.token = r.Header.Get("X-Relay-Token")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"7"}`))
	}))
	t.Cleanup(srv.Close)

	answer, err := ForwardProbe(context.Background(), srv.Client(), srv.URL+"/ignored-path", "secret-token")
	if err != nil {
		t.Fatalf("ForwardProbe: %v", err)
	}
	if answer.Status != http.StatusOK || !answer.JSON {
		t.Fatalf("answer = %+v, want 200 with a JSON body", answer)
	}
	wantTarget := srv.URL // scheme://host, path discarded
	if got.target != wantTarget || got.path != forwardProbePath || got.token != "secret-token" {
		t.Fatalf("relay saw target %q path %q token %q; want %q %q %q",
			got.target, got.path, got.token, wantTarget, forwardProbePath, "secret-token")
	}
}

// TestForwardProbeClassification pins the answers the readiness gate keys
// on: the worker's documented 404 for a wrong token, its 502 for a failed
// upstream fetch, and a 200 that is not relay JSON. Transport failures stay
// errors, never classifications.
func TestForwardProbeClassification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus int
		wantJSON   bool
	}{
		{
			name: "wrong token answered 404",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"not found"}`))
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "upstream fetch failed answered 502",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"error":"upstream fetch failed"}`))
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "200 that is not relay JSON",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("just text"))
			},
			wantStatus: http.StatusOK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			t.Cleanup(srv.Close)
			answer, err := ForwardProbe(context.Background(), srv.Client(), srv.URL, "token")
			if err != nil {
				t.Fatalf("ForwardProbe: %v", err)
			}
			if answer.Status != tc.wantStatus || answer.JSON != tc.wantJSON {
				t.Fatalf("answer = %+v, want status %d json %v", answer, tc.wantStatus, tc.wantJSON)
			}
		})
	}

	// Tokenless probes (legacy relay rows) must not send an X-Relay-Token
	// header at all — the gateway never invents one.
	var seenToken bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenToken = r.Header.Get("X-Relay-Token") != ""
		w.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)
	if _, err := ForwardProbe(context.Background(), srv.Client(), srv.URL, ""); err != nil {
		t.Fatalf("ForwardProbe: %v", err)
	}
	if seenToken {
		t.Fatal("tokenless probe sent an X-Relay-Token header")
	}

	// A refusing endpoint is a transport error, exactly like the version
	// probe: the caller must not guess a verdict without an answer.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + closed.Addr().String()
	_ = closed.Close()
	if _, err := ForwardProbe(context.Background(), &http.Client{}, url, ""); err == nil {
		t.Fatal("forward probe of a refusing endpoint must be a transport error")
	}
}
