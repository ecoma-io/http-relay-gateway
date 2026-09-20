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
