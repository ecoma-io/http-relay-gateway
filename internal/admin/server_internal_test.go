package admin

import (
	"encoding/json"
	"strings"
	"testing"

	"http-relay-gateway/internal/store"
)

func TestTokenNeverExposed(t *testing.T) {
	// relayView redacts deployment tokens to last-4. Deployments only exist
	// from the deployers on, so the redaction is exercised at the unit seam.
	row := store.RelayRow{
		ID: 1, Name: "managed-1", Provider: "vercel", Origin: store.OriginManaged,
		Deployment: &store.DeploymentRow{
			Status: "active", AuthToken: "super-secret-token-abcd",
		},
	}
	view := relayView(row)
	if view.Deployment == nil || view.Deployment.TokenLast4 != "abcd" {
		t.Fatalf("tokenLast4 wrong: %+v", view.Deployment)
	}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "super-secret-token") {
		t.Fatalf("full token leaked into the relay view: %s", raw)
	}
	if strings.Contains(string(raw), "authToken") {
		t.Fatalf("raw auth token field present: %s", raw)
	}
	// Short tokens expose nothing at all.
	short := relayView(store.RelayRow{Deployment: &store.DeploymentRow{AuthToken: "abc"}})
	if short.Deployment.TokenLast4 != "" {
		t.Fatalf("short token leaked: %q", short.Deployment.TokenLast4)
	}
}
