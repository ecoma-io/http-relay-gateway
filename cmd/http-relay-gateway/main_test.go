package main

import (
	"testing"

	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/store"
)

func TestRelayInputLegacy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active bool
	}{
		{"active", true},
		{"inactive", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relay, ok, err := relayInput(store.RelayRow{
				ID: 1, Name: "edge", Provider: "vercel", URL: "https://edge.example.com",
				Active: tc.active, Origin: store.OriginLegacy,
			}, nil, nil, true)
			if err != nil || !ok {
				t.Fatalf("ok=%v err=%v, want the legacy relay in the pool", ok, err)
			}
			if relay.Active != tc.active || relay.Token != "" {
				t.Fatalf("relay = %+v, want active=%v tokenless", relay, tc.active)
			}
		})
	}
}

func TestRelayInputManagedWithoutDeploymentServesOwnURL(t *testing.T) {
	relay, ok, err := relayInput(store.RelayRow{
		ID: 1, Name: "edge", Provider: "vercel", URL: "https://edge.example.com",
		Active: true, Origin: store.OriginManaged,
	}, nil, nil, true)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v, want the deploymentless relay in the pool", ok, err)
	}
	if relay.URL.Host != "edge.example.com" || relay.Token != "" {
		t.Fatalf("relay = %+v, want the row URL and no token", relay)
	}
	if relay.Origin != pool.OriginManaged {
		t.Fatalf("relay origin = %q, want managed", relay.Origin)
	}
}

func TestRelayInputManagedDeploymentOverridesURLAndAuthenticates(t *testing.T) {
	relay, ok, err := relayInput(store.RelayRow{
		ID: 1, Name: "edge", Provider: "vercel", URL: "https://pre-deploy.example.com",
		Active: false, Origin: store.OriginManaged,
		Deployment: &store.DeploymentRow{
			URL: "https://deployed.example.com", AuthToken: "tok-1234", Status: "active",
		},
	}, nil, nil, true)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v, want the deployed relay in the pool", ok, err)
	}
	if relay.URL.Host != "deployed.example.com" {
		t.Fatalf("relay URL = %v, want the deployment URL", relay.URL)
	}
	if relay.Token != "tok-1234" {
		t.Fatalf("relay token = %q, want the deployment token", relay.Token)
	}
	if !relay.Active {
		t.Fatal("an active deployment must force the relay active")
	}
}

func TestRelayInputSkips(t *testing.T) {
	cases := map[string]store.RelayRow{
		"unknown origin": {ID: 1, Name: "x", Provider: "vercel", URL: "https://x.example", Origin: "bogus"},
		"no host":        {ID: 1, Name: "x", Provider: "vercel", URL: "not-a-url", Origin: store.OriginLegacy},
		"ftp scheme":     {ID: 1, Name: "x", Provider: "vercel", URL: "ftp://x.example", Origin: store.OriginLegacy},
		// A deployment that exists but is not active — paused by its
		// platform, stale, unreachable, failed — owns the relay's serving
		// and is not serving: neither its URL nor the row's placeholder may
		// reach the pool.
		"paused deployment": {ID: 1, Name: "x", Provider: "vercel", URL: "https://row.example",
			Origin: store.OriginManaged, InactiveDeployment: true},
	}
	for name, row := range cases {
		if _, ok, _ := relayInput(row, nil, nil, true); ok {
			t.Fatalf("%s: relay unexpectedly joined the pool", name)
		}
	}
}

func TestRelayInputRejectsBadPolicyOnServingRelay(t *testing.T) {
	bad := `{"strip":["connection"]}`
	if _, _, err := relayInput(store.RelayRow{
		ID: 1, Name: "x", Provider: "vercel", URL: "https://x.example",
		Origin: store.OriginLegacy, HeaderPolicy: &bad,
	}, nil, nil, true); err == nil {
		t.Fatal("denied header policy accepted on a serving relay")
	}
}

func TestRelayInputUnreadyNeverJoinsPool(t *testing.T) {
	// The readiness gate is the admission authority: even a fully serving
	// row (legacy active, or managed with an active deployment) contributes
	// nothing until the reconciler verified it end to end.
	for name, row := range map[string]store.RelayRow{
		"legacy active": {ID: 1, Name: "x", Provider: "vercel",
			URL: "https://x.example", Active: true, Origin: store.OriginLegacy},
		"managed active deployment": {ID: 1, Name: "x", Provider: "vercel",
			URL: "https://row.example", Active: true, Origin: store.OriginManaged,
			Deployment: &store.DeploymentRow{URL: "https://deployed.example",
				AuthToken: "tok", Status: "active"}},
	} {
		if _, ok, err := relayInput(row, nil, nil, false); err != nil || ok {
			t.Fatalf("%s: ready=false still joined the pool (ok=%v err=%v)", name, ok, err)
		}
	}
}
