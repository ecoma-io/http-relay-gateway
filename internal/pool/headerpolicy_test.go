package pool

import "testing"

func TestParseHeaderPolicyNilAndEmpty(t *testing.T) {
	for name, raw := range map[string]*string{
		"nil":          nil,
		"empty":        ptr(""),
		"blank":        ptr("  "),
		"empty object": ptr(`{}`),
	} {
		policy, err := ParseHeaderPolicy(raw)
		if err != nil || policy != nil {
			t.Fatalf("%s: policy = %+v (err %v), want nil/nil (verbatim)", name, policy, err)
		}
	}
}

func ptr(s string) *string { return &s }

func TestParseHeaderPolicyCanonicalizes(t *testing.T) {
	raw := ptr(`{"strip":["x-internal-trace"],"set":{"x-relayed-by":["gateway"]}}`)
	policy, err := ParseHeaderPolicy(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Strip) != 1 || policy.Strip[0] != "X-Internal-Trace" {
		t.Fatalf("strip = %v, want canonical X-Internal-Trace", policy.Strip)
	}
	if got := policy.Set["X-Relayed-By"]; len(got) != 1 || got[0] != "gateway" {
		t.Fatalf("set = %v", policy.Set)
	}
}

func TestParseHeaderPolicyRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"invalid json":      `{"strip":`,
		"denied strip":      `{"strip":["host"]}`,
		"denied set":        `{"set":{"content-length":["1"]}}`,
		"hop-by-hop strip":  `{"strip":["connection"]}`,
		"relay spec strip":  `{"strip":["x-relay-target"]}`,
		"relay token strip": `{"strip":["x-relay-token"]}`,
		"empty set value":   `{"set":{"x-relayed-by":[""]}}`,
		"blank set value":   `{"set":{"x-a":["  "]}}`,
	}
	for name, raw := range cases {
		if _, err := ParseHeaderPolicy(ptr(raw)); err == nil {
			t.Fatalf("%s: accepted %q, want error", name, raw)
		}
	}
}

func TestInheritHeaderPolicy(t *testing.T) {
	t.Run("nil sides pass through", func(t *testing.T) {
		relayPolicy, _ := ParseHeaderPolicy(ptr(`{"strip":["x-a"]}`))
		if got := InheritHeaderPolicy(relayPolicy, nil); got != relayPolicy {
			t.Fatal("provider nil: want the relay policy back")
		}
		providerPolicy, _ := ParseHeaderPolicy(ptr(`{"strip":["x-b"]}`))
		if got := InheritHeaderPolicy(nil, providerPolicy); got != providerPolicy {
			t.Fatal("relay nil: want the provider policy back")
		}
		if got := InheritHeaderPolicy(nil, nil); got != nil {
			t.Fatal("both nil: want nil")
		}
	})

	t.Run("strip unions and wins over inherited set", func(t *testing.T) {
		provider, _ := ParseHeaderPolicy(ptr(`{"strip":["x-p"],"set":{"x-a":["provider"],"x-keep":["p"]}}`))
		relay, _ := ParseHeaderPolicy(ptr(`{"strip":["x-a"],"set":{"x-b":["relay"]}}`))
		merged := InheritHeaderPolicy(relay, provider)
		if merged == nil {
			t.Fatal("want a merged policy")
		}
		// Union of strips, provider order first (X-B lives in the relay's set).
		if len(merged.Strip) != 2 || merged.Strip[0] != "X-P" || merged.Strip[1] != "X-A" {
			t.Fatalf("merged strip = %v", merged.Strip)
		}
		// The relay's strip removed the provider's X-A set entry; the rest survive.
		if _, ok := merged.Set["X-A"]; ok {
			t.Fatalf("stripped header survived the inherited set: %v", merged.Set)
		}
		if got := merged.Set["X-Keep"]; len(got) != 1 || got[0] != "p" {
			t.Fatalf("inherited X-Keep = %v", got)
		}
		if got := merged.Set["X-B"]; len(got) != 1 || got[0] != "relay" {
			t.Fatalf("relay override X-B = %v", got)
		}
	})

	t.Run("relay set overrides provider set per name", func(t *testing.T) {
		provider, _ := ParseHeaderPolicy(ptr(`{"set":{"x-same":["provider"],"x-only-p":["p"]}}`))
		relay, _ := ParseHeaderPolicy(ptr(`{"set":{"x-same":["relay"]}}`))
		merged := InheritHeaderPolicy(relay, provider)
		if got := merged.Set["X-Same"]; len(got) != 1 || got[0] != "relay" {
			t.Fatalf("X-Same = %v, want the relay value", got)
		}
		if got := merged.Set["X-Only-P"]; len(got) != 1 || got[0] != "p" {
			t.Fatalf("X-Only-P = %v, want the provider value", got)
		}
	})

	t.Run("relay strip of an inherited set entry stays a strip", func(t *testing.T) {
		provider, _ := ParseHeaderPolicy(ptr(`{"set":{"x-a":["provider"]}}`))
		relay, _ := ParseHeaderPolicy(ptr(`{"strip":["x-a"]}`))
		got := InheritHeaderPolicy(relay, provider)
		if got == nil || len(got.Strip) != 1 || got.Strip[0] != "X-A" || len(got.Set) != 0 {
			t.Fatalf("merge = %+v, want a strip-only policy (the strip still removes the client header)", got)
		}
	})
}
