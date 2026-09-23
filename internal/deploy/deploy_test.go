package deploy

import (
	"strings"
	"testing"
)

// A scope's fingerprint is the only form of it that may reach a log line or
// a record: pin values arrive from the environment and can carry secrets,
// so the fingerprint must identify a scope without ever containing it.
func TestScopeFingerprint(t *testing.T) {
	teamA := Scope{Team: "team-alpha"}
	teamB := Scope{Team: "team-beta"}
	acct := Scope{Account: "023e105f4ecef8ad9ca31a8372d0c353"}
	org := Scope{Organization: "e17a0e6b-7ba7-4a6e-9dbd-1f9a83d4db0a"}

	// Equal pins fingerprint identically — the value the registry and the
	// reconciler compare across passes.
	for _, s := range []Scope{teamA, teamB, acct, org, {}} {
		if again := s.FP(); again != s.FP() {
			t.Fatalf("fingerprint of %+v is not deterministic: %q then %q", s, s.FP(), again)
		}
	}

	// Distinct pins — and distinct pin splits — fingerprint apart.
	got := map[string]Scope{}
	for _, s := range []Scope{teamA, teamB, acct, org,
		{Team: "a", Account: "bc"}, {Team: "ab", Account: "c"}} {
		fp := s.FP()
		if fp == "" {
			t.Fatalf("%+v fingerprinted empty", s)
		}
		if prev, clash := got[fp]; clash {
			t.Fatalf("%+v and %+v share the fingerprint %q", prev, s, fp)
		}
		got[fp] = s
	}

	// No pins at all: the empty fingerprint, the one value that says
	// "nothing pinned" rather than hashing to something.
	if empty := (Scope{}).FP(); empty != "" {
		t.Fatalf("unpinned scope fingerprint = %q, want \"\"", empty)
	}

	// The fingerprint must never carry a pin value: the whole point of the
	// form is that raw pins (which resolve ${VAR} secrets) stay out of logs.
	pins := []string{"team-alpha", "team-beta", "023e105f4ecef8ad9ca31a8372d0c353",
		"e17a0e6b-7ba7-4a6e-9dbd-1f9a83d4db0a", "a", "bc", "ab", "c"}
	for _, s := range []Scope{teamA, teamB, acct, org, {Team: "a", Account: "bc"},
		{Team: "ab", Account: "c", Organization: "e17a0e6b-7ba7-4a6e-9dbd-1f9a83d4db0a"}} {
		for _, pin := range pins {
			if len(pin) >= 8 && strings.Contains(s.FP(), pin) {
				t.Fatalf("fingerprint %q of %+v contains the pin value", s.FP(), s)
			}
		}
	}
}

// The token decides whether platform calls succeed, never which project an
// identity resolves to — so it must not take part in the scope.
func TestCredentialScopeCarriesPinsOnly(t *testing.T) {
	base := Credential{Token: "tok-1", Team: "team-alpha"}
	rotated := Credential{Token: "tok-2", Team: "team-alpha"}
	if base.Scope() != rotated.Scope() {
		t.Fatalf("credential rotation moved the scope: %+v vs %+v", base.Scope(), rotated.Scope())
	}
	if base.Scope() != (Scope{Team: "team-alpha"}) {
		t.Fatalf("scope = %+v, want the pins verbatim", base.Scope())
	}
	moved := base
	moved.Team = "team-beta"
	if moved.Scope() == base.Scope() {
		t.Fatal("a moved pin did not move the scope")
	}
}
