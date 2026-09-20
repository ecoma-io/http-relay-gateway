package relayversion

import (
	"encoding/json"
	"strings"
	"testing"
)

// The artifact is the single source of the worker version: it must parse,
// carry a non-empty version, and match what the package exports. A broken
// build here would deploy an unknown generation to the whole fleet.
func TestArtifactMatchesExportedVersion(t *testing.T) {
	var parsed struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(artifact, &parsed); err != nil {
		t.Fatalf("embedded version.json: %v", err)
	}
	if parsed.Version == "" {
		t.Fatal("embedded version.json carries no version")
	}
	if Version != parsed.Version {
		t.Fatalf("Version = %q, artifact says %q", Version, parsed.Version)
	}
}

// The version rides into worker source and platform payloads as a plain
// string: it must stay single-line and free of anything a header, env var or
// JSON string value cannot carry.
func TestVersionIsDeploySafe(t *testing.T) {
	if strings.ContainsAny(Version, "\n\r\"\\") {
		t.Fatalf("version %q is not safe to embed in payloads", Version)
	}
	if len(Version) > 32 {
		t.Fatalf("version %q is unreasonably long", Version)
	}
}
