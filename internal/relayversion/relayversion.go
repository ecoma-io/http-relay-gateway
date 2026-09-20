// Package relayversion exposes the deployed relay worker's version as a
// release-managed artifact rather than a hand-edited constant. release-please
// rewrites version.json's "version" field on every release (see
// release-please-config.json extra-files), the binary embeds that file, and
// every deployment injects the value as the worker's RELAY_VERSION — so a
// gateway upgrade and the fleet's worker generation can never drift apart by
// an unsynced edit. A live worker reports the value from GET /__relay/version
// and the reconciler redeploys on any mismatch.
package relayversion

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed version.json
var artifact []byte

// Version is the relay worker generation this binary deploys and expects.
// It is resolved once at package initialization from the embedded artifact:
// a build that ships an unreadable or empty artifact is a broken build and
// must fail at startup, never silently deploy an unknown generation.
var Version = mustParse()

func mustParse() string {
	var parsed struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(artifact, &parsed); err != nil {
		panic(fmt.Sprintf("relayversion: embedded version.json is not valid JSON: %v", err))
	}
	if parsed.Version == "" {
		panic("relayversion: embedded version.json carries no version")
	}
	return parsed.Version
}
