// Package workers embeds the relay worker source deployed to every platform.
// Assemble builds the single file a platform receives — the shared core
// concatenated with that platform's entry — so no platform ever resolves a
// second module.
package workers

import (
	"embed"
	"fmt"
)

//go:embed core.js entry.vercel.js entry.cloudflare.js entry.deno.js
var files embed.FS

// Assemble returns the complete worker source for one platform: core.js,
// a newline, then the platform's entry. The conformance suite
// (conformance_test.mjs) builds the same concatenation from the same files —
// the two must never diverge.
func Assemble(platform string) (string, error) {
	entry := "entry." + platform + ".js"
	core, err := files.ReadFile("core.js")
	if err != nil {
		return "", err
	}
	entrySrc, err := files.ReadFile(entry)
	if err != nil {
		return "", fmt.Errorf("platform %q has no worker entry: %w", platform, err)
	}
	return string(core) + "\n" + string(entrySrc), nil
}

// MustAssemble is Assemble for a platform known at build time to exist.
func MustAssemble(platform string) string {
	src, err := Assemble(platform)
	if err != nil {
		panic(err)
	}
	return src
}
