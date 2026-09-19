package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validConfig = `
log-level: info
max-retries: 2
failure-threshold: 3
cooldown: 30s
providers:
  vercel:
    max-body: 4.5mb
  cloudflare:
    max-body: 100mb
relays:
  - name: vercel-1
    provider: vercel
    url: https://v1.example
  - provider: cloudflare
    url: https://c1.example
`

func TestLoadRuntimeValid(t *testing.T) {
	cfg, err := LoadRuntime(writeTemp(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != "info" || cfg.MaxRetries != 2 || cfg.FailureThreshold != 3 || cfg.Cooldown != 30*time.Second {
		t.Fatalf("scalars wrong: %+v", cfg)
	}
	if got := cfg.ProviderMaxBody("vercel"); got != 4_500_000 {
		t.Fatalf("vercel max-body = %d, want 4500000", got)
	}
	if got := cfg.ProviderMaxBody("cloudflare"); got != 100_000_000 {
		t.Fatalf("cloudflare max-body = %d, want 100000000", got)
	}
	if got := cfg.ProviderMaxBody("deno"); got != DefaultMaxBody {
		t.Fatalf("default max-body = %d, want %d", got, DefaultMaxBody)
	}
	if cfg.MaxBufferBytes() != 100_000_000 {
		t.Fatalf("buffer cap = %d, want 100000000", cfg.MaxBufferBytes())
	}
	if len(cfg.Relays) != 2 {
		t.Fatalf("relays = %d, want 2", len(cfg.Relays))
	}
	// Explicit name kept, implicit name derived, active defaults to true.
	if cfg.Relays[0].Name != "vercel-1" || cfg.Relays[0].Provider != "vercel" || !cfg.Relays[0].Active {
		t.Fatalf("relay 0 wrong: %+v", cfg.Relays[0])
	}
	if cfg.Relays[1].Name != "cloudflare-2" || !cfg.Relays[1].Active {
		t.Fatalf("relay 1 wrong: %+v", cfg.Relays[1])
	}
}

func TestLoadRuntimeDefaults(t *testing.T) {
	cfg, err := LoadRuntime(writeTemp(t, "relays:\n  - provider: vercel\n    url: https://v1.example\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != DefaultLogLevel || cfg.MaxRetries != DefaultMaxRetries ||
		cfg.FailureThreshold != DefaultFailureThreshold || cfg.Cooldown != DefaultCooldown {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	if cfg.MaxBufferBytes() != DefaultMaxBody {
		t.Fatalf("buffer cap = %d, want %d", cfg.MaxBufferBytes(), DefaultMaxBody)
	}
}

func TestLoadRuntimeUnknownKeyRejected(t *testing.T) {
	_, err := LoadRuntime(writeTemp(t, "api-keys: [x]\n"+validConfig))
	if err == nil || !strings.Contains(err.Error(), "decode runtime config") {
		t.Fatalf("want strict-decode error for unknown key, got %v", err)
	}
}

func TestLoadRuntimeValidationErrors(t *testing.T) {
	cases := []struct {
		name, config, wantErr string
	}{
		{"no relays", "relays: []\n", "at least one relay"},
		{"reserved provider", "relays:\n  - provider: all\n    url: https://x.example\n", "reserved"},
		{"admin path provider", "relays:\n  - provider: stats\n    url: https://x.example\n", "reserved"},
		{"bad url", "relays:\n  - provider: vercel\n    url: not-a-url\n", "absolute http(s)"},
		{"bad log level", "log-level: verbose\n" + validConfig, "log-level"},
		{"zero retries", "max-retries: 0\n" + validConfig, "max-retries"},
		{"zero threshold", "failure-threshold: 0\n" + validConfig, "failure-threshold"},
		{"bad cooldown", "cooldown: soon\n" + validConfig, "cooldown"},
		{"zero cooldown", "cooldown: 0s\nrelays:\n  - provider: vercel\n    url: https://v1.example\n", "positive"},
		{"duplicate names", "relays:\n  - name: a\n    provider: vercel\n    url: https://a.example\n  - name: a\n    provider: vercel\n    url: https://b.example\n", "duplicate"},
		{"bad provider max-body", "providers:\n  vercel:\n    max-body: 2.5\nrelays:\n  - provider: vercel\n    url: https://v1.example\n", "byte count"},
		{"unknown unit", "providers:\n  vercel:\n    max-body: 4tib\nrelays:\n  - provider: vercel\n    url: https://v1.example\n", "unknown size unit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadRuntime(writeTemp(t, tc.config))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseBodySize(t *testing.T) {
	cases := []struct {
		raw  any
		want int64
	}{
		{nil, DefaultMaxBody},
		{1024, 1024},
		{"4.5mb", 4_500_000},
		{"100MB", 100_000_000},
		{"8MiB", 8 << 20},
		{"1kb", 1000},
		{"2 gb", 2_000_000_000},
	}
	for _, tc := range cases {
		got, err := parseBodySize(tc.raw)
		if err != nil || got != tc.want {
			t.Fatalf("parseBodySize(%v) = %d, %v; want %d", tc.raw, got, err, tc.want)
		}
	}
	for _, bad := range []any{0, -5, "0kb", "mb", "4tib", 2.5, true} {
		if got, err := parseBodySize(bad); err == nil {
			t.Fatalf("parseBodySize(%v) = %d, want error", bad, got)
		}
	}
}

func TestLoadRuntimeMissingFile(t *testing.T) {
	_, err := LoadRuntime(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want not-found error", err)
	}
}
