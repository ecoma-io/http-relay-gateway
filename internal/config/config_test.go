package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"http-relay-gateway/internal/deploy"
)

// writeConfig writes a desired-state file into a fresh temp dir and returns
// its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadValidFileResolvesRelaysAndDefaults(t *testing.T) {
	t.Setenv("RELAY_TEST_VERCEL_TOKEN", "vc_secret")
	t.Setenv("RELAY_TEST_CF_TOKEN", "cf_secret")

	path := writeConfig(t, `
settings:
  log_level: debug
  max_retries: 5
relays:
  - name: web-relay
    provider: Vercel
    token: ${RELAY_TEST_VERCEL_TOKEN}
    team: my-team
  - name: edge-relay
    provider: cloudflare
    token_file: /run/secrets/cf-token
    account: acc_123
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Relays) != 2 {
		t.Fatalf("got %d relays, want 2", len(cfg.Relays))
	}
	first := cfg.Relays[0]
	if first.Name != "web-relay" || first.Provider != "vercel" {
		t.Errorf("relay 0 identity = %s/%s, want vercel/web-relay", first.Provider, first.Name)
	}
	if first.Token != "vc_secret" {
		t.Errorf("relay 0 token = %q, want the interpolated env value", first.Token)
	}
	if first.Team != "my-team" {
		t.Errorf("relay 0 team = %q, want my-team", first.Team)
	}
	second := cfg.Relays[1]
	if second.Provider != "cloudflare" || second.Account != "acc_123" {
		t.Errorf("relay 1 = %s/%s account %q, want cloudflare/edge-relay acc_123",
			second.Provider, second.Name, second.Account)
	}
	if second.TokenFile != "/run/secrets/cf-token" {
		t.Errorf("relay 1 token_file = %q, want /run/secrets/cf-token", second.TokenFile)
	}

	// Keys absent from the file keep their documented defaults.
	want := DefaultSettings()
	want.LogLevel = "debug"
	want.MaxRetries = 5
	if cfg.Settings != want {
		t.Errorf("settings = %+v, want %+v (file overrides over defaults)", cfg.Settings, want)
	}
}

func TestInterpolationResolvesMultipleReferences(t *testing.T) {
	t.Setenv("RELAY_TEST_A", "one")
	t.Setenv("RELAY_TEST_B", "two")

	got, err := interpolate("pre-${RELAY_TEST_A}-mid-${RELAY_TEST_B}-post")
	if err != nil {
		t.Fatalf("interpolate: %v", err)
	}
	if got != "pre-one-mid-two-post" {
		t.Errorf("interpolate = %q, want %q", got, "pre-one-mid-two-post")
	}
}

func TestInterpolationErrors(t *testing.T) {
	t.Setenv("RELAY_TEST_EMPTY", "")

	cases := []struct {
		name    string
		value   string
		wantErr string
	}{
		{
			name:    "unset variable",
			value:   "${RELAY_TEST_UNSET_VAR}",
			wantErr: "environment variable RELAY_TEST_UNSET_VAR is not set",
		},
		{
			name:    "empty variable",
			value:   "${RELAY_TEST_EMPTY}",
			wantErr: "environment variable RELAY_TEST_EMPTY is not set",
		},
		{
			name:    "unclosed reference",
			value:   "prefix-${RELAY_TEST_A",
			wantErr: `unclosed ${ reference in "prefix-${RELAY_T…"`,
		},
		{
			name:    "empty reference",
			value:   "x-${}",
			wantErr: `empty ${} reference`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := interpolate(tc.value)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("interpolate(%q) error = %v, want it to contain %q", tc.value, err, tc.wantErr)
			}
		})
	}
}

func TestInterpolationErrorRedactsLongSecrets(t *testing.T) {
	// The raw value stands in for a credential with a typo'd reference: the
	// error must show only its first 16 characters, never the whole secret.
	raw := "supersecret-token-value-${RELAY_TEST_UNSET_VAR"
	_, err := interpolate(raw)
	if err == nil {
		t.Fatal("interpolate: want an error for the unset reference")
	}
	msg := err.Error()
	if !strings.Contains(msg, "supersecret-toke…") {
		t.Errorf("error %q does not carry the 16-char redacted shape", msg)
	}
	if strings.Contains(msg, raw) {
		t.Errorf("error %q leaks the full raw value", msg)
	}
}

func TestStrictSchemaRejectsUnknownKeys(t *testing.T) {
	cases := []struct {
		name     string
		file     string
		wantWarn string
	}{
		{
			name:     "unknown top-level key",
			file:     "listen_addr: :8080\nrelays: []\n",
			wantWarn: "listen_addr",
		},
		{
			name:     "unknown settings key",
			file:     "settings:\n  retries: 3\nrelays: []\n",
			wantWarn: "retries",
		},
		{
			name:     "unknown relay key",
			file:     "relays:\n  - name: web-relay\n    provider: vercel\n    token: t\n    url: https://x\n",
			wantWarn: "url",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.file))
			if err == nil || !strings.Contains(err.Error(), tc.wantWarn) {
				t.Fatalf("Load error = %v, want it to mention %q", err, tc.wantWarn)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil || !strings.Contains(err.Error(), "read desired state") {
		t.Fatalf("Load error = %v, want a read-desired-state failure", err)
	}
}

// TestExampleConfigLoads guards the committed reference layout: whatever is
// commented out in it, config.example.yaml must parse through the same
// strict loader a hand-written config.yaml goes through. The stub
// environment carries a non-empty value for every ${VAR} the example
// references, so the load also stays valid with entries uncommented — the
// test pins the file's YAML syntax, not its comment state.
func TestExampleConfigLoads(t *testing.T) {
	for _, name := range []string{
		"VERCEL_TOKEN", "VERCEL_TEAM_ID",
		"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID",
		"DENO_DEPLOY_TOKEN",
	} {
		t.Setenv(name, "stub-"+strings.ToLower(name))
	}

	// go test runs the binary in the package directory; the example lives
	// two levels up, next to compose.yaml.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cfg, err := Load(filepath.Join(wd, "..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("Load(config.example.yaml): %v", err)
	}
	// The settings block ships fully commented out, so the example loads
	// with the documented defaults.
	if cfg.Settings != DefaultSettings() {
		t.Errorf("settings = %+v, want the documented defaults %+v", cfg.Settings, DefaultSettings())
	}
}

func TestSettingsAllKeysOverride(t *testing.T) {
	path := writeConfig(t, `
settings:
  log_level: WARN
  max_retries: 16
  failure_threshold: 1
  cooldown: 0s
  stream_threshold_bytes: 1048576
  dial_timeout: 250ms
  response_header_timeout: 0s
  verify_interval: 15s
  revive_scan_interval: 5m
  verify_backoff_base: 2s
  verify_backoff_max: 2s
  verify_recover_max: 1s
  verify_demote_after: 1
relays: []
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Settings{
		LogLevel:              "warn", // normalized lowercase
		MaxRetries:            16,
		FailureThreshold:      1,
		Cooldown:              0,
		StreamThresholdBytes:  1048576,
		DialTimeout:           250 * time.Millisecond,
		ResponseHeaderTimeout: 0,
		VerifyInterval:        15 * time.Second,
		ReviveScanInterval:    5 * time.Minute,
		VerifyBackoffBase:     2 * time.Second,
		VerifyBackoffMax:      2 * time.Second, // max == base is allowed
		VerifyRecoverMax:      time.Second,
		VerifyDemoteAfter:     1,
	}
	if cfg.Settings != want {
		t.Errorf("settings = %+v, want %+v", cfg.Settings, want)
	}
}

func TestSettingsValidationFailures(t *testing.T) {
	cases := []struct {
		name    string
		setting string
		wantErr string
	}{
		{"max_retries above 16", "max_retries: 17", "settings.max_retries must be in [0, 16]"},
		{"max_retries negative", "max_retries: -1", "settings.max_retries must be in [0, 16]"},
		{"failure_threshold zero", "failure_threshold: 0", "settings.failure_threshold must be >= 1"},
		{"cooldown negative", "cooldown: -1s", "settings.cooldown must be >= 0"},
		{"stream threshold negative", "stream_threshold_bytes: -1", "settings.stream_threshold_bytes must be >= 0"},
		{"dial_timeout zero", "dial_timeout: 0s", "settings.dial_timeout must be positive"},
		{"response_header_timeout negative", "response_header_timeout: -1s", "settings.response_header_timeout must be >= 0"},
		{"verify_interval zero", "verify_interval: 0s", "settings.verify_interval must be positive"},
		{"revive_scan_interval zero", "revive_scan_interval: 0s", "settings.revive_scan_interval must be positive"},
		{"verify_backoff_base zero", "verify_backoff_base: 0s", "verify_backoff_base must be positive"},
		{"verify_backoff_max below base", "verify_backoff_base: 10s\n  verify_backoff_max: 5s", "verify_backoff_max >= base"},
		{"verify_recover_max zero", "verify_recover_max: 0s", "settings.verify_recover_max must be positive"},
		{"verify_demote_after zero", "verify_demote_after: 0", "settings.verify_demote_after must be >= 1"},
		{"bad log level", `log_level: verbose`, "settings.log_level"},
		{"malformed duration", "cooldown: soon", "settings.cooldown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := "settings:\n  " + tc.setting + "\nrelays: []\n"
			_, err := Load(writeConfig(t, file))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestSettingsErrorsAccumulate(t *testing.T) {
	_, err := Load(writeConfig(t, "settings:\n  max_retries: -3\n  failure_threshold: 0\nrelays: []\n"))
	if err == nil {
		t.Fatal("Load: want an error for two invalid settings")
	}
	msg := err.Error()
	if !strings.Contains(msg, "settings.max_retries") || !strings.Contains(msg, "settings.failure_threshold") {
		t.Errorf("error %q should report both invalid settings, not stop at the first", msg)
	}
}

func TestRelayValidationFailures(t *testing.T) {
	cases := []struct {
		name    string
		relay   string
		wantErr string
	}{
		{
			name:    "missing name",
			relay:   "provider: vercel\n    token: t",
			wantErr: "name is required",
		},
		{
			name:    "missing provider",
			relay:   "name: web-relay\n    token: t",
			wantErr: `provider "" is not one of the hard-coded providers`,
		},
		{
			name:    "unknown provider",
			relay:   "name: web-relay\n    provider: flyio\n    token: t",
			wantErr: `provider "flyio" is not one of the hard-coded providers`,
		},
		{
			name:    "team on cloudflare",
			relay:   "name: web-relay\n    provider: cloudflare\n    token: t\n    team: my-team",
			wantErr: "team is a vercel scope",
		},
		{
			name:    "account on vercel",
			relay:   "name: web-relay\n    provider: vercel\n    token: t\n    account: acc_1",
			wantErr: "account is a cloudflare scope pin",
		},
		{
			name:    "both token and token_file",
			relay:   "name: web-relay\n    provider: vercel\n    token: t\n    token_file: /tmp/t",
			wantErr: "set either token or token_file, not both",
		},
		{
			name:    "neither token nor token_file",
			relay:   "name: web-relay\n    provider: vercel",
			wantErr: "a provider credential is required",
		},
		{
			name:    "token interpolation failure",
			relay:   "name: web-relay\n    provider: vercel\n    token: ${RELAY_TEST_UNSET_VAR}",
			wantErr: "environment variable RELAY_TEST_UNSET_VAR is not set",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := "relays:\n  - " + tc.relay + "\n"
			_, err := Load(writeConfig(t, file))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// An inline token follows the same hygiene as a token file: the resolved
// value is trimmed, and one that is blank afterwards rejects the load. A
// trailing newline in a secret-manager-sourced env var must not silently
// ship as part of the credential. Assertions compare against the fixture
// without echoing the resolved value — the failure text never carries it.
func TestInlineTokenTrimmedAndBlankRejected(t *testing.T) {
	const want = "padded-fixture-secret"

	t.Run("env value with surrounding whitespace resolves trimmed", func(t *testing.T) {
		t.Setenv("RELAY_TEST_PADDED", " \t"+want+"\n")
		path := writeConfig(t, "relays:\n  - name: web-relay\n    provider: vercel\n    token: ${RELAY_TEST_PADDED}\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		got := cfg.Relays[0].Token
		if got != want {
			t.Errorf("inline token length = %d, want %d (the trimmed secret)", len(got), len(want))
		}
	})

	t.Run("literal token with surrounding whitespace resolves trimmed", func(t *testing.T) {
		path := writeConfig(t, "relays:\n  - name: web-relay\n    provider: vercel\n    token: \" "+want+" \"\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := cfg.Relays[0].Token; got != want {
			t.Errorf("inline token length = %d, want %d (the trimmed literal)", len(got), len(want))
		}
	})

	t.Run("env value blank after trimming rejects the load", func(t *testing.T) {
		t.Setenv("RELAY_TEST_BLANKISH", " \n\t")
		path := writeConfig(t, "relays:\n  - name: web-relay\n    provider: vercel\n    token: ${RELAY_TEST_BLANKISH}\n")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "token is blank") {
			t.Fatalf("Load error = %v, want a blank-token rejection", err)
		}
	})

	t.Run("whitespace-only literal token rejects the load", func(t *testing.T) {
		path := writeConfig(t, "relays:\n  - name: web-relay\n    provider: vercel\n    token: \"   \"\n")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "token is blank") {
			t.Fatalf("Load error = %v, want a blank-token rejection", err)
		}
	})
}

func TestRelayNameLengthBound(t *testing.T) {
	long := strings.Repeat("a", 129)
	_, err := Load(writeConfig(t, "relays:\n  - name: "+long+"\n    provider: vercel\n    token: t\n"))
	if err == nil || !strings.Contains(err.Error(), "at most 128 characters") {
		t.Fatalf("Load error = %v, want the 128-character bound", err)
	}
}

func TestRelayDuplicateNamesRejected(t *testing.T) {
	// The name check is identity-wide: even on different providers two relays
	// may not share a name.
	_, err := Load(writeConfig(t, `
relays:
  - name: web-relay
    provider: vercel
    token: t
  - name: web-relay
    provider: deno
    token: t
`))
	if err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("Load error = %v, want a duplicate-name failure", err)
	}
}

func TestRelaySlugCollisionRejected(t *testing.T) {
	// "web relay" and "web_relay" slug to the same project on one provider —
	// they would fight over a single deployment.
	_, err := Load(writeConfig(t, `
relays:
  - name: web relay
    provider: vercel
    token: t
  - name: web_relay
    provider: vercel
    token: t
`))
	if err == nil || !strings.Contains(err.Error(), "same vercel project") {
		t.Fatalf("Load error = %v, want a slug-collision failure", err)
	}
}

func TestRelayNormalizesNameAndProvider(t *testing.T) {
	path := writeConfig(t, `
relays:
  - name: "  padded relay  "
    provider: " Vercel "
    token: plain
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	r := cfg.Relays[0]
	if r.Name != "padded relay" || r.Provider != "vercel" {
		t.Errorf("relay = %q/%q, want trimmed name and lowercased provider", r.Name, r.Provider)
	}
}

func TestResolveToken(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "cf-token")
	if err := os.WriteFile(file, []byte("  cf_secret\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("   \n"), 0o600); err != nil {
		t.Fatalf("write empty token file: %v", err)
	}

	cases := []struct {
		name    string
		relay   Relay
		want    string
		wantErr string
	}{
		{
			name: "direct token",
			relay: Relay{
				Name:     "web-relay",
				Provider: deploy.PlatformVercel,
				Token:    "vc_secret",
			},
			want: "vc_secret",
		},
		{
			name: "token file read and trimmed",
			relay: Relay{
				Name:      "edge-relay",
				Provider:  deploy.PlatformCloudflare,
				TokenFile: file,
			},
			want: "cf_secret",
		},
		{
			name:    "missing token file",
			relay:   Relay{Name: "x", Provider: "vercel", TokenFile: filepath.Join(dir, "absent")},
			wantErr: "read secret file",
		},
		{
			name:    "empty token file",
			relay:   Relay{Name: "x", Provider: "vercel", TokenFile: empty},
			wantErr: "is empty",
		},
		{
			name:    "no credential at all",
			relay:   Relay{Name: "bare", Provider: "deno"},
			wantErr: "relay bare has no provider credential",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.relay.ResolveToken()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ResolveToken error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveToken: %v", err)
			}
			if got != tc.want {
				t.Errorf("ResolveToken = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveTokenRereadsFileEveryCall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	relay := Relay{TokenFile: path}

	if got, err := relay.ResolveToken(); err != nil || got != "first" {
		t.Fatalf("first ResolveToken = %q, %v; want first, nil", got, err)
	}
	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatalf("rotate token file: %v", err)
	}
	if got, err := relay.ResolveToken(); err != nil || got != "second" {
		t.Fatalf("second ResolveToken = %q, %v; want second, nil (fresh read)", got, err)
	}
}

// clearBootstrapEnv pins every bootstrap variable to the empty string, which
// LoadBootstrap treats exactly like unset — keeping tests independent of the
// developer's shell.
func clearBootstrapEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"LISTEN_ADDR", "CONFIG_FILE", "SHUTDOWN_GRACE",
		"RELAY_AUTH_TOKEN", "RELAY_AUTH_TOKEN_FILE",
	} {
		t.Setenv(key, "")
	}
}

func TestBootstrapDefaults(t *testing.T) {
	clearBootstrapEnv(t)
	t.Setenv("RELAY_AUTH_TOKEN", "relay-key")

	cfg, err := LoadBootstrap()
	if err != nil {
		t.Fatalf("LoadBootstrap: %v", err)
	}
	if cfg.ListenAddr != DefaultListenAddr {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, DefaultListenAddr)
	}
	if cfg.ConfigFile != DefaultConfigFile {
		t.Errorf("ConfigFile = %q, want %q", cfg.ConfigFile, DefaultConfigFile)
	}
	if cfg.ShutdownGrace != DefaultShutdownGrace {
		t.Errorf("ShutdownGrace = %s, want %s", cfg.ShutdownGrace, DefaultShutdownGrace)
	}
	if cfg.RelayKey != "relay-key" || cfg.RelayKeyFile != "" {
		t.Errorf("relay key = %q/%q, want env key with no file", cfg.RelayKey, cfg.RelayKeyFile)
	}
}

func TestBootstrapOverrides(t *testing.T) {
	clearBootstrapEnv(t)
	t.Setenv("LISTEN_ADDR", "127.0.0.1:20130")
	t.Setenv("CONFIG_FILE", "/etc/relay/desired.yaml")
	t.Setenv("SHUTDOWN_GRACE", "45s")
	t.Setenv("RELAY_AUTH_TOKEN_FILE", "/run/secrets/relay-key")

	cfg, err := LoadBootstrap()
	if err != nil {
		t.Fatalf("LoadBootstrap: %v", err)
	}
	if cfg.ListenAddr != "127.0.0.1:20130" || cfg.ConfigFile != "/etc/relay/desired.yaml" {
		t.Errorf("addresses = %q / %q, want the env overrides", cfg.ListenAddr, cfg.ConfigFile)
	}
	if cfg.ShutdownGrace != 45*time.Second {
		t.Errorf("ShutdownGrace = %s, want 45s", cfg.ShutdownGrace)
	}
	if cfg.RelayKeyFile != "/run/secrets/relay-key" || cfg.RelayKey != "" {
		t.Errorf("relay key = %q/%q, want file only", cfg.RelayKey, cfg.RelayKeyFile)
	}
}

func TestBootstrapRelayKeyExclusivity(t *testing.T) {
	cases := []struct {
		name    string
		token   string
		file    string
		wantErr string
	}{
		{name: "neither set", wantErr: "exactly one of RELAY_AUTH_TOKEN or RELAY_AUTH_TOKEN_FILE"},
		{name: "both set", token: "k", file: "/run/secrets/k", wantErr: "exactly one of RELAY_AUTH_TOKEN or RELAY_AUTH_TOKEN_FILE"},
		{name: "empty env value counts as unset", token: "", wantErr: "exactly one of RELAY_AUTH_TOKEN or RELAY_AUTH_TOKEN_FILE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearBootstrapEnv(t)
			t.Setenv("RELAY_AUTH_TOKEN", tc.token)
			t.Setenv("RELAY_AUTH_TOKEN_FILE", tc.file)
			_, err := LoadBootstrap()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadBootstrap error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestBootstrapShutdownGraceParsing(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr string
	}{
		{name: "unset keeps default", value: "", want: DefaultShutdownGrace},
		{name: "valid override", value: "90s", want: 90 * time.Second},
		{name: "malformed", value: "not-a-duration", wantErr: "SHUTDOWN_GRACE"},
		{name: "zero is invalid", value: "0s", wantErr: "SHUTDOWN_GRACE must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearBootstrapEnv(t)
			t.Setenv("RELAY_AUTH_TOKEN", "k")
			t.Setenv("SHUTDOWN_GRACE", tc.value)
			cfg, err := LoadBootstrap()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadBootstrap error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadBootstrap: %v", err)
			}
			if cfg.ShutdownGrace != tc.want {
				t.Errorf("ShutdownGrace = %s, want %s", cfg.ShutdownGrace, tc.want)
			}
		})
	}
}

func TestBootstrapListenAddrValidation(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		wantErr string
	}{
		{name: "wildcard", addr: ":8080"},
		{name: "explicit ipv4 wildcard", addr: "0.0.0.0:20130"},
		{name: "ipv6 wildcard", addr: "[::]:8080"},
		{name: "loopback", addr: "127.0.0.1:20130"},
		{name: "hostname", addr: "relay.internal:8080"},
		{name: "no port", addr: "127.0.0.1", wantErr: "must be a host:port address"},
		{name: "port out of range", addr: "127.0.0.1:99999", wantErr: "invalid port"},
		{name: "port not a number", addr: "127.0.0.1:http", wantErr: "invalid port"},
		{name: "bad hostname", addr: "-nope.example:8080", wantErr: "invalid host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearBootstrapEnv(t)
			t.Setenv("RELAY_AUTH_TOKEN", "k")
			t.Setenv("LISTEN_ADDR", tc.addr)
			_, err := LoadBootstrap()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("LoadBootstrap: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadBootstrap error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestResolveRelayKey(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "relay-key")
	if err := os.WriteFile(file, []byte("  rotated-key\n"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	t.Run("env key passes through", func(t *testing.T) {
		cfg := &BootstrapConfig{RelayKey: "inline-key"}
		got, err := cfg.ResolveRelayKey()
		if err != nil || got != "inline-key" {
			t.Fatalf("ResolveRelayKey = %q, %v; want inline-key, nil", got, err)
		}
	})

	t.Run("file key trimmed and reread", func(t *testing.T) {
		cfg := &BootstrapConfig{RelayKeyFile: file}
		got, err := cfg.ResolveRelayKey()
		if err != nil || got != "rotated-key" {
			t.Fatalf("ResolveRelayKey = %q, %v; want rotated-key, nil", got, err)
		}
		if err := os.WriteFile(file, []byte("new-key"), 0o600); err != nil {
			t.Fatalf("rotate key file: %v", err)
		}
		got, err = cfg.ResolveRelayKey()
		if err != nil || got != "new-key" {
			t.Fatalf("ResolveRelayKey after rotation = %q, %v; want new-key, nil", got, err)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		cfg := &BootstrapConfig{RelayKeyFile: filepath.Join(dir, "absent")}
		_, err := cfg.ResolveRelayKey()
		if err == nil || !strings.Contains(err.Error(), "read secret file") {
			t.Fatalf("ResolveRelayKey error = %v, want a read-secret-file failure", err)
		}
	})
}

func TestKeysAndRelayAccessors(t *testing.T) {
	path := writeConfig(t, `
relays:
  - name: web-relay
    provider: vercel
    token: t
  - name: edge-relay
    provider: cloudflare
    token: t
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	keys := cfg.Keys()
	want := []deploy.RelayKey{
		{Provider: "vercel", Name: "web-relay"},
		{Provider: "cloudflare", Name: "edge-relay"},
	}
	if len(keys) != len(want) {
		t.Fatalf("Keys() = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("Keys()[%d] = %v, want %v", i, keys[i], want[i])
		}
	}

	if _, ok := cfg.Relay("vercel", "web-relay"); !ok {
		t.Error("Relay(vercel, web-relay) not found")
	}
	if _, ok := cfg.Relay("vercel", "absent"); ok {
		t.Error("Relay(vercel, absent) found but must not exist")
	}
	if _, ok := cfg.Relay("deno", "web-relay"); ok {
		t.Error("Relay(deno, web-relay) found but provider does not match")
	}
}

func TestDefaultSettings(t *testing.T) {
	want := Settings{
		LogLevel:              "info",
		MaxRetries:            2,
		FailureThreshold:      3,
		Cooldown:              30 * time.Second,
		StreamThresholdBytes:  0,
		DialTimeout:           5 * time.Second,
		ResponseHeaderTimeout: 0,
		VerifyInterval:        60 * time.Second,
		ReviveScanInterval:    10 * time.Minute,
		VerifyBackoffBase:     5 * time.Second,
		VerifyBackoffMax:      5 * time.Minute,
		VerifyRecoverMax:      15 * time.Second,
		VerifyDemoteAfter:     3,
	}
	if got := DefaultSettings(); got != want {
		t.Errorf("DefaultSettings() = %+v, want %+v", got, want)
	}
}
