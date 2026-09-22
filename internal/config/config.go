// Package config owns both configuration layers:
//
//   - the bootstrap environment (process-level: listeners, the desired-state
//     file path, the whole-process drain budget, the relay API key) — read
//     once at startup, restart to change;
//   - the desired-state file itself: the relay set and the runtime settings,
//     re-loaded live. A valid new file becomes the desired state; an invalid
//     one is logged and ignored, so the last-known-good state keeps serving.
//
// The desired-state file is the only source of intent. There is no database,
// no admin API, no second source of truth to keep in step.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"http-relay-gateway/internal/deploy"
)

// Defaults applied when the corresponding environment variable is unset.
const (
	DefaultListenAddr = ":8080"
	// DefaultConfigFile is the desired-state file the poller watches.
	DefaultConfigFile = "config.yaml"
	// DefaultShutdownGrace bounds the whole-process graceful drain. 20s fits
	// under a 30s docker stop_grace_period.
	DefaultShutdownGrace = 20 * time.Second
	// ReloadPollInterval is how often the desired-state file is re-read. The
	// content-hash poller is a design, not a gap: it needs no inotify, and
	// unlike a watcher it cannot miss events on bind mounts.
	ReloadPollInterval = time.Second
)

// BootstrapConfig contains process-level settings. They are read only when
// the process starts because changing the listening sockets or the relay key
// in place would require a coordinated restart.
type BootstrapConfig struct {
	ListenAddr    string
	ConfigFile    string
	ShutdownGrace time.Duration
	// RelayKey and RelayKeyFile are the two ways the relay API key is
	// supplied — exactly one must be set. The key is the credential the
	// gateway authenticates to every deployed worker with, injected at
	// deploy time and sent on every relay leg; workers reject anything else.
	// The env value is trimmed like the file's content. A worker deployed
	// without it would reject every request, so boot refuses to run without
	// one.
	RelayKey     string
	RelayKeyFile string
}

// LoadBootstrap applies bootstrap defaults and explicit environment overrides.
func LoadBootstrap() (*BootstrapConfig, error) {
	cfg := &BootstrapConfig{
		ListenAddr:    DefaultListenAddr,
		ConfigFile:    DefaultConfigFile,
		ShutdownGrace: DefaultShutdownGrace,
	}
	envStr("LISTEN_ADDR", &cfg.ListenAddr)
	envStr("CONFIG_FILE", &cfg.ConfigFile)
	envStr("RELAY_AUTH_TOKEN", &cfg.RelayKey)
	envStr("RELAY_AUTH_TOKEN_FILE", &cfg.RelayKeyFile)
	// Parsed inline rather than through a helper so a malformed value fails
	// fast instead of silently falling back to the default.
	if raw, ok := os.LookupEnv("SHUTDOWN_GRACE"); ok && raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("SHUTDOWN_GRACE %q must be a Go duration", raw)
		}
		cfg.ShutdownGrace = d
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *BootstrapConfig) validate() error {
	var errs []error
	if c.ConfigFile == "" {
		errs = append(errs, errors.New("CONFIG_FILE must not be empty"))
	}
	if c.ShutdownGrace <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_GRACE must be positive, got %s", c.ShutdownGrace))
	}
	// The env value is trimmed exactly like a key file's content; a value
	// that is blank after trimming is no key at all and fails boot here —
	// surfacing at reconcile time would only ever say "verification
	// failed", with nothing pointing at the whitespace.
	key := strings.TrimSpace(c.RelayKey)
	switch {
	case c.RelayKey != "" && key == "":
		errs = append(errs, errors.New(
			"RELAY_AUTH_TOKEN is blank: it carries only whitespace after trimming"))
	case (key == "") == (c.RelayKeyFile == ""):
		errs = append(errs, errors.New(
			"exactly one of RELAY_AUTH_TOKEN or RELAY_AUTH_TOKEN_FILE must be set: "+
				"it is the credential deployed workers authenticate with"))
	}
	if err := validateListenAddr("LISTEN_ADDR", c.ListenAddr); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ResolveRelayKey returns the relay API key. Both paths apply the same
// hygiene: the file is read and trimmed (readSecretFile), the inline env
// value is trimmed here — a trailing newline from a compose expansion or a
// systemd EnvironmentFile line would otherwise produce a key that fails
// authentication on every relay leg and probe with no hint at the
// whitespace — and a value blank after trimming is an error. Callers
// resolve it per reconcile pass, so a rotated secret file is picked up
// without a restart.
func (c *BootstrapConfig) ResolveRelayKey() (string, error) {
	if c.RelayKeyFile != "" {
		return readSecretFile(c.RelayKeyFile)
	}
	key := strings.TrimSpace(c.RelayKey)
	if key == "" {
		return "", errors.New("RELAY_AUTH_TOKEN is blank: it carries only whitespace after trimming")
	}
	return key, nil
}

func readSecretFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret file %s: %w", path, err)
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", fmt.Errorf("secret file %s is empty", path)
	}
	return key, nil
}

// Relay is one desired relay: the identity (provider, name) plus the
// provider credential that manages its deployment. Nothing else is needed —
// the deployment URL is discovered from the provider, never configured.
type Relay struct {
	// Name is the relay's identity half. The platform project slug derives
	// from it deterministically (deploy.ProjectName).
	Name string `yaml:"name"`
	// Provider is one of the hard-coded providers (deploy.Platforms).
	Provider string `yaml:"provider"`
	// Token is the provider management credential, with ${VAR} references
	// resolved from the environment at load time and the result trimmed
	// exactly like a token file's content. Exactly one of Token and
	// TokenFile must be set.
	Token string `yaml:"token"`
	// TokenFile names a secret file whose trimmed content is the provider
	// credential, re-read on every reconcile pass so a rotated credential is
	// picked up without a restart or a config touch.
	TokenFile string `yaml:"token_file"`
	// Team pins the Vercel team scope (optional). Without it the token's own
	// user scope is used; with a multi-team token the scope MUST be pinned —
	// discovery never guesses.
	Team string `yaml:"team"`
	// Account pins the Cloudflare account id (optional). When the token can
	// reach exactly one account it is resolved automatically; any other
	// account visibility requires the explicit pin.
	Account string `yaml:"account"`

	// team/account must never be set on the wrong provider; validated in
	// validateRelay via the provider switch.
}

// Settings is the typed view of the runtime tuning knobs. Every field has a
// default; the file overrides.
type Settings struct {
	LogLevel              string
	MaxRetries            int
	FailureThreshold      int
	Cooldown              time.Duration
	StreamThresholdBytes  int64
	DialTimeout           time.Duration
	ResponseHeaderTimeout time.Duration
	VerifyInterval        time.Duration
	ReviveScanInterval    time.Duration
	VerifyBackoffBase     time.Duration
	VerifyBackoffMax      time.Duration
	VerifyRecoverMax      time.Duration
	VerifyDemoteAfter     int
}

// Defaults for Settings, mirroring the documented behavior contract.
const (
	DefaultLogLevel              = "info"
	DefaultMaxRetries            = 2
	DefaultFailureThreshold      = 3
	DefaultCooldown              = 30 * time.Second
	DefaultStreamThresholdBytes  = 0 // 0 = buffer everything (opt-in streaming)
	DefaultDialTimeout           = 5 * time.Second
	DefaultResponseHeaderTimeout = 0 // 0 = no response-header timeout
	DefaultVerifyInterval        = 60 * time.Second
	DefaultReviveScanInterval    = 10 * time.Minute
	DefaultVerifyBackoffBase     = 5 * time.Second
	DefaultVerifyBackoffMax      = 5 * time.Minute
	DefaultVerifyRecoverMax      = 15 * time.Second
	DefaultVerifyDemoteAfter     = 3
)

func defaultSettings() Settings {
	return Settings{
		LogLevel:              DefaultLogLevel,
		MaxRetries:            DefaultMaxRetries,
		FailureThreshold:      DefaultFailureThreshold,
		Cooldown:              DefaultCooldown,
		StreamThresholdBytes:  DefaultStreamThresholdBytes,
		DialTimeout:           DefaultDialTimeout,
		ResponseHeaderTimeout: DefaultResponseHeaderTimeout,
		VerifyInterval:        DefaultVerifyInterval,
		ReviveScanInterval:    DefaultReviveScanInterval,
		VerifyBackoffBase:     DefaultVerifyBackoffBase,
		VerifyBackoffMax:      DefaultVerifyBackoffMax,
		VerifyRecoverMax:      DefaultVerifyRecoverMax,
		VerifyDemoteAfter:     DefaultVerifyDemoteAfter,
	}
}

// Config is one valid desired state: the relay set plus runtime settings.
// It is immutable once validated — reloads build a new one and swap.
type Config struct {
	Relays   []Relay
	Settings Settings
}

// DefaultSettings returns the settings a run uses while no desired-state
// file has loaded yet — every knob at its documented default.
func DefaultSettings() Settings { return defaultSettings() }

// --- file format ---

// fileRelay mirrors Relay in YAML. Secret-carrying fields stay raw here;
// interpolation happens in Load where a failure can reject the whole file.
type fileRelay struct {
	Name      string `yaml:"name"`
	Provider  string `yaml:"provider"`
	Token     string `yaml:"token"`
	TokenFile string `yaml:"token_file"`
	Team      string `yaml:"team"`
	Account   string `yaml:"account"`
}

type fileSettings struct {
	LogLevel              *string `yaml:"log_level"`
	MaxRetries            *int    `yaml:"max_retries"`
	FailureThreshold      *int    `yaml:"failure_threshold"`
	Cooldown              *string `yaml:"cooldown"`
	StreamThresholdBytes  *int64  `yaml:"stream_threshold_bytes"`
	DialTimeout           *string `yaml:"dial_timeout"`
	ResponseHeaderTimeout *string `yaml:"response_header_timeout"`
	VerifyInterval        *string `yaml:"verify_interval"`
	ReviveScanInterval    *string `yaml:"revive_scan_interval"`
	VerifyBackoffBase     *string `yaml:"verify_backoff_base"`
	VerifyBackoffMax      *string `yaml:"verify_backoff_max"`
	VerifyRecoverMax      *string `yaml:"verify_recover_max"`
	VerifyDemoteAfter     *int    `yaml:"verify_demote_after"`
}

type fileConfig struct {
	Settings fileSettings `yaml:"settings"`
	Relays   []fileRelay  `yaml:"relays"`
}

// Load reads, interpolates and validates the desired-state file. Every
// failure is a hard error carrying the file's problem: the caller keeps the
// last-known-good state and never applies a partial load. A missing file is
// reported as an error like any other — the caller decides whether an empty
// fleet is an acceptable start (it is, at boot; the watcher picks the file
// up the moment it appears).
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read desired state %s: %w", path, err)
	}
	return parse(raw)
}

func parse(raw []byte) (*Config, error) {
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // an unknown key is a typo the operator must see, not silent drift
	var fc fileConfig
	if err := dec.Decode(&fc); err != nil {
		return nil, fmt.Errorf("parse desired state: %w", err)
	}
	settings, err := fc.Settings.resolve()
	if err != nil {
		return nil, err
	}
	relays := make([]Relay, 0, len(fc.Relays))
	seen := make(map[string]int, len(fc.Relays))
	slugs := make(map[string]string, len(fc.Relays))
	for i, fr := range fc.Relays {
		relay, err := fr.resolve()
		if err != nil {
			return nil, fmt.Errorf("relays[%d]: %w", i, err)
		}
		if prev, dup := seen[relay.Name]; dup {
			return nil, fmt.Errorf("relays[%d]: duplicate name %q (also relays[%d])", i, relay.Name, prev)
		}
		seen[relay.Name] = i
		// Two distinct names that slug to the same project on one provider
		// would fight over a single platform deployment — a conflict this
		// file must resolve, not the reconciler at runtime.
		slugKey := relay.Provider + "/" + deploy.ProjectName(relay.Name)
		if prev, clash := slugs[slugKey]; clash {
			return nil, fmt.Errorf("relays[%d]: %q and %q map to the same %s project %q — rename one",
				i, prev, relay.Name, relay.Provider, deploy.ProjectName(relay.Name))
		}
		slugs[slugKey] = relay.Name
		relays = append(relays, relay)
	}
	return &Config{Relays: relays, Settings: settings}, nil
}

// resolve turns a file relay into a validated Relay with its credential
// interpolated from the environment.
func (fr fileRelay) resolve() (Relay, error) {
	relay := Relay{
		Name:      strings.TrimSpace(fr.Name),
		Provider:  strings.ToLower(strings.TrimSpace(fr.Provider)),
		Team:      strings.TrimSpace(fr.Team),
		Account:   strings.TrimSpace(fr.Account),
		TokenFile: strings.TrimSpace(fr.TokenFile),
	}
	if relay.Name == "" {
		return Relay{}, errors.New("name is required")
	}
	if len(relay.Name) > 128 {
		return Relay{}, errors.New("name must be at most 128 characters")
	}
	switch relay.Provider {
	case deploy.PlatformVercel:
	case deploy.PlatformCloudflare:
	case deploy.PlatformDeno:
	default:
		return Relay{}, fmt.Errorf(
			"provider %q is not one of the hard-coded providers: %s",
			relay.Provider, strings.Join(deploy.Platforms(), ", "))
	}
	if relay.Provider != deploy.PlatformVercel && relay.Team != "" {
		return Relay{}, fmt.Errorf("team is a vercel scope, not valid for %s", relay.Provider)
	}
	if relay.Provider != deploy.PlatformCloudflare && relay.Account != "" {
		return Relay{}, fmt.Errorf("account is a cloudflare scope pin, not valid for %s", relay.Provider)
	}
	switch {
	case fr.Token != "" && relay.TokenFile != "":
		return Relay{}, errors.New("set either token or token_file, not both")
	case fr.Token != "":
		token, err := interpolate(fr.Token)
		if err != nil {
			return Relay{}, fmt.Errorf("token: %w", err)
		}
		// Same hygiene as token_file (readSecretFile trims): a value that
		// picked up surrounding whitespace — a trailing newline from
		// $(cat …) or a secret manager — is trimmed, and one that is blank
		// afterwards rejects the load instead of deploying a whitespace
		// credential that 401s at every platform call.
		relay.Token = strings.TrimSpace(token)
		if relay.Token == "" {
			return Relay{}, errors.New("token is blank: it carries only whitespace after trimming")
		}
	case relay.TokenFile != "":
	default:
		return Relay{}, errors.New("a provider credential is required: set token (with ${VAR} references) or token_file")
	}
	return relay, nil
}

// ResolveToken returns the relay's provider credential, reading the secret
// file fresh when the relay points at one.
func (r Relay) ResolveToken() (string, error) {
	if r.TokenFile != "" {
		return readSecretFile(r.TokenFile)
	}
	if r.Token == "" {
		return "", fmt.Errorf("relay %s has no provider credential", r.Name)
	}
	return r.Token, nil
}

// interpolate resolves ${VAR} references against the environment. A
// reference to an unset or empty variable is an error — a credential that
// silently resolved to "" would deploy workers that authenticate with
// nothing.
func interpolate(s string) (string, error) {
	var b strings.Builder
	for {
		start := strings.Index(s, "${")
		if start < 0 {
			b.WriteString(s)
			break
		}
		end := strings.Index(s[start:], "}")
		if end < 0 {
			return "", fmt.Errorf("unclosed ${ reference in %q", redact(s))
		}
		name := s[start+2 : start+end]
		if name == "" {
			return "", fmt.Errorf("empty ${} reference in %q", redact(s))
		}
		value, ok := os.LookupEnv(name)
		if !ok || value == "" {
			return "", fmt.Errorf("environment variable %s is not set (referenced as ${%s})", name, name)
		}
		b.WriteString(s[:start])
		b.WriteString(value)
		s = s[start+end+1:]
	}
	return b.String(), nil
}

// redact makes an interpolation error message safe: the raw string may
// carry a literal secret, so only its shape is reported.
func redact(s string) string {
	if len(s) > 16 {
		return s[:16] + "…"
	}
	return s
}

// resolve turns the file settings into typed values, applying defaults for
// every absent key. Malformed values are errors, never silent defaults.
func (fs fileSettings) resolve() (Settings, error) {
	s := defaultSettings()
	var errs []error
	dur := func(what string, dst *time.Duration, raw *string) {
		if raw == nil {
			return
		}
		d, err := time.ParseDuration(*raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("settings.%s %q must be a duration (e.g. 30s)", what, *raw))
			return
		}
		*dst = d
	}
	intVal := func(what string, dst *int, raw *int) {
		if raw == nil {
			return
		}
		*dst = *raw
	}
	if fs.LogLevel != nil {
		level := strings.ToLower(strings.TrimSpace(*fs.LogLevel))
		switch level {
		case "debug", "info", "warn", "error":
			s.LogLevel = level
		default:
			errs = append(errs, fmt.Errorf("settings.log_level %q must be one of debug, info, warn, error", *fs.LogLevel))
		}
	}
	intVal("max_retries", &s.MaxRetries, fs.MaxRetries)
	intVal("failure_threshold", &s.FailureThreshold, fs.FailureThreshold)
	dur("cooldown", &s.Cooldown, fs.Cooldown)
	if fs.StreamThresholdBytes != nil {
		s.StreamThresholdBytes = *fs.StreamThresholdBytes
	}
	dur("dial_timeout", &s.DialTimeout, fs.DialTimeout)
	dur("response_header_timeout", &s.ResponseHeaderTimeout, fs.ResponseHeaderTimeout)
	dur("verify_interval", &s.VerifyInterval, fs.VerifyInterval)
	dur("revive_scan_interval", &s.ReviveScanInterval, fs.ReviveScanInterval)
	dur("verify_backoff_base", &s.VerifyBackoffBase, fs.VerifyBackoffBase)
	dur("verify_backoff_max", &s.VerifyBackoffMax, fs.VerifyBackoffMax)
	dur("verify_recover_max", &s.VerifyRecoverMax, fs.VerifyRecoverMax)
	intVal("verify_demote_after", &s.VerifyDemoteAfter, fs.VerifyDemoteAfter)

	// Range checks: the values below make the failure modes loud instead of
	// pathological (a negative timeout, a zero demote threshold that demotes
	// on the first blip, an unbounded retry storm).
	if s.MaxRetries < 0 || s.MaxRetries > 16 {
		errs = append(errs, fmt.Errorf("settings.max_retries must be in [0, 16], got %d", s.MaxRetries))
	}
	if s.FailureThreshold < 1 {
		errs = append(errs, fmt.Errorf("settings.failure_threshold must be >= 1, got %d", s.FailureThreshold))
	}
	if s.Cooldown < 0 {
		errs = append(errs, fmt.Errorf("settings.cooldown must be >= 0, got %s", s.Cooldown))
	}
	if s.StreamThresholdBytes < 0 {
		errs = append(errs, fmt.Errorf("settings.stream_threshold_bytes must be >= 0, got %d", s.StreamThresholdBytes))
	}
	if s.DialTimeout <= 0 {
		errs = append(errs, fmt.Errorf("settings.dial_timeout must be positive, got %s", s.DialTimeout))
	}
	if s.ResponseHeaderTimeout < 0 {
		errs = append(errs, fmt.Errorf("settings.response_header_timeout must be >= 0, got %s", s.ResponseHeaderTimeout))
	}
	if s.VerifyInterval <= 0 {
		errs = append(errs, fmt.Errorf("settings.verify_interval must be positive, got %s", s.VerifyInterval))
	}
	if s.ReviveScanInterval <= 0 {
		errs = append(errs, fmt.Errorf("settings.revive_scan_interval must be positive, got %s", s.ReviveScanInterval))
	}
	if s.VerifyBackoffBase <= 0 || s.VerifyBackoffMax < s.VerifyBackoffBase {
		errs = append(errs, fmt.Errorf(
			"settings.verify_backoff_base must be positive and verify_backoff_max >= base (base %s, max %s)",
			s.VerifyBackoffBase, s.VerifyBackoffMax))
	}
	if s.VerifyRecoverMax <= 0 {
		errs = append(errs, fmt.Errorf("settings.verify_recover_max must be positive, got %s", s.VerifyRecoverMax))
	}
	if s.VerifyDemoteAfter < 1 {
		errs = append(errs, fmt.Errorf("settings.verify_demote_after must be >= 1, got %d", s.VerifyDemoteAfter))
	}
	if err := errors.Join(errs...); err != nil {
		return Settings{}, err
	}
	return s, nil
}

// Keys returns the desired relay identities in file order.
func (c *Config) Keys() []deploy.RelayKey {
	out := make([]deploy.RelayKey, 0, len(c.Relays))
	for _, r := range c.Relays {
		out = append(out, deploy.RelayKey{Provider: r.Provider, Name: r.Name})
	}
	return out
}

// Relay returns the desired relay with the given identity.
func (c *Config) Relay(provider, name string) (Relay, bool) {
	for _, r := range c.Relays {
		if r.Provider == provider && r.Name == name {
			return r, true
		}
	}
	return Relay{}, false
}

func envStr(key string, dst *string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		*dst = v
	}
}

func validateListenAddr(name, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q must be a host:port address", name, addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s %q has an invalid port %q", name, addr, port)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return nil
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if !validListenerHostname(host) {
		return fmt.Errorf("%s %q has an invalid host", name, addr)
	}
	return nil
}

func validListenerHostname(host string) bool {
	if len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}
