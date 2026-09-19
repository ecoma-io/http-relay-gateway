package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Runtime defaults; every one is overridable in the runtime YAML.
const (
	DefaultLogLevel         = "info"
	DefaultMaxRetries       = 2
	DefaultFailureThreshold = 3
	DefaultCooldown         = 30 * time.Second
	// DefaultMaxBody applies to relays whose provider has no providers entry.
	DefaultMaxBody = 8 << 20 // 8 MiB
)

// reservedProviders cannot be used as a pin value: the first four collide
// with selector keywords (X-Relay-Provider header / path prefix), the rest
// with the gateway's own endpoints.
var reservedProviders = map[string]bool{
	"": true, "all": true, "none": true, "auto": true,
	"healthz": true, "stats": true,
}

// ProviderSpec carries the per-provider contract enforced by the edge
// platforms themselves: Vercel serverless rejects request bodies over ~4.5 MB,
// while Cloudflare Workers accept up to ~100 MB. The gateway buffers up to the
// largest configured limit and skips relays whose provider limit the body
// exceeds, so an oversized request routes to a provider that accepts it
// instead of failing at the edge.
type ProviderSpec struct {
	MaxBody int64
}

// RelaySpec is one validated edge relay from the runtime config.
type RelaySpec struct {
	Name     string
	Provider string // vercel | cloudflare | deno | ... (any label)
	URL      *url.URL
	Active   bool
}

// RuntimeConfig is the immutable set of values used by new client operations.
// Callers must replace the entire value on reload rather than mutate it.
type RuntimeConfig struct {
	LogLevel         string
	MaxRetries       int
	FailureThreshold int
	Cooldown         time.Duration
	Providers        map[string]ProviderSpec
	Relays           []RelaySpec
}

// ProviderMaxBody returns the request-body limit for provider, applying the
// default for providers without an explicit entry.
func (c *RuntimeConfig) ProviderMaxBody(provider string) int64 {
	if spec, ok := c.Providers[provider]; ok {
		return spec.MaxBody
	}
	return DefaultMaxBody
}

// MaxBufferBytes is the buffer cap for any single request body: the largest
// per-provider limit, since provider selection happens per relay attempt
// after the body is already in memory.
func (c *RuntimeConfig) MaxBufferBytes() int64 {
	max := int64(DefaultMaxBody)
	for _, spec := range c.Providers {
		if spec.MaxBody > max {
			max = spec.MaxBody
		}
	}
	return max
}

type fileConfig struct {
	LogLevel         string                        `mapstructure:"log-level"`
	MaxRetries       *int                          `mapstructure:"max-retries"`
	FailureThreshold *int                          `mapstructure:"failure-threshold"`
	Cooldown         string                        `mapstructure:"cooldown"`
	Providers        map[string]providerFileConfig `mapstructure:"providers"`
	Relays           []relayFileConfig             `mapstructure:"relays"`
}

type providerFileConfig struct {
	MaxBody any `mapstructure:"max-body"`
}

type relayFileConfig struct {
	Name     string `mapstructure:"name"`
	Provider string `mapstructure:"provider"`
	URL      string `mapstructure:"url"`
	Active   *bool  `mapstructure:"active"`
}

// LoadRuntime reads and strictly validates the YAML runtime config at path.
// It creates an isolated Viper instance for every load so test and reload state
// cannot leak through Viper's package-global configuration. UnmarshalExact
// rejects unknown keys, so a mistyped or removed key fails validation and the
// last-known-good configuration keeps serving.
func LoadRuntime(path string) (*RuntimeConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("runtime config %q not found; copy config.example.yaml to config.yaml and set CONFIG_FILE if needed", path)
		}
		return nil, fmt.Errorf("read runtime config: %w", err)
	}
	var raw fileConfig
	if err := v.UnmarshalExact(&raw); err != nil {
		return nil, fmt.Errorf("decode runtime config: %w", err)
	}
	return runtimeFromFile(raw)
}

// runtimeFromFile applies defaults and validates every entry.
func runtimeFromFile(raw fileConfig) (*RuntimeConfig, error) {
	var errs []error
	cfg := &RuntimeConfig{
		LogLevel:         DefaultLogLevel,
		MaxRetries:       DefaultMaxRetries,
		FailureThreshold: DefaultFailureThreshold,
		Cooldown:         DefaultCooldown,
		Providers:        map[string]ProviderSpec{},
	}
	switch raw.LogLevel {
	case "":
	case "debug", "info", "warn", "error":
		cfg.LogLevel = raw.LogLevel
	default:
		errs = append(errs, fmt.Errorf("log-level must be one of debug, info, warn, error, got %q", raw.LogLevel))
	}
	if raw.MaxRetries != nil {
		if *raw.MaxRetries < 1 {
			errs = append(errs, fmt.Errorf("max-retries must be >= 1, got %d", *raw.MaxRetries))
		} else {
			cfg.MaxRetries = *raw.MaxRetries
		}
	}
	if raw.FailureThreshold != nil {
		if *raw.FailureThreshold < 1 {
			errs = append(errs, fmt.Errorf("failure-threshold must be >= 1, got %d", *raw.FailureThreshold))
		} else {
			cfg.FailureThreshold = *raw.FailureThreshold
		}
	}
	if raw.Cooldown != "" {
		d, err := time.ParseDuration(raw.Cooldown)
		if err != nil {
			errs = append(errs, fmt.Errorf("cooldown must be a Go duration"))
		} else if d <= 0 {
			errs = append(errs, fmt.Errorf("cooldown must be positive, got %s", d))
		} else {
			cfg.Cooldown = d
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	for name, rawProvider := range raw.Providers {
		// Viper lowercases YAML keys; provider labels are case-insensitive.
		maxBody, err := parseBodySize(rawProvider.MaxBody)
		if err != nil {
			return nil, fmt.Errorf("providers.%s.max-body: %w", name, err)
		}
		cfg.Providers[strings.ToLower(name)] = ProviderSpec{MaxBody: maxBody}
	}

	if len(raw.Relays) == 0 {
		return nil, errors.New("relays must contain at least one relay")
	}
	seen := make(map[string]struct{}, len(raw.Relays))
	for i, relay := range raw.Relays {
		spec, err := parseRelaySpec(relay, i)
		if err != nil {
			return nil, fmt.Errorf("relays[%d]: %w", i, err)
		}
		if _, duplicate := seen[spec.Name]; duplicate {
			return nil, fmt.Errorf("relays[%d]: duplicate relay name %q", i, spec.Name)
		}
		seen[spec.Name] = struct{}{}
		cfg.Relays = append(cfg.Relays, spec)
	}
	return cfg, nil
}

// parseRelaySpec validates one relay entry.
func parseRelaySpec(raw relayFileConfig, index int) (RelaySpec, error) {
	spec := RelaySpec{
		Provider: strings.ToLower(strings.TrimSpace(raw.Provider)),
	}
	if reservedProviders[spec.Provider] {
		return RelaySpec{}, fmt.Errorf("provider %q is reserved", raw.Provider)
	}
	spec.Name = raw.Name
	if spec.Name == "" {
		spec.Name = fmt.Sprintf("%s-%d", spec.Provider, index+1)
	}
	u, err := url.Parse(strings.TrimSpace(raw.URL))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return RelaySpec{}, fmt.Errorf("url %q must be an absolute http(s) URL", raw.URL)
	}
	spec.URL = u
	spec.Active = raw.Active == nil || *raw.Active
	return spec, nil
}

// parseBodySize accepts whole YAML integers (bytes) or strings with decimal
// ("4.5mb" = 4_500_000) or binary ("8MiB" = 8*2^20) unit suffixes. Only whole
// YAML integers pass the first branch: viper's weak typing would otherwise
// silently truncate 2.5 to 2 and turn a config typo into a quiet limit change.
func parseBodySize(raw any) (int64, error) {
	switch v := raw.(type) {
	case nil:
		return DefaultMaxBody, nil
	case int:
		if v < 1 {
			return 0, fmt.Errorf("must be a positive byte count, got %d", v)
		}
		return int64(v), nil
	case int64:
		if v < 1 {
			return 0, fmt.Errorf("must be a positive byte count, got %d", v)
		}
		return v, nil
	case string:
		return parseBodySizeString(v)
	default:
		return 0, errors.New(`must be a byte count or a size like "4.5mb"`)
	}
}

func parseBodySizeString(raw string) (int64, error) {
	s := strings.TrimSpace(strings.ToLower(raw))
	if s == "" {
		return 0, errors.New("must not be empty")
	}
	// Split digits (with optional fraction) from the unit suffix.
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	digits, unit := s[:i], strings.TrimSpace(s[i:])
	if digits == "" || unit == "" {
		return 0, fmt.Errorf("invalid size %q (want e.g. 4500000 or 4.5mb)", raw)
	}
	multiplier, ok := bodySizeUnit(unit)
	if !ok {
		return 0, fmt.Errorf("unknown size unit %q (want kb, mb, gb, kib, mib, gib)", unit)
	}
	value, err := parsePositiveFloat(digits)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	result := int64(value * float64(multiplier))
	if result < 1 {
		return 0, fmt.Errorf("must be a positive size, got %q", raw)
	}
	return result, nil
}

func bodySizeUnit(unit string) (int64, bool) {
	unit = strings.TrimSuffix(unit, "b")
	switch unit {
	case "k":
		return 1000, true
	case "m":
		return 1000 * 1000, true
	case "g":
		return 1000 * 1000 * 1000, true
	case "ki":
		return 1 << 10, true
	case "mi":
		return 1 << 20, true
	case "gi":
		return 1 << 30, true
	default:
		return 0, false
	}
}

// parsePositiveFloat parses a finite positive decimal without pulling in
// strconv's full float grammar.
func parsePositiveFloat(s string) (float64, error) {
	if len(s) > 18 { // bounds magnitude below float64 precision loss
		return 0, errors.New("too large")
	}
	var value float64
	var sawDigit bool
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			value = value*10 + float64(c-'0')
			sawDigit = true
		case c == '.':
			if !sawDigit {
				return 0, errors.New("no integer part")
			}
			frac := 0.0
			scale := 1.0
			for j := i + 1; j < len(s); j++ {
				if s[j] < '0' || s[j] > '9' {
					return 0, errors.New("bad fraction")
				}
				frac = frac*10 + float64(s[j]-'0')
				scale *= 10
			}
			value += frac / scale
			return value, nil
		default:
			return 0, errors.New("bad character")
		}
	}
	if !sawDigit {
		return 0, errors.New("no digits")
	}
	return value, nil
}
