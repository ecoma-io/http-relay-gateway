// Package config validates bootstrap settings and parses runtime relays.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func envStr(key string, dst *string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		*dst = v
	}
}

// Defaults applied when the corresponding environment variable is unset.
const (
	DefaultListenAddr = ":8080"
	// DefaultAdminAddr binds the management plane to loopback: the admin API
	// and its UI must never be reachable from the network the data plane
	// serves. Deployments that want it exposed change the bind explicitly.
	DefaultAdminAddr = "127.0.0.1:20131"
	// DefaultDataFile is the SQLite database holding relays, settings and
	// platform accounts.
	DefaultDataFile = "data/gateway.db"
	// DefaultShutdownGrace bounds the whole-process graceful drain. 20s fits
	// under a 30s docker stop_grace_period.
	DefaultShutdownGrace = 20 * time.Second
)

// BootstrapConfig contains process-level settings. They are intentionally read
// only when the process starts because changing the listening sockets or the
// database in place would require a coordinated server restart. Everything the
// management plane can change at runtime — the relay pool, body limits, the
// log level — lives in the database instead.
type BootstrapConfig struct {
	ListenAddr    string
	AdminAddr     string
	DataFile      string
	ShutdownGrace time.Duration
	// AdminCookieSecure sets the Secure attribute on admin session cookies.
	// Off by default because the admin plane is loopback HTTP; turn it on
	// when fronting the admin listener with HTTPS.
	AdminCookieSecure bool
}

// LoadBootstrap applies bootstrap defaults and explicit environment overrides.
func LoadBootstrap() (*BootstrapConfig, error) {
	cfg := &BootstrapConfig{
		ListenAddr:    DefaultListenAddr,
		AdminAddr:     DefaultAdminAddr,
		DataFile:      DefaultDataFile,
		ShutdownGrace: DefaultShutdownGrace,
	}
	envStr("LISTEN_ADDR", &cfg.ListenAddr)
	envStr("ADMIN_ADDR", &cfg.AdminAddr)
	envStr("DATA_FILE", &cfg.DataFile)
	// Parsed inline rather than through a helper so a malformed value fails
	// fast instead of silently falling back to the default.
	if raw, ok := os.LookupEnv("SHUTDOWN_GRACE"); ok && raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("SHUTDOWN_GRACE %q must be a Go duration", raw)
		}
		cfg.ShutdownGrace = d
	}
	if raw, ok := os.LookupEnv("ADMIN_COOKIE_SECURE"); ok && raw != "" {
		secure, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("ADMIN_COOKIE_SECURE %q must be a boolean", raw)
		}
		cfg.AdminCookieSecure = secure
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *BootstrapConfig) validate() error {
	var errs []error
	if strings.TrimSpace(c.DataFile) == "" {
		errs = append(errs, errors.New("DATA_FILE must not be empty"))
	}
	if c.ShutdownGrace <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_GRACE must be positive, got %s", c.ShutdownGrace))
	}
	if err := validateListenAddr("LISTEN_ADDR", c.ListenAddr); err != nil {
		errs = append(errs, err)
	}
	if err := validateListenAddr("ADMIN_ADDR", c.AdminAddr); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func validateListenAddr(name, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q must be a host:port address", name, addr)
	}
	if err := checkPort(port); err != nil {
		return fmt.Errorf("%s: %w", name, err)
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

func checkPort(port string) error {
	n, err := net.LookupPort("tcp", port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port %q", port)
	}
	return nil
}
