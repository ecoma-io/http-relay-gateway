package config

import (
	"testing"
	"time"
)

func TestLoadBootstrapDefaults(t *testing.T) {
	t.Setenv("CONFIG_FILE", "")
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("SHUTDOWN_GRACE", "")
	cfg, err := LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigFile != DefaultConfigFile || cfg.ListenAddr != DefaultListenAddr {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.ShutdownGrace != DefaultShutdownGrace {
		t.Fatalf("shutdown grace = %s", cfg.ShutdownGrace)
	}
}

func TestLoadBootstrapOverrides(t *testing.T) {
	t.Setenv("CONFIG_FILE", "/etc/relay/config.yaml")
	t.Setenv("LISTEN_ADDR", "127.0.0.1:20130")
	t.Setenv("SHUTDOWN_GRACE", "5s")
	cfg, err := LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigFile != "/etc/relay/config.yaml" || cfg.ListenAddr != "127.0.0.1:20130" || cfg.ShutdownGrace != 5*time.Second {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestLoadBootstrapRejectsBadValues(t *testing.T) {
	t.Run("grace not a duration", func(t *testing.T) {
		t.Setenv("SHUTDOWN_GRACE", "not-a-duration")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for non-duration SHUTDOWN_GRACE")
		}
	})
	t.Run("grace must be positive", func(t *testing.T) {
		t.Setenv("SHUTDOWN_GRACE", "0s")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for zero SHUTDOWN_GRACE")
		}
	})
	t.Run("port out of range", func(t *testing.T) {
		t.Setenv("LISTEN_ADDR", "127.0.0.1:99999")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for out-of-range port")
		}
	})
	t.Run("missing port", func(t *testing.T) {
		t.Setenv("LISTEN_ADDR", "no-port-here")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for address without port")
		}
	})
	t.Run("invalid hostname", func(t *testing.T) {
		t.Setenv("LISTEN_ADDR", "under_score:8080")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for invalid hostname")
		}
	})
}
