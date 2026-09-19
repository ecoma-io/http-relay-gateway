package config

import (
	"testing"
	"time"
)

func TestLoadBootstrapDefaults(t *testing.T) {
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("ADMIN_ADDR", "")
	t.Setenv("DATA_FILE", "")
	t.Setenv("SHUTDOWN_GRACE", "")
	t.Setenv("ADMIN_COOKIE_SECURE", "")
	cfg, err := LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != DefaultListenAddr {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.AdminAddr != DefaultAdminAddr || cfg.DataFile != DefaultDataFile {
		t.Fatalf("unexpected admin/data defaults: %+v", cfg)
	}
	if cfg.ShutdownGrace != DefaultShutdownGrace {
		t.Fatalf("shutdown grace = %s", cfg.ShutdownGrace)
	}
	if cfg.AdminCookieSecure {
		t.Fatal("admin cookie secure must default off (loopback HTTP)")
	}
}

func TestLoadBootstrapOverrides(t *testing.T) {
	t.Setenv("LISTEN_ADDR", "127.0.0.1:20130")
	t.Setenv("ADMIN_ADDR", "127.0.0.1:20131")
	t.Setenv("DATA_FILE", "/var/lib/relay/gateway.db")
	t.Setenv("SHUTDOWN_GRACE", "5s")
	t.Setenv("ADMIN_COOKIE_SECURE", "true")
	cfg, err := LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "127.0.0.1:20130" || cfg.ShutdownGrace != 5*time.Second {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.AdminAddr != "127.0.0.1:20131" || cfg.DataFile != "/var/lib/relay/gateway.db" {
		t.Fatalf("admin/data overrides not applied: %+v", cfg)
	}
	if !cfg.AdminCookieSecure {
		t.Fatal("ADMIN_COOKIE_SECURE=true not applied")
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
	t.Run("admin addr without port", func(t *testing.T) {
		t.Setenv("ADMIN_ADDR", "no-port-here")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for ADMIN_ADDR without port")
		}
	})
	t.Run("empty data file", func(t *testing.T) {
		t.Setenv("DATA_FILE", " ")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for blank DATA_FILE")
		}
	})
	t.Run("cookie secure not a boolean", func(t *testing.T) {
		t.Setenv("ADMIN_COOKIE_SECURE", "sometimes")
		if _, err := LoadBootstrap(); err == nil {
			t.Fatal("want error for non-boolean ADMIN_COOKIE_SECURE")
		}
	})
}
