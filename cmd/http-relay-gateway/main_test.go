package main

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestHealthcheckURL(t *testing.T) {
	cases := map[string]string{
		":8080":           "http://127.0.0.1:8080/healthz",
		"0.0.0.0:20130":   "http://127.0.0.1:20130/healthz",
		"127.0.0.1:20130": "http://127.0.0.1:20130/healthz",
		"[::]:8080":       "http://[::1]:8080/healthz",
		"[::1]:8080":      "http://[::1]:8080/healthz",
	}
	for addr, want := range cases {
		if got := healthcheckURL(addr); got != want {
			t.Errorf("healthcheckURL(%q) = %q, want %q", addr, got, want)
		}
	}
}

func TestParseZerologLevel(t *testing.T) {
	cases := map[string]zerolog.Level{
		"debug": zerolog.DebugLevel,
		"info":  zerolog.InfoLevel,
		"warn":  zerolog.WarnLevel,
		"error": zerolog.ErrorLevel,
		"":      zerolog.InfoLevel,
		"bogus": zerolog.InfoLevel,
	}
	for level, want := range cases {
		if got := parseZerologLevel(level); got != want {
			t.Errorf("parseZerologLevel(%q) = %v, want %v", level, got, want)
		}
	}
}
