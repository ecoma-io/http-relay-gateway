package main

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"http-relay-gateway/internal/readiness"
	"http-relay-gateway/internal/store"
)

// BenchmarkBuildGeneration measures the full database → immutable pool
// rebuild: settings, providers, relays, registry sync and the per-relay
// admission filter, at a 128-relay fleet size.
func BenchmarkBuildGeneration(b *testing.B) {
	s, err := store.Open(filepath.Join(b.TempDir(), "gateway.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	providers := make([]store.ProviderRow, 4)
	for i := range providers {
		providers[i] = store.ProviderRow{Name: fmt.Sprintf("p%d", i), MaxBody: 4_500_000}
	}
	relays := make([]store.LegacyRelay, 0, 128)
	for i := range 128 {
		relays = append(relays, store.LegacyRelay{
			Name: fmt.Sprintf("relay-%d", i), Provider: providers[i%4].Name,
			URL: fmt.Sprintf("http://127.0.0.1:%d", 20000+i), Active: true,
		})
	}
	if _, err := s.SyncLegacy(store.RuntimeValues{MaxRetries: 2, LogLevel: "info"}, providers, relays); err != nil {
		b.Fatal(err)
	}
	if err := s.SetSettings(map[string]string{"stream_threshold_bytes": "1048576"}); err != nil {
		b.Fatal(err)
	}

	reg := readiness.New(readiness.Config{BackoffBase: time.Millisecond, BackoffMax: time.Second, RecoverMax: 100 * time.Millisecond, DemoteAfter: 3})
	rows, err := s.Relays()
	if err != nil {
		b.Fatal(err)
	}
	for _, row := range rows {
		reg.Ready(readiness.Key{Provider: row.Provider, Name: row.Name}, time.Millisecond)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		gen, err := buildGeneration(s, reg)
		if err != nil {
			b.Fatal(err)
		}
		if gen.state == nil {
			b.Fatal("generation state is nil")
		}
	}
}
