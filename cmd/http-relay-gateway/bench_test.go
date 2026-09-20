package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/readiness"
)

// BenchmarkBuildState measures the per-generation cost: verified snapshot
// scan, URL parse, body-limit resolution and the lifecycle projection.
func BenchmarkBuildState(b *testing.B) {
	const n = 10
	providers := []string{deploy.PlatformVercel, deploy.PlatformCloudflare, deploy.PlatformDeno}
	keys := make([]readiness.Key, 0, n)
	for i := range n {
		keys = append(keys, readiness.Key{
			Provider: providers[i%len(providers)],
			Name:     fmt.Sprintf("relay-%02d", i),
		})
	}
	reg := readiness.New(readiness.Config{})
	reg.Sync(keys)
	for _, k := range keys {
		gen, ok := reg.GenerationOf(k)
		if !ok {
			b.Fatalf("key %v missing after Sync", k)
		}
		reg.Ready(k, gen, "http://"+k.Name+".example.internal", "bench-key", time.Millisecond)
	}

	settings := config.DefaultSettings()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if st := buildState(reg, settings, zerolog.Nop()); st.Pool.ReadyCount() != n {
			b.Fatalf("pool serves %d relays, want the %d verified", st.Pool.ReadyCount(), n)
		}
	}
}
