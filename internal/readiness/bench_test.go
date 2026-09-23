package readiness

import (
	"fmt"
	"testing"
)

// benchRegistry builds a mid-size fleet: every key configured, half of them
// admitted with parseable URLs, mirroring the steady state the pool builder
// and /stats read from.
func benchRegistry(b *testing.B) (*Registry, []Key) {
	b.Helper()
	keys := make([]Key, 10)
	for i := range keys {
		keys[i] = key("cloudflare", fmt.Sprintf("relay-%02d", i))
	}
	r := New(Config{Now: newClock().Now})
	r.Sync(members(keys...))
	for i, k := range keys {
		if i%2 == 0 {
			admitBench(b, r, k)
		}
	}
	return r, keys
}

func admitBench(b *testing.B, r *Registry, k Key) {
	b.Helper()
	release, gen, ok := r.Begin(k)
	if !ok {
		b.Fatalf("Begin failed on idle relay %v", k)
	}
	r.Ready(k, gen, "https://"+k.Name+".example", "bench-relay-key", 0)
	release()
}

func BenchmarkRegistryIsReady(b *testing.B) {
	r, keys := benchRegistry(b)
	b.ResetTimer()
	for range b.N {
		_ = r.IsReady(keys[0])
	}
}

func BenchmarkRegistrySnapshot(b *testing.B) {
	r, _ := benchRegistry(b)
	b.ResetTimer()
	for range b.N {
		_ = r.Snapshot()
	}
}

func BenchmarkRegistryServing(b *testing.B) {
	r, _ := benchRegistry(b)
	b.ResetTimer()
	for range b.N {
		_ = r.Serving()
	}
}
