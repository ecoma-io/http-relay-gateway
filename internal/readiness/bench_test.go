package readiness

import (
	"fmt"
	"testing"
	"time"
)

// benchRegistry builds a registry mirroring 128 relays in mixed lifecycle
// states, the shape a warmed-up gateway operates on.
func benchRegistry(b *testing.B, ready, unready, failed int) *Registry {
	b.Helper()
	r := New(Config{BackoffBase: time.Millisecond, BackoffMax: time.Second, RecoverMax: 100 * time.Millisecond, DemoteAfter: 3})
	n := ready + unready + failed
	present := make([]Key, 0, n)
	for i := range n {
		present = append(present, Key{Provider: "vercel", Name: fmt.Sprintf("relay-%d", i)})
	}
	r.Sync(present)
	i := 0
	for range ready {
		r.Ready(present[i], time.Millisecond)
		i++
	}
	for range unready {
		r.Failing(present[i], ReasonProbeFailed, time.Millisecond)
		r.Failing(present[i], ReasonProbeFailed, time.Millisecond)
		r.Failing(present[i], ReasonProbeFailed, time.Millisecond)
		i++
	}
	for range failed {
		r.Failing(present[i], ReasonUnreachable, time.Millisecond)
		i++
	}
	return r
}

// BenchmarkIsReady is the admission decision per relay per generation build.
func BenchmarkIsReady(b *testing.B) {
	r := benchRegistry(b, 64, 32, 32)
	keys := r.Snapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, k := range keys {
			if !r.IsReady(k.Key) && k.State == StateReady {
				b.Fatal("ready relay reported not ready")
			}
		}
	}
}

// BenchmarkSnapshot is the full lifecycle snapshot rendered by /stats.
func BenchmarkSnapshot(b *testing.B) {
	r := benchRegistry(b, 64, 32, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if got := r.Snapshot(); len(got) != 128 {
			b.Fatalf("snapshot length = %d, want 128", len(got))
		}
	}
}
