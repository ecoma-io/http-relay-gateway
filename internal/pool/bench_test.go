package pool

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

// BenchmarkReadyCount measures the readiness gate's counter on a
// realistic-sized pool — the /readyz admission answer, called per request
// on an empty pool and per build elsewhere.
func BenchmarkReadyCount(b *testing.B) {
	const n = 128
	in := Input{FailureThreshold: 3, Cooldown: 30 * time.Second}
	for i := range n {
		u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", 10000+i))
		if err != nil {
			b.Fatal(err)
		}
		in.Relays = append(in.Relays, RelayInput{
			ID: int64(i), Name: fmt.Sprintf("relay-%d", i),
			Provider: "vercel", URL: u, Active: i%2 == 0, Origin: OriginLegacy,
		})
	}
	p, err := New(in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if p.ReadyCount() != n/2 {
			b.Fatal("unexpected ready count")
		}
	}
}
