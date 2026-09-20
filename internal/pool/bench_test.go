package pool

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

// BenchmarkPoolPick measures the hot-path selection: the per-key cursor walk
// over healthy candidates, the shape every forwarded request pays for.
func BenchmarkPoolPick(b *testing.B) {
	const n = 10
	providers := []string{"vercel", "cloudflare", "deno"}
	relays := make([]RelayInput, 0, n)
	for i := range n {
		provider := providers[i%len(providers)]
		u, err := url.Parse(fmt.Sprintf("http://relay-%02d.example.internal", i))
		if err != nil {
			b.Fatalf("parse relay url: %v", err)
		}
		relays = append(relays, RelayInput{
			Name:     fmt.Sprintf("relay-%02d", i),
			Provider: provider,
			URL:      u,
			Token:    "bench-token",
			MaxBody:  ProviderMaxBody(provider),
		})
	}
	p, err := New(Input{FailureThreshold: 3, Cooldown: time.Second, Relays: relays})
	if err != nil {
		b.Fatalf("pool.New: %v", err)
	}

	b.Run("all", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if r := p.Pick(KeyAll); r == nil {
				b.Fatal("Pick returned nil")
			}
		}
	})
	b.Run("provider", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if r := p.Pick("vercel"); r == nil {
				b.Fatal("Pick returned nil")
			}
		}
	})
}
