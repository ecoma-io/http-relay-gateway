package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"http-relay-gateway/internal/logging"
)

func TestPollerDetectsContentChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("a: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	poller := NewPoller(path, 10*time.Millisecond, logging.Nop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go poller.Run(ctx, logging.Nop())

	// Atomic replace (tmp+rename), the hardest case for event-based watchers.
	tmp := filepath.Join(dir, "config.yaml.tmp")
	if err := os.WriteFile(tmp, []byte("a: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}

	select {
	case <-poller.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("poller never reported the content change")
	}
}

func TestPollerIgnoresUnchangedContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte("a: 1\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	poller := NewPoller(path, 10*time.Millisecond, logging.Nop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go poller.Run(ctx, logging.Nop())

	// Rewrite the identical content several times: no change signal may fire.
	// The replacement is atomic (tmp+rename) on purpose — a raw in-place
	// write exposes a truncate window, and a poll tick landing inside it
	// reads an empty file (a successful read, not an error) and reports a
	// phantom change. That is a real-world no-op for the gateway (the
	// transient "" fails validation, last-known-good keeps serving, the next
	// tick self-heals) but as a race it made this test flaky on loaded CI
	// runners; here every concurrent read must observe a complete file.
	for range 3 {
		tmp := filepath.Join(dir, "config.yaml.tmp")
		if err := os.WriteFile(tmp, content, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	// Thirty ticks leave no scheduling excuse: the hash is compared against
	// the baseline on every one of them and must never differ.
	select {
	case <-poller.Changes():
		t.Fatal("unchanged content reported as a change")
	case <-time.After(300 * time.Millisecond):
	}
}
