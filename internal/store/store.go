// Package store is the SQLite persistence layer and the single source of
// truth for settings, providers, platform accounts, relays and their
// deployments. The data plane never touches the database once its pool is
// built, so every statement comes from the admin plane and one connection
// serializes them all; every mutation funnels through notify() so the
// applier can rebuild serving state from Changes().
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver
)

// Store wraps the database handle and the coalesced change signal.
type Store struct {
	db      *sql.DB
	changes chan struct{}
}

// Open creates the parent directory if needed, opens the SQLite database,
// applies the pragma policy and every pending migration, and returns a
// ready store. Any failure here is fatal at startup by design: a broken
// data file must not boot into a half-configured gateway.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	// One connection serializes every statement: v1 has no concurrent writer
	// worth optimizing for (the data plane is database-free after pool
	// build), and a single connection sidesteps SQLITE_BUSY entirely while
	// WAL keeps a crashed process's committed transactions durable.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	// journal_mode returns the resulting mode as a row. A filesystem that
	// silently downgrades WAL (network mounts, some overlayfs) would undermine
	// the durability reasoning above, so it is a hard error rather than a
	// quiet fallback.
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	if mode != "wal" {
		_ = db.Close()
		return nil, fmt.Errorf("journal_mode = %q, want wal: the data directory must be on a WAL-capable filesystem", mode)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{db: db, changes: make(chan struct{}, 1)}, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// Changes receives one coalesced signal per committed mutation. The applier
// re-reads current database state on wake, so signals dropped while it is
// busy lose nothing.
func (s *Store) Changes() <-chan struct{} { return s.changes }

// notify records one pending change without blocking: the buffered slot
// merges any burst of mutations into a single wakeup.
func (s *Store) notify() {
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

// IsEmpty reports whether the database holds no user state yet — the first
// boot, when the legacy YAML config may be imported wholesale.
func (s *Store) IsEmpty() (bool, error) {
	var relays, settings int
	if err := s.db.QueryRow(
		`SELECT (SELECT COUNT(*) FROM relays), (SELECT COUNT(*) FROM settings)`,
	).Scan(&relays, &settings); err != nil {
		return false, err
	}
	return relays == 0 && settings == 0, nil
}
