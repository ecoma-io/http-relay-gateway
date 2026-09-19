package store

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrate applies every migration numbered above the recorded schema_version,
// each inside one transaction together with its schema_version row. Shipped
// files are append-only: an applied migration is never edited, and a new one
// takes the next number, so an upgraded binary replays exactly the files the
// previous one never saw.
func migrate(db *sql.DB) error {
	current, err := schemaVersion(db)
	if err != nil {
		return err
	}
	names, err := migrationNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		version, err := migrationVersion(name)
		if err != nil {
			return err
		}
		if version <= current {
			continue
		}
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%s: %w", name, err)
		}
		// The version row commits atomically with the DDL above: a crash
		// mid-migration leaves the database at the old version and the next
		// boot replays the file from scratch.
		if _, err := tx.Exec(
			`INSERT INTO schema_version (version, applied_at) VALUES (?, strftime('%s', 'now'))`,
			version,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%s: record version: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("%s: commit: %w", name, err)
		}
	}
	return nil
}

// schemaVersion returns the highest applied migration version, 0 for a
// database without any.
func schemaVersion(db *sql.DB) (int, error) {
	var tables int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'`,
	).Scan(&tables); err != nil {
		return 0, err
	}
	if tables == 0 {
		return 0, nil
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

// migrationVersion parses the leading "0001_"-style numeric prefix that
// orders and identifies every migration file.
func migrationVersion(name string) (int, error) {
	prefix, _, found := strings.Cut(name, "_")
	if !found {
		return 0, fmt.Errorf("migration %q has no numeric prefix", name)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("migration %q has a non-numeric prefix", name)
	}
	return version, nil
}
