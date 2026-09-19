package store

import (
	"database/sql"
	"fmt"
)

// RuntimeValues are the settings the legacy YAML config still owns while the
// bridge exists. Anything outside this set is database-native and survives
// every sync untouched.
type RuntimeValues struct {
	LogLevel         string
	MaxRetries       int
	FailureThreshold int
	CooldownMs       int64
}

// LegacyRelay is one relay entry from the legacy YAML config.
type LegacyRelay struct {
	Name     string
	Provider string
	URL      string
	Active   bool
}

// SyncLegacy makes the database match a parsed legacy config in one
// transaction: it upserts the bridge-owned settings and provider limits,
// inserts relays new to the database (in file order), refreshes the ones it
// knows, and deletes bridge-owned relays that left the file. Managed relays,
// account rows and non-bridge settings are never touched. It returns how
// many relays were newly inserted — the whole set on first boot.
//
// The transaction is all-or-nothing so a failed sync can never leave the
// database half-matched with a file the poller has already moved past.
func (s *Store) SyncLegacy(vals RuntimeValues, providers []ProviderRow, relays []LegacyRelay) (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := setSetting(tx, keyLogLevel, vals.LogLevel); err != nil {
		return 0, err
	}
	for _, pair := range []struct {
		key   string
		value string
	}{
		{keyMaxRetries, fmt.Sprint(vals.MaxRetries)},
		{keyFailureThreshold, fmt.Sprint(vals.FailureThreshold)},
		{keyCooldownMs, fmt.Sprint(vals.CooldownMs)},
	} {
		if err := setSetting(tx, pair.key, pair.value); err != nil {
			return 0, err
		}
	}
	for _, provider := range providers {
		if err := upsertProvider(tx, provider); err != nil {
			return 0, err
		}
	}

	existing, err := legacyRelayNames(tx)
	if err != nil {
		return 0, err
	}
	inserted := 0
	for _, relay := range relays {
		if existing[relay.Name] {
			if err := updateLegacyRelay(tx, relay); err != nil {
				return 0, err
			}
			continue
		}
		if err := insertLegacyRelay(tx, relay); err != nil {
			return 0, err
		}
		inserted++
	}
	if err := deleteLegacyRelaysNotIn(tx, relays); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	s.notify()
	return inserted, nil
}

// deleteLegacyRelaysNotIn removes every bridge-owned relay whose name is not
// in the given set — the YAML is authoritative for legacy entries. Managed
// relays are exempt by definition.
func deleteLegacyRelaysNotIn(tx *sql.Tx, keep []LegacyRelay) error {
	if len(keep) == 0 {
		_, err := tx.Exec(`DELETE FROM relays WHERE origin = 'legacy'`)
		return err
	}
	query := `DELETE FROM relays WHERE origin = 'legacy' AND name NOT IN (`
	args := make([]any, 0, len(keep))
	for i, relay := range keep {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args = append(args, relay.Name)
	}
	query += `)`
	_, err := tx.Exec(query, args...)
	return err
}
