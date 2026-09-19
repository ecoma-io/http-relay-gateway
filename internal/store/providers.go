package store

import "database/sql"

// Providers returns every provider row ordered by name.
func (s *Store) Providers() ([]ProviderRow, error) {
	rows, err := s.db.Query(`SELECT name, max_body, header_policy FROM providers ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]ProviderRow, 0, 8)
	for rows.Next() {
		var row ProviderRow
		var policy sql.NullString
		if err := rows.Scan(&row.Name, &row.MaxBody, &policy); err != nil {
			return nil, err
		}
		if policy.Valid {
			row.HeaderPolicy = &policy.String
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// upsertProvider writes the body-size contract inside the caller's
// transaction. An existing header_policy is deliberately preserved: the
// bridge (and later the admin plane) manages providers' max-body, while the
// policy belongs to the relay management surface.
func upsertProvider(tx *sql.Tx, row ProviderRow) error {
	_, err := tx.Exec(
		`INSERT INTO providers (name, max_body, header_policy, updated_at)
		 VALUES (?, ?, NULL, strftime('%s', 'now'))
		 ON CONFLICT (name) DO UPDATE SET max_body = excluded.max_body, updated_at = excluded.updated_at`,
		row.Name, row.MaxBody,
	)
	return err
}

// ReplaceProviders swaps the whole provider set in one transaction — the
// PUT semantics of the management surface, which owns both max_body and
// header_policy. A policy of "" is stored as NULL (verbatim forwarding).
// Providers referenced by relays may be removed freely: the pool resolves a
// missing provider to the default body limit at build time.
func (s *Store) ReplaceProviders(rows []ProviderRow) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`DELETE FROM providers`); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := tx.Exec(
			`INSERT INTO providers (name, max_body, header_policy, updated_at)
			 VALUES (?, ?, NULLIF(?, ''), strftime('%s', 'now'))`,
			row.Name, row.MaxBody, row.HeaderPolicy,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}
