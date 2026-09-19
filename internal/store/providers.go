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
