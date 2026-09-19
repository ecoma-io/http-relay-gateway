package store

import (
	"database/sql"
)

// Relays returns every relay ordered by id — insertion order, which the pool
// uses as its round-robin order. The active deployment (if any) is joined in;
// relays whose deployment is pending, stale or failed come back without one.
func (s *Store) Relays() ([]RelayRow, error) {
	rows, err := s.db.Query(`
		SELECT r.id, r.name, r.provider, r.url, r.active, r.origin, r.account_id,
		       r.header_policy, r.created_at, r.updated_at,
		       d.id, d.account_id, d.platform, d.project, d.external_id, d.url,
		       d.version, d.auth_token, d.status, d.last_error,
		       d.last_checked_at, d.deployed_at, d.created_at, d.updated_at
		FROM relays r
		LEFT JOIN deployments d ON d.relay_id = r.id AND d.status = 'active'
		ORDER BY r.id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]RelayRow, 0, 16)
	for rows.Next() {
		var (
			row         RelayRow
			origin      string
			accountID   sql.NullInt64
			policy      sql.NullString
			dID         sql.NullInt64
			dAccountID  sql.NullInt64
			dPlatform   sql.NullString
			dProject    sql.NullString
			dExternalID sql.NullString
			dURL        sql.NullString
			dVersion    sql.NullString
			dToken      sql.NullString
			dStatus     sql.NullString
			dLastError  sql.NullString
			dCheckedAt  sql.NullInt64
			dDeployedAt sql.NullInt64
			dCreatedAt  sql.NullInt64
			dUpdatedAt  sql.NullInt64
		)
		if err := rows.Scan(
			&row.ID, &row.Name, &row.Provider, &row.URL, &row.Active, &origin,
			&accountID, &policy, &row.CreatedAt, &row.UpdatedAt,
			&dID, &dAccountID, &dPlatform, &dProject, &dExternalID, &dURL,
			&dVersion, &dToken, &dStatus, &dLastError,
			&dCheckedAt, &dDeployedAt, &dCreatedAt, &dUpdatedAt,
		); err != nil {
			return nil, err
		}
		row.Origin = RelayOrigin(origin)
		if accountID.Valid {
			id := accountID.Int64
			row.AccountID = &id
		}
		if policy.Valid {
			row.HeaderPolicy = &policy.String
		}
		if dID.Valid {
			row.Deployment = &DeploymentRow{
				ID:            dID.Int64,
				RelayID:       row.ID,
				AccountID:     dAccountID.Int64,
				Platform:      dPlatform.String,
				Project:       dProject.String,
				ExternalID:    dExternalID.String,
				URL:           dURL.String,
				Version:       dVersion.String,
				AuthToken:     dToken.String,
				Status:        dStatus.String,
				LastError:     dLastError.String,
				LastCheckedAt: dCheckedAt.Int64,
				DeployedAt:    dDeployedAt.Int64,
				CreatedAt:     dCreatedAt.Int64,
				UpdatedAt:     dUpdatedAt.Int64,
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// legacyRelayNames returns the names of every bridge-owned relay. It runs on
// the caller's transaction: with the single-connection pool the transaction
// owns the only connection, so any s.db query here would wait on itself
// forever.
func legacyRelayNames(tx *sql.Tx) (map[string]bool, error) {
	rows, err := tx.Query(`SELECT name FROM relays WHERE origin = 'legacy'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names[name] = true
	}
	return names, rows.Err()
}

// insertLegacyRelay adds one bridge-owned relay inside the caller's
// transaction; its autoincrement id fixes its position in the pool order.
func insertLegacyRelay(tx *sql.Tx, relay LegacyRelay) error {
	active := 0
	if relay.Active {
		active = 1
	}
	_, err := tx.Exec(
		`INSERT INTO relays (name, provider, url, active, origin, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 'legacy', strftime('%s', 'now'), strftime('%s', 'now'))`,
		relay.Name, relay.Provider, relay.URL, active,
	)
	return err
}

// updateLegacyRelay refreshes the YAML-owned columns of an existing
// bridge-owned relay, keeping its id (pool position), origin, account and
// header policy untouched.
func updateLegacyRelay(tx *sql.Tx, relay LegacyRelay) error {
	active := 0
	if relay.Active {
		active = 1
	}
	_, err := tx.Exec(
		`UPDATE relays SET provider = ?, url = ?, active = ?, updated_at = strftime('%s', 'now')
		 WHERE name = ? AND origin = 'legacy'`,
		relay.Provider, relay.URL, active, relay.Name,
	)
	return err
}
