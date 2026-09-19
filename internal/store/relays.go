package store

import (
	"database/sql"
	"errors"
)

// ErrDuplicateName reports a relay or provider name already in use.
var ErrDuplicateName = errors.New("name already in use")

// ErrNoRelay reports that no relay row carries the given id.
var ErrNoRelay = errors.New("relay not found")

// CreateRelay inserts a management-plane relay (origin managed, no
// deployment yet) and returns its id — the pool position once it deploys.
// A header policy of "" is stored as NULL (verbatim forwarding).
func (s *Store) CreateRelay(name, provider, url string, active bool, accountID *int64, headerPolicy *string) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var taken int
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM relays WHERE name = ?)`, name).Scan(&taken); err != nil {
		return 0, err
	}
	if taken == 1 {
		return 0, ErrDuplicateName
	}
	flags := 0
	if active {
		flags = 1
	}
	// The origin encodes who owns the lifecycle: with an account the relay
	// is born managed (the reconciler deploys its worker); without one it is
	// an external URL — adoptable, never probed, never redeployed.
	origin := OriginLegacy
	if accountID != nil {
		origin = OriginManaged
	}
	res, err := tx.Exec(
		`INSERT INTO relays (name, provider, url, active, origin, account_id, header_policy, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), strftime('%s', 'now'), strftime('%s', 'now'))`,
		name, provider, url, flags, origin, accountID, headerPolicy,
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	s.notify()
	return id, nil
}

// Relay returns one relay row, or ErrNoRelay. It reuses the Relays() join so
// the active-deployment logic lives in exactly one place; the table is small
// enough that scanning it whole costs nothing.
func (s *Store) Relay(id int64) (RelayRow, error) {
	rows, err := s.Relays()
	if err != nil {
		return RelayRow{}, err
	}
	for _, row := range rows {
		if row.ID == id {
			return row, nil
		}
	}
	return RelayRow{}, ErrNoRelay
}

// RelayPatch is a partial relay update; nil fields stay untouched. A non-nil
// empty-string HeaderPolicy clears the stored policy (verbatim forwarding).
type RelayPatch struct {
	Name         *string
	Provider     *string
	URL          *string
	Active       *bool
	HeaderPolicy *string
}

// UpdateRelay applies the patch to one relay. ErrNoRelay when the id does
// not exist; ErrDuplicateName when renaming onto an existing name.
func (s *Store) UpdateRelay(id int64, patch RelayPatch) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM relays WHERE id = ?)`, id).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return ErrNoRelay
	}
	if patch.Name != nil && *patch.Name != "" {
		var taken int
		if err := tx.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM relays WHERE name = ? AND id != ?)`, *patch.Name, id,
		).Scan(&taken); err != nil {
			return err
		}
		if taken == 1 {
			return ErrDuplicateName
		}
	}
	if patch.Name != nil {
		if _, err := tx.Exec(`UPDATE relays SET name = ?, updated_at = strftime('%s', 'now') WHERE id = ?`, *patch.Name, id); err != nil {
			return err
		}
	}
	if patch.Provider != nil {
		if _, err := tx.Exec(`UPDATE relays SET provider = ?, updated_at = strftime('%s', 'now') WHERE id = ?`, *patch.Provider, id); err != nil {
			return err
		}
	}
	if patch.URL != nil {
		if _, err := tx.Exec(`UPDATE relays SET url = ?, updated_at = strftime('%s', 'now') WHERE id = ?`, *patch.URL, id); err != nil {
			return err
		}
	}
	if patch.Active != nil {
		flags := 0
		if *patch.Active {
			flags = 1
		}
		if _, err := tx.Exec(`UPDATE relays SET active = ?, updated_at = strftime('%s', 'now') WHERE id = ?`, flags, id); err != nil {
			return err
		}
	}
	if patch.HeaderPolicy != nil {
		if _, err := tx.Exec(
			`UPDATE relays SET header_policy = NULLIF(?, ''), updated_at = strftime('%s', 'now') WHERE id = ?`,
			*patch.HeaderPolicy, id,
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

// DeleteRelay removes one relay row; its deployment (if any) cascades.
// ErrNoRelay when the id does not exist.
func (s *Store) DeleteRelay(id int64) error {
	res, err := s.db.Exec(`DELETE FROM relays WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNoRelay
	}
	s.notify()
	return nil
}

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
