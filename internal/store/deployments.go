package store

import (
	"database/sql"
	"errors"
)

// ErrNoDeployment reports that the relay has no deployment row.
var ErrNoDeployment = errors.New("deployment not found")

// Deployment returns the relay's deployment row regardless of status — the
// reconciler probes stale and unreachable relays too — or ErrNoDeployment.
func (s *Store) Deployment(relayID int64) (DeploymentRow, error) {
	var row DeploymentRow
	err := s.db.QueryRow(`
		SELECT id, relay_id, account_id, platform, project, external_id, url, version,
		       auth_token, status, last_error, last_checked_at, deployed_at, created_at, updated_at
		FROM deployments WHERE relay_id = ?`, relayID).
		Scan(&row.ID, &row.RelayID, &row.AccountID, &row.Platform, &row.Project,
			&row.ExternalID, &row.URL, &row.Version, &row.AuthToken, &row.Status,
			&row.LastError, &row.LastCheckedAt, &row.DeployedAt, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentRow{}, ErrNoDeployment
	}
	if err != nil {
		return DeploymentRow{}, err
	}
	return row, nil
}

// execer is the shared surface of *sql.DB and *sql.Tx: the store's single
// connection means transaction work must run on the tx, not the pool.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// UpsertDeployment writes the one deployment row a relay carries: the first
// deploy inserts, a redeploy overwrites. Only a row with status active joins
// the pool (see Relays()), so a deployment reaches the serving pool exactly
// when its version has been verified live.
func (s *Store) UpsertDeployment(row DeploymentRow) error {
	if err := upsertDeployment(s.db, row); err != nil {
		return err
	}
	s.notify()
	return nil
}

// upsertDeployment writes the deployment row on whatever handle the caller
// owns — the pool, or a transaction (adoption lands the relay conversion
// and the deployment atomically).
func upsertDeployment(ex execer, row DeploymentRow) error {
	_, err := ex.Exec(`
		INSERT INTO deployments (relay_id, account_id, platform, project, external_id, url,
		                         version, auth_token, status, last_error, last_checked_at,
		                         deployed_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'), strftime('%s', 'now'))
		ON CONFLICT(relay_id) DO UPDATE SET
			account_id = excluded.account_id,
			platform = excluded.platform,
			project = excluded.project,
			external_id = excluded.external_id,
			url = excluded.url,
			version = excluded.version,
			auth_token = excluded.auth_token,
			status = excluded.status,
			last_error = excluded.last_error,
			last_checked_at = excluded.last_checked_at,
			deployed_at = excluded.deployed_at,
			updated_at = strftime('%s', 'now')`,
		row.RelayID, row.AccountID, row.Platform, row.Project, row.ExternalID, row.URL,
		row.Version, row.AuthToken, row.Status, row.LastError, row.LastCheckedAt,
		row.DeployedAt,
	)
	return err
}

// SetDeploymentStatus records a probe's verdict: the new status, a sanitized
// last error, and the check time. Draining a relay to stale or unreachable
// removes it from the pool (the join only admits active); a probe pass is a
// database mutation like any other and notifies the applier.
func (s *Store) SetDeploymentStatus(relayID int64, status, lastError string, checkedAt int64) error {
	res, err := s.db.Exec(
		`UPDATE deployments SET status = ?, last_error = ?, last_checked_at = ?, updated_at = strftime('%s', 'now')
		 WHERE relay_id = ?`,
		status, lastError, checkedAt, relayID,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNoDeployment
	}
	s.notify()
	return nil
}

// AdoptRelay converts a legacy relay to managed in one transaction: origin,
// account, and the first deployment row land together, so a crash halfway
// leaves the relay wholly unadopted rather than half-managed.
func (s *Store) AdoptRelay(relayID, accountID int64, deployment DeploymentRow) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var (
		exists  int
		origin  string
		account sql.NullInt64
	)
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM relays WHERE id = ?), origin, account_id FROM relays WHERE id = ?`,
		relayID, relayID).Scan(&exists, &origin, &account); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoRelay
		}
		return err
	}
	if exists == 0 {
		return ErrNoRelay
	}
	var accountExists int
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM platform_accounts WHERE id = ?)`, accountID).Scan(&accountExists); err != nil {
		return err
	}
	if accountExists == 0 {
		return ErrNoAccount
	}
	if _, err := tx.Exec(`UPDATE relays SET origin = 'managed', account_id = ?, updated_at = strftime('%s', 'now') WHERE id = ?`,
		accountID, relayID); err != nil {
		return err
	}
	if err := upsertDeployment(tx, deployment); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}

// DeploymentRelayIDsByStatus lists the relay ids whose deployment row is in
// the given status — the revival scan's way to find the paused fleet.
func (s *Store) DeploymentRelayIDsByStatus(status string) ([]int64, error) {
	rows, err := s.db.Query(`SELECT relay_id FROM deployments WHERE status = ? ORDER BY relay_id`, status)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := make([]int64, 0, 8)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// PlatformCounts returns the deployment status distribution — the fleet
// view's drift summary. Statuses absent from the table are omitted.
func (s *Store) PlatformCounts() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM deployments GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	counts := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[status] = n
	}
	return counts, rows.Err()
}
