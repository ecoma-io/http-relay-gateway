package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrNoAccount reports that no platform account row carries the given id.
var ErrNoAccount = errors.New("account not found")

// ErrAccountInUse reports that managed relays or deployments still reference
// the account; a non-forced delete refuses.
var ErrAccountInUse = errors.New("account still in use by managed relays")

// ErrBadPlatform reports an account platform outside the supported set.
var ErrBadPlatform = errors.New("unknown platform")

// ValidPlatform reports whether the platform is one the deployers support.
func ValidPlatform(platform string) bool {
	for _, known := range Platforms {
		if known == platform {
			return true
		}
	}
	return false
}

// Accounts returns every platform account ordered by id.
func (s *Store) Accounts() ([]AccountRow, error) {
	rows, err := s.db.Query(`
		SELECT id, name, platform, account_ref, verified_at, created_at, updated_at
		FROM platform_accounts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []AccountRow{}
	for rows.Next() {
		var row AccountRow
		if err := rows.Scan(&row.ID, &row.Name, &row.Platform, &row.AccountRef,
			&row.VerifiedAt, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// Account returns one platform account, or ErrNoAccount.
func (s *Store) Account(id int64) (AccountRow, error) {
	var row AccountRow
	err := s.db.QueryRow(`
		SELECT id, name, platform, account_ref, verified_at, created_at, updated_at
		FROM platform_accounts WHERE id = ?`, id).
		Scan(&row.ID, &row.Name, &row.Platform, &row.AccountRef,
			&row.VerifiedAt, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AccountRow{}, ErrNoAccount
	}
	if err != nil {
		return AccountRow{}, err
	}
	return row, nil
}

// CreateAccount inserts one platform account (its token already verified by
// the caller) and returns its id.
func (s *Store) CreateAccount(name, platform, token, accountRef string, verifiedAt int64) (int64, error) {
	if !ValidPlatform(platform) {
		return 0, fmt.Errorf("%w: %s", ErrBadPlatform, platform)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var taken int
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM platform_accounts WHERE name = ?)`, name).Scan(&taken); err != nil {
		return 0, err
	}
	if taken == 1 {
		return 0, ErrDuplicateName
	}
	res, err := tx.Exec(
		`INSERT INTO platform_accounts (name, platform, token, account_ref, verified_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, strftime('%s', 'now'), strftime('%s', 'now'))`,
		name, platform, token, accountRef, verifiedAt,
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

// AccountPatch is a partial account update; nil fields stay untouched. A
// token change is the caller's opportunity to re-verify.
type AccountPatch struct {
	Name       *string
	Token      *string
	AccountRef *string
}

// UpdateAccount applies the patch to one account.
func (s *Store) UpdateAccount(id int64, patch AccountPatch) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM platform_accounts WHERE id = ?)`, id).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return ErrNoAccount
	}
	if patch.Name != nil && *patch.Name != "" {
		var taken int
		if err := tx.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM platform_accounts WHERE name = ? AND id != ?)`, *patch.Name, id,
		).Scan(&taken); err != nil {
			return err
		}
		if taken == 1 {
			return ErrDuplicateName
		}
	}
	if patch.Name != nil {
		if _, err := tx.Exec(`UPDATE platform_accounts SET name = ?, updated_at = strftime('%s', 'now') WHERE id = ?`,
			*patch.Name, id); err != nil {
			return err
		}
	}
	if patch.Token != nil {
		if _, err := tx.Exec(`UPDATE platform_accounts SET token = ?, updated_at = strftime('%s', 'now') WHERE id = ?`,
			*patch.Token, id); err != nil {
			return err
		}
	}
	if patch.AccountRef != nil {
		if _, err := tx.Exec(`UPDATE platform_accounts SET account_ref = ?, updated_at = strftime('%s', 'now') WHERE id = ?`,
			*patch.AccountRef, id); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}

// MarkAccountVerified refreshes the verified timestamp and canonical account
// reference after a successful platform check.
func (s *Store) MarkAccountVerified(id int64, accountRef string, at int64) error {
	res, err := s.db.Exec(
		`UPDATE platform_accounts SET account_ref = ?, verified_at = ?, updated_at = strftime('%s', 'now') WHERE id = ?`,
		accountRef, at, id,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNoAccount
	}
	s.notify()
	return nil
}

// DeleteAccount removes one account. When managed relays or deployments
// still reference it and force is false, ErrAccountInUse comes back and
// nothing changes. Force detaches the relays (they survive as managed
// relays without a deployment) and drops the deployments, then deletes the
// account.
func (s *Store) DeleteAccount(id int64, force bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM platform_accounts WHERE id = ?)`, id).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return ErrNoAccount
	}
	var relays, deployments int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM relays WHERE account_id = ?`, id).Scan(&relays); err != nil {
		return err
	}
	if err := tx.QueryRow(`SELECT COUNT(*) FROM deployments WHERE account_id = ?`, id).Scan(&deployments); err != nil {
		return err
	}
	if relays+deployments > 0 && !force {
		return ErrAccountInUse
	}
	if deployments > 0 {
		if _, err := tx.Exec(`DELETE FROM deployments WHERE account_id = ?`, id); err != nil {
			return err
		}
	}
	if relays > 0 {
		if _, err := tx.Exec(`UPDATE relays SET account_id = NULL, updated_at = strftime('%s', 'now') WHERE account_id = ?`, id); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM platform_accounts WHERE id = ?`, id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}
