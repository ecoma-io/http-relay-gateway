package store

import (
	"database/sql"
	"errors"
	"strconv"
	"time"
)

// Admin settings keys. They live in the settings table but are never part of
// the runtime-setting surface: the management API cannot read or write them.
const (
	keyAdminPasswordHash = "admin_password_hash"
	keyAdminJWTSecret    = "admin_jwt_secret"
	keyAdminSetupAt      = "admin_setup_at"
)

// ErrSetupDone reports that the admin plane already has a password; setup is
// a one-time operation.
var ErrSetupDone = errors.New("admin setup already completed")

// AdminSetup stores the password hash, the session-signing secret and the
// setup timestamp in one transaction. A second call fails with ErrSetupDone:
// the hash check and the writes share the transaction, and the single
// connection serializes transactions, so a concurrent double setup cannot
// both pass the check.
func (s *Store) AdminSetup(passwordHash, jwtSecret string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var existing string
	err = tx.QueryRow(
		`SELECT value FROM settings WHERE key = ?`, keyAdminPasswordHash,
	).Scan(&existing)
	if err == nil {
		return ErrSetupDone
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := setSetting(tx, keyAdminPasswordHash, passwordHash); err != nil {
		return err
	}
	if err := setSetting(tx, keyAdminJWTSecret, jwtSecret); err != nil {
		return err
	}
	if err := setSetting(tx, keyAdminSetupAt, strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
		return err
	}
	// No notify(): credentials never affect the serving pool, and a spurious
	// generation rebuild would only log noise during first-run setup.
	return tx.Commit()
}

// AdminCredentials returns the stored password hash and session-signing
// secret. found is false while the admin plane still awaits first-run setup.
func (s *Store) AdminCredentials() (passwordHash, jwtSecret string, found bool, err error) {
	rows, err := s.db.Query(
		`SELECT key, value FROM settings WHERE key IN (?, ?, ?)`,
		keyAdminPasswordHash, keyAdminJWTSecret, keyAdminSetupAt,
	)
	if err != nil {
		return "", "", false, err
	}
	defer func() { _ = rows.Close() }()
	values := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return "", "", false, err
		}
		values[key] = value
	}
	if err := rows.Err(); err != nil {
		return "", "", false, err
	}
	hash, ok := values[keyAdminPasswordHash]
	secret, okSecret := values[keyAdminJWTSecret]
	return hash, secret, ok && okSecret, nil
}

// AdminSetupAt returns the Unix timestamp of first-run setup, or false while
// setup is pending.
func (s *Store) AdminSetupAt() (int64, bool, error) {
	var value string
	err := s.db.QueryRow(
		`SELECT value FROM settings WHERE key = ?`, keyAdminSetupAt,
	).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	at, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return at, true, nil
}
