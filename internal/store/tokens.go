package store

import (
	"database/sql"
	"errors"
)

// ErrNotFound is returned by token reads when the referenced row does not
// exist or holds no active credential.
var ErrNotFound = errors.New("not found")

// Tokens funnels every credential read and write through one type: when
// tokens gain encryption at rest or redaction rules, this is the only file
// that changes. Token values must never appear in logs or API responses —
// callers expose last-4 digests at most.
type Tokens struct {
	s *Store
}

// Tokens returns the credential-isolated view of the store.
func (s *Store) Tokens() Tokens { return Tokens{s: s} }

// RelayAuthToken returns the auth token the gateway presents to a managed
// relay. Only an active deployment carries one; legacy relays and relays
// without a verified deployment return ErrNotFound.
func (t Tokens) RelayAuthToken(relayID int64) (string, error) {
	var token string
	err := t.s.db.QueryRow(
		`SELECT auth_token FROM deployments WHERE relay_id = ? AND status = 'active'`,
		relayID,
	).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return token, err
}

// PlatformToken returns the stored credential for a platform account.
func (t Tokens) PlatformToken(accountID int64) (string, error) {
	var token string
	err := t.s.db.QueryRow(
		`SELECT token FROM platform_accounts WHERE id = ?`,
		accountID,
	).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return token, err
}
