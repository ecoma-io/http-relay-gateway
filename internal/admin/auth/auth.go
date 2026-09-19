// Package auth holds the admin plane's credential primitives: bcrypt password
// hashing, the session JWT and its cookie, and the login rate limiter. It is
// deliberately HTTP-free so every piece is unit-testable in isolation.
package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// bcryptCost balances first-login latency against offline cracking cost; 12
// is the current baseline recommendation and stays fast enough for an
// interactive login (~250ms on server hardware).
const bcryptCost = 12

// HashPassword hashes an admin password with bcrypt.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// ComparePassword reports whether the password matches the stored hash.
func ComparePassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// NewSecret returns 32 bytes of crypto/rand as unpadded base64url — the
// session-signing secret generated once at setup, and the entropy source for
// any other per-install secret.
func NewSecret() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// CookieName is the session cookie of the admin plane.
const CookieName = "relay_admin"

// TokenTTL bounds one admin login session.
const TokenTTL = 12 * time.Hour

// ErrInvalidToken reports a session JWT that fails signature or claims
// validation.
var ErrInvalidToken = errors.New("invalid session token")

// Issuer signs and verifies admin session JWTs (HS256) with the secret
// generated at first-run setup.
type Issuer struct {
	secret []byte
}

// NewIssuer decodes the stored base64url secret.
func NewIssuer(secret string) (*Issuer, error) {
	raw, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(raw) < 32 {
		return nil, errors.New("admin session secret is malformed")
	}
	return &Issuer{secret: raw}, nil
}

// Issue mints one session token valid from now until TokenTTL later.
func (i *Issuer) Issue(now time.Time) (token string, expires time.Time, err error) {
	expires = now.Add(TokenTTL)
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "admin",
		"iat": now.Unix(),
		"exp": expires.Unix(),
	}).SignedString(i.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign session: %w", err)
	}
	return token, expires, nil
}

// Verify validates signature, algorithm and expiry. now is injectable so
// expiry behavior is testable.
func (i *Issuer) Verify(token string, now time.Time) error {
	parsed, err := jwt.Parse(token, func(*jwt.Token) (any, error) { return i.secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithTimeFunc(func() time.Time { return now }),
	)
	if err != nil || !parsed.Valid {
		return ErrInvalidToken
	}
	return nil
}
