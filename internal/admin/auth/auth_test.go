package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestHashAndComparePassword(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !ComparePassword(hash, "correct horse battery staple") {
		t.Fatal("correct password rejected")
	}
	if ComparePassword(hash, "wrong password") {
		t.Fatal("wrong password accepted")
	}
	if strings.Contains(hash, "correct") {
		t.Fatal("hash contains plaintext")
	}
}

func TestNewSecret(t *testing.T) {
	a, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("two secrets identical")
	}
	if len(a) < 40 { // 32 bytes base64url
		t.Fatalf("secret too short: %d chars", len(a))
	}
}

func TestIssuerRoundTrip(t *testing.T) {
	secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewIssuer(secret)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	token, expires, err := issuer.Issue(now)
	if err != nil {
		t.Fatal(err)
	}
	if !expires.Equal(now.Add(TokenTTL)) {
		t.Fatalf("expires = %v, want now+TTL", expires)
	}
	if err := issuer.Verify(token, now.Add(time.Hour)); err != nil {
		t.Fatalf("fresh token rejected: %v", err)
	}
	if err := issuer.Verify(token, now.Add(TokenTTL+time.Minute)); err == nil {
		t.Fatal("expired token accepted")
	}
	if err := issuer.Verify(token+"x", now); err == nil {
		t.Fatal("tampered token accepted")
	}

	other, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	otherIssuer, err := NewIssuer(other)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherIssuer.Verify(token, now); err == nil {
		t.Fatal("token from another secret accepted")
	}
}

func TestIssuerRejectsWrongAlgorithm(t *testing.T) {
	secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewIssuer(secret)
	if err != nil {
		t.Fatal(err)
	}
	// HS512 signed with the right key is still rejected: the algorithm is
	// pinned at verify time, so an alg-swap header cannot downgrade the check.
	forged := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{"sub": "admin"})
	signed, err := forged.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	if err := issuer.Verify(signed, time.Now()); err == nil {
		t.Fatal("HS512 token accepted by an HS256 issuer")
	}
}

func TestNewIssuerRejectsBadSecret(t *testing.T) {
	if _, err := NewIssuer(""); err == nil {
		t.Fatal("empty secret accepted")
	}
	if _, err := NewIssuer("short"); err == nil {
		t.Fatal("short secret accepted")
	}
	if _, err := NewIssuer("not base64url !!!"); err == nil {
		t.Fatal("non-base64 secret accepted")
	}
	// 43 base64url chars = 32 bytes: the smallest valid secret.
	if _, err := NewIssuer(strings.Repeat("A", 43)); err != nil {
		t.Fatalf("valid 32-byte secret rejected: %v", err)
	}
}
