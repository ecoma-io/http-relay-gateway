package sanitize

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestSanitizeStripsQuotedURLs(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "transport error loses its relay URL and dial address",
			in:   `Get "https://private-relay.example.com/secret/path": dial tcp 10.0.0.1:443: connect: connection refused`,
			want: `Get "…": dial tcp [redacted]:443: connect: connection refused`,
		},
		{
			name: "url.Error formatting",
			in:   fmt.Sprintf(`%s %q: %s`, "Post", "https://relay.example/x?y=1", "context deadline exceeded"),
			want: `Post "…": context deadline exceeded`,
		},
		{
			name: "quoted URL with userinfo disappears entirely",
			in:   `Get "https://user:pass@host.example/path": net/http: timeout`,
			want: `Get "…": net/http: timeout`,
		},
		{
			name: "multiple quoted URLs",
			in:   `Get "https://a.example/1": retry: Post "https://b.example/2": reset`,
			want: `Get "…": retry: Post "…": reset`,
		},
		{
			name: "escaped quote inside the URL token",
			in:   `Get "https://a.example/x\"y": boom`,
			want: `Get "…": boom`,
		},
		{
			name: "unterminated URL token redacts through the end",
			in:   `Get "https://secret-host.example/secret/path`,
			want: `Get "…"`,
		},
		{
			name: "unterminated quote without a URL stays unchanged",
			in:   `field "truncated value`,
			want: `field "truncated value`,
		},
		{
			name: "escaped quote does not split a later URL token",
			in:   `field "value with \" quote" then Get "https://secret-host.example/path": timeout`,
			want: `field "value with \" quote" then Get "…": timeout`,
		},
		{
			name: "multiple URL tokens keep each cause",
			in:   `Get "https://secret-host.example/one": retry: Post "https://second-host.example/two": reset`,
			want: `Get "…": retry: Post "…": reset`,
		},
		{
			name: "non-URL quoted tokens survive",
			in:   `field "value" is invalid near "other"`,
			want: `field "value" is invalid near "other"`,
		},
		{
			name: "bare URL keeps userinfo redaction",
			in:   "cannot dial https://alice:s3cret@relay.example:8080 now",
			want: "cannot dial https://[redacted]@relay.example:8080 now",
		},
		{
			name: "clean text passes through untouched",
			in:   "connection refused",
			want: "connection refused",
		},
		{
			name: "newline collapses to a space",
			in:   "line one\nline two",
			want: "line one line two",
		},
		{
			name: "ANSI sequence stripped",
			in:   "\x1b[31mred\x1b[0m",
			want: "red",
		},
		{
			name: "long input bounded to MaxLength runes",
			in:   strings.Repeat("x", MaxLength+100),
			want: strings.Repeat("x", MaxLength) + "…",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Sanitize(tt.in); got != tt.want {
				t.Errorf("Sanitize(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSanitizeRedactsDialAddresses(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		want   string
		secret string
	}{
		{
			name:   "dial TCP hostname keeps port and cause",
			in:     `dial tcp secret-host.example:443: connect: connection refused`,
			want:   `dial tcp [redacted]:443: connect: connection refused`,
			secret: "secret-host.example",
		},
		{
			name:   "dial TCP IPv4 keeps port and cause",
			in:     `dial tcp 10.123.45.67:8443: i/o timeout`,
			want:   `dial tcp [redacted]:8443: i/o timeout`,
			secret: "10.123.45.67",
		},
		{
			name:   "dial TCP IPv6 keeps port and cause",
			in:     `dial tcp [fd00::42]:443: network is unreachable`,
			want:   `dial tcp [redacted]:443: network is unreachable`,
			secret: "fd00::42",
		},
		{
			name:   "lookup hostname and resolver keep DNS cause",
			in:     `dial tcp: lookup secret-host.example on 10.0.0.53:53: no such host`,
			want:   `dial tcp: lookup [redacted] on [redacted]:53: no such host`,
			secret: "secret-host.example",
		},
		{
			name:   "lookup resolver IPv4 is redacted",
			in:     `lookup public.example on 10.0.0.53:53: no such host`,
			want:   `lookup [redacted] on [redacted]:53: no such host`,
			secret: "10.0.0.53",
		},
		{
			name:   "lookup resolver IPv6 keeps DNS cause",
			in:     `lookup secret-host.example on [fd00::53]:53: no such host`,
			want:   `lookup [redacted] on [redacted]:53: no such host`,
			secret: "fd00::53",
		},
		{
			name:   "connect target keeps cause",
			in:     `dial tcp: connect: connection refused to secret-host.example:443`,
			want:   `dial tcp: connect: connection refused to [redacted]:443`,
			secret: "secret-host.example",
		},
		{
			name:   "unrelated address-shaped text stays unchanged",
			in:     `cache peer secret-host.example:443 is unavailable`,
			want:   `cache peer secret-host.example:443 is unavailable`,
			secret: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sanitize(tt.in)
			if got != tt.want {
				t.Errorf("Sanitize(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if tt.secret != "" && strings.Contains(got, tt.secret) {
				t.Error("sanitized diagnostic leaks the sentinel address")
			}
		})
	}
}

func TestErrorString(t *testing.T) {
	if got := ErrorString(nil); got != "" {
		t.Errorf("ErrorString(nil) = %q, want empty", got)
	}
	err := &url.Error{Op: "Get", URL: "https://private.example/relay", Err: errors.New("dial tcp: refused")}
	want := `Get "…": dial tcp: refused`
	if got := ErrorString(err); got != want {
		t.Errorf("ErrorString = %q, want %q", got, want)
	}
}
