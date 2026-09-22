package sanitize

import (
	"errors"
	"fmt"
	"net"
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

func TestSanitizeRedactsConnIOAddresses(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		want   string
		secret string
	}{
		{
			name:   "mid-body reset keeps both ports and the cause",
			in:     `read tcp 127.0.0.1:52400->127.0.0.1:443: read: connection reset by peer`,
			want:   `read tcp [redacted]:52400->[redacted]:443: read: connection reset by peer`,
			secret: "127.0.0.1",
		},
		{
			name:   "write broken pipe keeps both ports and the cause",
			in:     `write tcp 10.123.45.67:51000->10.0.0.1:8443: write: broken pipe`,
			want:   `write tcp [redacted]:51000->[redacted]:8443: write: broken pipe`,
			secret: "10.123.45.67",
		},
		{
			name:   "read IPv6 keeps both ports and the cause",
			in:     `read tcp [fd00::42]:50000->[2001:db8::1]:443: use of closed network connection`,
			want:   `read tcp [redacted]:50000->[redacted]:443: use of closed network connection`,
			secret: "fd00::42",
		},
		{
			name:   "OpError without a source endpoint still redacts the peer",
			in:     `write tcp 10.0.0.1:443: write: broken pipe`,
			want:   `write tcp [redacted]:443: write: broken pipe`,
			secret: "10.0.0.1",
		},
		{
			// A declared-length body write wraps the OpError in a readfrom
			// layer that carries both endpoints itself — every layer must
			// lose its addresses.
			name:   "readfrom body-write wrapper redacts at both layers",
			in:     `Post "…": readfrom tcp 127.0.0.1:41754->127.0.0.1:37445: write tcp 127.0.0.1:41754->127.0.0.1:37445: write: connection reset by peer`,
			want:   `Post "…": readfrom tcp [redacted]:41754->[redacted]:37445: write tcp [redacted]:41754->[redacted]:37445: write: connection reset by peer`,
			secret: "127.0.0.1",
		},
		{
			name:   "redaction is idempotent",
			in:     `read tcp [redacted]:52400->[redacted]:443: read: connection reset by peer`,
			want:   `read tcp [redacted]:52400->[redacted]:443: read: connection reset by peer`,
			secret: "",
		},
		{
			name:   "arrow-shaped text without an OpError prefix stays unchanged",
			in:     `route a:1->b:2 denotes the mapping`,
			want:   `route a:1->b:2 denotes the mapping`,
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

// The real error reaches Sanitize through net.OpError's own formatting, not a
// hand-typed string — pin that the constructed form sanitizes too.
func TestErrorStringRedactsNetOpError(t *testing.T) {
	err := &net.OpError{
		Op:     "read",
		Net:    "tcp",
		Source: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 52400},
		Addr:   &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443},
		Err:    errors.New("read: connection reset by peer"),
	}
	got := ErrorString(err)
	want := `read tcp [redacted]:52400->[redacted]:443: read: connection reset by peer`
	if got != want {
		t.Errorf("ErrorString = %q, want %q", got, want)
	}
	if strings.Contains(got, "127.0.0.1") {
		t.Error("sanitized OpError leaks the endpoint address")
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
