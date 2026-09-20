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
			name: "transport error loses its relay URL, keeps the cause",
			in:   `Get "https://private-relay.example.com/secret/path": dial tcp 10.0.0.1:443: connect: connection refused`,
			want: `Get "…": dial tcp 10.0.0.1:443: connect: connection refused`,
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
