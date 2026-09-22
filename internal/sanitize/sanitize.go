// Package sanitize provides shared safe-diagnostics helpers for pool
// snapshots, /stats, and structured logs. It strips quoted URL tokens,
// redacts URL userinfo, neutralizes terminal controls and ANSI sequences,
// and bounds output. Zero dependencies beyond the standard library.
package sanitize

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// MaxLength bounds sanitized diagnostic text in runes before the ellipsis.
const MaxLength = 512

const redacted = "[redacted]@"

var (
	dialTCPAddress = regexp.MustCompile(`\bdial tcp (\[[^]]+\]|[^\s:]+):(\d+)`)
	lookupName     = regexp.MustCompile(`\blookup ([^\s:]+)(:|\s+on\s)`) // net.DNSError formatting
	lookupServer   = regexp.MustCompile(`\bon (\[[^]]+\]|[^\s:]+):(\d+):`)
	connectTo      = regexp.MustCompile(`\bconnect: ([^\s]+(?: [^\s]+)*) to (\[[^]]+\]|[^\s:]+):(\d+)`)
)

// Sanitize returns bounded diagnostic text with quoted URL tokens, relay dial
// addresses, and URL userinfo stripped or redacted, and terminal controls and
// ANSI sequences neutralized.
func Sanitize(s string) string {
	if !needsSanitizing(s) {
		return s
	}
	return bound(cleanControls(redactDialAddresses(stripQuotedURLs(redactUserinfo(s)))))
}

// needsSanitizing reports whether any stage of Sanitize would change s, so the
// clean common case skips every intermediate string copy. The control scan is
// byte-wise on purpose: multi-byte UTF-8 sequences never contain bytes below
// 0x20 or 0x7f, and every control rune encodes as one such byte.
func needsSanitizing(s string) bool {
	if len(s) > MaxLength {
		return true // bound() truncates
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return true // stripANSI consumes ESC; cleanControls rewrites controls
		}
	}
	return strings.Contains(s, "://") ||
		strings.Contains(s, "dial ") ||
		strings.Contains(s, "lookup ") ||
		strings.Contains(s, "connect: ")
}

// ErrorString sanitizes err.Error(), returning "" for nil.
func ErrorString(err error) string {
	if err == nil {
		return ""
	}
	return Sanitize(err.Error())
}

// redactUserinfo replaces scheme://userinfo@ with scheme://[redacted]@.
// It uses the last '@' within the URL token so synthetic passwords
// containing '/', '?', or '@' are still fully redacted while the
// host:port identity after the final '@' is preserved.
func redactUserinfo(value string) string {
	for searchFrom := 0; ; {
		idx := strings.Index(value[searchFrom:], "://")
		if idx < 0 {
			return value
		}
		schemeAt := searchFrom + idx
		userinfoStart := schemeAt + len("://")
		tokenEnd := tokenEndIndex(value, userinfoStart)
		token := value[userinfoStart:tokenEnd]
		at := strings.LastIndexByte(token, '@')
		if at < 0 {
			searchFrom = tokenEnd
			if searchFrom >= len(value) {
				return value
			}
			continue
		}
		at += userinfoStart
		value = value[:userinfoStart] + redacted + value[at+1:]
		searchFrom = userinfoStart + len(redacted)
	}
}

// stripQuotedURLs replaces every double-quoted URL token with "…".
// net/http folds the request URL into its transport errors
// (`Get "https://…": dial tcp …`), and relay origins are private: the URL
// must never survive into /stats, error bodies, or logs — only the cause
// behind it does. Unquoted URLs keep flowing so userinfo redaction below
// can still act on them. An unterminated URL token is redacted through the
// remainder of the string, since a truncated diagnostic must fail closed.
func stripQuotedURLs(value string) string {
	for i := 0; i < len(value); {
		start := strings.IndexByte(value[i:], '"')
		if start < 0 {
			return value
		}
		start += i
		end := quotedTokenEnd(value, start)
		if end < 0 {
			if strings.Contains(value[start:], "://") {
				return value[:start] + `"…"`
			}
			return value
		}
		if strings.Contains(value[start:end+1], "://") {
			value = value[:start] + `"…"` + value[end+1:]
			i = start + len(`"…"`)
			continue
		}
		i = end + 1
	}
	return value
}

// redactDialAddresses removes private relay targets from standard-library
// transport errors while preserving their address family, port, and cause.
func redactDialAddresses(value string) string {
	value = dialTCPAddress.ReplaceAllString(value, "dial tcp [redacted]:$2")
	value = lookupName.ReplaceAllString(value, "lookup [redacted]$2")
	value = lookupServer.ReplaceAllString(value, "on [redacted]:$2:")
	return connectTo.ReplaceAllString(value, "connect: $1 to [redacted]:$3")
}

// quotedTokenEnd returns the index of the quote closing the token opened at
// start, honoring %q-style backslash escapes, or -1 when unterminated.
func quotedTokenEnd(value string, start int) int {
	for end := start + 1; end < len(value); end++ {
		switch value[end] {
		case '\\':
			end++ // skip the escaped byte
		case '"':
			return end
		}
	}
	return -1
}

func tokenEndIndex(s string, from int) int {
	for i := from; i < len(s); {
		c := s[i]
		// End tokens on whitespace, controls, quotes, angle brackets, backtick.
		if c <= 0x20 || c == 0x7f || c == '"' || c == '\'' || c == '<' || c == '>' || c == '`' || c == 0x1b {
			return i
		}
		i++
	}
	return len(s)
}

// cleanControls strips ANSI escape sequences and neutralizes remaining
// terminal controls. Newlines, carriage returns, and tabs become a single
// space so diagnostics stay single-line; other C0 controls, DEL, and ESC
// remnants are removed.
func cleanControls(value string) string {
	// Strip common ANSI sequences first so "[31m" remnants do not survive.
	value = stripANSI(value)
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range value {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			b.WriteByte(' ')
		case r == 0x1b:
			// Drop any surviving ESC.
			continue
		case r < 0x20 || r == 0x7f:
			continue
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func stripANSI(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] != 0x1b {
			b.WriteByte(s[i])
			i++
			continue
		}
		// ESC found: consume sequence.
		i++ // skip ESC
		if i >= len(s) {
			break
		}
		switch s[i] {
		case '[':
			// CSI: ESC [ params intermediates final(@-~).
			i++
			for i < len(s) {
				c := s[i]
				i++
				if c >= 0x40 && c <= 0x7e {
					break
				}
			}
		case ']':
			// OSC: ESC ] ... BEL or ESC \.
			i++
			for i < len(s) {
				if s[i] == 0x07 {
					i++
					break
				}
				if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '\\' {
					i += 2
					break
				}
				i++
			}
		case '(', ')', '#':
			// Two-byte sequences.
			i++
			if i < len(s) {
				i++
			}
		default:
			// Single-char sequence: drop the designator.
			i++
		}
	}
	return b.String()
}

func bound(value string) string {
	if utf8.RuneCountInString(value) <= MaxLength {
		return value
	}
	return string([]rune(value)[:MaxLength]) + "…"
}
