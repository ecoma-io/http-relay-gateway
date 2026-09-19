package deploy

import "testing"

func TestProjectName(t *testing.T) {
	cases := []struct{ name, want string }{
		{"web-relay", "web-relay"},
		{"Web Relay!", "web-relay"},
		{"  --odd__name--  ", "odd-name"},
		{"résumé relay", "r-sum-relay"},
		{"", "relay"},
		{"!!!", "relay"},
		{"01234567890123456789012345678901234567890123456789", "0123456789012345678901234567890123456789"},
	}
	for _, tc := range cases {
		if got := ProjectName(tc.name); got != tc.want {
			t.Errorf("ProjectName(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
