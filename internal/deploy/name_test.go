package deploy

import "testing"

func TestProjectName(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		// Identity: a plain name maps to itself.
		{"web-relay", "web-relay"},
		{"relay7", "relay7"},
		// Case and separator normalization: everything not [a-z0-9] becomes a
		// single dash; runs collapse; edges trim.
		{"Web Relay!", "web-relay"},
		{"  --odd__name--  ", "odd-name"},
		{"a...b...c", "a-b-c"},
		{"UPPER_case", "upper-case"},
		// Unicode is stripped, not transliterated.
		{"résumé relay", "r-sum-relay"},
		{"🚀 launch", "launch"},
		// Nothing usable falls back to the deterministic default.
		{"", "relay"},
		{"!!!", "relay"},
		{"___", "relay"},
		// The 40-character platform bound: capped, then re-trimmed so the cap
		// never lands on a trailing dash.
		{"01234567890123456789012345678901234567890123456789", "0123456789012345678901234567890123456789"},
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	for _, tc := range cases {
		if got := ProjectName(tc.name); got != tc.want {
			t.Errorf("ProjectName(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestProjectNameDeterministic(t *testing.T) {
	for _, name := range []string{"Web Relay!", "résumé relay", "0123456789012345678901234567890123456789x"} {
		first := ProjectName(name)
		for range 5 {
			if again := ProjectName(name); again != first {
				t.Fatalf("ProjectName(%q) is not deterministic: %q then %q", name, first, again)
			}
		}
	}
}
