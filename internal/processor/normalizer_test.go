package processor

import "testing"

func TestNormalizeQuery(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"  iPhone   15  PRO ", "iphone 15 pro"},
		{"SAMSUNG", "samsung"},
		{"   ", ""},
		{"платье	красное", "платье красное"},
	}

	for _, c := range cases {
		got := NormalizeQuery(c.input)
		if got != c.expected {
			t.Fatalf("NormalizeQuery(%q) = %q, want %q", c.input, got, c.expected)
		}
	}
}
