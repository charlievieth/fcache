package main

import "testing"

func TestDigits(t *testing.T) {
	tests := map[string]bool{
		"1":         true,
		"123":       true,
		"123.45":    false,
		"123.45.":   false,
		"123.45.67": false,
		".":         false,
	}
	for v, want := range tests {
		got := digits([]byte(v))
		if got != want {
			t.Errorf("digits(%q) = %t; want: %t", v, got, want)
		}
	}
}

func TestDigitsDot(t *testing.T) {
	tests := map[string]bool{
		"123":       true,
		"123.45":    true,
		"123.45.":   false,
		"123.45.67": false,
		".":         false,
	}
	for v, want := range tests {
		got := digitsDot([]byte(v))
		if got != want {
			t.Errorf("digitsDot(%q) = %t; want: %t", v, got, want)
		}
	}
}
