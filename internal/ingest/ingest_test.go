package ingest

import "testing"

func TestMatches(t *testing.T) {
	cases := []struct {
		p, e string
		want bool
	}{
		{"*", "payment.succeeded", true},
		{"payment.succeeded", "payment.succeeded", true},
		{"payment.*", "payment.succeeded", true},
		{"payment.*", "payment.refund.created", true},
		{"payment.*", "payments.x", false},
		{"payment.*", "payment", false},
		{"payment.succeeded", "payment.failed", false},
	}
	for _, c := range cases {
		if got := Matches(c.p, c.e); got != c.want {
			t.Errorf("Matches(%q,%q)=%v", c.p, c.e, got)
		}
	}
}

func TestValidation(t *testing.T) {
	for _, s := range []string{"payment.succeeded", "a", "monitor.alert_fired", "x-y.z"} {
		if !ValidEventType(s) {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range []string{"", ".a", "a.", "a..b", "a b", "payment.*", "*"} {
		if ValidEventType(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
	for _, s := range []string{"*", "payment.*", "payment.succeeded"} {
		if !ValidPattern(s) {
			t.Errorf("pattern %q should be valid", s)
		}
	}
	for _, s := range []string{"", "pay*", "*.x", "a.*.b", "**"} {
		if ValidPattern(s) {
			t.Errorf("pattern %q should be invalid", s)
		}
	}
}
