package v2

import (
	"net/http/httptest"
	"testing"
)

func TestNormalizeReleaseMode(t *testing.T) {
	cases := map[string]string{
		"":                  "RELEASE_ONLY",
		"  ":                "RELEASE_ONLY",
		"release_only":      "RELEASE_ONLY",
		"cancel":            "RELEASE_ONLY",
		"pay":               "PAY",
		"PAY":               "PAY",
		"release_and_debit": "PAY",
		"bogus":             "BOGUS",
	}
	for in, want := range cases {
		if got := normalizeReleaseMode(in); got != want {
			t.Errorf("normalizeReleaseMode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSetIdempotencyHit(t *testing.T) {
	rec := httptest.NewRecorder()
	setIdempotencyHit(rec, false)
	if rec.Header().Get("Idempotency-Hit") != "" {
		t.Fatal("header must not be set on a fresh request")
	}
	setIdempotencyHit(rec, true)
	if rec.Header().Get("Idempotency-Hit") != "true" {
		t.Fatal("header must be set on a replay")
	}
}
