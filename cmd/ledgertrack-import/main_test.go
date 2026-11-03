package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const samples = "../../docs/import/samples"

func run(t *testing.T, args ...string) error {
	t.Helper()
	root := newRoot()
	root.SetArgs(args)
	root.SetOut(os.Stderr)
	return root.Execute()
}

func TestValidateSamples(t *testing.T) {
	if err := run(t, "validate", "--input", samples, "--precision", "NGN=2", "--precision", "USDT=6"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateFailsWithExitCode1(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"products.jsonl", "clients.jsonl", "accounts.jsonl", "balances.jsonl", "liens.jsonl"} {
		b, err := os.ReadFile(filepath.Join(samples, f)) //nolint:gosec // path comes from the operator/CLI, not from request input
		if err != nil {
			t.Fatal(err)
		}
		if f == "balances.jsonl" {
			b = []byte(`{"source_id":"b-1","account_source_id":"a-1","currency":"NGN","amount":"1.001","available":"1.001","lien":"0"}` + "\n")
		}
		if err := os.WriteFile(filepath.Join(dir, f), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	err := run(t, "validate", "--input", dir)
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != 1 {
		t.Fatalf("want exit code 1, got %v", err)
	}
}

func TestApplyRequiresConfirmation(t *testing.T) {
	err := run(t, "apply", "--input", samples, "--url", "http://127.0.0.1:1")
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != 2 {
		t.Fatalf("want exit code 2 without --yes, got %v", err)
	}
}

func TestParsePrecisions(t *testing.T) {
	got, err := parsePrecisions([]string{"usd=2", "USDT=6"})
	if err != nil || got["USD"] != 2 || got["USDT"] != 6 {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"USD", "USD=x", "=2", "USD=-1", "USD=19"} {
		if _, err := parsePrecisions([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
