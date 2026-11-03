package main

import (
	"context"
	"errors"
	"math/big"
	"testing"
)

func feed(as ...account) <-chan account {
	c := make(chan account, len(as))
	for _, a := range as {
		c <- a
	}
	close(c)
	return c
}

func TestReconcileFindsEveryKindOfDifference(t *testing.T) {
	ok := account{"ok", "NGN", "10.5000", "12.0000", "1.5000", ""}
	remotes := map[string]remote{
		"ok":    {Status: 200, Currency: "NGN", Available: "10.5", Actual: "12", Locked: "1.50"},
		"drift": {Status: 200, Currency: "NGN", Available: "10.51", Actual: "12", Locked: "1.5"},
		"gone":  {Status: 404},
		"ccy":   {Status: 200, Currency: "GHS", Available: "1", Actual: "1", Locked: "0"},
	}
	fetch := func(_ context.Context, id string) (remote, error) {
		if id == "boom" {
			return remote{}, errors.New("timeout")
		}
		return remotes[id], nil
	}
	var got []mismatch
	sum := run(context.Background(), feed(ok,
		account{"drift", "NGN", "10.5", "12", "1.5", ""},
		account{"gone", "NGN", "0", "0", "0", ""},
		account{"ccy", "NGN", "1", "1", "0", ""},
		account{"boom", "NGN", "0", "0", "0", ""}), fetch, 4, func(m mismatch) { got = append(got, m) })
	if sum.Checked != 5 || sum.Mismatches != 4 || sum.Missing != 1 || sum.Errors != 1 {
		t.Fatalf("summary %+v", sum)
	}
	kinds := map[string]string{}
	for _, m := range got {
		kinds[m.ID] = m.Kind
	}
	if kinds["drift"] != "balance" || kinds["gone"] != "missing" || kinds["ccy"] != "balance" || kinds["boom"] != "error" || kinds["ok"] != "" {
		t.Fatalf("kinds %+v", kinds)
	}
}

func TestToleranceCoversHalfCentRounding(t *testing.T) {
	old := tolerance
	defer func() { tolerance = old }()
	a := account{ID: "a", Currency: "NGN", Available: "100.0049", Actual: "100.0049", Locked: "0"}
	r := remote{Status: 200, Currency: "NGN", Available: "100.00", Actual: "100.00", Locked: "0"}
	if check(a, r, nil) == nil {
		t.Fatal("a 0.0049 difference must fail with zero tolerance")
	}
	tolerance, _ = new(big.Rat).SetString("0.005")
	if m := check(a, r, nil); m != nil {
		t.Fatalf("0.0049 is within 0.005: %+v", m)
	}
	a.Available = "100.0051"
	if check(a, r, nil) == nil {
		t.Fatal("0.0051 is outside 0.005")
	}
}

func TestKnownImportExceptionsAreReportedNotFailed(t *testing.T) {
	ok := remote{Status: 200, Currency: "USD", Available: "1227.81", Actual: "1227.81", Locked: "0"}
	// Bankman's own actual (1240.15) is not available + locked; the import takes available + locked.
	inv := account{ID: "inv", Currency: "USD", Available: "1227.81", Actual: "1240.15", Locked: "0", Anomaly: "balance_invariant"}
	if m := check(inv, ok, nil); m == nil || m.Kind != "expected" {
		t.Fatalf("invariant break must be expected, got %+v", m)
	}
	// An anomalous account that is also wrong in a field the import preserves still fails.
	bad := inv
	bad.Available = "1200.00"
	if m := check(bad, ok, nil); m == nil || m.Kind != "balance" {
		t.Fatalf("a difference the import does not explain must fail, got %+v", m)
	}
	neg := account{ID: "neg", Currency: "USD", Available: "-250", Actual: "250.29", Locked: "500.29", Anomaly: "negative_available"}
	zero := remote{Status: 200, Currency: "USD", Available: "0", Actual: "0", Locked: "0"}
	if m := check(neg, zero, nil); m == nil || m.Kind != "expected" {
		t.Fatalf("negative available imported at zero must be expected, got %+v", m)
	}
	// Without the anomaly marker the same difference fails.
	neg.Anomaly = ""
	if m := check(neg, zero, nil); m == nil || m.Kind != "balance" {
		t.Fatalf("unmarked difference must fail, got %+v", m)
	}
	sum := run(context.Background(), feed(inv), func(context.Context, string) (remote, error) { return ok, nil }, 1, func(mismatch) {})
	if sum.Mismatches != 0 || sum.Expected != 1 {
		t.Fatalf("expected one exception and no failure: %+v", sum)
	}
}
