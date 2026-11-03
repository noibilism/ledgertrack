package main

import (
	"context"
	"errors"
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
	ok := account{"ok", "NGN", "10.5000", "12.0000", "1.5000"}
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
		account{"drift", "NGN", "10.5", "12", "1.5"},
		account{"gone", "NGN", "0", "0", "0"},
		account{"ccy", "NGN", "1", "1", "0"},
		account{"boom", "NGN", "0", "0", "0"}), fetch, 4, func(m mismatch) { got = append(got, m) })
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
