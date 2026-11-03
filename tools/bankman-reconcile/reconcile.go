package main

import (
	"context"
	"math/big"
	"sync"
)

// account is what Bankman's database holds for one account.
type account struct {
	ID, Currency, Available, Actual, Locked string
	// Anomaly marks a Bankman row the import cannot copy as is: "negative_available" (imported at zero, locked
	// amount left out) or "balance_invariant" (actual != available + locked, imported as available + locked).
	// The export lists the same accounts in export_issues.csv and finance_followup.csv.
	Anomaly string
}

// remote is what LedgerTrack's Bankman-compatible API reports for the same account.
type remote struct {
	Status                    int
	Available, Actual, Locked string
	Currency                  string
}

type mismatch struct {
	ID     string               `json:"id"`
	Kind   string               `json:"kind"` // missing | balance | error | expected
	Fields map[string][2]string `json:"fields,omitempty"`
	Error  string               `json:"error,omitempty"`
}

type summary struct {
	Checked    int `json:"checked"`
	Mismatches int `json:"mismatches"`
	Missing    int `json:"missing"`
	Errors     int `json:"errors"`
	// Expected counts accounts that differ only in the way the import is known to change them. They do not
	// fail the run; each is still printed with kind "expected".
	Expected int `json:"expected"`
}

// tolerance is the largest absolute difference still counted as equal. It is zero unless RECONCILE_TOLERANCE
// sets it. Bankman keeps 4 decimals and LedgerTrack 2, so the import rounds to cents (half-even) and a balance
// can differ by up to half a cent from the import alone; 0.005 covers exactly that.
var tolerance = new(big.Rat)

// same compares two decimal strings by value, so 10.5 and 10.5000 agree, within the tolerance.
func same(a, b string) bool {
	x, ok1 := new(big.Rat).SetString(a)
	y, ok2 := new(big.Rat).SetString(b)
	if !ok1 || !ok2 {
		return false
	}
	d := new(big.Rat).Sub(x, y)
	return d.Abs(d).Cmp(tolerance) <= 0
}

func check(a account, r remote, err error) *mismatch {
	switch {
	case err != nil:
		return &mismatch{ID: a.ID, Kind: "error", Error: err.Error()}
	case r.Status == 404:
		return &mismatch{ID: a.ID, Kind: "missing"}
	case r.Status/100 != 2:
		return &mismatch{ID: a.ID, Kind: "error", Error: "status " + itoa(r.Status)}
	}
	diff := map[string][2]string{}
	if !same(a.Available, r.Available) {
		diff["available_balance"] = [2]string{a.Available, r.Available}
	}
	if !same(a.Actual, r.Actual) {
		diff["actual_balance"] = [2]string{a.Actual, r.Actual}
	}
	if !same(a.Locked, r.Locked) {
		diff["locked_amount"] = [2]string{a.Locked, r.Locked}
	}
	if a.Currency != r.Currency {
		diff["currency"] = [2]string{a.Currency, r.Currency}
	}
	if len(diff) == 0 {
		return nil
	}
	excused := map[string][2]string{}
	for field := range diff {
		if excusedBy(a.Anomaly, field) {
			excused[field] = diff[field]
			delete(diff, field)
		}
	}
	if len(diff) > 0 {
		return &mismatch{ID: a.ID, Kind: "balance", Fields: diff}
	}
	return &mismatch{ID: a.ID, Kind: "expected", Fields: excused, Error: a.Anomaly}
}

// excusedBy says whether the import is known to change a field for this kind of anomalous account.
func excusedBy(anomaly, field string) bool {
	switch anomaly {
	case "negative_available":
		return field == "available_balance" || field == "actual_balance" || field == "locked_amount"
	case "balance_invariant":
		return field == "actual_balance"
	}
	return false
}

func itoa(n int) string { return big.NewInt(int64(n)).String() }

// run checks every account with a pool of workers and reports each difference through report, which is
// called from one goroutine at a time.
func run(ctx context.Context, accounts <-chan account, fetch func(context.Context, string) (remote, error), workers int, report func(mismatch)) summary {
	var (
		mu  sync.Mutex
		sum summary
		wg  sync.WaitGroup
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range accounts {
				r, err := fetch(ctx, a.ID)
				m := check(a, r, err)
				mu.Lock()
				sum.Checked++
				if m != nil {
					switch m.Kind {
					case "expected":
						sum.Expected++
					case "missing":
						sum.Mismatches++
						sum.Missing++
					case "error":
						sum.Mismatches++
						sum.Errors++
					default:
						sum.Mismatches++
					}
					report(*m)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return sum
}
