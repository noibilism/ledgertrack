package main

import (
	"context"
	"math/big"
	"sync"
)

// account is what Bankman's database holds for one account.
type account struct {
	ID, Currency, Available, Actual, Locked string
}

// remote is what LedgerTrack's Bankman-compatible API reports for the same account.
type remote struct {
	Status                    int
	Available, Actual, Locked string
	Currency                  string
}

type mismatch struct {
	ID     string               `json:"id"`
	Kind   string               `json:"kind"` // missing | balance | error
	Fields map[string][2]string `json:"fields,omitempty"`
	Error  string               `json:"error,omitempty"`
}

type summary struct {
	Checked    int `json:"checked"`
	Mismatches int `json:"mismatches"`
	Missing    int `json:"missing"`
	Errors     int `json:"errors"`
}

// same compares two decimal strings by value, so 10.5 and 10.5000 agree.
func same(a, b string) bool {
	x, ok1 := new(big.Rat).SetString(a)
	y, ok2 := new(big.Rat).SetString(b)
	return ok1 && ok2 && x.Cmp(y) == 0
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
	if len(diff) > 0 {
		return &mismatch{ID: a.ID, Kind: "balance", Fields: diff}
	}
	return nil
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
					sum.Mismatches++
					switch m.Kind {
					case "missing":
						sum.Missing++
					case "error":
						sum.Errors++
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
