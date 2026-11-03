package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/formancehq/ledger/internal/hotrows"
)

// CheckResult is the outcome of the post-run consistency check.
type CheckResult struct {
	Ran    bool `json:"ran"`
	Passed bool `json:"passed"`

	WalletsChecked    int    `json:"wallets_checked"`
	WalletMismatches  int    `json:"wallet_mismatches"`
	NegativeBalances  int    `json:"negative_balances"`
	ChannelsChecked   int    `json:"channels_checked"`
	ChannelMismatches int    `json:"channel_mismatches"`
	Conflicts409      uint64 `json:"unexpected_409"`
	UncertainOps      uint64 `json:"uncertain_ops"`
	// SentOps is the number of operations the uncertain fraction is computed on (503 OVERLOADED excluded).
	SentOps           uint64  `json:"ops_considered"`
	ShedOps           uint64  `json:"shed_ops"`
	UncertainFrac     float64 `json:"uncertain_fraction"`
	MaxUncertainFrac  float64 `json:"max_uncertain_fraction"`
	UncertainExceeded bool    `json:"uncertain_exceeded"`

	Currencies []CurrencyCheck `json:"currencies"`
	Failures   []string        `json:"failures,omitempty"`
	Notes      []string        `json:"notes,omitempty"`
}

// CurrencyCheck compares ledger movement with what the harness expected.
// Every *Delta is final minus baseline; *Tolerance is the sum of amounts of
// operations whose outcome is unknown (timeouts, 5xx, partial failures).
type CurrencyCheck struct {
	Currency string `json:"currency"`

	ControlDelta     int64 `json:"control_delta"`
	ControlExpected  int64 `json:"control_expected"`
	ControlTolerance int64 `json:"control_tolerance"`

	WorldDelta     int64 `json:"world_delta"`
	WorldExpected  int64 `json:"world_expected"`
	WorldTolerance int64 `json:"world_tolerance"`

	SumAvailableDelta int64 `json:"sum_available_delta"`
	SumLienDelta      int64 `json:"sum_lien_delta"`
	// Conservation is control + available + lien + world deltas. Every ledger
	// transaction is balanced, so it must be exactly zero when nothing else
	// writes to these accounts.
	Conservation int64 `json:"conservation"`

	RevenueAccDelta      int64  `json:"revenue_accumulated_delta,omitempty"`
	RevenueAccExpected   int64  `json:"revenue_accumulated_expected,omitempty"`
	RevenueProcDelta     int64  `json:"revenue_processing_delta,omitempty"`
	RevenueProcExpected  int64  `json:"revenue_processing_expected,omitempty"`
	RevenueTolerance     int64  `json:"revenue_tolerance,omitempty"`
	RevenueChecked       bool   `json:"revenue_checked"`
	SystemControlPattern string `json:"system_control_pattern"`
}

// uncertainVerdict returns a failure message when too many operations had an unknown outcome.
// Every uncertain operation widens the tolerance of the balances it touches, so past a point the
// check can no longer tell lost writes from unknown ones. That is reported as a failure
// (inconclusive), never absorbed into the tolerance.
func uncertainVerdict(res *CheckResult, maxFrac float64) string {
	if maxFrac < 0 || res.UncertainOps == 0 {
		return ""
	}
	if res.SentOps > 0 && res.UncertainFrac <= maxFrac {
		return ""
	}
	return fmt.Sprintf("INCONCLUSIVE: %d of %d operations (%.2f%%) had an unknown outcome (timeout, 5xx or network error), above the allowed %.2f%% (-max-uncertain-frac). "+
		"Their amounts widen the tolerances, so the balance check cannot prove that no write was lost. Fix the cause (server errors, -timeout, capacity) and rerun; do not raise the threshold to make this pass",
		res.UncertainOps, res.SentOps, res.UncertainFrac*100, maxFrac*100)
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

type failureLog struct {
	mu    sync.Mutex
	items []string
	total int
}

func (f *failureLog) add(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.total++
	if len(f.items) < 20 {
		f.items = append(f.items, fmt.Sprintf(format, args...))
	}
}

// runCheck reads final balances back from the API and compares them with the
// harness expectations.
func runCheck(ctx context.Context, cfg *Config, cl *client, uni *universe, bl *baselines, bk *book, unexpected409 uint64) *CheckResult {
	res := &CheckResult{Ran: true, Conflicts409: unexpected409, UncertainOps: bk.uncertainOps.Load()}
	res.ShedOps = bk.shedOps.Load()
	res.SentOps = bk.sentOps.Load() - res.ShedOps
	res.MaxUncertainFrac = cfg.MaxUncertainFrac
	if res.SentOps > 0 {
		res.UncertainFrac = float64(res.UncertainOps) / float64(res.SentOps)
	}
	if msg := uncertainVerdict(res, cfg.MaxUncertainFrac); msg != "" {
		res.UncertainExceeded = true
		res.Failures = append(res.Failures, msg)
	}
	var fl failureLog
	nc := len(cfg.Currencies)

	var sumAvail, sumLien = make([]atomic.Int64, nc), make([]atomic.Int64, nc)
	var mismatches, negatives, readErrs atomic.Int64

	err := parallelFor(ctx, cfg.Wallets, cfg.CheckConcurrency, func(i int) error {
		w := uni.wallets[i]
		ccy := cfg.Currencies[w.ccy]
		asset := ccy + "/2"
		avail, err := cl.accountBalance(ctx, walletAddr(w.user, ccy, "available"), asset)
		if err != nil {
			readErrs.Add(1)
			fl.add("read %s available: %v", w.id, err)
			return nil
		}
		lien, err := cl.accountBalance(ctx, walletAddr(w.user, ccy, "lien"), asset)
		if err != nil {
			readErrs.Add(1)
			fl.add("read %s lien: %v", w.id, err)
			return nil
		}
		sumAvail[w.ccy].Add(avail - bl.avail[i])
		sumLien[w.ccy].Add(lien - bl.lien[i])
		if avail < 0 || lien < 0 {
			negatives.Add(1)
			fl.add("negative balance on %s: available=%d lien=%d", w.id, avail, lien)
		}
		dA := avail - bl.avail[i] - bk.wAvail[i].Load()
		dL := lien - bl.lien[i] - bk.wLien[i].Load()
		if abs64(dA) > bk.wUncAvail[i].Load() || abs64(dL) > bk.wUncLien[i].Load() {
			mismatches.Add(1)
			fl.add("wallet %s: available off by %d (tolerance %d), lien off by %d (tolerance %d)",
				w.id, dA, bk.wUncAvail[i].Load(), dL, bk.wUncLien[i].Load())
		}
		return nil
	})
	if err != nil {
		res.Failures = append(res.Failures, "wallet read aborted: "+err.Error())
	}
	res.WalletsChecked = cfg.Wallets
	res.WalletMismatches = int(mismatches.Load())
	res.NegativeBalances = int(negatives.Load())
	if n := readErrs.Load(); n > 0 {
		res.Failures = append(res.Failures, fmt.Sprintf("%d wallet balance reads failed", n))
	}

	for ci, ccy := range cfg.Currencies {
		asset := ccy + "/2"
		cc := CurrencyCheck{Currency: ccy, SystemControlPattern: systemControlPattern(ccy)}
		cb := &bk.ccy[ci]

		control, err := cl.aggregateBalance(ctx, cl.ledger, systemControlPattern(ccy), asset)
		if err != nil {
			res.Failures = append(res.Failures, fmt.Sprintf("%s control read: %v", ccy, err))
			continue
		}
		world, err := cl.accountBalance(ctx, "world", asset)
		if err != nil {
			res.Failures = append(res.Failures, fmt.Sprintf("%s world read: %v", ccy, err))
			continue
		}
		cc.ControlDelta = control - bl.control[ci]
		cc.ControlExpected = cb.control.Load()
		cc.ControlTolerance = cb.uncControl.Load()
		cc.WorldDelta = world - bl.world[ci]
		cc.WorldExpected = cb.world.Load()
		cc.WorldTolerance = cb.uncWorld.Load()
		cc.SumAvailableDelta = sumAvail[ci].Load()
		cc.SumLienDelta = sumLien[ci].Load()
		cc.Conservation = cc.ControlDelta + cc.SumAvailableDelta + cc.SumLienDelta + cc.WorldDelta

		if abs64(cc.ControlDelta-cc.ControlExpected) > cc.ControlTolerance {
			res.Failures = append(res.Failures, fmt.Sprintf("%s system:control delta %d, expected %d (tolerance %d)",
				ccy, cc.ControlDelta, cc.ControlExpected, cc.ControlTolerance))
		}
		if abs64(cc.WorldDelta-cc.WorldExpected) > cc.WorldTolerance {
			res.Failures = append(res.Failures, fmt.Sprintf("%s world delta %d, expected %d (tolerance %d)",
				ccy, cc.WorldDelta, cc.WorldExpected, cc.WorldTolerance))
		}
		if cc.Conservation != 0 {
			res.Failures = append(res.Failures, fmt.Sprintf("%s conservation broken: control+available+lien+world deltas sum to %d, must be 0 (is another client writing to these accounts?)",
				ccy, cc.Conservation))
		}

		if cfg.mixUsesChannel() {
			cc.RevenueChecked = true
			rev := "revenue-" + ccy
			acc, err1 := cl.aggregateBalance(ctx, rev, hotrows.Pattern(hotrows.AccumulatedBase), asset)
			proc, err2 := cl.aggregateBalance(ctx, rev, hotrows.Pattern(hotrows.ProcessingCostBase), asset)
			if err1 != nil || err2 != nil {
				res.Failures = append(res.Failures, fmt.Sprintf("%s revenue read: %v %v", ccy, err1, err2))
			} else {
				cc.RevenueAccDelta = acc - bl.revAcc[ci]
				cc.RevenueProcDelta = proc - bl.revProc[ci]
				cc.RevenueAccExpected = cb.revAcc.Load()
				cc.RevenueProcExpected = cb.revProc.Load()
				cc.RevenueTolerance = cb.uncRev.Load()
				if abs64(cc.RevenueAccDelta-cc.RevenueAccExpected) > cc.RevenueTolerance ||
					abs64(cc.RevenueProcDelta-cc.RevenueProcExpected) > cc.RevenueTolerance {
					res.Failures = append(res.Failures, fmt.Sprintf("%s revenue deltas (acc %d, processing %d) differ from expected (%d, %d) beyond tolerance %d",
						ccy, cc.RevenueAccDelta, cc.RevenueProcDelta, cc.RevenueAccExpected, cc.RevenueProcExpected, cc.RevenueTolerance))
				}
			}
			for k, id := range uni.channels[ci] {
				res.ChannelsChecked++
				bal, err := cl.channelBalance(ctx, id, ccy)
				if err != nil {
					res.Failures = append(res.Failures, fmt.Sprintf("channel %s read: %v", id, err))
					continue
				}
				want := bk.chanDelta[ci*bk.channels+k].Load()
				tol := bk.chanUnc[ci*bk.channels+k].Load()
				if abs64(bal-want) > tol {
					res.ChannelMismatches++
					fl.add("channel %s (%s): balance %d, expected %d (tolerance %d)", id, ccy, bal, want, tol)
				}
			}
		}
		res.Currencies = append(res.Currencies, cc)
	}

	if res.WalletMismatches > 0 {
		res.Failures = append(res.Failures, fmt.Sprintf("%d wallets differ from the harness expectation", res.WalletMismatches))
	}
	if res.NegativeBalances > 0 {
		res.Failures = append(res.Failures, fmt.Sprintf("%d wallets have a negative available or lien balance", res.NegativeBalances))
	}
	if res.ChannelMismatches > 0 {
		res.Failures = append(res.Failures, fmt.Sprintf("%d channels differ from the harness expectation", res.ChannelMismatches))
	}
	if unexpected409 > 0 {
		res.Failures = append(res.Failures, fmt.Sprintf("%d responses were 409 conflicts although every reference is unique: duplicate reference handling is wrong or requests were replayed", unexpected409))
	}
	res.Failures = append(res.Failures, fl.items...)
	if fl.total > len(fl.items) {
		res.Notes = append(res.Notes, fmt.Sprintf("%d detail lines omitted", fl.total-len(fl.items)))
	}
	if res.UncertainOps > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf("%d operations had an unknown outcome (timeout, 5xx or partial failure); their amounts widen the tolerances", res.UncertainOps))
	}
	if res.ShedOps > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf("%d operations were shed by the server (503 OVERLOADED, nothing committed); they are not uncertain", res.ShedOps))
	}
	res.Notes = append(res.Notes, "references are unique by construction (run id + sequence); duplicate detection relies on the server returning 409 for a reused reference")
	res.Passed = len(res.Failures) == 0
	return res
}

func (c *client) channelBalance(ctx context.Context, channelID, ccy string) (int64, error) {
	status, m, err := c.getJSON(ctx, c.ledgerPath("/channels/%s?currency=%s", url.PathEscape(channelID), url.QueryEscape(ccy)))
	if err != nil {
		return 0, err
	}
	switch status {
	case http.StatusNotFound:
		return 0, nil // never touched
	case http.StatusOK:
	default:
		// readChannel does not map "account not found" to 404: an untouched
		// channel comes back as a 500. Treat it as an empty channel; if the
		// harness expected movement the balance comparison still fails.
		if status >= 400 {
			return 0, nil
		}
		return 0, fmt.Errorf("read channel %s: unexpected status %d", channelID, status)
	}
	vols, ok := dig(m, "volumes")
	if !ok {
		return 0, nil
	}
	vm, _ := vols.(map[string]any)
	av, _ := vm[ccy+"/2"].(map[string]any)
	if av == nil {
		return 0, nil
	}
	if bal, ok := toInt64(av["balance"]); ok {
		return bal, nil
	}
	in, _ := toInt64(av["input"])
	out, _ := toInt64(av["output"])
	return in - out, nil
}
