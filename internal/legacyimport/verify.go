package legacyimport

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"sync"
)

// VerifyRow is one line of the reconciliation report.
type VerifyRow struct {
	RowType     string // "account" or "currency_total"
	SourceID    string // account source_id, or the currency for currency_total rows
	RemoteID    string
	WalletID    string
	Currency    string
	ExpAvail    string
	ActAvail    string
	ExpLien     string
	ActLien     string
	ExpTotal    string
	ActTotal    string
	SourceDebit string // currency_total rows only
	Result      string // PASS or FAIL
	Note        string
}

// VerifyReport is the outcome of Verify.
type VerifyReport struct {
	Rows     []VerifyRow
	Failures int
}

// Pass reports whether every row passed, with zero tolerance.
func (r *VerifyReport) Pass() bool { return r.Failures == 0 }

// WriteCSV writes the report.
func (r *VerifyReport) WriteCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"row_type", "source_id", "remote_id", "wallet_id", "currency",
		"expected_available", "actual_available", "expected_lien", "actual_lien",
		"expected_total", "actual_total", "migration_source_debit", "result", "note"}); err != nil {
		return err
	}
	for _, x := range r.Rows {
		if err := cw.Write([]string{x.RowType, x.SourceID, x.RemoteID, x.WalletID, x.Currency,
			x.ExpAvail, x.ActAvail, x.ExpLien, x.ActLien, x.ExpTotal, x.ActTotal, x.SourceDebit, x.Result, x.Note}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// Summary renders the failing rows and the per-currency lines.
func (r *VerifyReport) Summary() string {
	var b strings.Builder
	accounts := 0
	for _, x := range r.Rows {
		if x.RowType == "account" {
			accounts++
		}
	}
	for _, x := range r.Rows {
		switch {
		case x.RowType == "currency_total":
			fmt.Fprintf(&b, "%s %s: expected=%s imported=%s migration_source_debit=%s %s\n", x.Result, x.SourceID, x.ExpTotal, x.ActTotal, x.SourceDebit, x.Note)
		case x.Result != "PASS":
			fmt.Fprintf(&b, "FAIL account %s (%s): available %s/%s lien %s/%s %s\n", x.SourceID, x.WalletID, x.ActAvail, x.ExpAvail, x.ActLien, x.ExpLien, x.Note)
		}
	}
	verdict := "PASS"
	if !r.Pass() {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "%d accounts checked, %d failing rows: %s\n", accounts, r.Failures, verdict)
	return b.String()
}

// Verify reads every imported account back and compares it with the input. It
// never writes. With deep, it also checks each opening and lien transaction by
// reference.
func Verify(ctx context.Context, c *Client, prep *Prepared, snap *Snapshot, opt Options, deep bool) (*VerifyReport, error) {
	opt.defaults()
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	accts := prep.DS.Accounts
	rows := make([]VerifyRow, len(accts))
	actualTotals := make([]*big.Int, len(accts))

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	sem := make(chan struct{}, opt.Concurrency)
	for i := range accts {
		i := i
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			row, tot, err := verifyAccount(ctx, c, prep, snap, opt, accts[i], deep)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			rows[i], actualTotals[i] = row, tot
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	rep := &VerifyReport{Rows: rows}
	sumActual := map[string]*big.Int{}
	for i, a := range accts {
		ccy := prep.AcctCurrency[a.SourceID]
		if actualTotals[i] != nil {
			if sumActual[ccy] == nil {
				sumActual[ccy] = new(big.Int)
			}
			sumActual[ccy].Add(sumActual[ccy], actualTotals[i])
		}
	}

	codes := make([]string, 0, len(prep.Totals))
	for k := range prep.Totals {
		codes = append(codes, k)
	}
	sort.Strings(codes)
	for _, ccy := range codes {
		t := prep.Totals[ccy]
		src := opt.source(ccy)
		vols, _, err := c.LedgerVolumes(ctx, src)
		if err != nil {
			return nil, fmt.Errorf("reading migration source %s: %w", src, err)
		}
		asset := opt.asset(ccy, t.Precision)
		debit := new(big.Int).Neg(vols[asset].Balance())
		act := sumActual[ccy]
		if act == nil {
			act = new(big.Int)
		}
		row := VerifyRow{
			RowType: "currency_total", SourceID: ccy, Currency: ccy,
			ExpTotal: FormatAmount(t.Total, t.Precision), ActTotal: FormatAmount(act, t.Precision),
			SourceDebit: FormatAmount(debit, t.Precision), Result: "PASS",
		}
		var notes []string
		if act.Cmp(t.Total) != 0 {
			notes = append(notes, "imported balances differ from input")
		}
		if debit.Cmp(t.Total) != 0 {
			notes = append(notes, fmt.Sprintf("debit on %s (%s) differs from input total", src, asset))
		}
		if len(notes) > 0 {
			row.Result = "FAIL"
			row.Note = strings.Join(notes, "; ")
		}
		rep.Rows = append(rep.Rows, row)
	}
	for _, r := range rep.Rows {
		if r.Result != "PASS" {
			rep.Failures++
		}
	}
	return rep, nil
}

func verifyAccount(ctx context.Context, c *Client, prep *Prepared, snap *Snapshot, opt Options, a *AccountRec, deep bool) (VerifyRow, *big.Int, error) {
	ccy := prep.AcctCurrency[a.SourceID]
	prec := prep.Precision[ccy]
	b := prep.BalanceByAcct[a.SourceID]
	expAvail, expLien := new(big.Int), new(big.Int)
	if b != nil {
		expAvail, expLien = b.available, b.lien
	}
	expTotal := new(big.Int).Add(expAvail, expLien)
	row := VerifyRow{
		RowType: "account", SourceID: a.SourceID, Currency: ccy, Result: "PASS",
		ExpAvail: FormatAmount(expAvail, prec), ExpLien: FormatAmount(expLien, prec), ExpTotal: FormatAmount(expTotal, prec),
	}
	fail := func(format string, args ...any) {
		row.Result = "FAIL"
		if row.Note != "" {
			row.Note += "; "
		}
		row.Note += fmt.Sprintf(format, args...)
	}

	ra, dup := snap.account(a.SourceID)
	if dup {
		fail("several accounts carry this legacy_source_id")
		return row, nil, nil
	}
	if ra == nil {
		fail("account not found on the server")
		return row, nil, nil
	}
	row.RemoteID, row.WalletID = ra.ID, ra.WalletID
	if n := metaString(ra.Metadata, metaLegacyAccountNum); n != a.LegacyAccountNumber {
		fail("legacy_account_number %q differs from input %q", n, a.LegacyAccountNumber)
	}
	if ra.Currency != ccy {
		fail("currency %s differs from input %s", ra.Currency, ccy)
	}
	if want := a.StatusOrDefault(); !statusMatches(want, ra) {
		// Reported, not failed: statuses legitimately move after cutover.
		row.Note += "status " + ra.Status + " (input " + want + "); "
	}

	asset := opt.asset(ccy, prec)
	read := func(addr string) (*big.Int, error) {
		vols, _, err := c.LedgerVolumes(ctx, addr)
		if err != nil {
			return nil, err
		}
		for k := range vols {
			if k != asset {
				fail("%s holds unexpected asset %s", addr, k)
			}
		}
		return vols[asset].Balance(), nil
	}
	actAvail, err := read(availableAddress(ra.WalletID, ra.Currency))
	if err != nil {
		return row, nil, err
	}
	actLien, err := read(lienAddress(ra.WalletID, ra.Currency))
	if err != nil {
		return row, nil, err
	}
	row.ActAvail, row.ActLien = FormatAmount(actAvail, prec), FormatAmount(actLien, prec)
	actTotal := new(big.Int).Add(actAvail, actLien)
	row.ActTotal = FormatAmount(actTotal, prec)
	if actAvail.Cmp(expAvail) != 0 {
		fail("available differs by %s", FormatAmount(new(big.Int).Sub(actAvail, expAvail), prec))
	}
	if actLien.Cmp(expLien) != 0 {
		fail("lien differs by %s", FormatAmount(new(big.Int).Sub(actLien, expLien), prec))
	}

	if deep {
		if b != nil && b.total.Sign() > 0 {
			_, want := buildOpening(opt, prec, b, ra)
			if tx, err := c.FindTransactionByReference(ctx, want.Reference); err != nil {
				return row, nil, err
			} else if tx == nil {
				fail("opening transaction %s missing", want.Reference)
			} else if merr := matchTx(tx, want); merr != nil {
				fail("%v", merr)
			}
		}
		for _, l := range prep.LiensByAcct[a.SourceID] {
			_, want := buildLien(opt, prec, ccy, l, ra)
			if tx, err := c.FindTransactionByReference(ctx, want.Reference); err != nil {
				return row, nil, err
			} else if tx == nil {
				fail("lien transaction %s missing", want.Reference)
			} else if merr := matchTx(tx, want); merr != nil {
				fail("%v", merr)
			}
		}
	}
	row.Note = strings.TrimSuffix(row.Note, "; ")
	return row, actTotal, nil
}

func statusMatches(want string, ra *RemoteAccount) bool {
	if want == "frozen" {
		return ra.Status == "active" && ra.FreezeDebits
	}
	return ra.Status == want
}
