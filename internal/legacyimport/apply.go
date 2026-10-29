package legacyimport

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type outcome string

const (
	outCreated  outcome = "created"
	outUpdated  outcome = "updated"
	outSkipped  outcome = "skipped"
	outConflict outcome = "conflict"
	outBlocked  outcome = "blocked"
	outFailed   outcome = "failed"
)

// ResultItem is one executed unit of work that did not end in plain success.
type ResultItem struct {
	Kind     string
	SourceID string
	Outcome  string
	Detail   string
}

// Result summarises an apply run.
type Result struct {
	mu     sync.Mutex
	Counts map[string]map[string]int
	Issues []ResultItem
	// FinalDeferred is set when the final status passes (suspend, freeze,
	// close, retire) were skipped because earlier phases had problems. Moving a
	// client or account out of "active" before everything else is in place
	// would stop the remaining account openings.
	FinalDeferred bool
}

func newResult() *Result { return &Result{Counts: map[string]map[string]int{}} }

func (r *Result) add(kind, id string, o outcome, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Counts[kind] == nil {
		r.Counts[kind] = map[string]int{}
	}
	r.Counts[kind][string(o)]++
	if o == outConflict || o == outBlocked || o == outFailed {
		r.Issues = append(r.Issues, ResultItem{Kind: kind, SourceID: id, Outcome: string(o), Detail: detail})
	}
}

// Failed returns the number of failed, conflicting and blocked items.
func (r *Result) Failed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.Counts {
		n += m[string(outFailed)] + m[string(outConflict)] + m[string(outBlocked)]
	}
	return n
}

// Summary renders the counts as text.
func (r *Result) Summary() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	kinds := make([]string, 0, len(r.Counts))
	for k := range r.Counts {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	var b strings.Builder
	for _, k := range kinds {
		fmt.Fprintf(&b, "%-14s", k)
		for _, o := range []outcome{outCreated, outUpdated, outSkipped, outConflict, outBlocked, outFailed} {
			if n := r.Counts[k][string(o)]; n > 0 {
				fmt.Fprintf(&b, " %s=%d", o, n)
			}
		}
		b.WriteString("\n")
	}
	if r.FinalDeferred {
		b.WriteString("final status passes deferred: fix the problems below and re-run apply\n")
	}
	iss := append([]ResultItem(nil), r.Issues...)
	sort.Slice(iss, func(i, j int) bool {
		if iss[i].Kind != iss[j].Kind {
			return iss[i].Kind < iss[j].Kind
		}
		return iss[i].SourceID < iss[j].SourceID
	})
	for i, it := range iss {
		if i == 50 {
			fmt.Fprintf(&b, "... and %d more, see the state file\n", len(iss)-50)
			break
		}
		fmt.Fprintf(&b, "  %s %s %s: %s\n", it.Outcome, it.Kind, it.SourceID, it.Detail)
	}
	return b.String()
}

// Engine executes the import against the API.
type Engine struct {
	c    *Client
	prep *Prepared
	snap *Snapshot
	st   *State
	opt  Options
	res  *Result

	refreshMu sync.Mutex
}

// NewEngine builds an engine. The snapshot must be fresh.
func NewEngine(c *Client, prep *Prepared, snap *Snapshot, st *State, opt Options) *Engine {
	opt.defaults()
	return &Engine{c: c, prep: prep, snap: snap, st: st, opt: opt, res: newResult()}
}

type task struct {
	kind string
	id   string
	run  func(ctx context.Context) (outcome, string, error)
}

// Apply runs every phase in dependency order. It is safe to run repeatedly.
func (e *Engine) Apply(ctx context.Context) (*Result, error) {
	if err := e.opt.Validate(); err != nil {
		return nil, err
	}
	e.st.BeginRun()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Periodic checkpoint. Losing up to a second of state is harmless because
	// every step re-checks the server.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = e.st.Flush()
			}
		}
	}()
	defer func() {
		close(done)
		wg.Wait()
		_ = e.st.Flush()
	}()

	p := e.prep
	var products, clients, accounts, balances, liens, accountsFinal, clientsFinal, productsFinal []task
	for _, pr := range p.DS.Products {
		pr := pr
		products = append(products, task{KindProduct, pr.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doProduct(ctx, pr) }})
		productsFinal = append(productsFinal, task{KindProductFinal, pr.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doProductFinal(ctx, pr) }})
	}
	for _, cl := range p.DS.Clients {
		cl := cl
		clients = append(clients, task{KindClient, cl.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doClient(ctx, cl) }})
		clientsFinal = append(clientsFinal, task{KindClientFinal, cl.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doClientFinal(ctx, cl) }})
	}
	for _, a := range p.DS.Accounts {
		a := a
		accounts = append(accounts, task{KindAccount, a.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doAccount(ctx, a) }})
		accountsFinal = append(accountsFinal, task{KindAccountFinal, a.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doAccountFinal(ctx, a) }})
	}
	for _, b := range p.DS.Balances {
		b := b
		if p.BalanceByAcct[b.AccountSourceID] != b {
			continue // invalid rows are rejected by Prepare
		}
		balances = append(balances, task{KindBalance, b.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doBalance(ctx, b) }})
	}
	for _, l := range p.DS.Liens {
		l := l
		if l.amount == nil {
			continue
		}
		liens = append(liens, task{KindLien, l.SourceID, func(ctx context.Context) (outcome, string, error) { return e.doLien(ctx, l) }})
	}

	phases := []struct {
		name  string
		tasks []task
	}{
		{"products", products}, {"clients", clients}, {"accounts", accounts},
		{"opening balances", balances}, {"liens", liens},
		{"account statuses", accountsFinal}, {"client statuses", clientsFinal}, {"product statuses", productsFinal},
	}
	for _, ph := range phases {
		if ctx.Err() != nil {
			break
		}
		if strings.HasSuffix(ph.name, "statuses") && e.res.Failed() > 0 {
			e.res.mu.Lock()
			e.res.FinalDeferred = true
			e.res.mu.Unlock()
			e.opt.Logf("skipping %s: earlier phases have problems", ph.name)
			continue
		}
		e.opt.Logf("phase %s: %d items", ph.name, len(ph.tasks))
		e.runPhase(ctx, cancel, ph.tasks)
		if err := e.st.Flush(); err != nil {
			return e.res, fmt.Errorf("writing state: %w", err)
		}
	}
	if err := ctx.Err(); err != nil && !e.opt.FailFast {
		return e.res, err
	}
	return e.res, nil
}

func (e *Engine) runPhase(ctx context.Context, cancel context.CancelFunc, tasks []task) {
	ch := make(chan task)
	var wg sync.WaitGroup
	for i := 0; i < e.opt.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ch {
				if ctx.Err() != nil {
					continue
				}
				e.execute(ctx, t)
				if e.opt.FailFast && e.res.Failed() > 0 {
					cancel()
				}
			}
		}()
	}
	for _, t := range tasks {
		ch <- t
	}
	close(ch)
	wg.Wait()
}

func (e *Engine) execute(ctx context.Context, t task) {
	o, detail, err := t.run(ctx)
	switch {
	case err != nil && ctx.Err() != nil:
		// Interrupted: leave the previous checkpoint untouched.
		return
	case err != nil:
		e.st.Fail(t.kind, t.id, StatusFailed, err)
		e.res.add(t.kind, t.id, outFailed, err.Error())
		e.opt.Logf("FAILED %s %s: %v", t.kind, t.id, err)
	case o == outConflict:
		e.st.Fail(t.kind, t.id, StatusConflict, errors.New(detail))
		e.res.add(t.kind, t.id, o, detail)
		e.opt.Logf("CONFLICT %s %s: %s", t.kind, t.id, detail)
	case o == outBlocked:
		e.st.Fail(t.kind, t.id, StatusBlocked, errors.New(detail))
		e.res.add(t.kind, t.id, o, detail)
	default:
		e.st.Update(t.kind, t.id, func(it *ItemState) {
			it.Status = StatusDone
			it.Detail = string(o) + ": " + detail
			it.Error = ""
		})
		e.res.add(t.kind, t.id, o, detail)
	}
}

// refreshClients reloads the client index; serialised so concurrent orphan
// recoveries do not hammer the API.
func (e *Engine) refreshClients(ctx context.Context) error {
	e.refreshMu.Lock()
	defer e.refreshMu.Unlock()
	return e.snap.RefreshClients(ctx, e.c)
}

func (e *Engine) refreshAccounts(ctx context.Context) error {
	e.refreshMu.Lock()
	defer e.refreshMu.Unlock()
	return e.snap.RefreshAccounts(ctx, e.c)
}

func isAmbiguous(err error) bool {
	var ae *AmbiguousError
	return errors.As(err, &ae)
}

const maxCreateAttempts = 3

// ---- products --------------------------------------------------------------

func (e *Engine) doProduct(ctx context.Context, p *ProductRec) (outcome, string, error) {
	out := outSkipped
	rp := e.snap.product(p.Code)
	if rp == nil {
		e.st.Set(KindProduct, p.SourceID, StatusInflight, "creating")
		for attempt := 0; attempt < maxCreateAttempts && rp == nil; attempt++ {
			created, err := e.c.CreateProduct(ctx, productBody(p))
			switch {
			case err == nil:
				rp = created
			case IsConflict(err) || isAmbiguous(err):
				// Product codes are unique: look the product up by code.
				if rerr := e.snap.refreshProducts(ctx, e.c); rerr != nil {
					return outFailed, "", rerr
				}
				rp = e.snap.product(p.Code)
				if rp == nil && !isAmbiguous(err) {
					return outFailed, "", err
				}
			default:
				return outFailed, "", err
			}
		}
		if rp == nil {
			return outFailed, "", errors.New("product create outcome unknown after retries")
		}
		out = outCreated
	}
	if !strings.EqualFold(rp.Currency, p.Currency) {
		return outConflict, fmt.Sprintf("product %s exists with currency %s, input says %s", p.Code, rp.Currency, p.Currency), nil
	}
	if rp.Status == "retired" && p.StatusOrDefault() == "active" {
		return outConflict, fmt.Sprintf("product %s is retired on the server, input says active", p.Code), nil
	}
	if rp.Status == "draft" {
		act, err := e.c.ActivateProduct(ctx, rp.ID)
		if err != nil {
			return outFailed, "", err
		}
		rp = act
		if out == outSkipped {
			out = outUpdated
		}
	}
	e.snap.putProduct(rp)
	e.st.Update(KindProduct, p.SourceID, func(it *ItemState) { it.RemoteID = rp.ID })
	return out, "product " + rp.Code + " " + rp.Status, nil
}

func (e *Engine) doProductFinal(ctx context.Context, p *ProductRec) (outcome, string, error) {
	if p.StatusOrDefault() != "retired" {
		return outSkipped, "target status active", nil
	}
	rp := e.snap.product(p.Code)
	if rp == nil {
		return outBlocked, "product missing", nil
	}
	if rp.Status != "active" {
		return outSkipped, "status " + rp.Status, nil
	}
	r, err := e.c.RetireProduct(ctx, rp.ID)
	if err != nil {
		return outFailed, "", err
	}
	e.snap.putProduct(r)
	return outUpdated, "retired", nil
}

// ---- clients ---------------------------------------------------------------

func (e *Engine) doClient(ctx context.Context, cl *ClientRec) (outcome, string, error) {
	out := outSkipped
	rc, dup := e.snap.client(cl.SourceID)
	if dup {
		return outConflict, "several clients on the server carry legacy_source_id " + cl.SourceID, nil
	}
	if rc == nil {
		prior := e.st.Get(KindClient, cl.SourceID)
		wasInflight := prior != nil && prior.CreateStarted
		if !wasInflight && strings.TrimSpace(cl.RegistrationNumber) != "" {
			if l := e.snap.lookalikes(cl); len(l) > 0 {
				switch {
				case e.opt.AdoptUnmarkedClients && len(l) == 1 && l[0].Status == "pending":
					rc = l[0]
					e.opt.Logf("adopting unmarked client %s for %s (--adopt-unmarked-clients)", rc.ClientNumber, cl.SourceID)
				default:
					return outConflict, fmt.Sprintf("client %s (%s) already exists on the server without an import marker; resolve manually or use --adopt-unmarked-clients for pending clients", l[0].ClientNumber, l[0].legalName()), nil
				}
			}
		}
		if rc != nil {
			out = outUpdated
		}
		if rc == nil {
			e.st.Set(KindClient, cl.SourceID, StatusInflight, "creating")
		}
		for attempt := 0; attempt < maxCreateAttempts && rc == nil; attempt++ {
			if wasInflight || attempt > 0 {
				// A previous attempt may have succeeded without us seeing it.
				if err := e.refreshClients(ctx); err != nil {
					return outFailed, "", err
				}
				if again, dup := e.snap.client(cl.SourceID); dup {
					return outConflict, "several clients carry legacy_source_id " + cl.SourceID, nil
				} else if again != nil {
					rc = again
					break
				}
				cands := e.snap.orphanCandidates(cl)
				if len(cands) > 1 {
					return outConflict, fmt.Sprintf("%d unmarked pending clients match %q, cannot pick the orphan of a previous run", len(cands), cl.LegalName), nil
				}
				if len(cands) == 1 {
					rc = cands[0]
					e.opt.Logf("adopting orphan client %s for %s", rc.ClientNumber, cl.SourceID)
					break
				}
			}
			created, err := e.c.CreateClient(ctx, clientBody(cl))
			if err == nil {
				rc = created
				break
			}
			if !isAmbiguous(err) {
				return outFailed, "", err
			}
			wasInflight = true
		}
		if rc == nil {
			return outFailed, "", errors.New("client create outcome unknown after retries")
		}
		if out == outSkipped {
			out = outCreated
		}
	}
	if rc.legalName() != strings.TrimSpace(cl.LegalName) && rc.legalName() != "" {
		return outConflict, fmt.Sprintf("client %s legal_name %q differs from input %q", rc.ClientNumber, rc.legalName(), cl.LegalName), nil
	}
	e.st.Update(KindClient, cl.SourceID, func(it *ItemState) {
		it.RemoteID = rc.ID
		if it.Extra == nil {
			it.Extra = map[string]string{}
		}
		it.Extra["client_number"] = rc.ClientNumber
	})

	// KYC: submit once (tagged with the marker), then verify.
	rec, err := e.findOrSubmitKYC(ctx, rc, cl)
	if err != nil {
		return outFailed, "", err
	}
	switch rec.Status {
	case "pending":
		if _, err := e.c.VerifyKYC(ctx, rc.ID, rec.ID, KYCVerifier); err != nil {
			return outFailed, "", err
		}
		if out == outSkipped {
			out = outUpdated
		}
	case "verified":
	default:
		return outConflict, fmt.Sprintf("KYC record %s is %s on the server", rec.ID, rec.Status), nil
	}
	fresh, err := e.c.GetClient(ctx, rc.ID)
	if err != nil {
		return outFailed, "", err
	}
	if fresh.LegacyID() != cl.SourceID {
		return outFailed, "", fmt.Errorf("client %s has no legacy marker after KYC submission", fresh.ClientNumber)
	}
	rc = fresh

	// Activate (the account opening flow needs an active client; suspension and
	// closure are applied in the final pass).
	if clientRank(cl.StatusOrDefault()) >= 1 && cl.KYCLevel >= 1 && rc.Status == "pending" {
		act, err := e.c.ActivateClient(ctx, rc.ID)
		if err != nil {
			return outFailed, "", err
		}
		if act.LegacyID() == "" {
			act.KYCData = fresh.KYCData
		}
		rc = act
		if out == outSkipped {
			out = outUpdated
		}
	}
	e.snap.putClient(rc)
	return out, fmt.Sprintf("client %s %s kyc=%d", rc.ClientNumber, rc.Status, rc.KYCLevel), nil
}

func (e *Engine) findOrSubmitKYC(ctx context.Context, rc *RemoteClient, cl *ClientRec) (*RemoteKYC, error) {
	find := func() (*RemoteKYC, error) {
		recs, err := e.c.ListKYC(ctx, rc.ID)
		if err != nil {
			return nil, err
		}
		var found *RemoteKYC
		for i := range recs {
			if id, _ := recs[i].Payload[metaLegacySourceID].(string); id == cl.SourceID && recs[i].Level == cl.KYCLevel {
				found = &recs[i]
			}
		}
		return found, nil
	}
	rec, err := find()
	if err != nil || rec != nil {
		return rec, err
	}
	for attempt := 0; attempt < maxCreateAttempts; attempt++ {
		sub, err := e.c.SubmitKYC(ctx, rc.ID, kycBody(cl))
		if err == nil {
			return sub, nil
		}
		if !isAmbiguous(err) {
			return nil, err
		}
		if rec, ferr := find(); ferr != nil || rec != nil {
			return rec, ferr
		}
	}
	return nil, errors.New("KYC submission outcome unknown after retries")
}

func (e *Engine) doClientFinal(ctx context.Context, cl *ClientRec) (outcome, string, error) {
	want := cl.StatusOrDefault()
	if want == "pending" || want == "active" {
		return outSkipped, "target status " + want, nil
	}
	rc, _ := e.snap.client(cl.SourceID)
	if rc == nil {
		return outBlocked, "client missing", nil
	}
	if clientRank(rc.Status) >= clientRank(want) {
		return outSkipped, "status " + rc.Status, nil
	}
	var (
		upd *RemoteClient
		err error
	)
	if want == "suspended" {
		upd, err = e.c.SuspendClient(ctx, rc.ID, "legacy status: suspended ("+cl.SourceID+")")
	} else {
		upd, err = e.c.CloseClient(ctx, rc.ID)
	}
	if err != nil {
		return outFailed, "", err
	}
	if upd.LegacyID() == "" {
		upd.KYCData = rc.KYCData
	}
	e.snap.putClient(upd)
	return outUpdated, "now " + upd.Status, nil
}

// ---- accounts --------------------------------------------------------------

func (e *Engine) doAccount(ctx context.Context, a *AccountRec) (outcome, string, error) {
	out := outSkipped
	ra, dup := e.snap.account(a.SourceID)
	if dup {
		return outConflict, "several accounts carry legacy_source_id " + a.SourceID, nil
	}
	rc, _ := e.snap.client(a.ClientSourceID)
	rp := e.snap.product(a.ProductCode)
	if ra == nil {
		if rc == nil || rc.Status != "active" {
			return outBlocked, "client " + a.ClientSourceID + " is not imported and active", nil
		}
		if rp == nil || rp.Status != "active" {
			return outBlocked, "product " + a.ProductCode + " is not imported and active", nil
		}
		wallet := walletIDFor(rc.ClientNumber, rp.Code)
		wasInflight := false
		if prior := e.st.Get(KindAccount, a.SourceID); prior != nil && prior.CreateStarted {
			wasInflight = true
		}
		if other := e.snap.accountByWallet(wallet); other != nil {
			if other.LegacyID() != "" && other.LegacyID() != a.SourceID {
				return outConflict, fmt.Sprintf("wallet %s already belongs to legacy account %s", wallet, other.LegacyID()), nil
			}
			if !wasInflight {
				return outConflict, fmt.Sprintf("wallet %s already exists on the server without an import marker", wallet), nil
			}
			ra = other
		}
		if ra == nil {
			e.st.Set(KindAccount, a.SourceID, StatusInflight, "opening")
			body := map[string]any{"client_id": rc.ID, "product_id": rp.ID, "metadata": accountMetadata(a)}
			for attempt := 0; attempt < maxCreateAttempts && ra == nil; attempt++ {
				created, err := e.c.OpenAccount(ctx, body)
				switch {
				case err == nil:
					ra = created
				case isAmbiguous(err) || IsConflict(err):
					// The wallet id is deterministic, so a repeat is detectable.
					if rerr := e.refreshAccounts(ctx); rerr != nil {
						return outFailed, "", rerr
					}
					if other := e.snap.accountByWallet(wallet); other != nil {
						if other.LegacyID() != "" && other.LegacyID() != a.SourceID {
							return outConflict, fmt.Sprintf("wallet %s already belongs to legacy account %s", wallet, other.LegacyID()), nil
						}
						ra = other
					} else if !isAmbiguous(err) {
						return outFailed, "", err
					}
				default:
					return outFailed, "", err
				}
			}
			if ra == nil {
				return outFailed, "", errors.New("account open outcome unknown after retries")
			}
			out = outCreated
		}
	} else {
		if rc != nil && ra.ClientID != rc.ID {
			return outConflict, fmt.Sprintf("account %s belongs to client %s, expected %s", ra.AccountNumber, ra.ClientID, rc.ID), nil
		}
		if rp != nil && ra.ProductID != rp.ID {
			return outConflict, fmt.Sprintf("account %s belongs to product %s, expected %s", ra.AccountNumber, ra.ProductID, rp.ID), nil
		}
		if n, _ := ra.Metadata[metaLegacyAccountNum].(string); n != a.LegacyAccountNumber {
			return outConflict, fmt.Sprintf("account %s carries legacy_account_number %q, input says %q", ra.AccountNumber, n, a.LegacyAccountNumber), nil
		}
	}
	if a.StatusOrDefault() != "pending" && ra.Status == "pending" {
		act, err := e.c.ActivateAccount(ctx, ra.ID)
		if err != nil {
			return outFailed, "", err
		}
		if act.Metadata == nil {
			act.Metadata = ra.Metadata
		}
		if act.WalletID == "" {
			act.WalletID, act.Currency = ra.WalletID, ra.Currency
		}
		ra = act
		if out == outSkipped {
			out = outUpdated
		}
	}
	e.snap.putAccount(ra)
	e.st.Update(KindAccount, a.SourceID, func(it *ItemState) {
		it.RemoteID = ra.ID
		if it.Extra == nil {
			it.Extra = map[string]string{}
		}
		it.Extra["wallet_id"] = ra.WalletID
		it.Extra["account_number"] = ra.AccountNumber
	})
	return out, fmt.Sprintf("account %s wallet %s %s", ra.AccountNumber, ra.WalletID, ra.Status), nil
}

func (e *Engine) doAccountFinal(ctx context.Context, a *AccountRec) (outcome, string, error) {
	want := a.StatusOrDefault()
	if want == "pending" || want == "active" {
		return outSkipped, "target status " + want, nil
	}
	ra, _ := e.snap.account(a.SourceID)
	if ra == nil {
		return outBlocked, "account missing", nil
	}
	var (
		upd *RemoteAccount
		err error
	)
	switch want {
	case "frozen":
		if ra.Status != "active" && ra.Status != "dormant" {
			return outSkipped, "status " + ra.Status, nil
		}
		if ra.FreezeDebits {
			return outSkipped, "already frozen", nil
		}
		upd, err = e.c.FreezeAccount(ctx, ra.ID)
	case "dormant":
		if ra.Status != "active" {
			return outSkipped, "status " + ra.Status, nil
		}
		upd, err = e.c.DormantAccount(ctx, ra.ID)
	case "suspended":
		if accountRank(ra.Status) >= 2 {
			return outSkipped, "status " + ra.Status, nil
		}
		upd, err = e.c.SuspendAccount(ctx, ra.ID)
	case "closed":
		if ra.Status == "closed" {
			return outSkipped, "already closed", nil
		}
		upd, err = e.c.CloseAccount(ctx, ra.ID)
	}
	if err != nil {
		return outFailed, "", err
	}
	if upd.WalletID == "" {
		upd.WalletID, upd.Currency, upd.Metadata = ra.WalletID, ra.Currency, ra.Metadata
	}
	e.snap.putAccount(upd)
	return outUpdated, "now " + upd.Status, nil
}

// ---- ledger postings ---------------------------------------------------------

func (e *Engine) doBalance(ctx context.Context, b *BalanceRec) (outcome, string, error) {
	if b.total.Sign() == 0 {
		return outSkipped, "zero balance, nothing to post", nil
	}
	ra, _ := e.snap.account(b.AccountSourceID)
	if ra == nil {
		return outBlocked, "account " + b.AccountSourceID + " is not imported", nil
	}
	if prior := e.st.Get(KindBalance, b.SourceID); prior != nil && prior.Status == StatusDone && prior.Extra["tx_id"] != "" {
		return outSkipped, "already posted (tx " + prior.Extra["tx_id"] + ")", nil
	}
	req, want := buildOpening(e.opt, e.prep.Precision[b.Currency], b, ra)
	return e.post(ctx, KindBalance, b.SourceID, req, want)
}

func (e *Engine) doLien(ctx context.Context, l *LienRec) (outcome, string, error) {
	ra, _ := e.snap.account(l.AccountSourceID)
	if ra == nil {
		return outBlocked, "account " + l.AccountSourceID + " is not imported", nil
	}
	if prior := e.st.Get(KindLien, l.SourceID); prior != nil && prior.Status == StatusDone && prior.Extra["tx_id"] != "" {
		return outSkipped, "already posted (tx " + prior.Extra["tx_id"] + ")", nil
	}
	ccy := e.prep.AcctCurrency[l.AccountSourceID]
	req, want := buildLien(e.opt, e.prep.Precision[ccy], ccy, l, ra)
	return e.post(ctx, KindLien, l.SourceID, req, want)
}

// post sends a deterministic-reference transaction. It is idempotent by
// construction: a repeat is answered by the idempotency hit or by a 409, in
// which case the existing transaction is compared with the expected one.
func (e *Engine) post(ctx context.Context, kind, id string, req TxRequest, want expectedTx) (outcome, string, error) {
	tx, hit, err := e.c.CreateTransaction(ctx, req)
	if err != nil {
		if !IsConflict(err) {
			return outFailed, "", err
		}
		existing, ferr := e.c.FindTransactionByReference(ctx, req.Reference)
		if ferr != nil {
			return outFailed, "", ferr
		}
		if existing == nil {
			return outFailed, "", fmt.Errorf("conflict on reference %q but no such transaction found: %w", req.Reference, err)
		}
		if merr := matchTx(existing, want); merr != nil {
			return outConflict, merr.Error(), nil
		}
		tx, hit = existing, true
	} else if merr := matchTx(tx, want); merr != nil {
		// The server accepted it but answered with something unexpected.
		return outFailed, "", fmt.Errorf("posted transaction does not match the request: %w", merr)
	}
	e.st.Update(kind, id, func(it *ItemState) {
		it.RemoteID = string(tx.ID)
		if it.Extra == nil {
			it.Extra = map[string]string{}
		}
		it.Extra["tx_id"] = string(tx.ID)
		it.Extra["reference"] = req.Reference
	})
	if hit {
		return outSkipped, "reference " + req.Reference + " already posted as tx " + string(tx.ID), nil
	}
	return outCreated, "posted tx " + string(tx.ID) + " ref " + req.Reference, nil
}
