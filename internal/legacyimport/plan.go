package legacyimport

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// PlanAction is what apply would do with one item.
type PlanAction string

const (
	ActionCreate   PlanAction = "create"
	ActionUpdate   PlanAction = "update"
	ActionSkip     PlanAction = "skip"
	ActionConflict PlanAction = "conflict"
	ActionBlocked  PlanAction = "blocked"
)

// PlanItem is one line of the dry run.
type PlanItem struct {
	Kind     string     `json:"kind"`
	SourceID string     `json:"source_id"`
	Action   PlanAction `json:"action"`
	Detail   string     `json:"detail,omitempty"`
}

// Plan is the dry-run result.
type Plan struct {
	Items []PlanItem `json:"items"`
}

// Counts returns kind -> action -> count.
func (p *Plan) Counts() map[string]map[PlanAction]int {
	out := map[string]map[PlanAction]int{}
	for _, it := range p.Items {
		if out[it.Kind] == nil {
			out[it.Kind] = map[PlanAction]int{}
		}
		out[it.Kind][it.Action]++
	}
	return out
}

// Conflicts returns the number of conflicting items.
func (p *Plan) Conflicts() int {
	n := 0
	for _, it := range p.Items {
		if it.Action == ActionConflict {
			n++
		}
	}
	return n
}

// Summary renders counts followed by every conflict.
func (p *Plan) Summary() string {
	counts := p.Counts()
	kinds := make([]string, 0, len(counts))
	for k := range counts {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	var b strings.Builder
	for _, k := range kinds {
		fmt.Fprintf(&b, "%-14s", k)
		for _, a := range []PlanAction{ActionCreate, ActionUpdate, ActionSkip, ActionConflict, ActionBlocked} {
			if n := counts[k][a]; n > 0 {
				fmt.Fprintf(&b, " %s=%d", a, n)
			}
		}
		b.WriteString("\n")
	}
	for _, it := range p.Items {
		if it.Action == ActionConflict {
			fmt.Fprintf(&b, "  conflict %s %s: %s\n", it.Kind, it.SourceID, it.Detail)
		}
	}
	return b.String()
}

// BuildPlan compares the dataset with the server and the state file without
// writing anything. st may be nil.
func BuildPlan(ctx context.Context, c *Client, prep *Prepared, snap *Snapshot, st *State, opt Options) (*Plan, error) {
	opt.defaults()
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	pl := &Plan{}
	add := func(kind, id string, a PlanAction, format string, args ...any) {
		pl.Items = append(pl.Items, PlanItem{Kind: kind, SourceID: id, Action: a, Detail: fmt.Sprintf(format, args...)})
	}
	inflight := func(kind, id string) bool {
		if st == nil {
			return false
		}
		it := st.Get(kind, id)
		return it != nil && it.CreateStarted
	}

	// Products.
	for _, p := range prep.DS.Products {
		rp := snap.product(p.Code)
		switch {
		case rp == nil:
			add(KindProduct, p.SourceID, ActionCreate, "create product %s (%s) and activate", p.Code, p.Currency)
		case !strings.EqualFold(rp.Currency, p.Currency):
			add(KindProduct, p.SourceID, ActionConflict, "product %s exists with currency %s, input says %s", p.Code, rp.Currency, p.Currency)
		case rp.Status == "retired" && p.StatusOrDefault() == "active":
			add(KindProduct, p.SourceID, ActionConflict, "product %s is retired on the server, input says active", p.Code)
		case rp.Status == "draft":
			add(KindProduct, p.SourceID, ActionUpdate, "activate existing product %s", p.Code)
		case p.StatusOrDefault() == "retired" && rp.Status == "active":
			add(KindProduct, p.SourceID, ActionUpdate, "retire product %s after import", p.Code)
		default:
			add(KindProduct, p.SourceID, ActionSkip, "product %s exists (%s)", p.Code, rp.Status)
		}
	}

	// Clients.
	for _, cl := range prep.DS.Clients {
		rc, dup := snap.client(cl.SourceID)
		want := cl.StatusOrDefault()
		switch {
		case dup:
			add(KindClient, cl.SourceID, ActionConflict, "several clients on the server carry legacy_source_id %s", cl.SourceID)
		case rc == nil:
			if strings.TrimSpace(cl.RegistrationNumber) != "" && !inflight(KindClient, cl.SourceID) {
				if l := snap.lookalikes(cl); len(l) > 0 {
					if opt.AdoptUnmarkedClients && len(l) == 1 && l[0].Status == "pending" {
						add(KindClient, cl.SourceID, ActionUpdate, "adopt unmarked pending client %s, then submit and verify KYC", l[0].ClientNumber)
						continue
					}
					add(KindClient, cl.SourceID, ActionConflict, "client %s (%s) already exists without an import marker", l[0].ClientNumber, l[0].legalName())
					continue
				}
			}
			add(KindClient, cl.SourceID, ActionCreate, "create corporate client, submit and verify KYC level %d, activate, then status %s", cl.KYCLevel, want)
		case rc.legalName() != "" && rc.legalName() != strings.TrimSpace(cl.LegalName):
			add(KindClient, cl.SourceID, ActionConflict, "client %s legal_name %q differs from input %q", rc.ClientNumber, rc.legalName(), cl.LegalName)
		default:
			var steps []string
			if rc.KYCStatus != "verified" || rc.KYCLevel < cl.KYCLevel {
				steps = append(steps, "complete KYC")
			}
			if clientRank(want) >= 1 && cl.KYCLevel >= 1 && rc.Status == "pending" {
				steps = append(steps, "activate")
			}
			if clientRank(want) >= 2 && clientRank(rc.Status) < clientRank(want) {
				steps = append(steps, "set status "+want)
			}
			if len(steps) == 0 {
				add(KindClient, cl.SourceID, ActionSkip, "client %s exists (%s)", rc.ClientNumber, rc.Status)
			} else {
				add(KindClient, cl.SourceID, ActionUpdate, "client %s: %s", rc.ClientNumber, strings.Join(steps, ", "))
			}
		}
	}

	// Accounts.
	for _, a := range prep.DS.Accounts {
		ra, dup := snap.account(a.SourceID)
		rc, _ := snap.client(a.ClientSourceID)
		rp := snap.product(a.ProductCode)
		want := a.StatusOrDefault()
		switch {
		case dup:
			add(KindAccount, a.SourceID, ActionConflict, "several accounts on the server carry legacy_source_id %s", a.SourceID)
		case ra == nil:
			if rc != nil && rp != nil {
				wallet := walletIDFor(rc.ClientNumber, rp.Code)
				if other := snap.accountByWallet(wallet); other != nil && !inflight(KindAccount, a.SourceID) {
					add(KindAccount, a.SourceID, ActionConflict, "wallet %s already exists on the server (account %s, legacy id %q)", wallet, other.AccountNumber, other.LegacyID())
					continue
				}
			}
			add(KindAccount, a.SourceID, ActionCreate, "open account for client %s on product %s, activate, then status %s", a.ClientSourceID, a.ProductCode, want)
		case rc != nil && ra.ClientID != rc.ID, rp != nil && ra.ProductID != rp.ID:
			add(KindAccount, a.SourceID, ActionConflict, "account %s is attached to a different client or product than the input", ra.AccountNumber)
		case metaString(ra.Metadata, metaLegacyAccountNum) != a.LegacyAccountNumber:
			add(KindAccount, a.SourceID, ActionConflict, "account %s carries legacy_account_number %q, input says %q", ra.AccountNumber, metaString(ra.Metadata, metaLegacyAccountNum), a.LegacyAccountNumber)
		default:
			var steps []string
			if want != "pending" && ra.Status == "pending" {
				steps = append(steps, "activate")
			}
			if want == "frozen" && !ra.FreezeDebits || (want != "frozen" && accountRank(want) >= 2 && accountRank(ra.Status) < accountRank(want)) {
				steps = append(steps, "set status "+want)
			}
			if len(steps) == 0 {
				add(KindAccount, a.SourceID, ActionSkip, "account %s exists (%s)", ra.AccountNumber, ra.Status)
			} else {
				add(KindAccount, a.SourceID, ActionUpdate, "account %s: %s", ra.AccountNumber, strings.Join(steps, ", "))
			}
		}
	}

	// Postings.
	lookup := func(kind, id string, req TxRequest, want expectedTx, amountLabel string) error {
		if st != nil {
			if it := st.Get(kind, id); it != nil && it.Status == StatusDone && it.Extra["tx_id"] != "" {
				add(kind, id, ActionSkip, "posted per state file (tx %s)", it.Extra["tx_id"])
				return nil
			}
		}
		tx, err := c.FindTransactionByReference(ctx, req.Reference)
		if err != nil {
			return err
		}
		if tx == nil {
			add(kind, id, ActionCreate, "post %s via reference %s", amountLabel, req.Reference)
			return nil
		}
		if merr := matchTx(tx, want); merr != nil {
			add(kind, id, ActionConflict, "%v", merr)
			return nil
		}
		add(kind, id, ActionSkip, "already posted as tx %s", tx.ID)
		return nil
	}
	for _, b := range prep.DS.Balances {
		if prep.BalanceByAcct[b.AccountSourceID] != b {
			continue
		}
		prec := prep.Precision[b.Currency]
		if b.total.Sign() == 0 {
			add(KindBalance, b.SourceID, ActionSkip, "zero balance, nothing to post")
			continue
		}
		ra, _ := snap.account(b.AccountSourceID)
		if ra == nil {
			add(KindBalance, b.SourceID, ActionCreate, "post %s %s once the account exists (reference import:opening:%s)", b.Amount, b.Currency, b.SourceID)
			continue
		}
		req, want := buildOpening(opt, prec, b, ra)
		if err := lookup(KindBalance, b.SourceID, req, want, b.Amount+" "+b.Currency); err != nil {
			return nil, err
		}
	}
	for _, l := range prep.DS.Liens {
		if l.amount == nil {
			continue
		}
		ccy := prep.AcctCurrency[l.AccountSourceID]
		ra, _ := snap.account(l.AccountSourceID)
		if ra == nil {
			add(KindLien, l.SourceID, ActionCreate, "post lien %s %s once the account exists (reference %s)", l.Amount, ccy, l.Reference)
			continue
		}
		req, want := buildLien(opt, prep.Precision[ccy], ccy, l, ra)
		if err := lookup(KindLien, l.SourceID, req, want, "lien "+l.Amount+" "+ccy); err != nil {
			return nil, err
		}
	}
	return pl, nil
}

func metaString(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}
