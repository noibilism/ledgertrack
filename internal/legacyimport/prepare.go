package legacyimport

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"
)

// PrepareOptions drive semantic validation.
type PrepareOptions struct {
	// Precision maps currency code to fractional digits. For plan/apply/verify
	// this comes from GET /v2/ledgertrack/currencies and is authoritative.
	Precision map[string]int
	// Authoritative means Precision came from the server: a currency missing
	// from it is an error and a conflicting products.jsonl precision is an
	// error. When false (offline validate) missing currencies fall back to
	// DefaultPrecision with a warning.
	Authoritative    bool
	DefaultPrecision int
	// TrimZeros tolerates all-zero fractional digits beyond the precision.
	TrimZeros bool
	// AllowSynthesizedEvidence permits kyc_level 2 and 3, for which the tool
	// must fabricate placeholder documents / evidence references to satisfy
	// the LedgerTrack KYC submission rules.
	AllowSynthesizedEvidence bool
}

// CurrencyTotal aggregates the balances of one currency.
type CurrencyTotal struct {
	Currency  string
	Precision int
	Accounts  int
	Total     *big.Int
	Available *big.Int
	Lien      *big.Int
}

// Prepared is a validated, indexed dataset with amounts converted to minor units.
type Prepared struct {
	DS            *Dataset
	Precision     map[string]int
	ProductBySrc  map[string]*ProductRec
	ProductByCode map[string]*ProductRec // key: upper-case code
	ClientBySrc   map[string]*ClientRec
	AccountBySrc  map[string]*AccountRec
	BalanceByAcct map[string]*BalanceRec
	LiensByAcct   map[string][]*LienRec
	Totals        map[string]*CurrencyTotal
	// Currency of each account (from its product).
	AcctCurrency map[string]string
}

var (
	codeRe     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	currencyRe = regexp.MustCompile(`^[A-Z][A-Z0-9]{1,15}$`)
	zeroDecRe  = regexp.MustCompile(`^0(\.0*)?$`)
)

type productRules struct {
	MinOpeningBalance   *string  `json:"min_opening_balance"`
	RequiresKYCLevel    *int     `json:"requires_kyc_level"`
	EligibleClientTypes []string `json:"eligible_client_types"`
}

func (p *ProductRec) parsedRules() productRules {
	var r productRules
	if len(p.Rules) > 0 {
		_ = json.Unmarshal(p.Rules, &r)
	}
	return r
}

func oneOf(v string, set []string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// Prepare validates referential integrity and amounts and converts amounts to
// minor units. It performs no network access.
func Prepare(ds *Dataset, opt PrepareOptions) (*Prepared, []Issue) {
	var issues []Issue
	if opt.DefaultPrecision == 0 && !opt.Authoritative {
		opt.DefaultPrecision = 2
	}
	p := &Prepared{
		DS:            ds,
		Precision:     map[string]int{},
		ProductBySrc:  map[string]*ProductRec{},
		ProductByCode: map[string]*ProductRec{},
		ClientBySrc:   map[string]*ClientRec{},
		AccountBySrc:  map[string]*AccountRec{},
		BalanceByAcct: map[string]*BalanceRec{},
		LiensByAcct:   map[string][]*LienRec{},
		Totals:        map[string]*CurrencyTotal{},
		AcctCurrency:  map[string]string{},
	}
	add := func(i Issue) { issues = append(issues, i) }

	// Products.
	for _, pr := range ds.Products {
		f := FileProducts
		if pr.SourceID == "" {
			add(errIssue(f, pr.Line, "source_id is empty"))
			continue
		}
		if prev, dup := p.ProductBySrc[pr.SourceID]; dup {
			add(errIssue(f, pr.Line, "duplicate source_id %q (first seen on line %d)", pr.SourceID, prev.Line))
			continue
		}
		p.ProductBySrc[pr.SourceID] = pr
		if !codeRe.MatchString(pr.Code) {
			add(errIssue(f, pr.Line, "code %q must match %s", pr.Code, codeRe))
		}
		if pr.Currency != "" && pr.Code != pr.Currency && !strings.HasSuffix(pr.Code, "-"+pr.Currency) {
			add(warnIssue(f, pr.Line, "code %q does not end with its currency %q: the wallet API reads the currency from the end of the wallet id, so /wallets/* cannot operate on these accounts (name the code %q or end it with -%s)", pr.Code, pr.Currency, pr.Currency, pr.Currency))
		}
		key := strings.ToUpper(pr.Code)
		if prev, dup := p.ProductByCode[key]; dup {
			add(errIssue(f, pr.Line, "duplicate product code %q (first seen on line %d)", pr.Code, prev.Line))
		} else {
			p.ProductByCode[key] = pr
		}
		if !currencyRe.MatchString(pr.Currency) {
			add(errIssue(f, pr.Line, "currency %q must match %s", pr.Currency, currencyRe))
		}
		if !oneOf(pr.StatusOrDefault(), ProductStatuses) {
			add(errIssue(f, pr.Line, "status %q must be one of %v", pr.Status, ProductStatuses))
		}
		rules := pr.parsedRules()
		if rules.MinOpeningBalance != nil && !zeroDecRe.MatchString(strings.TrimSpace(*rules.MinOpeningBalance)) {
			add(errIssue(f, pr.Line, "rules.min_opening_balance must be 0: accounts are opened without a deposit and the opening balance is posted separately"))
		}
		// Precision resolution.
		if srv, ok := opt.Precision[pr.Currency]; ok {
			if pr.Precision != nil && *pr.Precision != srv {
				add(errIssue(f, pr.Line, "precision %d in file differs from currency registry precision %d for %s", *pr.Precision, srv, pr.Currency))
			}
			p.Precision[pr.Currency] = srv
		} else if opt.Authoritative {
			add(errIssue(f, pr.Line, "currency %s is not enabled in the LedgerTrack currency registry", pr.Currency))
		} else if pr.Precision != nil {
			if prev, ok := p.Precision[pr.Currency]; ok && prev != *pr.Precision {
				add(errIssue(f, pr.Line, "precision %d for %s conflicts with another product (%d)", *pr.Precision, pr.Currency, prev))
			}
			p.Precision[pr.Currency] = *pr.Precision
		}
	}
	if !opt.Authoritative {
		seen := map[string]bool{}
		for _, pr := range ds.Products {
			if _, ok := p.Precision[pr.Currency]; !ok && !seen[pr.Currency] {
				seen[pr.Currency] = true
				p.Precision[pr.Currency] = opt.DefaultPrecision
				add(warnIssue(FileProducts, pr.Line, "no precision known for %s, assuming %d (set it in products.jsonl or pass --precision %s=N)", pr.Currency, opt.DefaultPrecision, pr.Currency))
			}
		}
	}

	// Clients.
	acctCount := map[string]int{}
	for _, a := range ds.Accounts {
		acctCount[a.ClientSourceID]++
	}
	regSeen := map[string]*ClientRec{}
	twinSeen := map[string]*ClientRec{}
	for _, c := range ds.Clients {
		f := FileClients
		if c.SourceID == "" {
			add(errIssue(f, c.Line, "source_id is empty"))
			continue
		}
		if prev, dup := p.ClientBySrc[c.SourceID]; dup {
			add(errIssue(f, c.Line, "duplicate source_id %q (first seen on line %d)", c.SourceID, prev.Line))
			continue
		}
		p.ClientBySrc[c.SourceID] = c
		if strings.TrimSpace(c.LegalName) == "" {
			add(errIssue(f, c.Line, "legal_name is empty"))
		}
		if !oneOf(c.StatusOrDefault(), ClientStatuses) {
			add(errIssue(f, c.Line, "status %q must be one of %v", c.Status, ClientStatuses))
		}
		if c.KYCLevel < 0 || c.KYCLevel > 3 {
			add(errIssue(f, c.Line, "kyc_level %d out of range 0..3", c.KYCLevel))
		}
		if c.KYCLevel >= 1 && c.kycIdentifier() == "" {
			add(errIssue(f, c.Line, "kyc_level %d needs kyc_identifier, registration_number or tax_id (LedgerTrack requires national_id_number from level 1)", c.KYCLevel))
		}
		if c.KYCLevel >= 2 && !opt.AllowSynthesizedEvidence {
			add(errIssue(f, c.Line, "kyc_level %d requires documents (level 2) or biometric/in-person evidence (level 3) that the legacy export cannot provide; pass --allow-synthesized-kyc-evidence to import placeholder evidence tagged provider=legacy-import", c.KYCLevel))
		}
		if c.LegacyCreatedAt != "" {
			if _, err := time.Parse(time.RFC3339, c.LegacyCreatedAt); err != nil {
				add(errIssue(f, c.Line, "legacy_created_at is not RFC3339: %v", err))
			}
		}
		if c.StatusOrDefault() == "pending" && acctCount[c.SourceID] > 0 {
			add(errIssue(f, c.Line, "client has accounts but status is pending: accounts can only be opened for active clients"))
		}
		if c.KYCLevel < 1 && acctCount[c.SourceID] > 0 {
			add(errIssue(f, c.Line, "client has accounts but kyc_level is %d: LedgerTrack only activates clients with verified KYC level 1 or higher", c.KYCLevel))
		}
		if c.RegistrationNumber != "" {
			if prev, ok := regSeen[c.RegistrationNumber]; ok {
				add(warnIssue(f, c.Line, "registration_number %q also used by client %q (line %d)", c.RegistrationNumber, prev.SourceID, prev.Line))
			} else {
				regSeen[c.RegistrationNumber] = c
			}
		}
		twin := strings.Join([]string{strings.TrimSpace(c.LegalName), strings.TrimSpace(c.Phone), strings.TrimSpace(c.RegistrationNumber)}, "\x00")
		if prev, ok := twinSeen[twin]; ok {
			add(errIssue(f, c.Line, "client is indistinguishable from %q (line %d): same legal_name, phone and registration_number. The importer recovers from crashes by matching on these fields, so they must be unique per client", prev.SourceID, prev.Line))
		} else {
			twinSeen[twin] = c
		}
		if acctCount[c.SourceID] == 0 {
			add(warnIssue(f, c.Line, "client %q has no accounts", c.SourceID))
		}
	}

	// Accounts.
	pairSeen := map[string]*AccountRec{}
	legacyNumSeen := map[string]*AccountRec{}
	for _, a := range ds.Accounts {
		f := FileAccounts
		if a.SourceID == "" {
			add(errIssue(f, a.Line, "source_id is empty"))
			continue
		}
		if prev, dup := p.AccountBySrc[a.SourceID]; dup {
			add(errIssue(f, a.Line, "duplicate source_id %q (first seen on line %d)", a.SourceID, prev.Line))
			continue
		}
		p.AccountBySrc[a.SourceID] = a
		cl, okC := p.ClientBySrc[a.ClientSourceID]
		if !okC {
			add(errIssue(f, a.Line, "client_source_id %q not found in %s", a.ClientSourceID, FileClients))
		}
		pr, okP := p.ProductByCode[strings.ToUpper(a.ProductCode)]
		if !okP {
			add(errIssue(f, a.Line, "product_code %q not found in %s", a.ProductCode, FileProducts))
		} else {
			p.AcctCurrency[a.SourceID] = pr.Currency
		}
		if !oneOf(a.StatusOrDefault(), AccountStatuses) {
			add(errIssue(f, a.Line, "status %q must be one of %v", a.Status, AccountStatuses))
		}
		if okC && okP {
			key := a.ClientSourceID + "\x00" + strings.ToUpper(pr.Code)
			if prev, dup := pairSeen[key]; dup {
				add(errIssue(f, a.Line, "client %q already has an account for product %q (line %d): LedgerTrack allows one account per client and product", a.ClientSourceID, pr.Code, prev.Line))
			} else {
				pairSeen[key] = a
			}
			rules := pr.parsedRules()
			if rules.RequiresKYCLevel != nil && cl.KYCLevel < *rules.RequiresKYCLevel {
				add(errIssue(f, a.Line, "client kyc_level %d is below product %q requires_kyc_level %d", cl.KYCLevel, pr.Code, *rules.RequiresKYCLevel))
			}
			if len(rules.EligibleClientTypes) > 0 && !oneOf("corporate", rules.EligibleClientTypes) {
				add(errIssue(f, a.Line, "product %q is not eligible for corporate clients", pr.Code))
			}
		}
		if prev, dup := legacyNumSeen[a.LegacyAccountNumber]; dup {
			add(warnIssue(f, a.Line, "legacy_account_number %q also used by account %q (line %d)", a.LegacyAccountNumber, prev.SourceID, prev.Line))
		} else {
			legacyNumSeen[a.LegacyAccountNumber] = a
		}
		if a.OpenedAt != "" {
			if _, err := time.Parse(time.RFC3339, a.OpenedAt); err != nil {
				add(errIssue(f, a.Line, "opened_at is not RFC3339: %v", err))
			}
		}
	}

	// Balances.
	balSrc := map[string]*BalanceRec{}
	amt := AmountOptions{TrimZeros: opt.TrimZeros}
	for _, b := range ds.Balances {
		f := FileBalances
		if prev, dup := balSrc[b.SourceID]; dup {
			add(errIssue(f, b.Line, "duplicate source_id %q (first seen on line %d)", b.SourceID, prev.Line))
			continue
		}
		balSrc[b.SourceID] = b
		acct, ok := p.AccountBySrc[b.AccountSourceID]
		if !ok {
			add(errIssue(f, b.Line, "account_source_id %q not found in %s", b.AccountSourceID, FileAccounts))
			continue
		}
		if prev, dup := p.BalanceByAcct[b.AccountSourceID]; dup {
			add(errIssue(f, b.Line, "account %q already has a balance row (line %d): exactly one opening balance per account", b.AccountSourceID, prev.Line))
			continue
		}
		if ccy := p.AcctCurrency[b.AccountSourceID]; ccy != "" && ccy != b.Currency {
			add(errIssue(f, b.Line, "currency %s differs from the currency %s of the account's product", b.Currency, ccy))
			continue
		}
		prec, okp := p.Precision[b.Currency]
		if !okp {
			add(errIssue(f, b.Line, "no precision known for currency %s", b.Currency))
			continue
		}
		var errs bool
		var perr error
		if b.total, perr = ParseAmount(b.Amount, prec, amt); perr != nil {
			add(errIssue(f, b.Line, "amount: %v", perr))
			errs = true
		}
		if b.available, perr = ParseAmount(b.Available, prec, amt); perr != nil {
			add(errIssue(f, b.Line, "available: %v", perr))
			errs = true
		}
		if b.lien, perr = ParseAmount(b.Lien, prec, amt); perr != nil {
			add(errIssue(f, b.Line, "lien: %v", perr))
			errs = true
		}
		if errs {
			continue
		}
		sum := new(big.Int).Add(b.available, b.lien)
		if sum.Cmp(b.total) != 0 {
			add(errIssue(f, b.Line, "amount %s != available %s + lien %s", b.Amount, b.Available, b.Lien))
			continue
		}
		if b.AsOf != "" {
			if _, err := time.Parse(time.RFC3339, b.AsOf); err != nil {
				add(errIssue(f, b.Line, "as_of is not RFC3339: %v", err))
			}
		}
		if acct.StatusOrDefault() == "closed" && b.total.Sign() != 0 {
			add(errIssue(f, b.Line, "account %q is closed but has a non-zero balance %s", acct.SourceID, b.Amount))
			continue
		}
		p.BalanceByAcct[b.AccountSourceID] = b
		t := p.Totals[b.Currency]
		if t == nil {
			t = &CurrencyTotal{Currency: b.Currency, Precision: prec, Total: new(big.Int), Available: new(big.Int), Lien: new(big.Int)}
			p.Totals[b.Currency] = t
		}
		t.Accounts++
		t.Total.Add(t.Total, b.total)
		t.Available.Add(t.Available, b.available)
		t.Lien.Add(t.Lien, b.lien)
	}
	for _, a := range ds.Accounts {
		if _, ok := p.BalanceByAcct[a.SourceID]; !ok && len(ds.Balances) > 0 {
			add(warnIssue(FileAccounts, a.Line, "account %q has no balance row, it will be opened with a zero balance", a.SourceID))
		}
	}

	// Liens.
	lienSrc := map[string]*LienRec{}
	refSeen := map[string]*LienRec{}
	lienSum := map[string]*big.Int{}
	for _, l := range ds.Liens {
		f := FileLiens
		if prev, dup := lienSrc[l.SourceID]; dup {
			add(errIssue(f, l.Line, "duplicate source_id %q (first seen on line %d)", l.SourceID, prev.Line))
			continue
		}
		lienSrc[l.SourceID] = l
		if prev, dup := refSeen[l.Reference]; dup {
			add(errIssue(f, l.Line, "duplicate reference %q (line %d): references are unique per ledger", l.Reference, prev.Line))
			continue
		}
		refSeen[l.Reference] = l
		if strings.HasPrefix(l.Reference, "import:") {
			add(errIssue(f, l.Line, "reference %q collides with the reserved import: namespace", l.Reference))
			continue
		}
		if _, ok := p.AccountBySrc[l.AccountSourceID]; !ok {
			add(errIssue(f, l.Line, "account_source_id %q not found in %s", l.AccountSourceID, FileAccounts))
			continue
		}
		if _, err := time.Parse(time.RFC3339, l.CreatedAt); err != nil {
			add(errIssue(f, l.Line, "created_at is not RFC3339: %v", err))
		}
		ccy := p.AcctCurrency[l.AccountSourceID]
		prec, okp := p.Precision[ccy]
		if !okp {
			add(errIssue(f, l.Line, "no precision known for the account currency %q", ccy))
			continue
		}
		v, err := ParseAmount(l.Amount, prec, amt)
		if err != nil {
			add(errIssue(f, l.Line, "amount: %v", err))
			continue
		}
		if v.Sign() <= 0 {
			add(errIssue(f, l.Line, "lien amount must be positive"))
			continue
		}
		l.amount = v
		p.LiensByAcct[l.AccountSourceID] = append(p.LiensByAcct[l.AccountSourceID], l)
		if lienSum[l.AccountSourceID] == nil {
			lienSum[l.AccountSourceID] = new(big.Int)
		}
		lienSum[l.AccountSourceID].Add(lienSum[l.AccountSourceID], v)
	}
	for acct, sum := range lienSum {
		b, ok := p.BalanceByAcct[acct]
		if !ok {
			add(errIssue(FileLiens, 0, "account %q has liens but no valid balance row", acct))
			continue
		}
		if sum.Cmp(b.lien) != 0 {
			prec := p.Precision[b.Currency]
			add(errIssue(FileBalances, b.Line, "lien %s differs from the sum of liens.jsonl for account %q (%s)", b.Lien, acct, FormatAmount(sum, prec)))
		}
	}
	for acct, b := range p.BalanceByAcct {
		if b.lien.Sign() > 0 && lienSum[acct] == nil {
			add(errIssue(FileBalances, b.Line, "account %q has lien %s but no rows in %s", acct, b.Lien, FileLiens))
		}
	}

	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].File != issues[j].File {
			return issues[i].File < issues[j].File
		}
		return issues[i].Line < issues[j].Line
	})
	return p, issues
}

// kycIdentifier is the value sent as national_id_number in the KYC payload.
func (c *ClientRec) kycIdentifier() string {
	switch {
	case strings.TrimSpace(c.KYCIdentifier) != "":
		return strings.TrimSpace(c.KYCIdentifier)
	case strings.TrimSpace(c.RegistrationNumber) != "":
		return strings.TrimSpace(c.RegistrationNumber)
	default:
		return strings.TrimSpace(c.TaxID)
	}
}

// TotalsSummary returns a stable, printable list of per-currency totals.
func (p *Prepared) TotalsSummary() []string {
	codes := make([]string, 0, len(p.Totals))
	for c := range p.Totals {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	out := make([]string, 0, len(codes))
	for _, c := range codes {
		t := p.Totals[c]
		out = append(out, fmt.Sprintf("%s: accounts=%d total=%s available=%s lien=%s (precision %d)", c, t.Accounts,
			FormatAmount(t.Total, t.Precision), FormatAmount(t.Available, t.Precision), FormatAmount(t.Lien, t.Precision), t.Precision))
	}
	return out
}
