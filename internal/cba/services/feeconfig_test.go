package services

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

// feeConfigRepositoryStub is an in-memory repositories.FeeConfigRepository.
type feeConfigRepositoryStub struct {
	overrides map[uuid.UUID]*models.FeeOverride
	taxRules  map[uuid.UUID]*models.TaxRule
}

func newFeeConfigRepositoryStub() *feeConfigRepositoryStub {
	return &feeConfigRepositoryStub{overrides: map[uuid.UUID]*models.FeeOverride{}, taxRules: map[uuid.UUID]*models.TaxRule{}}
}

func inForce(status string, from time.Time, to *time.Time, at time.Time) bool {
	return status == models.FeeConfigStatusActive && !from.After(at) && (to == nil || to.After(at))
}

func (s *feeConfigRepositoryStub) CreateOverride(_ context.Context, o *models.FeeOverride) error {
	if o.ID == uuid.Nil {
		o.ID = uuid.New()
	}
	c := *o
	s.overrides[o.ID] = &c
	return nil
}
func (s *feeConfigRepositoryStub) GetOverride(_ context.Context, id uuid.UUID) (*models.FeeOverride, error) {
	if o, ok := s.overrides[id]; ok {
		c := *o
		return &c, nil
	}
	return nil, postgres.ErrNotFound
}
func (s *feeConfigRepositoryStub) ListOverrides(_ context.Context, f repositories.FeeOverrideFilter) ([]models.FeeOverride, error) {
	out := []models.FeeOverride{}
	for _, o := range s.overrides {
		if f.ClientID != nil && (o.ClientID == nil || *o.ClientID != *f.ClientID) {
			continue
		}
		if f.AccountID != nil && (o.AccountID == nil || *o.AccountID != *f.AccountID) {
			continue
		}
		if f.Event != nil && o.Event != *f.Event {
			continue
		}
		if f.Status != nil && o.Status != *f.Status {
			continue
		}
		out = append(out, *o)
	}
	return out, nil
}
func (s *feeConfigRepositoryStub) UpdateOverride(_ context.Context, o *models.FeeOverride) error {
	c := *o
	s.overrides[o.ID] = &c
	return nil
}
func (s *feeConfigRepositoryStub) ActiveOverridesFor(_ context.Context, accountID, clientID uuid.UUID, event string, at time.Time) ([]models.FeeOverride, error) {
	out := []models.FeeOverride{}
	for _, o := range s.overrides {
		if o.Event != event || !inForce(o.Status, o.EffectiveFrom, o.EffectiveTo, at) {
			continue
		}
		if (o.AccountID != nil && *o.AccountID == accountID) || (o.ClientID != nil && *o.ClientID == clientID) {
			out = append(out, *o)
		}
	}
	return out, nil
}
func (s *feeConfigRepositoryStub) CreateTaxRule(_ context.Context, t *models.TaxRule) error {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	c := *t
	s.taxRules[t.ID] = &c
	return nil
}
func (s *feeConfigRepositoryStub) GetTaxRule(_ context.Context, id uuid.UUID) (*models.TaxRule, error) {
	if t, ok := s.taxRules[id]; ok {
		c := *t
		return &c, nil
	}
	return nil, postgres.ErrNotFound
}
func (s *feeConfigRepositoryStub) ListTaxRules(_ context.Context, f repositories.TaxRuleFilter) ([]models.TaxRule, error) {
	out := []models.TaxRule{}
	for _, t := range s.taxRules {
		if f.Country != nil && t.Country != *f.Country {
			continue
		}
		if f.Event != nil && t.Event != *f.Event {
			continue
		}
		if f.Status != nil && t.Status != *f.Status {
			continue
		}
		out = append(out, *t)
	}
	return out, nil
}
func (s *feeConfigRepositoryStub) UpdateTaxRule(_ context.Context, t *models.TaxRule) error {
	c := *t
	s.taxRules[t.ID] = &c
	return nil
}
func (s *feeConfigRepositoryStub) ActiveTaxRules(_ context.Context, country, event string, at time.Time) ([]models.TaxRule, error) {
	out := []models.TaxRule{}
	for _, t := range s.taxRules {
		if t.Country == country && t.Event == event && inForce(t.Status, t.EffectiveFrom, t.EffectiveTo, at) {
			out = append(out, *t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out, nil
}
func (s *feeConfigRepositoryStub) HasActiveTaxRules(_ context.Context, event string, at time.Time) (bool, error) {
	for _, t := range s.taxRules {
		if t.Event == event && inForce(t.Status, t.EffectiveFrom, t.EffectiveTo, at) {
			return true, nil
		}
	}
	return false, nil
}

type feeConfigFixture struct {
	svc        *DefaultFeeConfigService
	ctx        context.Context
	now        time.Time
	client     *models.Client
	product    *models.Product
	account    *models.Account
	glRepo     *glRepositoryStub
	clientRepo *clientRepositoryStub
	accounts   *accountRepositoryStub
	products   *productRepositoryStub
}

// newFeeConfigFixture builds a Nigerian client with one NGN account whose product charges a flat 1,400.00 on debits.
func newFeeConfigFixture(t *testing.T, country string) *feeConfigFixture {
	t.Helper()
	f := &feeConfigFixture{
		ctx: context.Background(), now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC),
		glRepo: newGLRepositoryStub(), clientRepo: newClientRepositoryStub(),
		accounts: newAccountRepositoryStub(), products: newProductRepositoryStub(),
	}
	f.svc = NewFeeConfigService(newFeeConfigRepositoryStub(), f.accounts, f.products, f.clientRepo, f.glRepo)
	f.svc.now = func() time.Time { return f.now }

	f.client = &models.Client{ID: uuid.New(), ClientNumber: "CL-1", Type: "corporate", Status: "active"}
	if country != "" {
		f.client.Contact = models.ClientContact{Address: &models.Address{Country: country}}
	}
	require.NoError(t, f.clientRepo.Create(f.ctx, f.client))

	f.product = &models.Product{
		ID: uuid.New(), Code: "NGN", Name: "NGN wallet", Category: "wallet", Currency: "NGN", Status: models.ProductStatusActive,
		FeeSchedule: &models.FeeSchedule{TransactionFees: []models.TransactionFee{{Event: "debit", Type: "flat", Value: "1400"}}},
	}
	require.NoError(t, f.products.Create(f.ctx, f.product))
	f.account = &models.Account{ID: uuid.New(), AccountNumber: "1000000001", ClientID: f.client.ID, ProductID: f.product.ID, Currency: "NGN", Status: models.AccountStatusActive, WalletID: "client-CL-1-NGN"}
	require.NoError(t, f.accounts.Create(f.ctx, f.account))

	require.NoError(t, f.glRepo.Create(f.ctx, &models.GLAccount{Code: "202200.NGN", Name: "Tax Payable", Type: models.GLAccountTypeLiability, NormalSide: models.GLSideCredit, Currency: "NGN", Ledger: "ledgertrack", Address: "gl:ngn:liabilities:payables:tax_payable", Status: models.GLAccountStatusActive}))
	require.NoError(t, f.glRepo.Create(f.ctx, &models.GLAccount{Code: "401100.NGN", Name: "Fees", Type: models.GLAccountTypeIncome, NormalSide: models.GLSideCredit, Currency: "NGN", Ledger: "ledgertrack", Address: "gl:ngn:income:fee_income:transaction_fees", Status: models.GLAccountStatusActive}))
	return f
}

func (f *feeConfigFixture) quote(t *testing.T, event string, minor int64) *FeeQuote {
	t.Helper()
	q, err := f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: f.account.ID, Event: event, AmountMinor: minor})
	require.NoError(t, err)
	return q
}

func (f *feeConfigFixture) vat(t *testing.T) *models.TaxRule {
	t.Helper()
	rule, err := f.svc.CreateTaxRule(f.ctx, CreateTaxRuleInput{
		Country: "ng", Code: "vat", Name: "VAT", Event: "debit", Base: "fee", Rate: "7.5",
		PayableAccounts: map[string]string{"NGN": "202200.NGN"},
	})
	require.NoError(t, err)
	return rule
}

func TestQuoteUsesProductDefault(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	q := f.quote(t, "debit", 10_000_000) // 100,000.00 NGN
	require.Equal(t, "product", q.FeeSource)
	require.Equal(t, "1400", q.Fee.String())
	require.True(t, q.Tax.IsZero())
	require.Equal(t, "101400", q.Total.String())
	require.Equal(t, int64(10_140_000), q.TotalMinor)

	none := f.quote(t, "credit", 10_000_000)
	require.Equal(t, "none", none.FeeSource)
	require.Empty(t, none.FeeLines)
	require.Equal(t, int64(10_000_000), none.TotalMinor)
}

func TestQuoteTaxOnFeeWorkedExample(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	rule := f.vat(t)
	q := f.quote(t, "debit", 10_000_000)
	require.Equal(t, "1400", q.Fee.String())
	require.Equal(t, "105", q.Tax.String())
	require.Equal(t, "1505", q.Gross.String())
	require.Equal(t, "101505", q.Total.String())
	require.Equal(t, int64(10_150_500), q.TotalMinor)
	require.Equal(t, int64(10_500), q.TaxMinor)
	require.Len(t, q.TaxLines, 1)
	require.Equal(t, rule.ID, q.TaxLines[0].RuleID)
	require.Equal(t, "202200.NGN", q.TaxLines[0].PayableAccountCode)
}

func TestQuoteTaxOnAmountWorkedExample(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	f.product.FeeSchedule.TransactionFees = []models.TransactionFee{{Event: "debit", Type: "percentage", Value: "1"}}
	_, err := f.svc.CreateTaxRule(f.ctx, CreateTaxRuleInput{
		Country: "NG", Code: "LEVY", Name: "Transfer levy", Event: "debit", Base: "amount", Rate: "0.5",
		PayableAccounts: map[string]string{"NGN": "202200.NGN"},
	})
	require.NoError(t, err)
	q := f.quote(t, "debit", 10_000_000)
	require.Equal(t, "1000", q.Fee.String())
	require.Equal(t, "500", q.Tax.String())
	require.Equal(t, "101500", q.Total.String())
}

func TestTaxIsPerCountry(t *testing.T) {
	f := newFeeConfigFixture(t, "GH")
	f.vat(t) // a Nigerian rule must not apply to a Ghanaian client
	q := f.quote(t, "debit", 10_000_000)
	require.True(t, q.Tax.IsZero())
	require.Empty(t, q.TaxLines)
}

func TestClientWithoutCountryIsRejectedOnlyWhenTaxRulesExist(t *testing.T) {
	f := newFeeConfigFixture(t, "")
	q := f.quote(t, "debit", 10_000_000)
	require.Equal(t, "1400", q.Fee.String(), "no tax rules configured: charge the fee")

	f.vat(t)
	_, err := f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: f.account.ID, Event: "debit", AmountMinor: 10_000_000})
	require.ErrorIs(t, err, ErrTaxCountryUnknown)
}

func TestCorporateIncorporationCountryWinsOverContactAddress(t *testing.T) {
	c := &models.Client{
		Contact:       models.ClientContact{Address: &models.Address{Country: "GH"}},
		CorporateData: &models.CorporateData{IncorporationCountry: " ng "},
	}
	require.Equal(t, "NG", ClientCountry(c))
	require.Equal(t, "GH", ClientCountry(&models.Client{Contact: c.Contact}))
	require.Equal(t, "", ClientCountry(&models.Client{Contact: models.ClientContact{Address: &models.Address{Country: "Nigeria"}}}))
}

func TestOverrideResolutionOrder(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	flat := func(v string) []models.TransactionFee { return []models.TransactionFee{{Type: "flat", Value: v}} }

	clientAll, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: flat("1000")})
	require.NoError(t, err)
	q := f.quote(t, "debit", 10_000_000)
	require.Equal(t, "client", q.FeeSource)
	require.Equal(t, "1000", q.Fee.String())
	require.Equal(t, &clientAll.ID, q.FeeOverrideID)

	clientProduct, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, ProductID: &f.product.ID, Currency: "NGN", Event: "debit", Rules: flat("800")})
	require.NoError(t, err)
	q = f.quote(t, "debit", 10_000_000)
	require.Equal(t, "800", q.Fee.String(), "client+product beats client-wide")
	require.Equal(t, &clientProduct.ID, q.FeeOverrideID)

	acct, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "account", AccountID: &f.account.ID, Event: "debit", Rules: flat("500")})
	require.NoError(t, err)
	q = f.quote(t, "debit", 10_000_000)
	require.Equal(t, "account", q.FeeSource)
	require.Equal(t, "500", q.Fee.String())
	require.Equal(t, &acct.ID, q.FeeOverrideID)
	require.Equal(t, "NGN", acct.Currency, "an account override takes the account's currency")

	// An override only replaces the event it names.
	require.Equal(t, "none", f.quote(t, "credit", 10_000_000).FeeSource)

	_, err = f.svc.RevokeOverride(f.ctx, acct.ID)
	require.NoError(t, err)
	require.Equal(t, "800", f.quote(t, "debit", 10_000_000).Fee.String(), "revoked override stops applying")
}

func TestOverrideReplacesAllProductRulesForTheEvent(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	f.product.FeeSchedule.TransactionFees = []models.TransactionFee{
		{Event: "debit", Type: "flat", Value: "50"},
		{Event: "debit", Type: "percentage", Value: "1"},
	}
	q := f.quote(t, "debit", 10_000_000)
	require.Equal(t, "1050", q.Fee.String())
	require.Len(t, q.FeeLines, 2)

	_, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: []models.TransactionFee{{Type: "percentage", Value: "0.5"}}})
	require.NoError(t, err)
	q = f.quote(t, "debit", 10_000_000)
	require.Equal(t, "500", q.Fee.String(), "the flat 50 does not stack on the override")
	require.Len(t, q.FeeLines, 1)
}

func TestOverrideForAnotherCurrencyDoesNotApply(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	_, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "USD", Event: "debit", Rules: []models.TransactionFee{{Type: "flat", Value: "1"}}})
	require.NoError(t, err)
	require.Equal(t, "product", f.quote(t, "debit", 10_000_000).FeeSource)
}

func TestOverrideEffectiveDates(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	from := f.now.Add(24 * time.Hour)
	_, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: []models.TransactionFee{{Type: "flat", Value: "10"}}, EffectiveFrom: &from})
	require.NoError(t, err)
	require.Equal(t, "product", f.quote(t, "debit", 10_000_000).FeeSource, "not in force yet")

	q, err := f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: f.account.ID, Event: "debit", AmountMinor: 10_000_000, At: from.Add(time.Hour)})
	require.NoError(t, err)
	require.Equal(t, "client", q.FeeSource)
	require.Equal(t, "10", q.Fee.String())
}

func TestOverlappingOverridesAreRejected(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	rules := []models.TransactionFee{{Type: "flat", Value: "10"}}
	to := f.now.Add(48 * time.Hour)
	_, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: rules, EffectiveTo: &to})
	require.NoError(t, err)

	_, err = f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: rules})
	require.ErrorIs(t, err, ErrFeeConfigConflict)

	// After the first one ends, the next may start.
	later := to
	_, err = f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: rules, EffectiveFrom: &later})
	require.NoError(t, err)

	// A different event, product or currency is a different target.
	_, err = f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "credit", Rules: rules})
	require.NoError(t, err)
	_, err = f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, ProductID: &f.product.ID, Currency: "NGN", Event: "debit", Rules: rules})
	require.NoError(t, err)
}

func TestCreateOverrideValidation(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	good := []models.TransactionFee{{Type: "flat", Value: "10"}}
	unknown := uuid.New()
	cases := map[string]CreateFeeOverrideInput{
		"no rules":                  {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit"},
		"bad event":                 {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "refund", Rules: good},
		"bad scope":                 {Scope: "global", Currency: "NGN", Event: "debit", Rules: good},
		"client scope no client":    {Scope: "client", Currency: "NGN", Event: "debit", Rules: good},
		"client scope no currency":  {Scope: "client", ClientID: &f.client.ID, Event: "debit", Rules: good},
		"unknown client":            {Scope: "client", ClientID: &unknown, Currency: "NGN", Event: "debit", Rules: good},
		"account scope with client": {Scope: "account", AccountID: &f.account.ID, ClientID: &f.client.ID, Event: "debit", Rules: good},
		"account currency mismatch": {Scope: "account", AccountID: &f.account.ID, Currency: "USD", Event: "debit", Rules: good},
		"unknown account":           {Scope: "account", AccountID: &unknown, Event: "debit", Rules: good},
		"product currency mismatch": {Scope: "client", ClientID: &f.client.ID, ProductID: &f.product.ID, Currency: "USD", Event: "debit", Rules: good},
		"invalid rule":              {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: []models.TransactionFee{{Type: "percentage", Value: "150"}}},
		"rule for another event":    {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: []models.TransactionFee{{Event: "credit", Type: "flat", Value: "1"}}},
	}
	for name, in := range cases {
		_, err := f.svc.CreateOverride(f.ctx, in)
		require.Error(t, err, name)
		require.True(t, errorsIsAny(err, ErrFeeConfigValidation, ErrFeeConfigNotFound), "%s: %v", name, err)
	}

	end := f.now.Add(-time.Hour)
	_, err := f.svc.CreateOverride(f.ctx, CreateFeeOverrideInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Event: "debit", Rules: good, EffectiveTo: &end})
	require.ErrorIs(t, err, ErrFeeConfigValidation, "effective_to before effective_from")
}

func errorsIsAny(err error, targets ...error) bool {
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func TestCreateTaxRuleValidation(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	ok := CreateTaxRuleInput{Country: "NG", Code: "VAT", Name: "VAT", Event: "debit", Base: "fee", Rate: "7.5", PayableAccounts: map[string]string{"NGN": "202200.NGN"}}
	mutate := func(fn func(*CreateTaxRuleInput)) CreateTaxRuleInput { in := ok; fn(&in); return in }
	cases := map[string]CreateTaxRuleInput{
		"country":         mutate(func(i *CreateTaxRuleInput) { i.Country = "Nigeria" }),
		"code":            mutate(func(i *CreateTaxRuleInput) { i.Code = "vat rate" }),
		"name":            mutate(func(i *CreateTaxRuleInput) { i.Name = " " }),
		"event":           mutate(func(i *CreateTaxRuleInput) { i.Event = "x" }),
		"base":            mutate(func(i *CreateTaxRuleInput) { i.Base = "gross" }),
		"rate":            mutate(func(i *CreateTaxRuleInput) { i.Rate = "-1" }),
		"no accounts":     mutate(func(i *CreateTaxRuleInput) { i.PayableAccounts = nil }),
		"missing account": mutate(func(i *CreateTaxRuleInput) { i.PayableAccounts = map[string]string{"NGN": "999999.NGN"} }),
		"wrong currency":  mutate(func(i *CreateTaxRuleInput) { i.PayableAccounts = map[string]string{"USD": "202200.NGN"} }),
		"not a liability": mutate(func(i *CreateTaxRuleInput) { i.PayableAccounts = map[string]string{"NGN": "401100.NGN"} }),
	}
	for name, in := range cases {
		_, err := f.svc.CreateTaxRule(f.ctx, in)
		require.ErrorIs(t, err, ErrFeeConfigValidation, name)
	}
	_, err := f.svc.CreateTaxRule(f.ctx, ok)
	require.NoError(t, err)
	_, err = f.svc.CreateTaxRule(f.ctx, ok)
	require.ErrorIs(t, err, ErrFeeConfigConflict, "same country, code and event overlapping")

	other := ok
	other.Code = "LEVY"
	_, err = f.svc.CreateTaxRule(f.ctx, other)
	require.NoError(t, err, "a different tax code coexists")
}

func TestTaxRuleWithoutPayableAccountForCurrencyFailsTheQuote(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	require.NoError(t, f.glRepo.Create(f.ctx, &models.GLAccount{Code: "202200.USD", Name: "Tax Payable", Type: models.GLAccountTypeLiability, NormalSide: models.GLSideCredit, Currency: "USD", Ledger: "ledgertrack", Address: "gl:usd:liabilities:payables:tax_payable", Status: models.GLAccountStatusActive}))
	_, err := f.svc.CreateTaxRule(f.ctx, CreateTaxRuleInput{Country: "NG", Code: "VAT", Name: "VAT", Event: "debit", Base: "fee", Rate: "7.5", PayableAccounts: map[string]string{"USD": "202200.USD"}})
	require.NoError(t, err)
	_, err = f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: f.account.ID, Event: "debit", AmountMinor: 10_000_000})
	require.ErrorIs(t, err, ErrTaxAccountMissing)
}

func TestRevokedTaxRuleStopsApplying(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	rule := f.vat(t)
	require.Equal(t, "105", f.quote(t, "debit", 10_000_000).Tax.String())
	f.now = f.now.Add(time.Hour)
	revoked, err := f.svc.RevokeTaxRule(f.ctx, rule.ID)
	require.NoError(t, err)
	require.Equal(t, models.FeeConfigStatusRevoked, revoked.Status)
	require.True(t, f.quote(t, "debit", 10_000_000).Tax.IsZero())
}

func TestQuoteRejectsBadRequests(t *testing.T) {
	f := newFeeConfigFixture(t, "NG")
	_, err := f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: f.account.ID, Event: "debit", AmountMinor: 0})
	require.ErrorIs(t, err, ErrFeeConfigValidation)
	_, err = f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: f.account.ID, Event: "refund", AmountMinor: 1})
	require.ErrorIs(t, err, ErrFeeConfigValidation)
	_, err = f.svc.Quote(f.ctx, FeeQuoteRequest{AccountID: uuid.New(), Event: "debit", AmountMinor: 1})
	require.ErrorIs(t, err, ErrFeeConfigNotFound)
}
