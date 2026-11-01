package services

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

type quoteStub struct {
	FeeConfigService
	quote *FeeQuote
	last  FeeQuoteRequest
}

func (q *quoteStub) Quote(_ context.Context, r FeeQuoteRequest) (*FeeQuote, error) {
	q.last = r
	return q.quote, nil
}

type glByCodeStub struct {
	repositories.GLAccountRepository
	accounts map[string]*models.GLAccount
}

func (g *glByCodeStub) GetByCode(_ context.Context, code string) (*models.GLAccount, error) {
	if a, ok := g.accounts[code]; ok {
		return a, nil
	}
	return nil, postgres.ErrNotFound
}

func glAcct(code, typ, currency, ledger string) *models.GLAccount {
	return &models.GLAccount{Code: code, Type: typ, Currency: currency, Ledger: ledger, Address: "gl:" + currency + ":" + code,
		Status: models.GLAccountStatusActive, Metadata: map[string]any{}}
}

func sampleQuote() *FeeQuote {
	return &FeeQuote{
		Event: "transfer", Currency: "NGN", FeeSource: "product",
		FeeMinor: 5000, TaxMinor: 375,
		FeeLines: []FeeQuoteLine{{Type: "flat", Amount: decimal.RequireFromString("50"), IncomeAccountCode: "401101.NGN"}},
		TaxLines: []TaxQuoteLine{{Code: "VAT", Amount: decimal.RequireFromString("3.75"), PayableAccountCode: "202210.NGN"}},
	}
}

func newChargeFixture(q *FeeQuote) (*DefaultChargeService, *quoteStub, *glByCodeStub) {
	fees := &quoteStub{quote: q}
	gl := &glByCodeStub{accounts: map[string]*models.GLAccount{
		"401101.NGN": glAcct("401101.NGN", models.GLAccountTypeIncome, "NGN", "ledgertrack"),
		"202210.NGN": glAcct("202210.NGN", models.GLAccountTypeLiability, "NGN", "ledgertrack"),
	}}
	return NewChargeService(fees, gl), fees, gl
}

func chargeReq() ChargeRequest {
	return ChargeRequest{AccountID: uuid.New(), Event: "transfer", AmountMinor: 100_000, Ledger: "ledgertrack"}
}

func TestChargePlanResolvesFeeAndTaxToAddresses(t *testing.T) {
	svc, fees, _ := newChargeFixture(sampleQuote())
	req := chargeReq()
	plan, err := svc.Plan(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, req.AccountID, fees.last.AccountID)
	require.Equal(t, int64(100_000), fees.last.AmountMinor)
	require.Equal(t, int64(5375), plan.GrossMinor)
	require.Equal(t, int64(5000), plan.FeeMinor)
	require.Equal(t, int64(375), plan.TaxMinor)
	require.Equal(t, []ChargeLine{
		{Kind: ChargeKindFee, Code: "401101.NGN", Address: "gl:NGN:401101.NGN", AmountMinor: 5000, Label: "flat"},
		{Kind: ChargeKindTax, Code: "202210.NGN", Address: "gl:NGN:202210.NGN", AmountMinor: 375, Label: "VAT"},
	}, plan.Lines)
}

func TestChargePlanIsNilWithoutFeeOrTax(t *testing.T) {
	svc, _, _ := newChargeFixture(&FeeQuote{Event: "transfer", Currency: "NGN"})
	plan, err := svc.Plan(context.Background(), chargeReq())
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestChargePlanSkipsZeroLines(t *testing.T) {
	q := sampleQuote()
	q.FeeLines = append(q.FeeLines, FeeQuoteLine{Type: "percentage", Amount: decimal.Zero}) // no income account needed for 0
	svc, _, _ := newChargeFixture(q)
	plan, err := svc.Plan(context.Background(), chargeReq())
	require.NoError(t, err)
	require.Len(t, plan.Lines, 2)
}

func TestChargePlanRefusesUnpostableConfiguration(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*FeeQuote, *glByCodeStub)
		want   string
	}{
		"fee rule has no income account":   {func(q *FeeQuote, _ *glByCodeStub) { q.FeeLines[0].IncomeAccountCode = "" }, "no income account"},
		"tax rule has no payable":          {func(q *FeeQuote, _ *glByCodeStub) { q.TaxLines[0].PayableAccountCode = "" }, "no payable account"},
		"income account missing":           {func(_ *FeeQuote, g *glByCodeStub) { delete(g.accounts, "401101.NGN") }, "does not exist"},
		"income account in other currency": {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["401101.NGN"].Currency = "GHS" }, "is in GHS"},
		"income account is a liability":    {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["401101.NGN"].Type = models.GLAccountTypeLiability }, "liability account"},
		"tax account is income":            {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["202210.NGN"].Type = models.GLAccountTypeIncome }, "income account"},
		"account closed":                   {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["401101.NGN"].Status = models.GLAccountStatusClosed }, "not active"},
		"header account":                   {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["401101.NGN"].Metadata["posting"] = false }, "header"},
		"header flag as text":              {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["202210.NGN"].Metadata["posting"] = "false" }, "header"},
		"account in another ledger":        {func(_ *FeeQuote, g *glByCodeStub) { g.accounts["401101.NGN"].Ledger = "other" }, "in ledger other"},
	} {
		t.Run(name, func(t *testing.T) {
			q := sampleQuote()
			svc, _, gl := newChargeFixture(q)
			tc.mutate(q, gl)
			plan, err := svc.Plan(context.Background(), chargeReq())
			require.ErrorIs(t, err, ErrChargeConfig)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, plan)
		})
	}
}

func TestChargePlanPostingFlagTrueIsAccepted(t *testing.T) {
	svc, _, gl := newChargeFixture(sampleQuote())
	gl.accounts["401101.NGN"].Metadata["posting"] = true
	gl.accounts["202210.NGN"].Metadata["posting"] = "true"
	plan, err := svc.Plan(context.Background(), chargeReq())
	require.NoError(t, err)
	require.NotNil(t, plan)
}
