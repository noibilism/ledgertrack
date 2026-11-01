package v2

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/auth"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

// feeConfigServiceForHTTPTests records what the controllers pass in and returns canned results.
type feeConfigServiceForHTTPTests struct {
	err            error
	lastOverride   services.CreateFeeOverrideInput
	lastTax        services.CreateTaxRuleInput
	lastOvFilter   repositories.FeeOverrideFilter
	lastTaxFilter  repositories.TaxRuleFilter
	lastQuote      services.FeeQuoteRequest
	lastRevokedID  uuid.UUID
	quoteToReturn  *services.FeeQuote
	overrideReturn *models.FeeOverride
	taxReturn      *models.TaxRule
}

func (s *feeConfigServiceForHTTPTests) CreateOverride(_ context.Context, in services.CreateFeeOverrideInput) (*models.FeeOverride, error) {
	s.lastOverride = in
	return s.overrideReturn, s.err
}
func (s *feeConfigServiceForHTTPTests) GetOverride(context.Context, uuid.UUID) (*models.FeeOverride, error) {
	return s.overrideReturn, s.err
}
func (s *feeConfigServiceForHTTPTests) ListOverrides(_ context.Context, f repositories.FeeOverrideFilter) ([]models.FeeOverride, error) {
	s.lastOvFilter = f
	return []models.FeeOverride{}, s.err
}
func (s *feeConfigServiceForHTTPTests) RevokeOverride(_ context.Context, id uuid.UUID) (*models.FeeOverride, error) {
	s.lastRevokedID = id
	return s.overrideReturn, s.err
}
func (s *feeConfigServiceForHTTPTests) CreateTaxRule(_ context.Context, in services.CreateTaxRuleInput) (*models.TaxRule, error) {
	s.lastTax = in
	return s.taxReturn, s.err
}
func (s *feeConfigServiceForHTTPTests) GetTaxRule(context.Context, uuid.UUID) (*models.TaxRule, error) {
	return s.taxReturn, s.err
}
func (s *feeConfigServiceForHTTPTests) ListTaxRules(_ context.Context, f repositories.TaxRuleFilter) ([]models.TaxRule, error) {
	s.lastTaxFilter = f
	return []models.TaxRule{}, s.err
}
func (s *feeConfigServiceForHTTPTests) RevokeTaxRule(_ context.Context, id uuid.UUID) (*models.TaxRule, error) {
	s.lastRevokedID = id
	return s.taxReturn, s.err
}
func (s *feeConfigServiceForHTTPTests) Quote(_ context.Context, req services.FeeQuoteRequest) (*services.FeeQuote, error) {
	s.lastQuote = req
	return s.quoteToReturn, s.err
}

func newFeeConfigRouterForTests(t *testing.T, svc services.FeeConfigService) http.Handler {
	t.Helper()
	systemController, mockLedger := newTestingSystemController(t, false)
	mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	return NewRouter(systemController, auth.NewNoAuth(), "develop", WithFeeConfigService(svc))
}

func TestFeeConfigRoutesAreLedgertrackOnly(t *testing.T) {
	t.Parallel()
	router := newFeeConfigRouterForTests(t, &feeConfigServiceForHTTPTests{})

	for _, target := range []string{"/other/fee-overrides", "/other/tax-rules"} {
		rec := doGL(t, router, http.MethodGet, target, nil)
		require.Equal(t, http.StatusNotFound, rec.Code, target)
	}
	rec := doGL(t, router, http.MethodPost, "/other/fees/quote", `{}`)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFeeOverrideEndpoints(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	clientID := uuid.New()
	svc := &feeConfigServiceForHTTPTests{overrideReturn: &models.FeeOverride{ID: id, Scope: "client", Status: "active"}}
	router := newFeeConfigRouterForTests(t, svc)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/fee-overrides",
		`{"scope":"client","client_id":"`+clientID.String()+`","currency":"NGN","event":"debit","rules":[{"event":"debit","type":"flat","value":"10"}]}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "client", svc.lastOverride.Scope)
	require.Equal(t, clientID, *svc.lastOverride.ClientID)
	require.Len(t, svc.lastOverride.Rules, 1)

	// unknown fields are rejected, not ignored
	rec = doGL(t, router, http.MethodPost, "/ledgertrack/fee-overrides", `{"scope":"client","surprise":1}`)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/fee-overrides?client_id="+clientID.String()+"&event=DEBIT&status=active", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, clientID, *svc.lastOvFilter.ClientID)
	require.Equal(t, "debit", *svc.lastOvFilter.Event)
	require.Equal(t, "active", *svc.lastOvFilter.Status)

	for _, bad := range []string{"client_id=nope", "account_id=nope", "status=gone"} {
		rec = doGL(t, router, http.MethodGet, "/ledgertrack/fee-overrides?"+bad, nil)
		assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)
	}

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/fee-overrides/"+id.String(), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/fee-overrides/not-a-uuid", nil)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/fee-overrides/"+id.String()+"/revoke", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, id, svc.lastRevokedID)
}

func TestTaxRuleEndpoints(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	svc := &feeConfigServiceForHTTPTests{taxReturn: &models.TaxRule{ID: id, Country: "NG", Status: "active"}}
	router := newFeeConfigRouterForTests(t, svc)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/tax-rules",
		`{"country":"NG","code":"VAT","name":"VAT","event":"debit","base":"fee","rate":"7.5","payable_accounts":{"NGN":"202100.NGN"}}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "fee", svc.lastTax.Base)
	require.Equal(t, "202100.NGN", svc.lastTax.PayableAccounts["NGN"])

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/tax-rules?country=ng&event=debit&status=revoked", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "NG", *svc.lastTaxFilter.Country)
	require.Equal(t, "revoked", *svc.lastTaxFilter.Status)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/tax-rules/"+id.String()+"/revoke", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, id, svc.lastRevokedID)
}

func TestFeeConfigErrorMapping(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err    error
		status int
		code   string
	}{
		"validation":      {services.ErrFeeConfigValidation, http.StatusBadRequest, common.ErrValidation},
		"country unknown": {services.ErrTaxCountryUnknown, http.StatusBadRequest, common.ErrValidation},
		"account missing": {services.ErrTaxAccountMissing, http.StatusBadRequest, common.ErrValidation},
		"overlap":         {services.ErrFeeConfigConflict, http.StatusConflict, common.ErrConflict},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			router := newFeeConfigRouterForTests(t, &feeConfigServiceForHTTPTests{err: tc.err})
			rec := doGL(t, router, http.MethodPost, "/ledgertrack/fees/quote",
				`{"account_id":"`+uuid.NewString()+`","event":"debit","amount_minor":100}`)
			assertErrorCode(t, rec, tc.status, tc.code)
		})
	}

	router := newFeeConfigRouterForTests(t, &feeConfigServiceForHTTPTests{err: services.ErrFeeConfigNotFound})
	rec := doGL(t, router, http.MethodGet, "/ledgertrack/tax-rules/"+uuid.NewString(), nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestQuoteFeesEndpoint(t *testing.T) {
	t.Parallel()
	accountID := uuid.New()
	svc := &feeConfigServiceForHTTPTests{quoteToReturn: &services.FeeQuote{
		AccountID: accountID, Event: "debit", Currency: "NGN",
		Amount: decimal.RequireFromString("100000"), Fee: decimal.RequireFromString("750"),
		Tax: decimal.RequireFromString("56.25"), Total: decimal.RequireFromString("100806.25"),
		FeeSource: "product",
	}}
	router := newFeeConfigRouterForTests(t, svc)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/fees/quote",
		`{"account_id":"`+accountID.String()+`","event":" Debit ","amount_minor":10000000,"at":"2026-11-01T10:00:00Z"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, accountID, svc.lastQuote.AccountID)
	require.Equal(t, "debit", svc.lastQuote.Event)
	require.EqualValues(t, 10000000, svc.lastQuote.AmountMinor)
	require.Equal(t, 2026, svc.lastQuote.At.Year())

	for _, body := range []string{
		`{"account_id":"nope","event":"debit","amount_minor":1}`,
		`{"account_id":"` + accountID.String() + `","event":"debit","amount_minor":1,"extra":true}`,
	} {
		rec = doGL(t, router, http.MethodPost, "/ledgertrack/fees/quote", body)
		assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)
	}
}
