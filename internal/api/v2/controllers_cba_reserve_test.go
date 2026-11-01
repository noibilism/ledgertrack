package v2

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/auth"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

type reserveServiceForHTTPTests struct {
	err           error
	lastCreate    services.CreateReserveRuleInput
	lastFilter    repositories.ReserveRuleFilter
	lastPreview   services.ReservePreviewRequest
	lastRevokedID uuid.UUID
	ruleReturn    *models.ReserveRule
	previewReturn *services.ReservePreview
}

func (s *reserveServiceForHTTPTests) CreateRule(_ context.Context, in services.CreateReserveRuleInput) (*models.ReserveRule, error) {
	s.lastCreate = in
	return s.ruleReturn, s.err
}
func (s *reserveServiceForHTTPTests) GetRule(context.Context, uuid.UUID) (*models.ReserveRule, error) {
	return s.ruleReturn, s.err
}
func (s *reserveServiceForHTTPTests) ListRules(_ context.Context, f repositories.ReserveRuleFilter) ([]models.ReserveRule, error) {
	s.lastFilter = f
	return []models.ReserveRule{}, s.err
}
func (s *reserveServiceForHTTPTests) RevokeRule(_ context.Context, id uuid.UUID) (*models.ReserveRule, error) {
	s.lastRevokedID = id
	return s.ruleReturn, s.err
}
func (s *reserveServiceForHTTPTests) Preview(_ context.Context, req services.ReservePreviewRequest) (*services.ReservePreview, error) {
	s.lastPreview = req
	return s.previewReturn, s.err
}

func newReserveRouterForTests(t *testing.T, svc services.ReserveRuleService) http.Handler {
	t.Helper()
	systemController, mockLedger := newTestingSystemController(t, false)
	mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	return NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveRuleService(svc))
}

func TestReserveRoutesAreLedgertrackOnly(t *testing.T) {
	t.Parallel()
	router := newReserveRouterForTests(t, &reserveServiceForHTTPTests{})
	rec := doGL(t, router, http.MethodGet, "/other/reserve-rules", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	rec = doGL(t, router, http.MethodPost, "/other/reserve-rules/preview", `{}`)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReserveRuleEndpoints(t *testing.T) {
	t.Parallel()
	id, clientID, accountID := uuid.New(), uuid.New(), uuid.New()
	svc := &reserveServiceForHTTPTests{ruleReturn: &models.ReserveRule{ID: id, Scope: "client", Status: "active"}}
	router := newReserveRouterForTests(t, svc)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules",
		`{"scope":"client","client_id":"`+clientID.String()+`","currency":"NGN","percentage":"10","hold_hours":72}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, clientID, *svc.lastCreate.ClientID)
	require.Equal(t, "10", svc.lastCreate.Percentage)
	require.Equal(t, 72, svc.lastCreate.HoldHours)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules", `{"scope":"client","surprise":1}`)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-rules?client_id="+clientID.String()+"&account_id="+accountID.String()+"&status=ACTIVE", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, clientID, *svc.lastFilter.ClientID)
	require.Equal(t, accountID, *svc.lastFilter.AccountID)
	require.Equal(t, "active", *svc.lastFilter.Status)

	for _, bad := range []string{"client_id=nope", "account_id=nope", "status=gone"} {
		rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-rules?"+bad, nil)
		assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)
	}

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-rules/"+id.String(), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-rules/not-a-uuid", nil)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules/"+id.String()+"/revoke", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, id, svc.lastRevokedID)
}

func TestReservePreviewEndpoint(t *testing.T) {
	t.Parallel()
	accountID := uuid.New()
	svc := &reserveServiceForHTTPTests{previewReturn: &services.ReservePreview{AccountID: accountID, AmountMinor: 1000, ReserveMinor: 100, AvailableMinor: 900, RuleSource: "client"}}
	router := newReserveRouterForTests(t, svc)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules/preview",
		`{"account_id":"`+accountID.String()+`","amount_minor":1000,"at":"2026-11-01T12:00:00Z"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, accountID, svc.lastPreview.AccountID)
	require.Equal(t, int64(1000), svc.lastPreview.AmountMinor)
	require.False(t, svc.lastPreview.At.IsZero())

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules/preview", `{"account_id":"nope","amount_minor":1}`)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)
}

func TestReserveErrorMapping(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err    error
		status int
		code   string
	}{
		"validation": {services.ErrReserveValidation, http.StatusBadRequest, common.ErrValidation},
		"overlap":    {services.ErrReserveConflict, http.StatusConflict, common.ErrConflict},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			router := newReserveRouterForTests(t, &reserveServiceForHTTPTests{err: tc.err})
			rec := doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules/preview",
				`{"account_id":"`+uuid.NewString()+`","amount_minor":100}`)
			assertErrorCode(t, rec, tc.status, tc.code)
		})
	}

	router := newReserveRouterForTests(t, &reserveServiceForHTTPTests{err: services.ErrReserveNotFound})
	rec := doGL(t, router, http.MethodGet, "/ledgertrack/reserve-rules/"+uuid.NewString(), nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
}
