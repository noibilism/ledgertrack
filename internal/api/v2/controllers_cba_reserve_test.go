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

type calendarServiceForHTTPTests struct {
	err        error
	lastCreate services.CalendarInput
	lastUpdate services.CalendarInput
	lastID     uuid.UUID
	ret        *models.BusinessCalendar
}

func (s *calendarServiceForHTTPTests) CreateCalendar(_ context.Context, in services.CalendarInput) (*models.BusinessCalendar, error) {
	s.lastCreate = in
	return s.ret, s.err
}
func (s *calendarServiceForHTTPTests) GetCalendar(_ context.Context, id uuid.UUID) (*models.BusinessCalendar, error) {
	s.lastID = id
	return s.ret, s.err
}
func (s *calendarServiceForHTTPTests) ListCalendars(context.Context) ([]models.BusinessCalendar, error) {
	return []models.BusinessCalendar{}, s.err
}
func (s *calendarServiceForHTTPTests) UpdateCalendar(_ context.Context, id uuid.UUID, in services.CalendarInput) (*models.BusinessCalendar, error) {
	s.lastID, s.lastUpdate = id, in
	return s.ret, s.err
}

func newCalendarRouterForTests(t *testing.T, svc services.BusinessCalendarService) http.Handler {
	t.Helper()
	systemController, mockLedger := newTestingSystemController(t, false)
	mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	return NewRouter(systemController, auth.NewNoAuth(), "develop", WithBusinessCalendarService(svc))
}

func TestBusinessCalendarEndpoints(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	svc := &calendarServiceForHTTPTests{ret: &models.BusinessCalendar{ID: id, Code: "NG"}}
	router := newCalendarRouterForTests(t, svc)

	body := `{"code":"NG","name":"Nigeria","timezone":"Africa/Lagos","working_days":[1,2,3,4,5],"open_time":"08:00","close_time":"17:00","holidays":["2026-12-25"]}`
	rec := doGL(t, router, http.MethodPost, "/ledgertrack/business-calendars", body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "Africa/Lagos", svc.lastCreate.Timezone)
	require.Equal(t, []int{1, 2, 3, 4, 5}, svc.lastCreate.WorkingDays)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/business-calendars", `{"code":"NG","surprise":1}`)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/business-calendars", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/business-calendars/"+id.String(), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/business-calendars/nope", nil)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodPut, "/ledgertrack/business-calendars/"+id.String(), body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, id, svc.lastID)
	require.Equal(t, "17:00", svc.lastUpdate.CloseTime)

	rec = doGL(t, router, http.MethodGet, "/other/business-calendars", nil)
	require.Equal(t, http.StatusNotFound, rec.Code, "ledgertrack only")

	for name, tc := range map[string]struct {
		err    error
		status int
		code   string
	}{
		"validation": {services.ErrReserveValidation, http.StatusBadRequest, common.ErrValidation},
		"duplicate":  {services.ErrReserveConflict, http.StatusConflict, common.ErrConflict},
	} {
		r := newCalendarRouterForTests(t, &calendarServiceForHTTPTests{err: tc.err})
		assertErrorCode(t, doGL(t, r, http.MethodPost, "/ledgertrack/business-calendars", body), tc.status, tc.code)
		_ = name
	}
	r := newCalendarRouterForTests(t, &calendarServiceForHTTPTests{err: services.ErrReserveNotFound})
	rec = doGL(t, r, http.MethodGet, "/ledgertrack/business-calendars/"+id.String(), nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReserveRuleAndPreviewCarryBusinessHoursAndCreditType(t *testing.T) {
	t.Parallel()
	calendarID, clientID, accountID := uuid.New(), uuid.New(), uuid.New()
	svc := &reserveServiceForHTTPTests{
		ruleReturn:    &models.ReserveRule{ID: uuid.New(), Scope: "client", Status: "active"},
		previewReturn: &services.ReservePreview{AccountID: accountID, RuleSource: "client"},
	}
	router := newReserveRouterForTests(t, svc)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules",
		`{"scope":"client","client_id":"`+clientID.String()+`","currency":"NGN","percentage":"10","hold_hours":48,"hold_mode":"business","calendar_id":"`+calendarID.String()+`","credit_types":["card","pos"]}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "business", svc.lastCreate.HoldMode)
	require.Equal(t, calendarID, *svc.lastCreate.CalendarID)
	require.Equal(t, []string{"card", "pos"}, svc.lastCreate.CreditTypes)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/reserve-rules/preview",
		`{"account_id":"`+accountID.String()+`","amount_minor":1000,"credit_type":"card"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "card", svc.lastPreview.CreditType)
}
