package v2

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/auth"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

type holdServiceForHTTPTests struct {
	err         error
	plan        *services.CreditPlan
	lastPlan    services.PlanCreditRequest
	confirmedID uuid.UUID
	confirmedTx int64
	lastFilter  repositories.ReserveHoldFilter
	lastRelease uuid.UUID
	hold        *models.ReserveHold

	drawPlan      *services.DrawPlan
	lastDraw      services.DrawRequest
	confirmedDraw string
	revertedDraw  string
}

func (s *holdServiceForHTTPTests) PlanCredit(_ context.Context, r services.PlanCreditRequest) (*services.CreditPlan, error) {
	s.lastPlan = r
	return s.plan, s.err
}
func (s *holdServiceForHTTPTests) ConfirmCredit(_ context.Context, id uuid.UUID, tx int64) error {
	s.confirmedID, s.confirmedTx = id, tx
	return nil
}
func (s *holdServiceForHTTPTests) GetHold(context.Context, uuid.UUID) (*models.ReserveHold, error) {
	return s.hold, s.err
}
func (s *holdServiceForHTTPTests) ListHolds(_ context.Context, f repositories.ReserveHoldFilter) ([]models.ReserveHold, error) {
	s.lastFilter = f
	return []models.ReserveHold{}, s.err
}
func (s *holdServiceForHTTPTests) ReleaseHold(_ context.Context, id uuid.UUID, _ string) (*models.ReserveHold, error) {
	s.lastRelease = id
	return s.hold, s.err
}
func (s *holdServiceForHTTPTests) DrawReserve(_ context.Context, r services.DrawRequest) (*services.DrawPlan, error) {
	s.lastDraw = r
	return s.drawPlan, s.err
}
func (s *holdServiceForHTTPTests) ConfirmDraw(_ context.Context, _, _, ref string) error {
	s.confirmedDraw = ref
	return nil
}
func (s *holdServiceForHTTPTests) RevertDraw(_ context.Context, _, _, ref string) error {
	s.revertedDraw = ref
	return nil
}
func (s *holdServiceForHTTPTests) RunRelease(context.Context, services.ReleaseOptions) (services.ReleaseStats, error) {
	return services.ReleaseStats{}, s.err
}

func TestReserveHoldEndpoints(t *testing.T) {
	t.Parallel()
	id, accountID, clientID := uuid.New(), uuid.New(), uuid.New()
	svc := &holdServiceForHTTPTests{hold: &models.ReserveHold{ID: id, Status: models.HoldStatusReleased}}
	systemController, mockLedger := newTestingSystemController(t, false)
	mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))

	rec := doGL(t, router, http.MethodGet, "/ledgertrack/reserve-holds?account_id="+accountID.String()+
		"&client_id="+clientID.String()+"&wallet_id=u1-NGN&status=HELD&due_before=2026-11-01T12:00:00Z&limit=50", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, accountID, *svc.lastFilter.AccountID)
	require.Equal(t, clientID, *svc.lastFilter.ClientID)
	require.Equal(t, "u1-NGN", *svc.lastFilter.WalletID)
	require.Equal(t, "held", *svc.lastFilter.Status)
	require.Equal(t, 50, svc.lastFilter.Limit)
	require.True(t, svc.lastFilter.DueBefore.Equal(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)))

	for _, bad := range []string{"account_id=nope", "client_id=nope", "status=gone", "due_before=yesterday", "limit=0", "limit=501", "limit=x"} {
		rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-holds?"+bad, nil)
		assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)
	}

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-holds/"+id.String(), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/reserve-holds/nope", nil)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)

	rec = doGL(t, router, http.MethodPost, "/ledgertrack/reserve-holds/"+id.String()+"/release", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, id, svc.lastRelease)

	// holds exist only on the ledgertrack ledger
	rec = doGL(t, router, http.MethodGet, "/other/reserve-holds", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReserveHoldErrorMapping(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err    error
		status int
	}{
		"not found": {services.ErrReserveNotFound, http.StatusNotFound},
		"conflict":  {services.ErrReserveConflict, http.StatusConflict},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			systemController, mockLedger := newTestingSystemController(t, false)
			mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(&holdServiceForHTTPTests{err: tc.err}))
			rec := doGL(t, router, http.MethodPost, "/ledgertrack/reserve-holds/"+uuid.NewString()+"/release", nil)
			require.Equal(t, tc.status, rec.Code)
		})
	}
}

func creditResult(wallet string) *ledger.CreatedTransaction {
	return &ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction().
			WithID(7).
			WithPostings(ledger.NewPosting("system:control:NGN", "users:"+wallet+":wallets:NGN:available", "NGN/2", big.NewInt(100))).
			WithPostCommitVolumes(ledger.PostCommitVolumes{
				"system:control:NGN":                         {"NGN/2": ledger.NewVolumesInt64(0, 100)},
				"users:" + wallet + ":wallets:NGN:available": {"NGN/2": ledger.NewVolumesInt64(1000, 0)},
			}),
	}
}

func TestCreditWalletSplitsWhenAReserveApplies(t *testing.T) {
	t.Parallel()
	holdID := uuid.New()
	releaseAt := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	svc := &holdServiceForHTTPTests{plan: &services.CreditPlan{
		Hold:           models.ReserveHold{ID: holdID, ReleaseAt: releaseAt},
		ReserveMinor:   1000,
		AvailableMinor: 9000,
	}}
	systemController, mockLedger := newTestingSystemController(t, true)
	mockLedger.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "[NGN/2 1000]")
			require.Contains(t, script, "destination = @users:u1:wallets:NGN:lien")
			require.Contains(t, script, "[NGN/2 9000]")
			require.Contains(t, script, "destination = @users:u1:wallets:NGN:available")
			md := params.Input.RunScript.Metadata
			require.Equal(t, holdID.String(), md["reserve_hold_id"])
			require.Equal(t, "1000", md["reserve_minor"])
			require.Equal(t, "2026-11-04T09:00:00Z", md["reserve_release_at"])
			require.Equal(t, "kept", md["note"], "caller metadata survives")
			return &ledger.Log{}, creditResult("u1"), false, nil
		})
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/wallets/u1-NGN/credit",
		`{"amount":10000,"reference":"cr-1","credit_type":"card","metadata":{"note":"kept"}}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "card", svc.lastPlan.CreditType)
	require.Equal(t, int64(10000), svc.lastPlan.AmountMinor)
	require.Equal(t, "cr-1", svc.lastPlan.Reference)
	require.Equal(t, holdID, svc.confirmedID)
	require.EqualValues(t, 7, svc.confirmedTx)
	require.Contains(t, rec.Body.String(), `"reserve"`)
	require.Contains(t, rec.Body.String(), `"reserve_minor":1000`)
}

func TestCreditWalletFullHoldPostsOnlyTheLienLeg(t *testing.T) {
	t.Parallel()
	svc := &holdServiceForHTTPTests{plan: &services.CreditPlan{
		Hold: models.ReserveHold{ID: uuid.New(), ReleaseAt: time.Now()}, ReserveMinor: 10000,
	}}
	systemController, mockLedger := newTestingSystemController(t, true)
	mockLedger.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Equal(t, 1, strings.Count(script, "send ["))
			require.Contains(t, script, "wallets:NGN:lien")
			return &ledger.Log{}, creditResult("u1"), false, nil
		})
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))
	rec := doGL(t, router, http.MethodPost, "/ledgertrack/wallets/u1-NGN/credit", `{"amount":10000,"reference":"cr-1"}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}

func TestCreditWalletUnchangedWithoutAReserve(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		target string
		svc    *holdServiceForHTTPTests
	}{
		"no rule applies": {"/ledgertrack/wallets/u1-NGN/credit", &holdServiceForHTTPTests{}},
		"other ledger":    {"/other/wallets/u1-NGN/credit", &holdServiceForHTTPTests{plan: &services.CreditPlan{Hold: models.ReserveHold{ID: uuid.New()}, ReserveMinor: 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			systemController, mockLedger := newTestingSystemController(t, true)
			mockLedger.EXPECT().
				CreateTransaction(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
					script := params.Input.RunScript.Script.Plain
					require.NotContains(t, script, "lien")
					require.Equal(t, 1, strings.Count(script, "send ["))
					require.NotContains(t, params.Input.RunScript.Metadata, "reserve_hold_id")
					return &ledger.Log{}, creditResult("u1"), false, nil
				})
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(tc.svc))
			rec := doGL(t, router, http.MethodPost, tc.target, `{"amount":10000,"reference":"cr-1"}`)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			require.NotContains(t, rec.Body.String(), `"reserve"`)
			require.Equal(t, uuid.Nil, tc.svc.confirmedID)
		})
	}
}

func TestCreditWalletRefusesWhenThePlanFails(t *testing.T) {
	t.Parallel()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop",
		WithReserveHoldService(&holdServiceForHTTPTests{err: services.ErrReserveValidation}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ledgertrack/wallets/u1-NGN/credit",
		strings.NewReader(`{"amount":10000,"reference":"cr-1"}`)))
	require.Equal(t, http.StatusBadRequest, rec.Code, "nothing is credited when the hold cannot be recorded")
}

func debitResult(wallet string) *ledger.CreatedTransaction {
	return &ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction().
			WithID(9).
			WithPostings(ledger.NewPosting("users:"+wallet+":wallets:NGN:lien", "system:control:NGN", "NGN/2", big.NewInt(100))).
			WithPostCommitVolumes(ledger.PostCommitVolumes{
				"system:control:NGN":                         {"NGN/2": ledger.NewVolumesInt64(100, 0)},
				"users:" + wallet + ":wallets:NGN:lien":      {"NGN/2": ledger.NewVolumesInt64(200, 100)},
				"users:" + wallet + ":wallets:NGN:available": {"NGN/2": ledger.NewVolumesInt64(1000, 100)},
			}),
	}
}

func TestDebitWalletDrawsFromTheReserveFirst(t *testing.T) {
	t.Parallel()
	svc := &holdServiceForHTTPTests{drawPlan: &services.DrawPlan{DrawnMinor: 3000, RemainderMinor: 7000}}
	systemController, mockLedger := newTestingSystemController(t, true)
	mockLedger.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "[NGN/2 3000]")
			require.Contains(t, script, "source = @users:u1:wallets:NGN:lien")
			require.Contains(t, script, "[NGN/2 7000]")
			require.Contains(t, script, "source = @users:u1:wallets:NGN:available")
			require.Equal(t, "3000", params.Input.RunScript.Metadata["reserve_drawn_minor"])
			return &ledger.Log{}, debitResult("u1"), false, nil
		})
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/wallets/u1-NGN/debit",
		`{"amount":10000,"reference":"cb-1","draw_from_reserve":true}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "cb-1", svc.lastDraw.Reference)
	require.Equal(t, int64(10000), svc.lastDraw.AmountMinor)
	require.Equal(t, "cb-1", svc.confirmedDraw)
	require.Empty(t, svc.revertedDraw)
	require.Contains(t, rec.Body.String(), `"drawn_minor":3000`)
}

func TestDebitWalletGivesTheReserveBackWhenFundsAreShort(t *testing.T) {
	t.Parallel()
	svc := &holdServiceForHTTPTests{drawPlan: &services.DrawPlan{DrawnMinor: 3000, RemainderMinor: 7000}}
	systemController, mockLedger := newTestingSystemController(t, true)
	mockLedger.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		Return(nil, nil, false, errors.New("insufficient funds"))
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/wallets/u1-NGN/debit",
		`{"amount":10000,"reference":"cb-1","draw_from_reserve":true}`)
	require.Equal(t, http.StatusPaymentRequired, rec.Code, rec.Body.String())
	require.Equal(t, "cb-1", svc.revertedDraw)
	require.Empty(t, svc.confirmedDraw)
}

func TestDebitWalletLeavesTheDrawPendingWhenTheOutcomeIsUnknown(t *testing.T) {
	t.Parallel()
	svc := &holdServiceForHTTPTests{drawPlan: &services.DrawPlan{DrawnMinor: 3000, RemainderMinor: 7000}}
	systemController, mockLedger := newTestingSystemController(t, true)
	mockLedger.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		Return(nil, nil, false, errors.New("connection reset"))
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/wallets/u1-NGN/debit",
		`{"amount":10000,"reference":"cb-1","draw_from_reserve":true}`)
	require.GreaterOrEqual(t, rec.Code, 500)
	require.Empty(t, svc.confirmedDraw, "the release job decides from the ledger")
	require.Empty(t, svc.revertedDraw)
}

func TestDebitWalletWithoutDrawFlagOrOnOtherLedgerIsUnchanged(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ target, body string }{
		"no flag":      {"/ledgertrack/wallets/u1-NGN/debit", `{"amount":10000,"reference":"d-1"}`},
		"other ledger": {"/other/wallets/u1-NGN/debit", `{"amount":10000,"reference":"d-1","draw_from_reserve":true}`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := &holdServiceForHTTPTests{drawPlan: &services.DrawPlan{DrawnMinor: 1, RemainderMinor: 1}}
			systemController, mockLedger := newTestingSystemController(t, true)
			mockLedger.EXPECT().
				CreateTransaction(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
					require.NotContains(t, params.Input.RunScript.Script.Plain, "lien")
					require.NotContains(t, params.Input.RunScript.Metadata, "reserve_drawn_minor")
					return &ledger.Log{}, debitResult("u1"), false, nil
				})
			router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(svc))
			rec := doGL(t, router, http.MethodPost, tc.target, tc.body)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			require.Empty(t, svc.lastDraw.Reference)
		})
	}
}

func TestDebitWalletRejectsDrawWithAChannel(t *testing.T) {
	t.Parallel()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReserveHoldService(&holdServiceForHTTPTests{}))
	rec := doGL(t, router, http.MethodPost, "/ledgertrack/wallets/u1-NGN/debit",
		`{"amount":10000,"reference":"cb-1","draw_from_reserve":true,"channelID":"c1","channelAmount":9000}`)
	assertErrorCode(t, rec, http.StatusBadRequest, common.ErrValidation)
}
