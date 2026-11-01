package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	bunpaginate "github.com/formancehq/go-libs/v3/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func availableCursor(addr string, v int64) *bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount] {
	return &bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{Data: []ledger.VolumesWithBalanceByAssetByAccount{{
		Account: addr, Asset: "USD/2",
		VolumesWithBalance: ledger.VolumesWithBalance{Input: big.NewInt(v), Output: big.NewInt(0), Balance: big.NewInt(v)},
	}}}
}

type chargeServiceForHTTPTests struct {
	plan *services.ChargePlan
	err  error
	last []services.ChargeRequest
}

func (c *chargeServiceForHTTPTests) Plan(_ context.Context, r services.ChargeRequest) (*services.ChargePlan, error) {
	c.last = append(c.last, r)
	return c.plan, c.err
}

type reserveAccountFixture struct {
	charges *chargeServiceForHTTPTests
	router  http.Handler
	ctrl    *LedgerController
	svc     *holdServiceForHTTPTests
	from    *models.Account
	to      *models.Account
}

func newReserveAccountFixture(t *testing.T, freezeFrom bool) *reserveAccountFixture {
	t.Helper()
	accountService, accountRepo, _, productRepo, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := &holdServiceForHTTPTests{}
	charges := &chargeServiceForHTTPTests{}
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService), WithReserveHoldService(svc), WithChargeService(charges))

	product := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID: product, Code: "USD", Name: "USD", Category: "current", Currency: "USD", Status: models.ProductStatusActive,
		Rules: models.ProductRules{AllowCredits: true, AllowDebits: true, MinBalance: "0"},
	}))
	mk := func(number, wallet string, freeze bool) *models.Account {
		a := &models.Account{ID: uuid.New(), AccountNumber: number, ClientID: uuid.New(), ProductID: product,
			Currency: "USD", Status: models.AccountStatusActive, WalletID: wallet, FreezeDebits: freeze}
		require.NoError(t, accountRepo.Create(context.Background(), a))
		return a
	}
	return &reserveAccountFixture{charges: charges, router: router, ctrl: ledgerController, svc: svc,
		from: mk("0000000041", "client-CL-2026-000041-USD", freezeFrom), to: mk("0000000042", "client-CL-2026-000042-USD", false)}
}

func (f *reserveAccountFixture) post(t *testing.T, id uuid.UUID, op string, body any) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+id.String()+"/"+op, api.Buffer(t, body)))
	return rec
}

func TestTransferAccountHoldsTheDestinationReserve(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	holdID := uuid.New()
	f.svc.plan = &services.CreditPlan{Hold: models.ReserveHold{ID: holdID}, ReserveMinor: 20, AvailableMinor: 80}

	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 500), nil)
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000042:wallets:USD:available", 0), nil)
	f.ctrl.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "[USD/2 20]")
			require.Contains(t, script, "destination = @users:client-CL-2026-000042:wallets:USD:lien")
			require.Contains(t, script, "[USD/2 80]")
			require.Contains(t, script, "destination = @users:client-CL-2026-000042:wallets:USD:available")
			require.NotContains(t, script, "overdraft", "the customer source must not be allowed to go negative")
			require.Equal(t, holdID.String(), params.Input.RunScript.Metadata["reserve_hold_id"])
			require.Equal(t, f.to.ID.String(), params.Input.RunScript.Metadata["destination_account_id"])
			return &ledger.Log{}, &ledger.CreatedTransaction{Transaction: ledger.NewTransaction().WithID(31).
				WithPostings(ledger.NewPosting("users:client-CL-2026-000041:wallets:USD:available", "users:client-CL-2026-000042:wallets:USD:lien", "USD/2", big.NewInt(20))).
				WithPostCommitVolumes(ledger.PostCommitVolumes{
					"users:client-CL-2026-000041:wallets:USD:available": {"USD/2": ledger.NewVolumesInt64(500, 100)},
				})}, false, nil
		})

	rec := f.post(t, f.from.ID, "transfer", AccountTransferRequest{DestinationAccountID: f.to.ID.String(), Amount: json.Number("100"), Reference: "t-1"})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "transfer", f.svc.lastPlan.CreditType, "transfers are credited as type transfer by default")
	require.Equal(t, f.to.WalletID, f.svc.lastPlan.WalletID, "the destination's reserve rules apply, not the source's")
	require.Equal(t, holdID, f.svc.confirmedID)
	require.EqualValues(t, 31, f.svc.confirmedTx)
	require.Contains(t, rec.Body.String(), `"reserve"`)
}

func TestDebitAccountDrawsFromTheReserve(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	f.svc.drawPlan = &services.DrawPlan{DrawnMinor: 30, RemainderMinor: 70}

	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 80), nil)
	f.ctrl.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "source = @users:client-CL-2026-000041:wallets:USD:lien")
			require.Contains(t, script, "source = @users:client-CL-2026-000041:wallets:USD:available")
			require.Equal(t, "30", params.Input.RunScript.Metadata["reserve_drawn_minor"])
			return &ledger.Log{}, &ledger.CreatedTransaction{Transaction: ledger.NewTransaction().WithID(32).
				WithPostings(ledger.NewPosting("users:client-CL-2026-000041:wallets:USD:lien", "system:control:USD", "USD/2", big.NewInt(30))).
				WithPostCommitVolumes(ledger.PostCommitVolumes{
					"users:client-CL-2026-000041:wallets:USD:lien":      {"USD/2": ledger.NewVolumesInt64(30, 30)},
					"users:client-CL-2026-000041:wallets:USD:available": {"USD/2": ledger.NewVolumesInt64(500, 100)},
					"system:control:USD":                                {"USD/2": ledger.NewVolumesInt64(100, 0)},
				})}, false, nil
		})

	rec := f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "cb-1", "draw_from_reserve": true})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, f.from.WalletID, f.svc.lastDraw.WalletID)
	require.Equal(t, "cb-1", f.svc.confirmedDraw)
	require.Empty(t, f.svc.revertedDraw)
	require.Contains(t, rec.Body.String(), `"drawn_minor":30`)
}

func TestDebitAccountGivesTheReserveBackWhenTheDebitIsRefused(t *testing.T) {
	f := newReserveAccountFixture(t, true) // debits are frozen, so validation refuses the debit after the draw
	f.svc.drawPlan = &services.DrawPlan{DrawnMinor: 30, RemainderMinor: 70}
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 80), nil)

	rec := f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "cb-2", "draw_from_reserve": true})
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Equal(t, "cb-2", f.svc.revertedDraw, "a refused debit must not leave the reserve consumed")
}

func samplePlan() *services.ChargePlan {
	return &services.ChargePlan{
		Event: "debit", Currency: "USD", FeeMinor: 50, TaxMinor: 4, GrossMinor: 54, FeeSource: "product",
		Lines: []services.ChargeLine{
			{Kind: services.ChargeKindFee, Code: "401101", Address: "gl:USD:401101", AmountMinor: 50, Label: "flat"},
			{Kind: services.ChargeKindTax, Code: "202210", Address: "gl:USD:202210", AmountMinor: 4, Label: "VAT"},
		},
	}
}

func chargeTx(id uint64) *ledger.CreatedTransaction {
	return &ledger.CreatedTransaction{Transaction: ledger.NewTransaction().WithID(id).
		WithPostings(ledger.NewPosting("users:client-CL-2026-000041:wallets:USD:available", "system:control:USD", "USD/2", big.NewInt(100))).
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"users:client-CL-2026-000041:wallets:USD:available": {"USD/2": ledger.NewVolumesInt64(500, 154)},
			"system:control:USD": {"USD/2": ledger.NewVolumesInt64(154, 0)},
		})}
}

func TestDebitAccountPostsFeeAndTaxToTheGL(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	f.charges.plan = samplePlan()
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 500), nil)
	f.ctrl.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "[USD/2 100]")
			require.Contains(t, script, "destination = @system:control:USD")
			require.Contains(t, script, "[USD/2 50]")
			require.Contains(t, script, "destination = @gl:USD:401101")
			require.Contains(t, script, "[USD/2 4]")
			require.Contains(t, script, "destination = @gl:USD:202210")
			require.NotContains(t, script, "overdraft", "the customer pays the charge, so no overdraft")
			md := params.Input.RunScript.Metadata
			require.Equal(t, "50", md["charge_fee_minor"])
			require.Equal(t, "4", md["charge_tax_minor"])
			require.Equal(t, "debit", md["charge_event"])
			return &ledger.Log{}, chargeTx(40), false, nil
		})

	rec := f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "d-1", "charge_fees": true})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Len(t, f.charges.last, 1)
	require.Equal(t, "debit", f.charges.last[0].Event)
	require.Equal(t, int64(100), f.charges.last[0].AmountMinor)
	require.Equal(t, "ledgertrack", f.charges.last[0].Ledger)
	require.Contains(t, rec.Body.String(), `"gross_minor":54`)
}

func TestDebitAccountChecksFundsAgainstAmountPlusCharge(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	f.charges.plan = samplePlan()
	// 120 is enough for the amount (100) but not for amount plus charge (154)
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 120), nil)

	rec := f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "d-2", "charge_fees": true})
	require.GreaterOrEqual(t, rec.Code, 400, rec.Body.String())
	require.Less(t, rec.Code, 500)
}

func TestChargeFeesIsOffByDefaultAndNeedsConfiguration(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 500), nil)
	f.ctrl.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			require.Equal(t, 1, strings.Count(params.Input.RunScript.Script.Plain, "send ["))
			require.NotContains(t, params.Input.RunScript.Metadata, "charge_fee_minor")
			return &ledger.Log{}, chargeTx(41), false, nil
		})
	rec := f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "d-3"})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Empty(t, f.charges.last, "without charge_fees the fee schedule is not even consulted")

	// a configuration that cannot be posted stops the debit before anything moves
	f.charges.err = fmt.Errorf("%w: the flat fee has no income account", services.ErrChargeConfig)
	rec = f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "d-4", "charge_fees": true})
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), ErrChargeConfiguration)

	// no fee schedule: the plan is nil and the debit is plain
	f.charges.err, f.charges.plan = nil, nil
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 500), nil)
	f.ctrl.EXPECT().CreateTransaction(gomock.Any(), gomock.Any()).Return(&ledger.Log{}, chargeTx(42), false, nil)
	rec = f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "d-5", "charge_fees": true})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), `"charges"`)
}

func TestDebitAccountRejectsChargeFeesWithAChannel(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	rec := f.post(t, f.from.ID, "debit", map[string]any{"amount": 100, "reference": "d-6", "charge_fees": true, "channelID": "c1", "channelAmount": 90})
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestCreditAccountTakesFeeAndTaxOutOfTheCredit(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	plan := samplePlan()
	plan.Event = "credit"
	f.charges.plan = plan
	holdID := uuid.New()
	f.svc.plan = &services.CreditPlan{Hold: models.ReserveHold{ID: holdID}, ReserveMinor: 9, AvailableMinor: 37}

	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 0), nil)
	f.ctrl.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "destination = @gl:USD:401101")
			require.Contains(t, script, "destination = @gl:USD:202210")
			require.Contains(t, script, "[USD/2 9]")
			require.Contains(t, script, "[USD/2 37]")
			require.Equal(t, 4, strings.Count(script, "send ["), "lien, available, fee and tax")
			require.Equal(t, 4, strings.Count(script, "allowing unbounded overdraft"), "all four legs are paid by the control account")
			return &ledger.Log{}, chargeTx(43), false, nil
		})

	rec := f.post(t, f.from.ID, "credit", map[string]any{"amount": 100, "reference": "c-1", "charge_fees": true})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, int64(46), f.svc.lastPlan.AmountMinor, "the reserve applies to what the wallet receives: 100 less 54")
	require.Contains(t, rec.Body.String(), `"received_minor":46`)
}

func TestCreditAccountRefusesChargeBiggerThanTheCredit(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	f.charges.plan = samplePlan()
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 0), nil)
	rec := f.post(t, f.from.ID, "credit", map[string]any{"amount": 54, "reference": "c-2", "charge_fees": true})
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestTransferAccountPostsFeeAndTaxFromTheSource(t *testing.T) {
	f := newReserveAccountFixture(t, false)
	plan := samplePlan()
	plan.Event = "transfer"
	f.charges.plan = plan

	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000041:wallets:USD:available", 500), nil)
	f.ctrl.EXPECT().GetVolumesWithBalances(gomock.Any(), gomock.Any()).Return(availableCursor("users:client-CL-2026-000042:wallets:USD:available", 0), nil)
	f.ctrl.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			script := params.Input.RunScript.Script.Plain
			require.Contains(t, script, "destination = @users:client-CL-2026-000042:wallets:USD:available")
			require.Contains(t, script, "destination = @gl:USD:401101")
			require.Contains(t, script, "destination = @gl:USD:202210")
			require.Equal(t, 3, strings.Count(script, "source = @users:client-CL-2026-000041:wallets:USD:available"))
			require.NotContains(t, script, "overdraft")
			require.Equal(t, "50", params.Input.RunScript.Metadata["charge_fee_minor"])
			return &ledger.Log{}, chargeTx(44), false, nil
		})
	rec := f.post(t, f.from.ID, "transfer", AccountTransferRequest{DestinationAccountID: f.to.ID.String(), Amount: json.Number("100"), Reference: "t-9", ChargeFees: true})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, f.from.ID, f.charges.last[0].AccountID, "the source pays, so the source's schedule applies")
	require.Equal(t, "transfer", f.charges.last[0].Event)
}
