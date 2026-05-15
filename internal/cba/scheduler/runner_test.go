package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

func TestInterestAccrualRunnerRunAccruesActiveAccounts(t *testing.T) {
	t.Parallel()

	accountID := uuid.New()
	accountRepo := &accountRepositoryStub{
		listFunc: func(_ context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
			require.NotNil(t, filter.Status)
			require.Equal(t, models.AccountStatusActive, *filter.Status)
			return []models.Account{{
				ID:       accountID,
				Status:   models.AccountStatusActive,
				WalletID: "wallet-1",
				Currency: "NGN",
			}}, nil
		},
	}

	var accruedBalance int64
	interestService := &interestServiceStub{
		accrueFunc: func(_ context.Context, id uuid.UUID, balanceMinorUnits int64, when time.Time) (*models.InterestAccrual, error) {
			require.Equal(t, accountID, id)
			require.Equal(t, time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), when)
			accruedBalance = balanceMinorUnits
			return &models.InterestAccrual{}, nil
		},
	}
	engine := &postingEngineStub{
		availableBalanceFunc: func(_ context.Context, account models.Account) (int64, error) {
			require.Equal(t, accountID, account.ID)
			return 125_000, nil
		},
	}

	runner := NewInterestAccrualRunner(logging.Testing(), accountRepo, interestService, engine, InterestAccrualRunnerConfig{
		Schedule: cron.Every(time.Minute),
	})

	err := runner.run(context.Background(), time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(125_000), accruedBalance)
}

func TestInterestPostingRunnerRunPostsDueInterest(t *testing.T) {
	t.Parallel()

	accountID := uuid.New()
	accountRepo := &accountRepositoryStub{
		listFunc: func(_ context.Context, _ repositories.AccountFilter) ([]models.Account, error) {
			return []models.Account{{
				ID:       accountID,
				WalletID: "wallet-2",
				Currency: "USD",
			}}, nil
		},
	}

	preview := &services.InterestPostingPreview{
		AccountID:      accountID,
		Currency:       "USD",
		PostableAmount: 250,
	}
	var postedReference string
	interestService := &interestServiceStub{
		isPostingDueFunc: func(_ context.Context, id uuid.UUID, when time.Time) (bool, error) {
			require.Equal(t, accountID, id)
			require.Equal(t, time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), when)
			return true, nil
		},
		previewPostingFunc: func(_ context.Context, id uuid.UUID) (*services.InterestPostingPreview, error) {
			require.Equal(t, accountID, id)
			return preview, nil
		},
		markPostedFunc: func(_ context.Context, got services.InterestPostingPreview, reference string) error {
			require.Equal(t, preview.PostableAmount, got.PostableAmount)
			postedReference = reference
			return nil
		},
	}

	creditCalled := false
	expenseCalled := false
	engine := &postingEngineStub{
		creditFunc: func(_ context.Context, account models.Account, amount int64, reference string, metadata map[string]string) error {
			creditCalled = true
			require.Equal(t, accountID, account.ID)
			require.Equal(t, int64(250), amount)
			require.Equal(t, "interest:"+accountID.String()+":2026-12-31", reference)
			require.Equal(t, "interest_posting", metadata["cba_operation"])
			return nil
		},
		recordInterestExpenseFunc: func(_ context.Context, currency, reference string, amount int64, metadata map[string]string) error {
			expenseCalled = true
			require.Equal(t, "USD", currency)
			require.Equal(t, int64(250), amount)
			require.Equal(t, "interest_posting", metadata["cba_operation"])
			require.Equal(t, "interest:"+accountID.String()+":2026-12-31", reference)
			return nil
		},
	}

	runner := NewInterestPostingRunner(logging.Testing(), accountRepo, interestService, engine, InterestPostingRunnerConfig{
		Schedule: cron.Every(time.Minute),
	})

	err := runner.run(context.Background(), time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, creditCalled)
	require.True(t, expenseCalled)
	require.Equal(t, "interest:"+accountID.String()+":2026-12-31", postedReference)
}

func TestMaintenanceFeeRunnerRunMarksPostingAsPosted(t *testing.T) {
	t.Parallel()

	accountID := uuid.New()
	posting := models.FeePosting{
		AccountID:       accountID,
		EventType:       "maintenance",
		Reference:       "maintenance:" + accountID.String() + ":2026-05-15",
		LinkedReference: "maintenance:" + accountID.String() + ":2026-05-15",
		Amount:          decimal.RequireFromString("2.50"),
		Currency:        "USD",
		Status:          models.FeePostingStatusPendingRecovery,
	}
	accountRepo := &accountRepositoryStub{
		listFunc: func(_ context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
			require.NotNil(t, filter.Status)
			require.Equal(t, models.AccountStatusActive, *filter.Status)
			return []models.Account{{
				ID:       accountID,
				Status:   models.AccountStatusActive,
				WalletID: "wallet-3",
				Currency: "USD",
			}}, nil
		},
	}
	feeRepo := &feePostingRepositoryStub{
		listPendingRecoveryFunc: func(context.Context) ([]models.FeePosting, error) {
			return []models.FeePosting{posting}, nil
		},
	}
	accountService := &accountServiceStub{
		getFunc: func(_ context.Context, id uuid.UUID) (*models.Account, error) {
			require.Equal(t, accountID, id)
			return &models.Account{
				ID:       accountID,
				Status:   models.AccountStatusActive,
				WalletID: "wallet-3",
				Currency: "USD",
			}, nil
		},
		validateDebitFunc: func(_ context.Context, id uuid.UUID, amount, currentBalance int64, usageAt time.Time) (*models.Account, error) {
			require.Equal(t, accountID, id)
			require.Equal(t, int64(250), amount)
			require.Equal(t, int64(1_000), currentBalance)
			require.Equal(t, time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), usageAt)
			return &models.Account{ID: accountID}, nil
		},
	}

	var prepared bool
	var markedPosted string
	feeService := &feeServiceStub{
		prepareMaintenanceFeeFunc: func(_ context.Context, id uuid.UUID, when time.Time) (*models.FeePosting, error) {
			prepared = true
			require.Equal(t, accountID, id)
			require.Equal(t, time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), when)
			return &posting, nil
		},
		markPostedFunc: func(_ context.Context, reference string) (*models.FeePosting, error) {
			markedPosted = reference
			return &posting, nil
		},
		markPendingRecoveryFunc: func(context.Context, string, map[string]any) (*models.FeePosting, error) {
			t.Fatalf("did not expect pending recovery")
			return nil, nil
		},
	}
	engine := &postingEngineStub{
		availableBalanceFunc: func(context.Context, models.Account) (int64, error) {
			return 1_000, nil
		},
		debitFunc: func(_ context.Context, account models.Account, amount int64, reference string, metadata map[string]string) error {
			require.Equal(t, accountID, account.ID)
			require.Equal(t, int64(250), amount)
			require.Equal(t, posting.Reference, reference)
			require.Equal(t, "fee_posting", metadata["cba_operation"])
			return nil
		},
		recordFeeIncomeFunc: func(_ context.Context, currency, reference string, amount int64, metadata map[string]string) error {
			require.Equal(t, "USD", currency)
			require.Equal(t, posting.Reference, reference)
			require.Equal(t, int64(250), amount)
			require.Equal(t, "maintenance", metadata["fee_event_type"])
			return nil
		},
	}

	runner := NewMaintenanceFeeRunner(logging.Testing(), accountRepo, feeRepo, accountService, feeService, engine, MaintenanceFeeRunnerConfig{
		Schedule: cron.Every(time.Minute),
	})

	err := runner.run(context.Background(), time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, posting.Reference, markedPosted)
}

func TestDormancyRunnerRunMarksInactiveAccountDormant(t *testing.T) {
	t.Parallel()

	accountID := uuid.New()
	productID := uuid.New()
	dormancyDays := 30
	accountRepo := &accountRepositoryStub{
		listFunc: func(_ context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
			require.NotNil(t, filter.Status)
			require.Equal(t, models.AccountStatusActive, *filter.Status)
			lastActivity := time.Date(2026, 4, 1, 13, 0, 0, 0, time.UTC)
			return []models.Account{{
				ID:             accountID,
				ProductID:      productID,
				Status:         models.AccountStatusActive,
				LastActivityAt: &lastActivity,
				OpenedAt:       time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			}}, nil
		},
	}
	productRepo := &productRepositoryStub{
		getFunc: func(_ context.Context, id uuid.UUID) (*models.Product, error) {
			require.Equal(t, productID, id)
			return &models.Product{
				ID: productID,
				Rules: models.ProductRules{
					DormancyDays: &dormancyDays,
				},
			}, nil
		},
	}
	dormantCalled := false
	accountService := &accountServiceStub{
		dormantFunc: func(_ context.Context, id uuid.UUID) (*models.Account, error) {
			dormantCalled = true
			require.Equal(t, accountID, id)
			return &models.Account{ID: accountID, Status: models.AccountStatusDormant}, nil
		},
	}

	runner := NewDormancyRunner(logging.Testing(), accountRepo, productRepo, accountService, DormancyRunnerConfig{
		Schedule: cron.Every(time.Minute),
	})

	err := runner.run(context.Background(), time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, dormantCalled)
}

type accountRepositoryStub struct {
	listFunc func(context.Context, repositories.AccountFilter) ([]models.Account, error)
}

func (s *accountRepositoryStub) Create(context.Context, *models.Account) error { return nil }
func (s *accountRepositoryStub) Update(context.Context, *models.Account) error { return nil }
func (s *accountRepositoryStub) Get(context.Context, uuid.UUID) (*models.Account, error) {
	return nil, nil
}
func (s *accountRepositoryStub) GetByNumber(context.Context, string) (*models.Account, error) {
	return nil, nil
}
func (s *accountRepositoryStub) GetByWalletID(context.Context, string) (*models.Account, error) {
	return nil, nil
}
func (s *accountRepositoryStub) List(ctx context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
	if s.listFunc != nil {
		return s.listFunc(ctx, filter)
	}
	return nil, nil
}

type productRepositoryStub struct {
	getFunc func(context.Context, uuid.UUID) (*models.Product, error)
}

func (s *productRepositoryStub) Create(context.Context, *models.Product) error { return nil }
func (s *productRepositoryStub) Update(context.Context, *models.Product) error { return nil }
func (s *productRepositoryStub) Get(ctx context.Context, id uuid.UUID) (*models.Product, error) {
	if s.getFunc != nil {
		return s.getFunc(ctx, id)
	}
	return nil, nil
}
func (s *productRepositoryStub) GetByCode(context.Context, string) (*models.Product, error) {
	return nil, nil
}
func (s *productRepositoryStub) List(context.Context, repositories.ProductFilter) ([]models.Product, error) {
	return nil, nil
}

type feePostingRepositoryStub struct {
	listPendingRecoveryFunc func(context.Context) ([]models.FeePosting, error)
}

func (s *feePostingRepositoryStub) Create(context.Context, *models.FeePosting) error { return nil }
func (s *feePostingRepositoryStub) Update(context.Context, *models.FeePosting) error { return nil }
func (s *feePostingRepositoryStub) GetByReference(context.Context, string) (*models.FeePosting, error) {
	return nil, nil
}
func (s *feePostingRepositoryStub) ListByAccount(context.Context, uuid.UUID) ([]models.FeePosting, error) {
	return nil, nil
}
func (s *feePostingRepositoryStub) ListPendingRecovery(ctx context.Context) ([]models.FeePosting, error) {
	if s.listPendingRecoveryFunc != nil {
		return s.listPendingRecoveryFunc(ctx)
	}
	return nil, nil
}

type accountServiceStub struct {
	getFunc           func(context.Context, uuid.UUID) (*models.Account, error)
	dormantFunc       func(context.Context, uuid.UUID) (*models.Account, error)
	validateDebitFunc func(context.Context, uuid.UUID, int64, int64, time.Time) (*models.Account, error)
}

func (s *accountServiceStub) Open(context.Context, services.OpenAccountInput) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) List(context.Context, repositories.AccountFilter) ([]models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) Get(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	if s.getFunc != nil {
		return s.getFunc(ctx, id)
	}
	return nil, nil
}
func (s *accountServiceStub) Activate(context.Context, uuid.UUID) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) Suspend(context.Context, uuid.UUID) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) Freeze(context.Context, uuid.UUID) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) Dormant(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	if s.dormantFunc != nil {
		return s.dormantFunc(ctx, id)
	}
	return nil, nil
}
func (s *accountServiceStub) Reactivate(context.Context, uuid.UUID) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) Close(context.Context, uuid.UUID, int64) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) ValidateCredit(context.Context, uuid.UUID, int64, int64, time.Time) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) ValidateDebit(ctx context.Context, id uuid.UUID, amount, currentBalance int64, usageAt time.Time) (*models.Account, error) {
	if s.validateDebitFunc != nil {
		return s.validateDebitFunc(ctx, id, amount, currentBalance, usageAt)
	}
	return nil, nil
}
func (s *accountServiceStub) ValidateLien(context.Context, uuid.UUID, int64, int64, time.Time) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) ValidateRelease(context.Context, uuid.UUID, string) (*models.Account, error) {
	return nil, nil
}
func (s *accountServiceStub) RecordCreditUsage(context.Context, uuid.UUID, int64, string, time.Time) error {
	return nil
}
func (s *accountServiceStub) RecordDebitUsage(context.Context, uuid.UUID, int64, string, time.Time) error {
	return nil
}
func (s *accountServiceStub) TouchActivity(context.Context, uuid.UUID, time.Time) (*models.Account, error) {
	return nil, nil
}

type interestServiceStub struct {
	accrueFunc         func(context.Context, uuid.UUID, int64, time.Time) (*models.InterestAccrual, error)
	isPostingDueFunc   func(context.Context, uuid.UUID, time.Time) (bool, error)
	previewPostingFunc func(context.Context, uuid.UUID) (*services.InterestPostingPreview, error)
	markPostedFunc     func(context.Context, services.InterestPostingPreview, string) error
}

func (s *interestServiceStub) Accrue(ctx context.Context, id uuid.UUID, balanceMinorUnits int64, when time.Time) (*models.InterestAccrual, error) {
	if s.accrueFunc != nil {
		return s.accrueFunc(ctx, id, balanceMinorUnits, when)
	}
	return nil, nil
}
func (s *interestServiceStub) IsPostingDue(ctx context.Context, id uuid.UUID, when time.Time) (bool, error) {
	if s.isPostingDueFunc != nil {
		return s.isPostingDueFunc(ctx, id, when)
	}
	return false, nil
}
func (s *interestServiceStub) PreviewPosting(ctx context.Context, id uuid.UUID) (*services.InterestPostingPreview, error) {
	if s.previewPostingFunc != nil {
		return s.previewPostingFunc(ctx, id)
	}
	return nil, nil
}
func (s *interestServiceStub) MarkPosted(ctx context.Context, preview services.InterestPostingPreview, reference string) error {
	if s.markPostedFunc != nil {
		return s.markPostedFunc(ctx, preview, reference)
	}
	return nil
}

type feeServiceStub struct {
	prepareMaintenanceFeeFunc func(context.Context, uuid.UUID, time.Time) (*models.FeePosting, error)
	markPostedFunc            func(context.Context, string) (*models.FeePosting, error)
	markPendingRecoveryFunc   func(context.Context, string, map[string]any) (*models.FeePosting, error)
}

func (s *feeServiceStub) PrepareTransactionFees(context.Context, uuid.UUID, string, int64, string) ([]models.FeePosting, error) {
	return nil, nil
}
func (s *feeServiceStub) PrepareMaintenanceFee(ctx context.Context, id uuid.UUID, when time.Time) (*models.FeePosting, error) {
	if s.prepareMaintenanceFeeFunc != nil {
		return s.prepareMaintenanceFeeFunc(ctx, id, when)
	}
	return nil, nil
}
func (s *feeServiceStub) MarkPosted(ctx context.Context, reference string) (*models.FeePosting, error) {
	if s.markPostedFunc != nil {
		return s.markPostedFunc(ctx, reference)
	}
	return nil, nil
}
func (s *feeServiceStub) MarkPendingRecovery(ctx context.Context, reference string, metadata map[string]any) (*models.FeePosting, error) {
	if s.markPendingRecoveryFunc != nil {
		return s.markPendingRecoveryFunc(ctx, reference, metadata)
	}
	return nil, nil
}

type postingEngineStub struct {
	availableBalanceFunc      func(context.Context, models.Account) (int64, error)
	creditFunc                func(context.Context, models.Account, int64, string, map[string]string) error
	debitFunc                 func(context.Context, models.Account, int64, string, map[string]string) error
	recordFeeIncomeFunc       func(context.Context, string, string, int64, map[string]string) error
	recordInterestExpenseFunc func(context.Context, string, string, int64, map[string]string) error
}

func (s *postingEngineStub) AvailableBalance(ctx context.Context, account models.Account) (int64, error) {
	if s.availableBalanceFunc != nil {
		return s.availableBalanceFunc(ctx, account)
	}
	return 0, nil
}
func (s *postingEngineStub) Credit(ctx context.Context, account models.Account, amount int64, reference string, metadata map[string]string) error {
	if s.creditFunc != nil {
		return s.creditFunc(ctx, account, amount, reference, metadata)
	}
	return nil
}
func (s *postingEngineStub) Debit(ctx context.Context, account models.Account, amount int64, reference string, metadata map[string]string) error {
	if s.debitFunc != nil {
		return s.debitFunc(ctx, account, amount, reference, metadata)
	}
	return nil
}
func (s *postingEngineStub) RecordFeeIncome(ctx context.Context, currency, reference string, amount int64, metadata map[string]string) error {
	if s.recordFeeIncomeFunc != nil {
		return s.recordFeeIncomeFunc(ctx, currency, reference, amount, metadata)
	}
	return nil
}
func (s *postingEngineStub) RecordInterestExpense(ctx context.Context, currency, reference string, amount int64, metadata map[string]string) error {
	if s.recordInterestExpenseFunc != nil {
		return s.recordInterestExpenseFunc(ctx, currency, reference, amount, metadata)
	}
	return nil
}
