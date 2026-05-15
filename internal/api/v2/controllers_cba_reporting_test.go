package v2

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
)

type interestAccrualRepositoryForHTTPTests struct {
	accruals map[uuid.UUID][]models.InterestAccrual
}

func newInterestAccrualRepositoryForHTTPTests() *interestAccrualRepositoryForHTTPTests {
	return &interestAccrualRepositoryForHTTPTests{accruals: map[uuid.UUID][]models.InterestAccrual{}}
}

func (s *interestAccrualRepositoryForHTTPTests) Create(_ context.Context, accrual *models.InterestAccrual) error {
	if accrual.ID == uuid.Nil {
		accrual.ID = uuid.New()
	}
	copied := *accrual
	s.accruals[accrual.AccountID] = append(s.accruals[accrual.AccountID], copied)
	return nil
}

func (s *interestAccrualRepositoryForHTTPTests) Update(_ context.Context, accrual *models.InterestAccrual) error {
	items := s.accruals[accrual.AccountID]
	for i := range items {
		if items[i].ID == accrual.ID {
			items[i] = *accrual
			s.accruals[accrual.AccountID] = items
			return nil
		}
	}
	return nil
}

func (s *interestAccrualRepositoryForHTTPTests) Get(_ context.Context, id uuid.UUID) (*models.InterestAccrual, error) {
	for _, items := range s.accruals {
		for _, item := range items {
			if item.ID == id {
				copied := item
				return &copied, nil
			}
		}
	}
	return nil, nil
}

func (s *interestAccrualRepositoryForHTTPTests) ListByAccount(_ context.Context, accountID uuid.UUID) ([]models.InterestAccrual, error) {
	items := s.accruals[accountID]
	ret := make([]models.InterestAccrual, len(items))
	copy(ret, items)
	return ret, nil
}

type feePostingRepositoryForHTTPTests struct {
	postings map[uuid.UUID][]models.FeePosting
}

func newFeePostingRepositoryForHTTPTests() *feePostingRepositoryForHTTPTests {
	return &feePostingRepositoryForHTTPTests{postings: map[uuid.UUID][]models.FeePosting{}}
}

func (s *feePostingRepositoryForHTTPTests) Create(_ context.Context, posting *models.FeePosting) error {
	if posting.ID == uuid.Nil {
		posting.ID = uuid.New()
	}
	copied := *posting
	s.postings[posting.AccountID] = append(s.postings[posting.AccountID], copied)
	return nil
}

func (s *feePostingRepositoryForHTTPTests) Update(_ context.Context, posting *models.FeePosting) error {
	items := s.postings[posting.AccountID]
	for i := range items {
		if items[i].ID == posting.ID {
			items[i] = *posting
			s.postings[posting.AccountID] = items
			return nil
		}
	}
	return nil
}

func (s *feePostingRepositoryForHTTPTests) GetByReference(_ context.Context, reference string) (*models.FeePosting, error) {
	for _, items := range s.postings {
		for _, item := range items {
			if item.Reference == reference {
				copied := item
				return &copied, nil
			}
		}
	}
	return nil, nil
}

func (s *feePostingRepositoryForHTTPTests) ListByAccount(_ context.Context, accountID uuid.UUID) ([]models.FeePosting, error) {
	items := s.postings[accountID]
	ret := make([]models.FeePosting, len(items))
	copy(ret, items)
	return ret, nil
}

func (s *feePostingRepositoryForHTTPTests) ListPendingRecovery(_ context.Context) ([]models.FeePosting, error) {
	ret := make([]models.FeePosting, 0)
	for _, items := range s.postings {
		for _, item := range items {
			if item.Status == models.FeePostingStatusPendingRecovery {
				ret = append(ret, item)
			}
		}
	}
	return ret, nil
}

func newReportingServiceForHTTPTests() (services.ReportingService, *clientRepositoryForHTTPTests, *accountRepositoryForHTTPTests, *interestAccrualRepositoryForHTTPTests, *feePostingRepositoryForHTTPTests) {
	clientRepo := newClientRepositoryForHTTPTests()
	accountRepo := newAccountRepositoryForHTTPTests()
	interestRepo := newInterestAccrualRepositoryForHTTPTests()
	feeRepo := newFeePostingRepositoryForHTTPTests()
	return services.NewReportingService(clientRepo, accountRepo, interestRepo, feeRepo), clientRepo, accountRepo, interestRepo, feeRepo
}

type clientPortfolioResponse struct {
	TotalAccounts int `json:"total_accounts"`
	Accounts      []struct {
		ID               uuid.UUID `json:"id"`
		AvailableBalance int64     `json:"available_balance"`
	} `json:"accounts"`
}

type accountStatementReportResponse struct {
	OpeningBalance   int64            `json:"opening_balance"`
	ClosingBalance   int64            `json:"closing_balance"`
	TotalCredits     int64            `json:"total_credits"`
	TotalDebits      int64            `json:"total_debits"`
	TransactionCount int              `json:"transaction_count"`
	Transactions     []map[string]any `json:"transactions"`
}

type dailyTransactionSummaryResponse struct {
	ReportDate       string `json:"report_date"`
	TransactionCount int64  `json:"transaction_count"`
	DebitCount       int64  `json:"debit_count"`
	CreditCount      int64  `json:"credit_count"`
	DebitAmount      int64  `json:"debit_amount"`
	CreditAmount     int64  `json:"credit_amount"`
}

type interestFeeReportResponse struct {
	Totals struct {
		InterestAccrued          string `json:"interest_accrued"`
		FeeTotal                 string `json:"fee_total"`
		FeePostedAmount          string `json:"fee_posted_amount"`
		FeePendingRecoveryAmount string `json:"fee_pending_recovery_amount"`
		FeePostedCount           int64  `json:"fee_posted_count"`
		FeePendingRecoveryCount  int64  `json:"fee_pending_recovery_count"`
	} `json:"totals"`
}

func TestListClientAccounts(t *testing.T) {
	accountService, accountRepo, clientRepo, _, _ := newAccountServiceForHTTPTests()
	kycRepo := newKYCRepositoryForHTTPTests()
	clientService := services.NewClientService(clientRepo, accountRepo)
	kycService := services.NewKYCService(clientRepo, kycRepo)
	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithClientService(clientService), WithKYCService(kycService), WithAccountService(accountService))

	clientID := uuid.New()
	require.NoError(t, clientRepo.Create(context.Background(), &models.Client{
		ID:           clientID,
		ClientNumber: "CL-2026-200001",
		Type:         models.ClientTypeIndividual,
		Status:       models.ClientStatusActive,
		KYCLevel:     1,
		KYCStatus:    models.KYCStatusVerified,
	}))
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000001001",
		ClientID:      clientID,
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-client-1",
	}))
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000001002",
		ClientID:      uuid.New(),
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-client-2",
	}))

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/clients/"+clientID.String()+"/accounts", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[accountListResponse](t, rec.Body)
	require.True(t, ok)
	require.Len(t, response.Accounts, 1)
	require.Equal(t, clientID, response.Accounts[0].ClientID)
}

func TestGetClientPortfolioReport(t *testing.T) {
	reportingService, clientRepo, accountRepo, _, _ := newReportingServiceForHTTPTests()
	kycRepo := newKYCRepositoryForHTTPTests()
	clientService := services.NewClientService(clientRepo, accountRepo)
	kycService := services.NewKYCService(clientRepo, kycRepo)
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithClientService(clientService), WithKYCService(kycService), WithReportingService(reportingService))

	clientID := uuid.New()
	require.NoError(t, clientRepo.Create(context.Background(), &models.Client{
		ID:           clientID,
		ClientNumber: "CL-2026-200002",
		Type:         models.ClientTypeIndividual,
		Status:       models.ClientStatusActive,
		KYCLevel:     1,
		KYCStatus:    models.KYCStatusVerified,
	}))
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:              uuid.New(),
		AccountNumber:   "0000002001",
		ClientID:        clientID,
		ProductID:       uuid.New(),
		Currency:        "USD",
		Status:          models.AccountStatusActive,
		WalletID:        "wallet-report-1",
		InterestAccrued: decimal.RequireFromString("2.50"),
	}))
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:              uuid.New(),
		AccountNumber:   "0000002002",
		ClientID:        clientID,
		ProductID:       uuid.New(),
		Currency:        "USD",
		Status:          models.AccountStatusDormant,
		WalletID:        "wallet-report-2",
		InterestAccrued: decimal.RequireFromString("1.25"),
	}))

	gomock.InOrder(
		ledgerController.EXPECT().GetAggregatedBalances(gomock.Any(), gomock.Any()).Return(ledger.BalancesByAssets{"USD/2": big.NewInt(150)}, nil),
		ledgerController.EXPECT().GetAggregatedBalances(gomock.Any(), gomock.Any()).Return(ledger.BalancesByAssets{"USD/2": big.NewInt(80)}, nil),
	)

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/reports/clients/"+clientID.String()+"/portfolio", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[clientPortfolioResponse](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, 2, response.TotalAccounts)
	require.Len(t, response.Accounts, 2)
	require.EqualValues(t, 150, response.Accounts[0].AvailableBalance)
}

func TestGetAccountStatementReport(t *testing.T) {
	reportingService, _, accountRepo, _, _ := newReportingServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReportingService(reportingService))

	accountID := uuid.New()
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:            accountID,
		AccountNumber: "0000003001",
		ClientID:      uuid.New(),
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-statement-1",
	}))

	availableAddress := "users:wallet-statement-1:wallets:USD:available"
	ledgerController.EXPECT().ListTransactions(gomock.Any(), gomock.Any()).Return(&bunpaginate.Cursor[ledger.Transaction]{
		Data: []ledger.Transaction{
			ledger.NewTransaction().
				WithPostings(ledger.NewPosting("system:control:USD", availableAddress, "USD/2", big.NewInt(100))).
				WithPostCommitVolumes(ledger.PostCommitVolumes{
					availableAddress: {"USD/2": ledger.NewVolumesInt64(100, 0)},
				}),
			ledger.NewTransaction().
				WithPostings(ledger.NewPosting(availableAddress, "system:control:USD", "USD/2", big.NewInt(40))).
				WithPostCommitVolumes(ledger.PostCommitVolumes{
					availableAddress: {"USD/2": ledger.NewVolumesInt64(100, 40)},
				}),
		},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/reports/accounts/"+accountID.String()+"/statement", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[accountStatementReportResponse](t, rec.Body)
	require.True(t, ok)
	require.EqualValues(t, 0, response.OpeningBalance)
	require.EqualValues(t, 60, response.ClosingBalance)
	require.EqualValues(t, 100, response.TotalCredits)
	require.EqualValues(t, 40, response.TotalDebits)
	require.EqualValues(t, 2, response.TransactionCount)
	require.Len(t, response.Transactions, 2)
}

func TestGetDailyTransactionSummaryReport(t *testing.T) {
	reportingService, _, accountRepo, _, _ := newReportingServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReportingService(reportingService))

	accountID := uuid.New()
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:            accountID,
		AccountNumber: "0000004001",
		ClientID:      uuid.New(),
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-daily-1",
	}))

	availableAddress := "users:wallet-daily-1:wallets:USD:available"
	ledgerController.EXPECT().ListTransactions(gomock.Any(), gomock.Any()).Return(&bunpaginate.Cursor[ledger.Transaction]{
		Data: []ledger.Transaction{
			ledger.NewTransaction().
				WithMetadata(map[string]string{"cba_operation": "credit"}).
				WithPostings(ledger.NewPosting("system:control:USD", availableAddress, "USD/2", big.NewInt(100))).
				WithPostCommitVolumes(ledger.PostCommitVolumes{
					availableAddress: {"USD/2": ledger.NewVolumesInt64(100, 0)},
				}),
			ledger.NewTransaction().
				WithMetadata(map[string]string{"cba_operation": "debit"}).
				WithPostings(ledger.NewPosting(availableAddress, "system:control:USD", "USD/2", big.NewInt(30))).
				WithPostCommitVolumes(ledger.PostCommitVolumes{
					availableAddress: {"USD/2": ledger.NewVolumesInt64(100, 30)},
				}),
		},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/reports/transactions/daily?date=2026-05-15&account_id="+accountID.String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[dailyTransactionSummaryResponse](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "2026-05-15", response.ReportDate)
	require.EqualValues(t, 2, response.TransactionCount)
	require.EqualValues(t, 1, response.CreditCount)
	require.EqualValues(t, 1, response.DebitCount)
	require.EqualValues(t, 100, response.CreditAmount)
	require.EqualValues(t, 30, response.DebitAmount)
}

func TestGetInterestFeeReport(t *testing.T) {
	reportingService, _, accountRepo, interestRepo, feeRepo := newReportingServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithReportingService(reportingService))

	accountID := uuid.New()
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:            accountID,
		AccountNumber: "0000005001",
		ClientID:      uuid.New(),
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-interest-fee-1",
	}))
	require.NoError(t, interestRepo.Create(context.Background(), &models.InterestAccrual{
		AccountID:   accountID,
		AccrualDate: time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
		Amount:      decimal.RequireFromString("1.25"),
		Posted:      false,
	}))
	require.NoError(t, feeRepo.Create(context.Background(), &models.FeePosting{
		AccountID: accountID,
		Reference: "fee-1",
		Amount:    decimal.RequireFromString("2.50"),
		Currency:  "USD",
		Status:    models.FeePostingStatusPendingRecovery,
	}))
	require.NoError(t, feeRepo.Create(context.Background(), &models.FeePosting{
		AccountID: accountID,
		Reference: "fee-2",
		Amount:    decimal.RequireFromString("1.00"),
		Currency:  "USD",
		Status:    models.FeePostingStatusPosted,
	}))

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/reports/interest-fees?account_id="+accountID.String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[interestFeeReportResponse](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "1.25", response.Totals.InterestAccrued)
	require.Equal(t, "3.5", response.Totals.FeeTotal)
	require.Equal(t, "1", response.Totals.FeePostedAmount)
	require.Equal(t, "2.5", response.Totals.FeePendingRecoveryAmount)
	require.EqualValues(t, 1, response.Totals.FeePostedCount)
	require.EqualValues(t, 1, response.Totals.FeePendingRecoveryCount)
}
