package v2

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"go.uber.org/mock/gomock"
)

type accountListResponse struct {
	Accounts []models.Account `json:"accounts"`
}

type accountDetailsResponseForTest struct {
	ID         uuid.UUID `json:"id"`
	WalletID   string    `json:"wallet_id"`
	Currency   string    `json:"currency"`
	WalletInfo struct {
		WalletID         string `json:"wallet_id"`
		Currency         string `json:"currency"`
		AvailableBalance int64  `json:"available_balance"`
		AvailableAddress string `json:"available_address"`
		LienAddress      string `json:"lien_address"`
	} `json:"wallet_info"`
}

func newAccountServiceForHTTPTests() (services.AccountService, *accountRepositoryForHTTPTests, *clientRepositoryForHTTPTests, *productRepositoryForHTTPTests, *dailyUsageRepositoryForHTTPTests) {
	accountRepo := newAccountRepositoryForHTTPTests()
	clientRepo := newClientRepositoryForHTTPTests()
	productRepo := newProductRepositoryForHTTPTests()
	dailyUsageRepo := newDailyUsageRepositoryForHTTPTests()
	return services.NewAccountService(accountRepo, clientRepo, productRepo, dailyUsageRepo), accountRepo, clientRepo, productRepo, dailyUsageRepo
}

func TestOpenAccount(t *testing.T) {
	accountService, _, clientRepo, productRepo, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	client := &models.Client{
		ID:           uuid.New(),
		ClientNumber: "CL-2026-000010",
		Type:         models.ClientTypeIndividual,
		Status:       models.ClientStatusActive,
		KYCLevel:     1,
		KYCStatus:    models.KYCStatusVerified,
		IndividualData: &models.IndividualData{
			FirstName: "Ada",
			LastName:  "Lovelace",
		},
	}
	require.NoError(t, clientRepo.Create(context.Background(), client))

	product := &models.Product{
		ID:       uuid.New(),
		Code:     "SAV-NGN-001",
		Name:     "Personal Savings NGN",
		Category: "savings",
		Currency: "NGN",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			MinOpeningBalance: "0",
			MinBalance:        "0",
			RequiresKYCLevel:  1,
		},
	}
	require.NoError(t, productRepo.Create(context.Background(), product))

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts", api.Buffer(t, services.OpenAccountInput{
		ClientID:       client.ID,
		ProductID:      product.ID,
		OpeningDeposit: json.Number("0"),
	}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	account, ok := api.DecodeSingleResponse[models.Account](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, models.AccountStatusPending, account.Status)
	require.Equal(t, "client-CL-2026-000010-SAV-NGN-001", account.WalletID)
}

func TestListAccounts(t *testing.T) {
	accountService, accountRepo, _, _, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	clientID := uuid.New()
	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000001",
		ClientID:      clientID,
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-1",
	}))

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/accounts?client_id="+clientID.String(), nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[accountListResponse](t, rec.Body)
	require.True(t, ok)
	require.Len(t, response.Accounts, 1)
}

func TestGetAccountBalance(t *testing.T) {
	accountService, accountRepo, _, _, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000002",
		ClientID:      uuid.New(),
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000011-CUR-USD-001",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000011-CUR-USD-001:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(250),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/accounts/"+account.ID.String()+"/balance", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[accountBalanceResponse](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, int64(250), response.Balance)
	require.Equal(t, "USD", response.Currency)
}

func TestGetAccountDetailsIncludesWalletInfo(t *testing.T) {
	accountService, accountRepo, _, _, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000008",
		ClientID:      uuid.New(),
		ProductID:     uuid.New(),
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000017-CUR-USD-001",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000017-CUR-USD-001:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(425),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)

	req := httptest.NewRequest(http.MethodGet, "/ledgertrack/accounts/"+account.ID.String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[accountDetailsResponseForTest](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, account.ID, response.ID)
	require.Equal(t, account.WalletID, response.WalletID)
	require.Equal(t, int64(425), response.WalletInfo.AvailableBalance)
	require.Equal(t, account.WalletID, response.WalletInfo.WalletID)
	require.Equal(t, "USD", response.WalletInfo.Currency)
	require.Equal(t, "users:client-CL-2026-000017-CUR-USD-001:wallets:USD:available", response.WalletInfo.AvailableAddress)
	require.Equal(t, "users:client-CL-2026-000017-CUR-USD-001:wallets:USD:lien", response.WalletInfo.LienAddress)
}

func TestCreditAccount(t *testing.T) {
	accountService, accountRepo, _, productRepo, dailyUsageRepo := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-001",
		Name:     "Savings USD",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowCredits: true,
			AllowDebits:  true,
			MinBalance:   "0",
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000003",
		ClientID:      uuid.New(),
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000012-SAV-USD-001",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000012-SAV-USD-001:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(200),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			require.Equal(t, "credit-ref-1", params.Input.RunScript.Reference)
			return &ledger.Log{}, &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction().
					WithPostings(ledger.NewPosting("system:control:USD", "users:client-CL-2026-000012-SAV-USD-001:wallets:USD:available", "USD/2", big.NewInt(50))).
					WithPostCommitVolumes(ledger.PostCommitVolumes{
						"users:client-CL-2026-000012-SAV-USD-001:wallets:USD:available": {
							"USD/2": ledger.NewVolumesInt64(250, 0),
						},
					}),
			}, false, nil
		})

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+account.ID.String()+"/credit", api.Buffer(t, WalletTransactionRequest{
		Amount:    json.Number("50"),
		Reference: "credit-ref-1",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
	require.True(t, ok)
	require.EqualValues(t, 200, response.BalanceBefore)
	require.EqualValues(t, 250, response.BalanceAfter)

	usage, err := dailyUsageRepo.GetForDate(context.Background(), account.ID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "50", usage.CreditAmount.String())
	require.Equal(t, int64(1), usage.CreditCount)
}

func TestDebitAccount(t *testing.T) {
	accountService, accountRepo, _, productRepo, dailyUsageRepo := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "CUR-USD-001",
		Name:     "Current USD",
		Category: "current",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowCredits: true,
			AllowDebits:  true,
			MinBalance:   "0",
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000004",
		ClientID:      uuid.New(),
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000013-CUR-USD-001",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000013-CUR-USD-001:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(200),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			require.Equal(t, "debit-ref-1", params.Input.RunScript.Reference)
			return &ledger.Log{}, &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction().
					WithPostings(ledger.NewPosting("users:client-CL-2026-000013-CUR-USD-001:wallets:USD:available", "system:control:USD", "USD/2", big.NewInt(75))).
					WithPostCommitVolumes(ledger.PostCommitVolumes{
						"users:client-CL-2026-000013-CUR-USD-001:wallets:USD:available": {
							"USD/2": ledger.NewVolumesInt64(200, 75),
						},
					}),
			}, false, nil
		})

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+account.ID.String()+"/debit", api.Buffer(t, WalletTransactionRequest{
		Amount:    json.Number("75"),
		Reference: "debit-ref-1",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
	require.True(t, ok)
	require.EqualValues(t, 200, response.BalanceBefore)
	require.EqualValues(t, 125, response.BalanceAfter)

	usage, err := dailyUsageRepo.GetForDate(context.Background(), account.ID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "75", usage.DebitAmount.String())
	require.Equal(t, int64(1), usage.DebitCount)
}

func TestDebitAccountRejectsFrozenAccount(t *testing.T) {
	accountService, accountRepo, _, productRepo, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "CUR-USD-002",
		Name:     "Current USD 2",
		Category: "current",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowCredits: true,
			AllowDebits:  true,
			MinBalance:   "0",
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000005",
		ClientID:      uuid.New(),
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000014-CUR-USD-002",
		FreezeDebits:  true,
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000014-CUR-USD-002:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(200),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+account.ID.String()+"/debit", api.Buffer(t, WalletTransactionRequest{
		Amount:    json.Number("20"),
		Reference: "debit-ref-2",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	err := api.ErrorResponse{}
	api.Decode(t, rec.Body, &err)
	require.EqualValues(t, common.ErrValidation, err.ErrorCode)
}

func TestLienAccount(t *testing.T) {
	accountService, accountRepo, _, productRepo, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-003",
		Name:     "Savings USD 3",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowCredits: true,
			AllowDebits:  true,
			MinBalance:   "0",
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000006",
		ClientID:      uuid.New(),
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000015-SAV-USD-003",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000015-SAV-USD-003:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(300),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			require.Equal(t, "lien-ref-1", params.Input.RunScript.Reference)
			return &ledger.Log{}, &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction().
					WithPostings(ledger.NewPosting("users:client-CL-2026-000015-SAV-USD-003:wallets:USD:available", "users:client-CL-2026-000015-SAV-USD-003:wallets:USD:lien", "USD/2", big.NewInt(100))).
					WithPostCommitVolumes(ledger.PostCommitVolumes{
						"users:client-CL-2026-000015-SAV-USD-003:wallets:USD:available": {
							"USD/2": ledger.NewVolumesInt64(300, 100),
						},
						"users:client-CL-2026-000015-SAV-USD-003:wallets:USD:lien": {
							"USD/2": ledger.NewVolumesInt64(100, 0),
						},
					}),
			}, false, nil
		})

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+account.ID.String()+"/lien", api.Buffer(t, WalletTransactionRequest{
		Amount:    json.Number("100"),
		Reference: "lien-ref-1",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
	require.True(t, ok)
	require.EqualValues(t, 300, response.BalanceBefore)
	require.EqualValues(t, 200, response.BalanceAfter)
}

func TestReleaseAccountLien(t *testing.T) {
	accountService, accountRepo, _, productRepo, _ := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-004",
		Name:     "Savings USD 4",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowCredits: true,
			AllowDebits:  true,
			MinBalance:   "0",
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000007",
		ClientID:      uuid.New(),
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000016-SAV-USD-004",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			require.Equal(t, "release-ref-1", params.Input.RunScript.Reference)
			return &ledger.Log{}, &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction().
					WithPostings(ledger.NewPosting("users:client-CL-2026-000016-SAV-USD-004:wallets:USD:lien", "users:client-CL-2026-000016-SAV-USD-004:wallets:USD:available", "USD/2", big.NewInt(80))).
					WithPostCommitVolumes(ledger.PostCommitVolumes{
						"users:client-CL-2026-000016-SAV-USD-004:wallets:USD:available": {
							"USD/2": ledger.NewVolumesInt64(180, 0),
						},
						"users:client-CL-2026-000016-SAV-USD-004:wallets:USD:lien": {
							"USD/2": ledger.NewVolumesInt64(80, 80),
						},
					}),
			}, false, nil
		})

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+account.ID.String()+"/lien/release", api.Buffer(t, ReleaseLienRequest{
		Amount:    json.Number("80"),
		Reference: "release-ref-1",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
	require.True(t, ok)
	require.EqualValues(t, 100, response.BalanceBefore)
	require.EqualValues(t, 180, response.BalanceAfter)
}

func TestDebitAccountRejectsDailyLimitExceeded(t *testing.T) {
	accountService, accountRepo, _, productRepo, dailyUsageRepo := newAccountServiceForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithAccountService(accountService))

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "CUR-USD-011",
		Name:     "Current USD",
		Category: "current",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowCredits: true,
			AllowDebits:  true,
			MinBalance:   "0",
			TransactionLimits: &models.TransactionLimits{
				DailyDebitLimit: strPtrHTTP("100"),
			},
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0000000009",
		ClientID:      uuid.New(),
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000018-CUR-USD-011",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))
	require.NoError(t, dailyUsageRepo.Create(context.Background(), &models.AccountDailyUsage{
		AccountID:    account.ID,
		UsageDate:    time.Now().UTC(),
		DebitAmount:  decimal.RequireFromString("90"),
		CreditAmount: decimal.Zero,
	}))

	expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
		Data: []ledger.VolumesWithBalanceByAssetByAccount{
			{
				Account: "users:client-CL-2026-000018-CUR-USD-011:wallets:USD:available",
				Asset:   "USD/2",
				VolumesWithBalance: ledger.VolumesWithBalance{
					Input:   big.NewInt(0),
					Output:  big.NewInt(0),
					Balance: big.NewInt(200),
				},
			},
		},
	}
	ledgerController.EXPECT().
		GetVolumesWithBalances(gomock.Any(), gomock.Any()).
		Return(&expectedCursor, nil)

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/accounts/"+account.ID.String()+"/debit", api.Buffer(t, WalletTransactionRequest{
		Amount:    json.Number("20"),
		Reference: "debit-ref-daily-limit",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	err := api.ErrorResponse{}
	api.Decode(t, rec.Body, &err)
	require.EqualValues(t, common.ErrValidation, err.ErrorCode)
}

func strPtrHTTP(v string) *string {
	return &v
}
