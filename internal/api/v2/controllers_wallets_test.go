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

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func testJSONNumber(v string) json.Number {
	return json.Number(v)
}

type walletTransactionResponse struct {
	BalanceBefore int64 `json:"balance_before"`
	BalanceAfter  int64 `json:"balance_after"`
}

func TestListCurrencies(t *testing.T) {
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	req := httptest.NewRequest(http.MethodGet, "/test/currencies", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	resp, ok := api.DecodeSingleResponse[map[string][]map[string]any](t, rec.Body)
	require.True(t, ok)
	require.NotEmpty(t, resp["currencies"])

	var usdFound bool
	for _, currency := range resp["currencies"] {
		if currency["code"] == "USD" {
			usdFound = true
			require.EqualValues(t, 2, currency["precision"])
			require.EqualValues(t, true, currency["enabled"])
		}
	}
	require.True(t, usdFound)
}

func TestCreateWallet(t *testing.T) {
	type testCase struct {
		name               string
		payload            CreateWalletRequest
		expectedStatusCode int
		expectedErrorCode  string
		expectedResponse   map[string]string
	}

	testCases := []testCase{
		{
			name: "nominal",
			payload: CreateWalletRequest{
				UserID:   "user123",
				Currency: "USD",
			},
			expectedStatusCode: http.StatusCreated,
			expectedResponse: map[string]string{
				"walletID": "user123-USD",
				"userID":   "user123",
				"currency": "USD",
			},
		},
		{
			name: "missing userID",
			payload: CreateWalletRequest{
				Currency: "USD",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name: "missing currency",
			payload: CreateWalletRequest{
				UserID: "user123",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name: "unsupported currency",
			payload: CreateWalletRequest{
				UserID:   "user123",
				Currency: "XXX",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			systemController, _ := newTestingSystemController(t, true)
			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, "/test/wallets", api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
			if tc.expectedStatusCode == http.StatusCreated {
				resp, ok := api.DecodeSingleResponse[map[string]string](t, rec.Body)
				require.True(t, ok)
				require.Equal(t, tc.expectedResponse, resp)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestCreditWallet(t *testing.T) {
	type testCase struct {
		name                 string
		walletID             string
		payload              WalletTransactionRequest
		expectedStatusCode   int
		expectedErrorCode    string
		expectControllerCall bool
		expectedScript       string
	}

	testCases := []testCase{
		{
			name:     "nominal",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount:    testJSONNumber("100"),
				Reference: "ref1",
			},
			expectedStatusCode:   http.StatusCreated,
			expectControllerCall: true,
			expectedScript: `
		send [USD/2 100] (
			source = @system:control:USD allowing unbounded overdraft
			destination = @users:user123:wallets:USD:available
		)
	`,
		},
		{
			name:     "invalid wallet ID",
			walletID: "invalid",
			payload: WalletTransactionRequest{
				Amount:    testJSONNumber("100"),
				Reference: "ref1",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:     "negative amount",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount:    testJSONNumber("-100"),
				Reference: "ref1",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:     "missing reference",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount: testJSONNumber("100"),
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			systemController, ledgerController := newTestingSystemController(t, true)

			if tc.expectControllerCall {
				ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
						require.Equal(t, tc.payload.Reference, params.Input.RunScript.Reference)
						require.Equal(t, strings.TrimSpace(tc.expectedScript), strings.TrimSpace(params.Input.RunScript.Script.Plain))
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction().
								WithPostings(
									ledger.NewPosting("system:control:USD", "users:user123:wallets:USD:available", "USD/2", big.NewInt(100)),
								).
								WithPostCommitVolumes(ledger.PostCommitVolumes{
									"system:control:USD": {
										"USD/2": ledger.NewVolumesInt64(0, 100),
									},
									"users:user123:wallets:USD:available": {
										"USD/2": ledger.NewVolumesInt64(150, 0),
									},
								}),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/credit", tc.walletID), api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
			if tc.expectedStatusCode == http.StatusCreated {
				response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
				require.True(t, ok)
				require.EqualValues(t, 50, response.BalanceBefore)
				require.EqualValues(t, 150, response.BalanceAfter)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestDebitWallet(t *testing.T) {
	type testCase struct {
		name                 string
		walletID             string
		payload              WalletTransactionRequest
		expectedStatusCode   int
		expectedErrorCode    string
		expectControllerCall bool
		expectedScript       string
	}

	testCases := []testCase{
		{
			name:     "nominal",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount:    testJSONNumber("100"),
				Reference: "ref1",
			},
			expectedStatusCode:   http.StatusCreated,
			expectControllerCall: true,
			expectedScript: `
		send [USD/2 100] (
			source = @users:user123:wallets:USD:available
			destination = @system:control:USD
		)
	`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			systemController, ledgerController := newTestingSystemController(t, true)

			if tc.expectControllerCall {
				ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
						require.Equal(t, tc.payload.Reference, params.Input.RunScript.Reference)
						require.Equal(t, strings.TrimSpace(tc.expectedScript), strings.TrimSpace(params.Input.RunScript.Script.Plain))
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction().
								WithPostings(
									ledger.NewPosting("users:user123:wallets:USD:available", "system:control:USD", "USD/2", big.NewInt(100)),
								).
								WithPostCommitVolumes(ledger.PostCommitVolumes{
									"system:control:USD": {
										"USD/2": ledger.NewVolumesInt64(100, 0),
									},
									"users:user123:wallets:USD:available": {
										"USD/2": ledger.NewVolumesInt64(200, 100),
									},
								}),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/debit", tc.walletID), api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
			response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
			require.True(t, ok)
			require.EqualValues(t, 200, response.BalanceBefore)
			require.EqualValues(t, 100, response.BalanceAfter)
		})
	}
}

func TestLienWallet(t *testing.T) {
	type testCase struct {
		name                 string
		walletID             string
		payload              WalletTransactionRequest
		expectedStatusCode   int
		expectedErrorCode    string
		expectControllerCall bool
		expectedScript       string
	}

	testCases := []testCase{
		{
			name:     "nominal",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount:    testJSONNumber("100"),
				Reference: "ref1",
			},
			expectedStatusCode:   http.StatusCreated,
			expectControllerCall: true,
			expectedScript: `
		send [USD/2 100] (
			source = @users:user123:wallets:USD:available
			destination = @users:user123:wallets:USD:lien
		)
	`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			systemController, ledgerController := newTestingSystemController(t, true)

			if tc.expectControllerCall {
				ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
						require.Equal(t, tc.payload.Reference, params.Input.RunScript.Reference)
						require.Equal(t, strings.TrimSpace(tc.expectedScript), strings.TrimSpace(params.Input.RunScript.Script.Plain))
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction().
								WithPostings(
									ledger.NewPosting("users:user123:wallets:USD:available", "users:user123:wallets:USD:lien", "USD/2", big.NewInt(100)),
								).
								WithPostCommitVolumes(ledger.PostCommitVolumes{
									"users:user123:wallets:USD:available": {
										"USD/2": ledger.NewVolumesInt64(250, 100),
									},
									"users:user123:wallets:USD:lien": {
										"USD/2": ledger.NewVolumesInt64(100, 0),
									},
								}),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/lien", tc.walletID), api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
			response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
			require.True(t, ok)
			require.EqualValues(t, 250, response.BalanceBefore)
			require.EqualValues(t, 150, response.BalanceAfter)
		})
	}
}

func TestReleaseLien(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, true)

	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			require.Equal(t, "ref1", params.Input.RunScript.Reference)
			require.Equal(t, strings.TrimSpace(`
				send [USD/2 100] (
					source = @users:user123:wallets:USD:lien
					destination = @users:user123:wallets:USD:available
				)
			`), strings.TrimSpace(params.Input.RunScript.Script.Plain))
			return &ledger.Log{}, &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction().
					WithPostings(
						ledger.NewPosting("users:user123:wallets:USD:lien", "users:user123:wallets:USD:available", "USD/2", big.NewInt(100)),
					).
					WithPostCommitVolumes(ledger.PostCommitVolumes{
						"users:user123:wallets:USD:lien": {
							"USD/2": ledger.NewVolumesInt64(100, 100),
						},
						"users:user123:wallets:USD:available": {
							"USD/2": ledger.NewVolumesInt64(175, 0),
						},
					}),
			}, false, nil
		})

	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	req := httptest.NewRequest(http.MethodPost, "/test/wallets/user123-USD/lien/release", api.Buffer(t, ReleaseLienRequest{
		Amount:    testJSONNumber("100"),
		Reference: "ref1",
	}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)

	response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
	require.True(t, ok)
	require.EqualValues(t, 75, response.BalanceBefore)
	require.EqualValues(t, 175, response.BalanceAfter)
}

func TestGetWalletBalances(t *testing.T) {
	type balance struct {
		Currency string `json:"currency"`
		Amount   int64  `json:"amount"`
	}
	type walletBalancesResponse struct {
		Balances []balance `json:"balances"`
	}

	t.Run("returns balances per currency", func(t *testing.T) {
		systemController, ledgerController := newTestingSystemController(t, true)
		ledgerController.EXPECT().
			GetAggregatedBalances(gomock.Any(), gomock.Any()).
			Return(ledger.BalancesByAssets{
				"USD/2": big.NewInt(150),
				"EUR/2": big.NewInt(90),
			}, nil)

		router := NewRouter(systemController, auth.NewNoAuth(), "develop")
		req := httptest.NewRequest(http.MethodGet, "/test/wallets/balances?userID=user123", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		response, ok := api.DecodeSingleResponse[walletBalancesResponse](t, rec.Body)
		require.True(t, ok)
		require.Len(t, response.Balances, 2)
		require.ElementsMatch(t, []balance{
			{Currency: "USD", Amount: 150},
			{Currency: "EUR", Amount: 90},
		}, response.Balances)
	})

	t.Run("returns empty slice instead of null", func(t *testing.T) {
		systemController, ledgerController := newTestingSystemController(t, true)
		ledgerController.EXPECT().
			GetAggregatedBalances(gomock.Any(), gomock.Any()).
			Return(ledger.BalancesByAssets{}, nil)

		router := NewRouter(systemController, auth.NewNoAuth(), "develop")
		req := httptest.NewRequest(http.MethodGet, "/test/wallets/balances?userID=user123&currency=USD", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		response, ok := api.DecodeSingleResponse[walletBalancesResponse](t, rec.Body)
		require.True(t, ok)
		require.NotNil(t, response.Balances)
		require.Len(t, response.Balances, 0)
	})
}
