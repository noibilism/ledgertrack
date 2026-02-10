package v2

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

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
				Amount:    100,
				Reference: "ref1",
			},
			expectedStatusCode:   http.StatusCreated,
			expectControllerCall: true,
			expectedScript: `
		send [USD/2 100] (
			source = @system:control:USD
			destination = @users:user123:wallets:USD:available
		)
	`,
		},
		{
			name:     "invalid wallet ID",
			walletID: "invalid",
			payload: WalletTransactionRequest{
				Amount:    100,
				Reference: "ref1",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:     "negative amount",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount:    -100,
				Reference: "ref1",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
		},
		{
			name:     "missing reference",
			walletID: "user123-USD",
			payload: WalletTransactionRequest{
				Amount: 100,
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
						require.Equal(t, tc.expectedScript, params.Input.RunScript.Script.Plain)
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction().WithPostings(
								ledger.NewPosting("system:control:USD", "users:user123:wallets:USD:available", "USD/2", big.NewInt(100)),
							),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/credit", tc.walletID), api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
			if tc.expectedStatusCode != http.StatusCreated {
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
				Amount:    100,
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
						require.Equal(t, tc.expectedScript, params.Input.RunScript.Script.Plain)
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction(),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/debit", tc.walletID), api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
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
				Amount:    100,
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
						require.Equal(t, tc.expectedScript, params.Input.RunScript.Script.Plain)
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction(),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/lien", tc.walletID), api.Buffer(t, tc.payload))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatusCode, rec.Code)
		})
	}
}
