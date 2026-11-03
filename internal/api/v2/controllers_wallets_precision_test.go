package v2

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestParseAmountPrecision(t *testing.T) {
	for _, tc := range []struct {
		name     string
		amount   string
		currency string
		expected int64
		fails    bool
	}{
		{name: "usd integer is minor units", amount: "100", currency: "USD", expected: 100},
		{name: "usd decimal", amount: "1.50", currency: "USD", expected: 150},
		{name: "usd float trap", amount: "0.29", currency: "USD", expected: 29},
		{name: "usd rejects 3 decimals", amount: "1.505", currency: "USD", fails: true},
		{name: "usdt integer is minor units", amount: "100", currency: "USDT", expected: 100},
		{name: "usdt decimal", amount: "1.5", currency: "USDT", expected: 1_500_000},
		{name: "usdt smallest unit", amount: "0.000001", currency: "USDT", expected: 1},
		{name: "usdt rejects 7 decimals", amount: "0.0000001", currency: "USDT", fails: true},
		{name: "jpy decimal with zero fraction", amount: "5.0", currency: "JPY", expected: 5},
		{name: "jpy rejects fraction", amount: "5.5", currency: "JPY", fails: true},
		{name: "unknown currency", amount: "1.5", currency: "XYZ", fails: true},
		{name: "unknown currency integer", amount: "100", currency: "XYZ", fails: true},
		{name: "empty", amount: "", currency: "XYZ", expected: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseAmount(testJSONNumber(tc.amount), tc.currency)
			if tc.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestCreditWalletAssetPrecision(t *testing.T) {
	scriptFor := func(asset, code string, amount int64) string {
		return fmt.Sprintf("send [%s %d] (\n\t\t\tsource = @system:control:%s allowing unbounded overdraft\n\t\t\tdestination = @users:user123:wallets:%s:available\n\t\t)", asset, amount, code, code)
	}

	for _, tc := range []struct {
		name           string
		walletID       string
		amount         string
		expectedStatus int
		asset          string
		expectedScript string
	}{
		{
			name:           "usdt decimal amount uses 6dp asset",
			walletID:       "user123-USDT",
			amount:         "1.5",
			expectedStatus: http.StatusCreated,
			asset:          "USDT/6",
			expectedScript: scriptFor("USDT/6", "USDT", 1_500_000),
		},
		{
			name:           "usdt integer amount is minor units",
			walletID:       "user123-USDT",
			amount:         "1500000",
			expectedStatus: http.StatusCreated,
			asset:          "USDT/6",
			expectedScript: scriptFor("USDT/6", "USDT", 1_500_000),
		},
		{
			name:           "jpy uses 0dp asset",
			walletID:       "user123-JPY",
			amount:         "500",
			expectedStatus: http.StatusCreated,
			asset:          "JPY/0",
			expectedScript: scriptFor("JPY/0", "JPY", 500),
		},
		{
			name:           "usdt rejects 7 decimals",
			walletID:       "user123-USDT",
			amount:         "1.5000001",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "usd rejects 3 decimals",
			walletID:       "user123-USD",
			amount:         "1.505",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "unknown currency is rejected, no default precision",
			walletID:       "user123-XYZ",
			amount:         "100",
			expectedStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			systemController, ledgerController := newTestingSystemController(t, true)

			if tc.expectedStatus == http.StatusCreated {
				code := strings.SplitN(tc.asset, "/", 2)[0]
				accountUser := fmt.Sprintf("users:user123:wallets:%s:available", code)
				ledgerController.EXPECT().
					CreateTransaction(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
						require.Equal(t, tc.expectedScript, strings.TrimSpace(params.Input.RunScript.Script.Plain))
						return &ledger.Log{}, &ledger.CreatedTransaction{
							Transaction: ledger.NewTransaction().
								WithPostings(
									ledger.NewPosting("system:control:"+code, accountUser, tc.asset, big.NewInt(100)),
								).
								WithPostCommitVolumes(ledger.PostCommitVolumes{
									"system:control:" + code: {
										tc.asset: ledger.NewVolumesInt64(0, 100),
									},
									accountUser: {
										tc.asset: ledger.NewVolumesInt64(150, 0),
									},
								}),
						}, false, nil
					})
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/credit", tc.walletID), api.Buffer(t, WalletTransactionRequest{
				Amount:    testJSONNumber(tc.amount),
				Reference: "ref1",
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedStatus, rec.Code)
			if tc.expectedStatus == http.StatusCreated {
				response, ok := api.DecodeSingleResponse[walletTransactionResponse](t, rec.Body)
				require.True(t, ok)
				require.EqualValues(t, 150, response.BalanceAfter)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, common.ErrValidation, err.ErrorCode)
			}
		})
	}
}
