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
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

// With CBA_SYSTEM_CONTROL_SHARDS > 1 the wallet credit and debit routes spread their counterpart over the
// control shards (same choice for a given wallet), like the CBA account routes. The default (unset or 1)
// keeps the single system:control:{CCY} account, which the other wallet tests assert.
func TestWalletCreditAndDebitUseTheSameControlShard(t *testing.T) {
	t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", "8")

	walletID := "shardy-USD"
	expected := systemControlAddress(walletID, "USD")
	require.True(t, strings.HasPrefix(expected, "system:control:USD:"), expected)

	for _, op := range []string{"credit", "debit"} {
		t.Run(op, func(t *testing.T) {
			systemController, ledgerController := newTestingSystemController(t, true)
			ledgerController.EXPECT().
				CreateTransaction(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
					require.Contains(t, params.Input.RunScript.Script.Plain, "@"+expected)
					require.NotContains(t, params.Input.RunScript.Script.Plain, "@system:control:USD ")
					return &ledger.Log{}, &ledger.CreatedTransaction{
						Transaction: ledger.NewTransaction().
							WithPostings(ledger.NewPosting(expected, "users:shardy:wallets:USD:available", "USD/2", big.NewInt(100))),
					}, false, nil
				})

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/%s", walletID, op),
				api.Buffer(t, WalletTransactionRequest{Amount: testJSONNumber("100"), Reference: "ref-" + op}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		})
	}
}
