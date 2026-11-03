package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/walletlock"
)

// defaultWalletLocker is the process wide wallet lock, shared with the transfers orchestrator
// (see package walletlock for why it exists).
var defaultWalletLocker = walletlock.Default

// createWalletTransaction runs ctrl.CreateTransaction while holding the in-process lock of the
// wallet. The lock is taken before any database transaction is opened, so waiting for it never
// holds a pooled connection.
func createWalletTransaction(
	r *http.Request,
	ctrl ledger.Controller,
	walletID string,
	params ledger.Parameters[ledger.CreateTransaction],
) (*ledgerinternal.Log, *ledgerinternal.CreatedTransaction, bool, error) {
	ctx := r.Context()
	unlock, err := defaultWalletLocker.Lock(ctx, walletlock.Key(chi.URLParam(r, "ledger"), walletID))
	if err != nil {
		return nil, nil, false, err
	}
	defer unlock()

	return ctrl.CreateTransaction(ctx, params)
}
