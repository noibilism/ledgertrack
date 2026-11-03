// Package reserveledger adapts the ledger controllers to services.ReserveLedger.
package reserveledger

import (
	"context"
	"fmt"
	"strings"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/query"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/walletaddr"
)

type Ledger struct {
	system systemcontroller.Controller
}

var _ services.ReserveLedger = (*Ledger)(nil)

func New(system systemcontroller.Controller) *Ledger {
	return &Ledger{system: system}
}

// Release moves the held share from the wallet lien to its available balance. The reference makes it
// idempotent: a duplicate reference means the release already happened.
func (l *Ledger) Release(ctx context.Context, hold models.ReserveHold, reference string) error {
	asset, err := currencyregistry.AssetFor(hold.Currency)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, asset, hold.AmountMinor, walletaddr.Lien(hold.WalletID, hold.Currency), walletaddr.Available(hold.WalletID, hold.Currency))

	ctrl, err := l.system.GetLedgerController(ctx, hold.Ledger)
	if err != nil {
		return err
	}
	_, _, _, err = ctrl.CreateTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
		Input: ledgercontroller.CreateTransaction{
			RunScript: vm.RunScript{
				Script:    vm.Script{Plain: script},
				Reference: reference,
				Metadata: metadata.Metadata{
					"reserve_hold_id":       hold.ID.String(),
					"reserve_credit_ref":    hold.CreditReference,
					"reserve_release_stage": "sla_or_manual",
				},
			},
			Runtime: ledgerinternal.RuntimeMachine,
		},
	})
	if isDuplicateReference(err) {
		return nil
	}
	return err
}

func (l *Ledger) FindCredit(ctx context.Context, ledgerName, reference string) (int64, bool, error) {
	ctrl, err := l.system.GetLedgerController(ctx, ledgerName)
	if err != nil {
		return 0, false, err
	}
	tx, err := ctrl.GetTransaction(ctx, storagecommon.ResourceQuery[any]{
		Builder: query.Match("reference", reference),
	})
	if err != nil {
		if postgres.IsNotFoundError(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if tx == nil || tx.ID == nil {
		return 0, false, nil
	}
	return int64(*tx.ID), true, nil
}

func isDuplicateReference(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "conflict") || strings.Contains(message, "duplicate reference")
}
