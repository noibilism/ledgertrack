package v2

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/services"
)

// reserveLedgerName is the only ledger where reserve rules apply.
const reserveLedgerName = "ledgertrack"

// reserveCreditScript credits a wallet and holds the reserve share in its lien in one transaction,
// so the split is atomic. A full hold (100%) posts only the lien leg. unbounded lets the source go
// negative (a system control account); a customer source must not.
func reserveCreditScript(asset, source, available, lien string, unbounded bool, plan *services.CreditPlan) string {
	mode := ""
	if unbounded {
		mode = " allowing unbounded overdraft"
	}
	script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s%s
			destination = @%s
		)
	`, asset, plan.ReserveMinor, source, mode, lien)
	if plan.AvailableMinor > 0 {
		script += fmt.Sprintf(`
		send [%s %d] (
			source = @%s%s
			destination = @%s
		)
	`, asset, plan.AvailableMinor, source, mode, available)
	}
	return script
}

// withReserveMetadata adds the hold to the transaction metadata so the ledger itself says what was held.
func withReserveMetadata(base metadata.Metadata, plan *services.CreditPlan) metadata.Metadata {
	out := metadata.Metadata{}
	for k, v := range base {
		out[k] = v
	}
	out["reserve_hold_id"] = plan.Hold.ID.String()
	out["reserve_minor"] = strconv.FormatInt(plan.ReserveMinor, 10)
	out["reserve_release_at"] = plan.Hold.ReleaseAt.UTC().Format(time.RFC3339)
	return out
}

// confirmReserveHold records that the credit is in the ledger. If this fails the credit is not failed:
// the release worker settles the hold from the ledger.
func confirmReserveHold(ctx context.Context, holds services.ReserveHoldService, plan *services.CreditPlan, tx *ledgerinternal.CreatedTransaction) {
	if tx == nil || tx.Transaction.ID == nil {
		return
	}
	if err := holds.ConfirmCredit(ctx, plan.Hold.ID, int64(*tx.Transaction.ID)); err != nil {
		fmt.Fprintf(os.Stderr, "reserve: confirming hold %s: %v\n", plan.Hold.ID, err)
	}
}

func reserveResponse(plan *services.CreditPlan) map[string]any {
	return map[string]any{
		"hold_id":         plan.Hold.ID,
		"reserve_minor":   plan.ReserveMinor,
		"available_minor": plan.AvailableMinor,
		"release_at":      plan.Hold.ReleaseAt.UTC(),
	}
}

// reserveDebitScript spends the held shares in the lien first and the rest from available, in one
// transaction. A debit that the held shares cover entirely posts only the lien leg.
func reserveDebitScript(asset, available, lien, destination string, plan *services.DrawPlan) string {
	script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, asset, plan.DrawnMinor, lien, destination)
	if plan.RemainderMinor > 0 {
		script += fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, asset, plan.RemainderMinor, available, destination)
	}
	return script
}

// withDrawMetadata records on the transaction how much of the debit came from the reserve.
func withDrawMetadata(base metadata.Metadata, plan *services.DrawPlan) metadata.Metadata {
	out := metadata.Metadata{}
	for k, v := range base {
		out[k] = v
	}
	out["reserve_drawn_minor"] = strconv.FormatInt(plan.DrawnMinor, 10)
	return out
}

// settleReserveDraw tells the register how the debit ended. A posted debit confirms the draw and a
// refused one (not enough funds) gives the held shares back. Any other outcome is unknown, so the
// draw stays pending and the release job settles it from the ledger.
func settleReserveDraw(ctx context.Context, holds services.ReserveHoldService, ledger, walletID, reference string, err error) {
	var serr error
	switch {
	case err == nil:
		serr = holds.ConfirmDraw(ctx, ledger, walletID, reference)
	case strings.Contains(strings.ToLower(err.Error()), "insufficient fund"):
		serr = holds.RevertDraw(ctx, ledger, walletID, reference)
	default:
		return
	}
	if serr != nil {
		fmt.Fprintf(os.Stderr, "reserve: settling draw %s: %v\n", reference, serr)
	}
}

func drawResponse(plan *services.DrawPlan) map[string]any {
	return map[string]any{"drawn_minor": plan.DrawnMinor, "available_minor": plan.RemainderMinor}
}
