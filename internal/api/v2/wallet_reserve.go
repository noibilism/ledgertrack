package v2

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/services"
)

// reserveLedgerName is the only ledger where reserve rules apply.
const reserveLedgerName = "ledgertrack"

// reserveCreditScript credits a wallet and holds the reserve share in its lien in one transaction,
// so the split is atomic. A full hold (100%) posts only the lien leg.
func reserveCreditScript(asset, source, available, lien string, plan *services.CreditPlan) string {
	script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, asset, plan.ReserveMinor, source, lien)
	if plan.AvailableMinor > 0 {
		script += fmt.Sprintf(`
		send [%s %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, asset, plan.AvailableMinor, source, available)
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
