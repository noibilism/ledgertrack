package v2

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"

	"github.com/formancehq/ledger/internal/cba/services"
)

// ErrChargeConfiguration is the error code for fee or tax configuration that cannot be posted
// (HTTP 422). The request is fine; the setup needs fixing.
const ErrChargeConfiguration = "CHARGE_CONFIGURATION"

// chargeSends posts the fee and tax lines of a charge from source to their GL accounts. They sit in
// the same transaction as the customer's own movement, so the customer leg, the fee and the tax commit
// together or not at all.
func chargeSends(asset, source string, plan *services.ChargePlan, unbounded bool) string {
	mode := ""
	if unbounded {
		mode = " allowing unbounded overdraft"
	}
	script := ""
	for _, line := range plan.Lines {
		script += fmt.Sprintf(`
		send [%s %d] (
			source = @%s%s
			destination = @%s
		)
	`, asset, line.AmountMinor, source, mode, line.Address)
	}
	return script
}

// withChargeMetadata records the charge on the transaction so the ledger says what was charged.
func withChargeMetadata(base metadata.Metadata, plan *services.ChargePlan) metadata.Metadata {
	out := metadata.Metadata{}
	for k, v := range base {
		out[k] = v
	}
	out["charge_event"] = plan.Event
	out["charge_fee_minor"] = strconv.FormatInt(plan.FeeMinor, 10)
	out["charge_tax_minor"] = strconv.FormatInt(plan.TaxMinor, 10)
	out["charge_fee_source"] = plan.FeeSource
	return out
}

func chargeResponse(plan *services.ChargePlan) map[string]any {
	return map[string]any{
		"event": plan.Event, "fee_minor": plan.FeeMinor, "tax_minor": plan.TaxMinor, "gross_minor": plan.GrossMinor,
		"fee_source": plan.FeeSource, "lines": plan.Lines,
	}
}

// planCharge resolves the charge of a request, or nil when charge_fees is not set or nothing is due.
// Charges apply on the ledgertrack ledger, where the GL accounts live.
func planCharge(r *http.Request, charges services.ChargeService, enabled bool, req services.ChargeRequest) (*services.ChargePlan, error) {
	if !enabled || charges == nil {
		return nil, nil
	}
	return charges.Plan(r.Context(), req)
}

func handleChargeError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, services.ErrChargeConfig) {
		api.WriteErrorResponse(w, http.StatusUnprocessableEntity, ErrChargeConfiguration, err)
		return
	}
	handleFeeConfigError(w, r, err)
}
