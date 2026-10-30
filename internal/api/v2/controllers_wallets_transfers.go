package v2

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
	"github.com/formancehq/ledger/internal/transfers"
)

// Error codes of the recoverable transfer flow. They are documented in
// docs/design/atomic-transfers.md.
const (
	// ErrTransferPendingCompletion: HTTP 500. The request was accepted and
	// recorded durably but did not finish. If the message says the principal
	// posted, the customer is already debited; the remaining legs are completed
	// automatically. The client must retry with the SAME reference.
	ErrTransferPendingCompletion = "TRANSFER_PENDING_COMPLETION"
	// ErrTransferInProgress: HTTP 409. Another request or the recovery worker is
	// driving this reference right now. Nothing was posted by this call.
	ErrTransferInProgress = "TRANSFER_IN_PROGRESS"
	// ErrTransferNeedsReview: HTTP 500. Automation gave up; manual action needed.
	ErrTransferNeedsReview = "TRANSFER_NEEDS_REVIEW"
)

// channelFees resolves the fee split the same way the handlers always did.
func channelFees(amount, channelAmount int64, computed *channelservices.ComputedChannelFees) (userFee, processingFee, netRevenue int64) {
	if computed != nil {
		return computed.UserFeeAmount, computed.ProcessingFee, computed.NetRevenueAmount
	}
	userFee = amount - channelAmount
	if userFee < 0 {
		userFee = 0
	}
	return userFee, 0, userFee
}

// runChannelTransfer executes a channel transfer through the durable
// orchestrator and renders the result. Response bodies keep the shape of the
// pre-existing handlers on success.
func runChannelTransfer(w http.ResponseWriter, r *http.Request, svc *transfers.Service, in transfers.Input) {
	in.UserLedger = chi.URLParam(r, "ledger")
	in.IdempotencyKey = r.Header.Get("Idempotency-Key")

	out, err := svc.Execute(r.Context(), in)
	if err != nil {
		writeTransferError(w, r, in.Kind, err)
		return
	}

	t := out.Transfer
	label := "wallet debit"
	if in.Kind == transfers.KindReleasePay {
		label = "lien release"
	}

	switch out.Status {
	case transfers.StatusCompleted:
		if out.Replayed {
			w.Header().Set("Idempotency-Hit", "true")
		}
		api.Created(w, t.Response(out.Warning))

	case transfers.StatusPending:
		api.WriteErrorResponse(w, http.StatusInternalServerError, ErrTransferPendingCompletion,
			errors.New(pendingMessage(label, t, out.Cause)))

	case transfers.StatusInProgress:
		api.WriteErrorResponse(w, http.StatusConflict, ErrTransferInProgress,
			fmt.Errorf("%s with reference %q is being processed by another request or by recovery; nothing was posted by this call. Retry with the same reference shortly", label, t.Reference))

	case transfers.StatusNeedsReview:
		api.WriteErrorResponse(w, http.StatusInternalServerError, ErrTransferNeedsReview,
			fmt.Errorf("%s with reference %q could not be completed automatically and needs manual review%s; do not retry with a new reference",
				label, t.Reference, postedSuffix(t)))

	default:
		common.InternalServerError(w, r, fmt.Errorf("unexpected transfer status %d", out.Status))
	}
}

func postedSuffix(t *transfers.Transfer) string {
	if t.UserTxID != nil {
		return fmt.Sprintf(" (the principal posted as txid %d)", *t.UserTxID)
	}
	return ""
}

func pendingMessage(label string, t *transfers.Transfer, cause error) string {
	reason := ""
	if cause != nil {
		reason = ": " + cause.Error()
	}
	if t.UserTxID != nil {
		return fmt.Sprintf(
			"the principal of the %s posted (txid %d, reference %q) but the transfer did not complete%s. "+
				"The remaining legs are pending and are completed automatically; retry with the same reference to check or advance it. Do not use a new reference.",
			label, *t.UserTxID, t.Reference, reason)
	}
	return fmt.Sprintf(
		"the outcome of the %s with reference %q could not be confirmed%s. It is recorded and will be completed or rejected automatically; "+
			"it may still be applied. Retry with the same reference. Do not use a new reference.",
		label, t.Reference, reason)
}

// writeTransferError maps errors raised before anything was posted exactly as
// the legacy handlers did.
func writeTransferError(w http.ResponseWriter, r *http.Request, kind transfers.Kind, err error) {
	lower := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, transfers.ErrReferenceConflict), errors.Is(err, transfers.ErrReferenceMismatch):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case kind == transfers.KindDebit && strings.Contains(lower, "insufficient fund"):
		api.WriteErrorResponse(w, http.StatusPaymentRequired, common.ErrInsufficientFund, err)
	case strings.Contains(lower, "conflict") || strings.Contains(lower, "duplicate reference"):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	default:
		common.HandleCommonWriteErrors(w, r, err)
	}
}
