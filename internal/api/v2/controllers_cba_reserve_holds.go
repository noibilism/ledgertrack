package v2

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

var holdStatuses = map[string]bool{
	models.HoldStatusPending: true, models.HoldStatusHeld: true, models.HoldStatusReleased: true,
	models.HoldStatusVoided: true, models.HoldStatusNeedsReview: true,
}

func listReserveHolds(s services.ReserveHoldService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := repositories.ReserveHoldFilter{}
		var err error
		if filter.AccountID, err = optionalUUID(q.Get("account_id"), "account_id"); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if filter.ClientID, err = optionalUUID(q.Get("client_id"), "client_id"); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if v := strings.TrimSpace(q.Get("wallet_id")); v != "" {
			filter.WalletID = &v
		}
		if v := strings.ToLower(strings.TrimSpace(q.Get("status"))); v != "" {
			if !holdStatuses[v] {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid status %q", v))
				return
			}
			filter.Status = &v
		}
		if v := strings.TrimSpace(q.Get("due_before")); v != "" {
			at, err := time.Parse(time.RFC3339, v)
			if err != nil {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid due_before %q, expected RFC 3339", v))
				return
			}
			filter.DueBefore = &at
		}
		if v := strings.TrimSpace(q.Get("limit")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 500 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("limit must be between 1 and 500"))
				return
			}
			filter.Limit = n
		}
		holds, err := s.ListHolds(r.Context(), filter)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{"holds": holds})
	}
}

func readReserveHold(s services.ReserveHoldService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		hold, err := s.GetHold(r.Context(), id)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, hold)
	}
}

// releaseReserveHold releases a held share now, ahead of its SLA.
func releaseReserveHold(s services.ReserveHoldService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		hold, err := s.ReleaseHold(r.Context(), id, "")
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, hold)
	}
}
