package v2

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

func createReserveRule(s services.ReserveRuleService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input services.CreateReserveRuleInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		rule, err := s.CreateRule(r.Context(), input)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Created(w, rule)
	}
}

func listReserveRules(s services.ReserveRuleService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := repositories.ReserveRuleFilter{}
		var err error
		if filter.ClientID, err = optionalUUID(q.Get("client_id"), "client_id"); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if filter.AccountID, err = optionalUUID(q.Get("account_id"), "account_id"); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if v := strings.ToLower(strings.TrimSpace(q.Get("status"))); v != "" {
			if v != models.ReserveStatusActive && v != models.ReserveStatusRevoked {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid status %q, expected %s or %s", v, models.ReserveStatusActive, models.ReserveStatusRevoked))
				return
			}
			filter.Status = &v
		}
		rules, err := s.ListRules(r.Context(), filter)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{"rules": rules})
	}
}

func readReserveRule(s services.ReserveRuleService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		rule, err := s.GetRule(r.Context(), id)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, rule)
	}
}

func revokeReserveRule(s services.ReserveRuleService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		rule, err := s.RevokeRule(r.Context(), id)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, rule)
	}
}

type reservePreviewBody struct {
	AccountID   string     `json:"account_id"`
	AmountMinor int64      `json:"amount_minor"`
	CreditType  string     `json:"credit_type,omitempty"`
	At          *time.Time `json:"at,omitempty"`
}

// previewReserve shows how a credit would be split between held and available. It posts nothing.
func previewReserve(s services.ReserveRuleService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body reservePreviewBody
		if !decodeStrictJSON(w, r, &body) {
			return
		}
		accountID, err := uuid.Parse(strings.TrimSpace(body.AccountID))
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid account_id: %w", err))
			return
		}
		req := services.ReservePreviewRequest{AccountID: accountID, AmountMinor: body.AmountMinor, CreditType: body.CreditType}
		if body.At != nil {
			req.At = *body.At
		}
		preview, err := s.Preview(r.Context(), req)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, preview)
	}
}

func handleReserveError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrReserveValidation):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrReserveConflict):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case errors.Is(err, services.ErrReserveNotFound):
		api.NotFound(w, err)
	default:
		common.HandleCommonErrors(w, r, err)
	}
}
