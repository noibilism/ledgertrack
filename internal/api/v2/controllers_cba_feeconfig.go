package v2

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

func createFeeOverride(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input services.CreateFeeOverrideInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		override, err := s.CreateOverride(r.Context(), input)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Created(w, override)
	}
}

func listFeeOverrides(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := repositories.FeeOverrideFilter{}
		var err error
		if filter.ClientID, err = optionalUUID(q.Get("client_id"), "client_id"); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if filter.AccountID, err = optionalUUID(q.Get("account_id"), "account_id"); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if v := strings.ToLower(strings.TrimSpace(q.Get("event"))); v != "" {
			filter.Event = &v
		}
		if filter.Status, err = optionalFeeConfigStatus(q.Get("status")); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		overrides, err := s.ListOverrides(r.Context(), filter)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{"overrides": overrides})
	}
}

func readFeeOverride(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		override, err := s.GetOverride(r.Context(), id)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, override)
	}
}

func revokeFeeOverride(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		override, err := s.RevokeOverride(r.Context(), id)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, override)
	}
}

func createTaxRule(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input services.CreateTaxRuleInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		rule, err := s.CreateTaxRule(r.Context(), input)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Created(w, rule)
	}
}

func listTaxRules(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := repositories.TaxRuleFilter{}
		if v := strings.ToUpper(strings.TrimSpace(q.Get("country"))); v != "" {
			filter.Country = &v
		}
		if v := strings.ToLower(strings.TrimSpace(q.Get("event"))); v != "" {
			filter.Event = &v
		}
		status, err := optionalFeeConfigStatus(q.Get("status"))
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		filter.Status = status
		rules, err := s.ListTaxRules(r.Context(), filter)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{"rules": rules})
	}
}

func readTaxRule(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		rule, err := s.GetTaxRule(r.Context(), id)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, rule)
	}
}

func revokeTaxRule(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		rule, err := s.RevokeTaxRule(r.Context(), id)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, rule)
	}
}

type feeQuoteBody struct {
	AccountID   string     `json:"account_id"`
	Event       string     `json:"event"`
	AmountMinor int64      `json:"amount_minor"`
	At          *time.Time `json:"at,omitempty"`
}

// quoteFees previews the fee and tax for a transaction. It reads configuration only and posts nothing.
func quoteFees(s services.FeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body feeQuoteBody
		if !decodeStrictJSON(w, r, &body) {
			return
		}
		accountID, err := uuid.Parse(strings.TrimSpace(body.AccountID))
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid account_id: %w", err))
			return
		}
		req := services.FeeQuoteRequest{
			AccountID:   accountID,
			Event:       strings.ToLower(strings.TrimSpace(body.Event)),
			AmountMinor: body.AmountMinor,
		}
		if body.At != nil {
			req.At = *body.At
		}
		quote, err := s.Quote(r.Context(), req)
		if err != nil {
			handleFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, quote)
	}
}

func feeConfigID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(strings.TrimSpace(chi.URLParam(r, "id")))
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid id: %w", err)
	}
	return id, nil
}

func optionalUUID(raw, name string) (*uuid.UUID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid %s %q", name, raw)
	}
	return &id, nil
}

func optionalFeeConfigStatus(raw string) (*string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return nil, nil
	}
	if value != models.FeeConfigStatusActive && value != models.FeeConfigStatusRevoked {
		return nil, fmt.Errorf("invalid status %q, expected %s or %s", value, models.FeeConfigStatusActive, models.FeeConfigStatusRevoked)
	}
	return &value, nil
}

func handleFeeConfigError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrFeeConfigValidation),
		errors.Is(err, services.ErrTaxCountryUnknown),
		errors.Is(err, services.ErrTaxAccountMissing):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrFeeConfigConflict):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case errors.Is(err, services.ErrFeeConfigNotFound):
		api.NotFound(w, err)
	default:
		common.HandleCommonErrors(w, r, err)
	}
}
