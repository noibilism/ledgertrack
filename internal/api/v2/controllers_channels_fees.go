package v2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	channelrepos "github.com/formancehq/ledger/internal/channels/repositories"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
	"github.com/go-chi/chi/v5"
)

type upsertChannelFeeConfigRequest struct {
	Currency      string                  `json:"currency"`
	Enabled       *bool                   `json:"enabled,omitempty"`
	UserFee       channelmodels.FeeStructure `json:"user_fee"`
	ProcessingFee channelmodels.FeeStructure `json:"processing_fee"`
	Actor         *string                 `json:"actor,omitempty"`
}

func getChannelFeeConfig(channelFeeConfigService channelservices.ChannelFeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelID := chi.URLParam(r, "channelID")

		cfg, err := channelFeeConfigService.GetConfig(r.Context(), channelID)
		if err != nil {
			handleChannelFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, cfg)
	}
}

func upsertChannelFeeConfig(channelFeeConfigService channelservices.ChannelFeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelID := chi.URLParam(r, "channelID")

		var req upsertChannelFeeConfigRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}

		cfg, err := channelFeeConfigService.UpsertConfig(r.Context(), channelservices.UpsertChannelFeeConfigRequest{
			ChannelID:     channelID,
			Currency:      req.Currency,
			Enabled:       enabled,
			UserFee:       req.UserFee,
			ProcessingFee: req.ProcessingFee,
			Actor:         req.Actor,
		})
		if err != nil {
			handleChannelFeeConfigError(w, r, err)
			return
		}

		api.Ok(w, cfg)
	}
}

func listChannelFeeConfigAudits(channelFeeConfigService channelservices.ChannelFeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelID := chi.URLParam(r, "channelID")

		limit := 50
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			i, err := strconv.Atoi(v)
			if err != nil || i < 0 {
				api.BadRequest(w, common.ErrValidation, errors.New("invalid limit"))
				return
			}
			limit = i
		}

		audits, err := channelFeeConfigService.ListAudits(r.Context(), channelID, limit)
		if err != nil {
			handleChannelFeeConfigError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"audits": audits,
		})
	}
}

func listChannelFeeConfigs(channelFeeConfigService channelservices.ChannelFeeConfigService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var filter channelrepos.ChannelFeeConfigFilter

		if currency := strings.TrimSpace(r.URL.Query().Get("currency")); currency != "" {
			c := strings.ToUpper(currency)
			filter.Currency = &c
		}

		if enabledStr := strings.TrimSpace(r.URL.Query().Get("enabled")); enabledStr != "" {
			v, err := strconv.ParseBool(enabledStr)
			if err != nil {
				api.BadRequest(w, common.ErrValidation, errors.New("invalid enabled"))
				return
			}
			filter.Enabled = &v
		}

		cfgs, err := channelFeeConfigService.ListConfigs(r.Context(), filter)
		if err != nil {
			handleChannelFeeConfigError(w, r, err)
			return
		}

		api.Ok(w, map[string]any{
			"configs": cfgs,
		})
	}
}

func handleChannelFeeConfigError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, channelservices.ErrChannelFeesNotFound):
		api.NotFound(w, err)
	case strings.Contains(err.Error(), channelservices.ErrChannelFeesValidation.Error()):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, channelservices.ErrChannelFeesValidation):
		api.BadRequest(w, common.ErrValidation, err)
	default:
		common.InternalServerError(w, r, err)
	}
}

