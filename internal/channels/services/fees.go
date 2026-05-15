package services

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	currencyregistry "github.com/formancehq/ledger/internal/currency"

	"github.com/formancehq/ledger/internal/channels/models"
	"github.com/formancehq/ledger/internal/channels/repositories"
)

var (
	ErrChannelFeesValidation = errors.New("channel fees validation failed")
	ErrChannelFeesNotFound   = errors.New("channel fees not found")
)

type ChannelFeeConfigService interface {
	UpsertConfig(context.Context, UpsertChannelFeeConfigRequest) (*models.ChannelFeeConfig, error)
	GetConfig(context.Context, string) (*models.ChannelFeeConfig, error)
	ListConfigs(context.Context, repositories.ChannelFeeConfigFilter) ([]models.ChannelFeeConfig, error)
	Compute(context.Context, ComputeChannelFeesRequest) (*ComputedChannelFees, error)
	Record(context.Context, *models.ChannelFeeRecord) error
	ListAudits(context.Context, string, int) ([]models.ChannelFeeConfigAudit, error)
}

type UpsertChannelFeeConfigRequest struct {
	ChannelID     string
	Currency      string
	Enabled       bool
	UserFee       models.FeeStructure
	ProcessingFee models.FeeStructure
	Actor         *string
}

type ComputeChannelFeesRequest struct {
	ChannelID       string
	Currency        string
	PrincipalAmount int64
	TotalAmount     *int64
}

type ComputedChannelFees struct {
	Currency         string `json:"currency"`
	ChannelID        string `json:"channel_id"`
	PrincipalAmount  int64  `json:"principal_amount"`
	UserFeeAmount    int64  `json:"user_fee_amount"`
	ProcessingFee    int64  `json:"processing_fee_amount"`
	TotalAmount      int64  `json:"total_amount"`
	NetRevenueAmount int64  `json:"net_revenue_amount"`
}

type DefaultChannelFeeConfigService struct {
	configRepo repositories.ChannelFeeConfigRepository
	auditRepo  repositories.ChannelFeeConfigAuditRepository
	recordRepo repositories.ChannelFeeRecordRepository
}

func NewChannelFeeConfigService(
	configRepo repositories.ChannelFeeConfigRepository,
	auditRepo repositories.ChannelFeeConfigAuditRepository,
	recordRepo repositories.ChannelFeeRecordRepository,
) ChannelFeeConfigService {
	return &DefaultChannelFeeConfigService{
		configRepo: configRepo,
		auditRepo:  auditRepo,
		recordRepo: recordRepo,
	}
}

func (s *DefaultChannelFeeConfigService) UpsertConfig(ctx context.Context, req UpsertChannelFeeConfigRequest) (*models.ChannelFeeConfig, error) {
	if strings.TrimSpace(req.ChannelID) == "" {
		return nil, fmt.Errorf("%w: channel_id is required", ErrChannelFeesValidation)
	}
	req.Currency = strings.ToUpper(strings.TrimSpace(req.Currency))
	if req.Currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrChannelFeesValidation)
	}

	var before map[string]any
	existing, err := s.configRepo.GetByChannelID(ctx, req.ChannelID)
	if err == nil && existing != nil {
		before = map[string]any{
			"channel_id":      existing.ChannelID,
			"currency":        existing.Currency,
			"enabled":         existing.Enabled,
			"user_fee":        existing.UserFee,
			"processing_fee":  existing.ProcessingFee,
			"updated_at":      existing.UpdatedAt,
		}
	}

	cfg := &models.ChannelFeeConfig{
		ChannelID:     req.ChannelID,
		Currency:      req.Currency,
		Enabled:       req.Enabled,
		UserFee:       req.UserFee,
		ProcessingFee: req.ProcessingFee,
	}
	if existing != nil && err == nil {
		cfg.ID = existing.ID
		cfg.CreatedAt = existing.CreatedAt
	}

	if err := s.configRepo.Upsert(ctx, cfg); err != nil {
		return nil, err
	}

	after := map[string]any{
		"channel_id":      cfg.ChannelID,
		"currency":        cfg.Currency,
		"enabled":         cfg.Enabled,
		"user_fee":        cfg.UserFee,
		"processing_fee":  cfg.ProcessingFee,
	}
	_ = s.auditRepo.Create(ctx, &models.ChannelFeeConfigAudit{
		ChannelID: cfg.ChannelID,
		Actor:     req.Actor,
		Action:    "upsert",
		Before:    before,
		After:     after,
		CreatedAt: time.Now().UTC(),
	})

	return cfg, nil
}

func (s *DefaultChannelFeeConfigService) GetConfig(ctx context.Context, channelID string) (*models.ChannelFeeConfig, error) {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", ErrChannelFeesValidation)
	}
	cfg, err := s.configRepo.GetByChannelID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (s *DefaultChannelFeeConfigService) ListConfigs(ctx context.Context, filter repositories.ChannelFeeConfigFilter) ([]models.ChannelFeeConfig, error) {
	return s.configRepo.List(ctx, filter)
}

func (s *DefaultChannelFeeConfigService) ListAudits(ctx context.Context, channelID string, limit int) ([]models.ChannelFeeConfigAudit, error) {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", ErrChannelFeesValidation)
	}
	return s.auditRepo.ListByChannelID(ctx, channelID, limit)
}

func (s *DefaultChannelFeeConfigService) Compute(ctx context.Context, req ComputeChannelFeesRequest) (*ComputedChannelFees, error) {
	channelID := strings.TrimSpace(req.ChannelID)
	if channelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", ErrChannelFeesValidation)
	}
	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	if currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrChannelFeesValidation)
	}
	if req.PrincipalAmount <= 0 {
		return nil, fmt.Errorf("%w: principal_amount must be positive", ErrChannelFeesValidation)
	}

	cfg, err := s.configRepo.GetByChannelID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if cfg == nil || !cfg.Enabled {
		total := req.PrincipalAmount
		if req.TotalAmount != nil && *req.TotalAmount > 0 {
			total = *req.TotalAmount
		}
		userFee := total - req.PrincipalAmount
		if userFee < 0 {
			userFee = 0
		}
		return &ComputedChannelFees{
			Currency:         currency,
			ChannelID:        channelID,
			PrincipalAmount:  req.PrincipalAmount,
			UserFeeAmount:    userFee,
			ProcessingFee:    0,
			TotalAmount:      total,
			NetRevenueAmount: userFee,
		}, nil
	}

	if strings.ToUpper(cfg.Currency) != currency {
		return nil, fmt.Errorf("%w: config currency mismatch", ErrChannelFeesValidation)
	}

	userFee, err := computeFeeAmount(cfg.UserFee, req.PrincipalAmount, currency)
	if err != nil {
		return nil, err
	}
	processingFee, err := computeFeeAmount(cfg.ProcessingFee, req.PrincipalAmount, currency)
	if err != nil {
		return nil, err
	}

	total := req.PrincipalAmount + userFee
	if req.TotalAmount != nil && *req.TotalAmount > 0 {
		if *req.TotalAmount != total {
			return nil, fmt.Errorf("%w: total_amount must equal principal + user_fee (expected %d)", ErrChannelFeesValidation, total)
		}
	}

	net := userFee - processingFee
	return &ComputedChannelFees{
		Currency:         currency,
		ChannelID:        channelID,
		PrincipalAmount:  req.PrincipalAmount,
		UserFeeAmount:    userFee,
		ProcessingFee:    processingFee,
		TotalAmount:      total,
		NetRevenueAmount: net,
	}, nil
}

func (s *DefaultChannelFeeConfigService) Record(ctx context.Context, rec *models.ChannelFeeRecord) error {
	if rec == nil {
		return fmt.Errorf("%w: record is required", ErrChannelFeesValidation)
	}
	return s.recordRepo.Create(ctx, rec)
}

func computeFeeAmount(structure models.FeeStructure, principal int64, currency string) (int64, error) {
	feeType := strings.ToLower(strings.TrimSpace(structure.Type))
	if feeType == "" || feeType == "none" {
		return 0, nil
	}

	switch feeType {
	case "flat":
		return clampFee(structure, mustMinor(structure.Flat, currency), currency)
	case "percentage":
		fee := percentOf(principal, structure.Percentage)
		return clampFee(structure, fee, currency)
	case "combined":
		fee := mustMinor(structure.Flat, currency) + percentOf(principal, structure.Percentage)
		return clampFee(structure, fee, currency)
	case "tiered":
		fee, err := tieredFee(structure.Layers, principal, currency)
		if err != nil {
			return 0, err
		}
		return clampFee(structure, fee, currency)
	case "layered":
		fee, err := layeredFee(structure.Layers, principal, currency)
		if err != nil {
			return 0, err
		}
		return clampFee(structure, fee, currency)
	default:
		return 0, fmt.Errorf("%w: unsupported fee type %s", ErrChannelFeesValidation, structure.Type)
	}
}

func clampFee(structure models.FeeStructure, fee int64, currency string) (int64, error) {
	if fee < 0 {
		fee = 0
	}
	if structure.Min != nil {
		min, err := minorFromString(*structure.Min, currency)
		if err != nil {
			return 0, err
		}
		if fee < min {
			fee = min
		}
	}
	if structure.Max != nil {
		max, err := minorFromString(*structure.Max, currency)
		if err != nil {
			return 0, err
		}
		if fee > max {
			fee = max
		}
	}
	return fee, nil
}

func tieredFee(layers []models.FeeLayer, principal int64, currency string) (int64, error) {
	if len(layers) == 0 {
		return 0, nil
	}
	candidates := make([]models.FeeLayer, 0, len(layers))
	for _, l := range layers {
		candidates = append(candidates, l)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return minorOrZero(candidates[i].From, currency) < minorOrZero(candidates[j].From, currency)
	})
	for _, l := range candidates {
		from := minorOrZero(l.From, currency)
		to := int64(0)
		if l.To != nil {
			to = minorOrZero(l.To, currency)
		}
		if principal < from {
			continue
		}
		if l.To != nil && principal > to {
			continue
		}
		return layerFee(l, principal, currency)
	}
	return 0, nil
}

func layeredFee(layers []models.FeeLayer, principal int64, currency string) (int64, error) {
	if len(layers) == 0 {
		return 0, nil
	}
	candidates := make([]models.FeeLayer, 0, len(layers))
	for _, l := range layers {
		candidates = append(candidates, l)
	}
	sort.Slice(candidates, func(i, j int) bool {
		a := minorOrZero(candidates[i].To, currency)
		if candidates[i].To == nil {
			a = math.MaxInt64
		}
		b := minorOrZero(candidates[j].To, currency)
		if candidates[j].To == nil {
			b = math.MaxInt64
		}
		return a < b
	})
	fee := int64(0)
	from := int64(0)
	for _, l := range candidates {
		to := minorOrZero(l.To, currency)
		if to == 0 {
			to = principal
		}
		if principal <= from {
			break
		}
		bracket := min64(principal, to) - from
		if bracket <= 0 {
			from = to
			continue
		}
		layerFee, err := layerFee(l, bracket, currency)
		if err != nil {
			return 0, err
		}
		fee += layerFee
		from = to
	}
	return fee, nil
}

func layerFee(layer models.FeeLayer, amount int64, currency string) (int64, error) {
	ret := int64(0)
	if layer.Flat != nil {
		x, err := minorFromString(*layer.Flat, currency)
		if err != nil {
			return 0, err
		}
		ret += x
	}
	if layer.Percentage != nil {
		ret += percentOf(amount, layer.Percentage)
	}
	return ret, nil
}

func percentOf(amount int64, percentage *string) int64 {
	if percentage == nil {
		return 0
	}
	p, err := decimal.NewFromString(strings.TrimSpace(*percentage))
	if err != nil {
		return 0
	}
	a := decimal.NewFromInt(amount)
	ret := a.Mul(p).Div(decimal.NewFromInt(100)).Round(0)
	return ret.IntPart()
}

func mustMinor(v *string, currency string) int64 {
	if v == nil {
		return 0
	}
	x, err := minorFromString(*v, currency)
	if err != nil {
		return 0
	}
	return x
}

func minorOrZero(v *string, currency string) int64 {
	if v == nil {
		return 0
	}
	x, err := minorFromString(*v, currency)
	if err != nil {
		return 0
	}
	return x
}

func minorFromString(value string, currency string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	d, err := decimal.NewFromString(value)
	if err != nil {
		return 0, err
	}
	def, ok := currencyregistry.Lookup(currency)
	if !ok {
		def = currencyregistry.Definition{Precision: 2, Enabled: true}
	}
	minor := d.Shift(int32(def.Precision)).Round(0)
	return minor.IntPart(), nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
