package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

var (
	ErrProductValidation            = errors.New("product validation failed")
	ErrProductNotFound              = errors.New("product not found")
	ErrProductAlreadyExists         = errors.New("product already exists")
	ErrProductInvalidStateTransition = errors.New("product state transition is invalid")
	ErrProductActivePatchRestricted = errors.New("active products can only update whitelisted fields")
)

type ProductService interface {
	Create(context.Context, CreateProductInput) (*models.Product, error)
	List(context.Context, repositories.ProductFilter) ([]models.Product, error)
	Get(context.Context, uuid.UUID) (*models.Product, error)
	Patch(context.Context, uuid.UUID, PatchProductInput) (*models.Product, error)
	Activate(context.Context, uuid.UUID) (*models.Product, error)
	Retire(context.Context, uuid.UUID) (*models.Product, error)
}

type ProductRulesInput struct {
	MinOpeningBalance    *string                    `json:"min_opening_balance,omitempty"`
	MinBalance           *string                    `json:"min_balance,omitempty"`
	MaxBalance           *string                    `json:"max_balance,omitempty"`
	AllowNegativeBalance *bool                      `json:"allow_negative_balance,omitempty"`
	AllowDebits          *bool                      `json:"allow_debits,omitempty"`
	AllowCredits         *bool                      `json:"allow_credits,omitempty"`
	RequiresKYCLevel     *int                       `json:"requires_kyc_level,omitempty"`
	EligibleClientTypes  []string                   `json:"eligible_client_types,omitempty"`
	DormancyDays         *int                       `json:"dormancy_days,omitempty"`
	TransactionLimits    *models.TransactionLimits  `json:"transaction_limits,omitempty"`
}

type CreateProductInput struct {
	Code           string                `json:"code"`
	Name           string                `json:"name"`
	Description    string                `json:"description"`
	Category       string                `json:"category"`
	Currency       string                `json:"currency"`
	Rules          *ProductRulesInput    `json:"rules,omitempty"`
	InterestConfig *models.InterestConfig `json:"interest_config,omitempty"`
	FeeSchedule    *models.FeeSchedule   `json:"fee_schedule,omitempty"`
}

type PatchProductInput struct {
	Code           *string                `json:"code,omitempty"`
	Name           *string                `json:"name,omitempty"`
	Description    *string                `json:"description,omitempty"`
	Category       *string                `json:"category,omitempty"`
	Currency       *string                `json:"currency,omitempty"`
	Rules          *ProductRulesInput     `json:"rules,omitempty"`
	InterestConfig **models.InterestConfig `json:"interest_config,omitempty"`
	FeeSchedule    **models.FeeSchedule   `json:"fee_schedule,omitempty"`
}

type DefaultProductService struct {
	productRepository repositories.ProductRepository
}

func NewProductService(productRepository repositories.ProductRepository) ProductService {
	return &DefaultProductService{
		productRepository: productRepository,
	}
}

func (s *DefaultProductService) Create(ctx context.Context, input CreateProductInput) (*models.Product, error) {
	product, err := buildProduct(input)
	if err != nil {
		return nil, err
	}

	if err := s.productRepository.Create(ctx, product); err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	return product, nil
}

func (s *DefaultProductService) List(ctx context.Context, filter repositories.ProductFilter) ([]models.Product, error) {
	return s.productRepository.List(ctx, filter)
}

func (s *DefaultProductService) Get(ctx context.Context, id uuid.UUID) (*models.Product, error) {
	product, err := s.productRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveProductRepositoryError(err)
	}
	return product, nil
}

func (s *DefaultProductService) Patch(ctx context.Context, id uuid.UUID, input PatchProductInput) (*models.Product, error) {
	product, err := s.productRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	if product.Status == models.ProductStatusActive && touchesRestrictedActiveFields(input) {
		return nil, fmt.Errorf("%w: code, category, currency, and rules are immutable after activation", ErrProductActivePatchRestricted)
	}

	applyPatch(product, input)
	if err := validateProduct(product); err != nil {
		return nil, err
	}

	if err := s.productRepository.Update(ctx, product); err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	return product, nil
}

func (s *DefaultProductService) Activate(ctx context.Context, id uuid.UUID) (*models.Product, error) {
	product, err := s.productRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	switch product.Status {
	case models.ProductStatusActive:
		return product, nil
	case models.ProductStatusDraft:
		product.Status = models.ProductStatusActive
	default:
		return nil, fmt.Errorf("%w: cannot activate product in status %s", ErrProductInvalidStateTransition, product.Status)
	}

	if err := validateProduct(product); err != nil {
		return nil, err
	}

	if err := s.productRepository.Update(ctx, product); err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	return product, nil
}

func (s *DefaultProductService) Retire(ctx context.Context, id uuid.UUID) (*models.Product, error) {
	product, err := s.productRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	switch product.Status {
	case models.ProductStatusRetired:
		return product, nil
	case models.ProductStatusActive:
		product.Status = models.ProductStatusRetired
	default:
		return nil, fmt.Errorf("%w: only active products can be retired", ErrProductInvalidStateTransition)
	}

	if err := s.productRepository.Update(ctx, product); err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	return product, nil
}

func buildProduct(input CreateProductInput) (*models.Product, error) {
	product := &models.Product{
		Code:           strings.TrimSpace(input.Code),
		Name:           strings.TrimSpace(input.Name),
		Description:    strings.TrimSpace(input.Description),
		Category:       strings.TrimSpace(input.Category),
		Currency:       strings.ToUpper(strings.TrimSpace(input.Currency)),
		Status:         models.ProductStatusDraft,
		Rules:          normalizeRules(input.Rules),
		InterestConfig: input.InterestConfig,
		FeeSchedule:    input.FeeSchedule,
	}

	if err := validateProduct(product); err != nil {
		return nil, err
	}

	return product, nil
}

func applyPatch(product *models.Product, input PatchProductInput) {
	if input.Code != nil {
		product.Code = strings.TrimSpace(*input.Code)
	}
	if input.Name != nil {
		product.Name = strings.TrimSpace(*input.Name)
	}
	if input.Description != nil {
		product.Description = strings.TrimSpace(*input.Description)
	}
	if input.Category != nil {
		product.Category = strings.TrimSpace(*input.Category)
	}
	if input.Currency != nil {
		product.Currency = strings.ToUpper(strings.TrimSpace(*input.Currency))
	}
	if input.Rules != nil {
		product.Rules = normalizeRules(input.Rules)
	}
	if input.InterestConfig != nil {
		product.InterestConfig = *input.InterestConfig
	}
	if input.FeeSchedule != nil {
		product.FeeSchedule = *input.FeeSchedule
	}
}

func touchesRestrictedActiveFields(input PatchProductInput) bool {
	return input.Code != nil || input.Category != nil || input.Currency != nil || input.Rules != nil
}

func normalizeRules(input *ProductRulesInput) models.ProductRules {
	ret := models.ProductRules{
		MinOpeningBalance:    "0",
		MinBalance:           "0",
		AllowNegativeBalance: false,
		AllowDebits:          true,
		AllowCredits:         true,
		RequiresKYCLevel:     0,
	}

	if input == nil {
		return ret
	}

	if input.MinOpeningBalance != nil {
		ret.MinOpeningBalance = strings.TrimSpace(*input.MinOpeningBalance)
	}
	if input.MinBalance != nil {
		ret.MinBalance = strings.TrimSpace(*input.MinBalance)
	}
	if input.MaxBalance != nil {
		maxBalance := strings.TrimSpace(*input.MaxBalance)
		ret.MaxBalance = &maxBalance
	}
	if input.AllowNegativeBalance != nil {
		ret.AllowNegativeBalance = *input.AllowNegativeBalance
	}
	if input.AllowDebits != nil {
		ret.AllowDebits = *input.AllowDebits
	}
	if input.AllowCredits != nil {
		ret.AllowCredits = *input.AllowCredits
	}
	if input.RequiresKYCLevel != nil {
		ret.RequiresKYCLevel = *input.RequiresKYCLevel
	}
	if input.EligibleClientTypes != nil {
		ret.EligibleClientTypes = append([]string(nil), input.EligibleClientTypes...)
	}
	if input.DormancyDays != nil {
		ret.DormancyDays = input.DormancyDays
	}
	if input.TransactionLimits != nil {
		ret.TransactionLimits = input.TransactionLimits
	}

	return ret
}

func validateProduct(product *models.Product) error {
	if product.Code == "" {
		return fmt.Errorf("%w: code is required", ErrProductValidation)
	}
	if product.Name == "" {
		return fmt.Errorf("%w: name is required", ErrProductValidation)
	}
	if product.Category == "" {
		return fmt.Errorf("%w: category is required", ErrProductValidation)
	}
	if product.Currency == "" {
		return fmt.Errorf("%w: currency is required", ErrProductValidation)
	}
	definition, ok := currencyregistry.Lookup(product.Currency)
	if !ok || !definition.Enabled {
		return fmt.Errorf("%w: currency %s is not enabled in wallet registry", ErrProductValidation, product.Currency)
	}

	switch product.Status {
	case models.ProductStatusDraft, models.ProductStatusActive, models.ProductStatusRetired:
	default:
		return fmt.Errorf("%w: invalid status %s", ErrProductValidation, product.Status)
	}

	if err := validateRules(product.Rules); err != nil {
		return err
	}
	if err := validateInterestConfig(product.InterestConfig); err != nil {
		return err
	}
	if err := validateFeeSchedule(product.FeeSchedule); err != nil {
		return err
	}

	return nil
}

func validateRules(rules models.ProductRules) error {
	minOpening, err := parseDecimalField("min_opening_balance", rules.MinOpeningBalance)
	if err != nil {
		return err
	}
	minBalance, err := parseDecimalField("min_balance", rules.MinBalance)
	if err != nil {
		return err
	}
	if minOpening.IsNegative() || minBalance.IsNegative() {
		return fmt.Errorf("%w: minimum balances cannot be negative", ErrProductValidation)
	}
	if rules.MaxBalance != nil {
		maxBalance, err := parseDecimalField("max_balance", *rules.MaxBalance)
		if err != nil {
			return err
		}
		if maxBalance.LessThan(minBalance) {
			return fmt.Errorf("%w: max_balance cannot be less than min_balance", ErrProductValidation)
		}
	}
	if rules.RequiresKYCLevel < 0 || rules.RequiresKYCLevel > 3 {
		return fmt.Errorf("%w: requires_kyc_level must be between 0 and 3", ErrProductValidation)
	}
	if rules.DormancyDays != nil && *rules.DormancyDays < 0 {
		return fmt.Errorf("%w: dormancy_days must be positive", ErrProductValidation)
	}
	for _, clientType := range rules.EligibleClientTypes {
		switch clientType {
		case models.ClientTypeIndividual, models.ClientTypeCorporate:
		default:
			return fmt.Errorf("%w: invalid eligible client type %s", ErrProductValidation, clientType)
		}
	}
	if rules.TransactionLimits != nil {
		limits := []*string{
			rules.TransactionLimits.DailyDebitLimit,
			rules.TransactionLimits.DailyCreditLimit,
			rules.TransactionLimits.SingleDebitLimit,
			rules.TransactionLimits.SingleCreditLimit,
		}
		for _, limit := range limits {
			if limit == nil {
				continue
			}
			value, err := decimal.NewFromString(strings.TrimSpace(*limit))
			if err != nil || value.IsNegative() {
				return fmt.Errorf("%w: transaction limit values must be non-negative decimals", ErrProductValidation)
			}
		}
	}

	return nil
}

func validateInterestConfig(config *models.InterestConfig) error {
	if config == nil {
		return nil
	}

	switch config.Type {
	case "", "none", "simple", "compound", "tiered":
	default:
		return fmt.Errorf("%w: invalid interest type %s", ErrProductValidation, config.Type)
	}
	if config.Type == "" || config.Type == "none" {
		return nil
	}

	if _, err := parseDecimalField("interest rate", config.Rate); err != nil {
		return err
	}

	switch config.AccrualFrequency {
	case "daily", "monthly":
	default:
		return fmt.Errorf("%w: invalid accrual_frequency %s", ErrProductValidation, config.AccrualFrequency)
	}
	switch config.PostingFrequency {
	case "monthly", "quarterly", "annually", "maturity":
	default:
		return fmt.Errorf("%w: invalid posting_frequency %s", ErrProductValidation, config.PostingFrequency)
	}
	if config.Type == "tiered" {
		if len(config.Tiers) == 0 {
			return fmt.Errorf("%w: tiered interest requires at least one tier", ErrProductValidation)
		}
		for _, tier := range config.Tiers {
			minBalance, err := parseDecimalField("tier min_balance", tier.MinBalance)
			if err != nil {
				return err
			}
			if minBalance.IsNegative() {
				return fmt.Errorf("%w: tier min_balance cannot be negative", ErrProductValidation)
			}
			if tier.MaxBalance != nil {
				maxBalance, err := parseDecimalField("tier max_balance", *tier.MaxBalance)
				if err != nil {
					return err
				}
				if maxBalance.LessThan(minBalance) {
					return fmt.Errorf("%w: tier max_balance cannot be less than min_balance", ErrProductValidation)
				}
			}
			if _, err := parseDecimalField("tier rate", tier.Rate); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateFeeSchedule(schedule *models.FeeSchedule) error {
	if schedule == nil {
		return nil
	}
	if schedule.MaintenanceFee != nil {
		if _, err := parseDecimalField("maintenance_fee.amount", schedule.MaintenanceFee.Amount); err != nil {
			return err
		}
		switch schedule.MaintenanceFee.Frequency {
		case "monthly", "annually":
		default:
			return fmt.Errorf("%w: invalid maintenance fee frequency %s", ErrProductValidation, schedule.MaintenanceFee.Frequency)
		}
		if schedule.MaintenanceFee.Currency != "" {
			definition, ok := currencyregistry.Lookup(schedule.MaintenanceFee.Currency)
			if !ok || !definition.Enabled {
				return fmt.Errorf("%w: maintenance fee currency %s is not enabled", ErrProductValidation, schedule.MaintenanceFee.Currency)
			}
		}
	}
	for _, fee := range schedule.TransactionFees {
		switch fee.Event {
		case "debit", "credit", "transfer":
		default:
			return fmt.Errorf("%w: invalid transaction fee event %s", ErrProductValidation, fee.Event)
		}
		switch fee.Type {
		case "flat", "percentage":
		default:
			return fmt.Errorf("%w: invalid transaction fee type %s", ErrProductValidation, fee.Type)
		}
		if _, err := parseDecimalField("transaction fee value", fee.Value); err != nil {
			return err
		}
		for _, maybeValue := range []*string{fee.Min, fee.Max} {
			if maybeValue == nil {
				continue
			}
			if _, err := parseDecimalField("transaction fee bounds", *maybeValue); err != nil {
				return err
			}
		}
		if fee.Currency != "" {
			definition, ok := currencyregistry.Lookup(fee.Currency)
			if !ok || !definition.Enabled {
				return fmt.Errorf("%w: transaction fee currency %s is not enabled", ErrProductValidation, fee.Currency)
			}
		}
	}

	return nil
}

func parseDecimalField(name, value string) (decimal.Decimal, error) {
	parsed, err := decimal.NewFromString(strings.TrimSpace(value))
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: %s must be a decimal", ErrProductValidation, name)
	}
	return parsed, nil
}

func resolveProductRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrProductNotFound
	case errors.Is(err, postgres.ErrConstraintsFailed{}):
		return ErrProductAlreadyExists
	default:
		return err
	}
}
