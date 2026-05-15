package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var (
	ErrFeeValidation    = errors.New("fee validation failed")
	ErrFeeNotApplicable = errors.New("fee is not applicable")
)

type FeeService interface {
	PrepareTransactionFees(context.Context, uuid.UUID, string, int64, string) ([]models.FeePosting, error)
	PrepareMaintenanceFee(context.Context, uuid.UUID, time.Time) (*models.FeePosting, error)
	MarkPosted(context.Context, string) (*models.FeePosting, error)
	MarkPendingRecovery(context.Context, string, map[string]any) (*models.FeePosting, error)
}

type DefaultFeeService struct {
	accountRepository repositories.AccountRepository
	productRepository repositories.ProductRepository
	feeRepository     repositories.FeePostingRepository
}

func NewFeeService(
	accountRepository repositories.AccountRepository,
	productRepository repositories.ProductRepository,
	feeRepository repositories.FeePostingRepository,
) FeeService {
	return &DefaultFeeService{
		accountRepository: accountRepository,
		productRepository: productRepository,
		feeRepository:     feeRepository,
	}
}

func (s *DefaultFeeService) PrepareTransactionFees(ctx context.Context, accountID uuid.UUID, event string, amountMinorUnits int64, linkedReference string) ([]models.FeePosting, error) {
	if amountMinorUnits <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrFeeValidation)
	}
	account, product, err := s.loadAccountAndProduct(ctx, accountID)
	if err != nil {
		return nil, err
	}
	schedule := product.FeeSchedule
	if schedule == nil || len(schedule.TransactionFees) == 0 {
		return nil, ErrFeeNotApplicable
	}

	fees := make([]models.FeePosting, 0)
	for idx, feeRule := range schedule.TransactionFees {
		if feeRule.Event != event {
			continue
		}
		feeAmount, currency, err := s.calculateTransactionFeeAmount(account.Currency, feeRule, amountMinorUnits)
		if err != nil {
			return nil, err
		}
		if feeAmount.LessThanOrEqual(decimal.Zero) {
			continue
		}
		reference := fmt.Sprintf("fee:%s:%s:%d", event, linkedReference, idx+1)
		if existing, err := s.feeRepository.GetByReference(ctx, reference); err == nil {
			fees = append(fees, *existing)
			continue
		} else if !postgres.IsNotFoundError(err) && !errors.Is(err, postgres.ErrNotFound) {
			return nil, err
		}

		posting := models.FeePosting{
			ID:              uuid.New(),
			AccountID:       account.ID,
			EventType:       event,
			Reference:       reference,
			LinkedReference: linkedReference,
			Amount:          feeAmount,
			Currency:        currency,
			Status:          models.FeePostingStatusPendingRecovery,
			Metadata: map[string]any{
				"fee_type":   feeRule.Type,
				"fee_event":  feeRule.Event,
				"account_id": account.ID.String(),
			},
		}
		if err := s.feeRepository.Create(ctx, &posting); err != nil {
			return nil, err
		}
		fees = append(fees, posting)
	}
	if len(fees) == 0 {
		return nil, ErrFeeNotApplicable
	}
	return fees, nil
}

func (s *DefaultFeeService) PrepareMaintenanceFee(ctx context.Context, accountID uuid.UUID, scheduledFor time.Time) (*models.FeePosting, error) {
	account, product, err := s.loadAccountAndProduct(ctx, accountID)
	if err != nil {
		return nil, err
	}
	schedule := product.FeeSchedule
	if schedule == nil || schedule.MaintenanceFee == nil {
		return nil, ErrFeeNotApplicable
	}
	if !isMaintenanceBoundary(schedule.MaintenanceFee.Frequency, scheduledFor) {
		return nil, ErrFeeNotApplicable
	}

	amount, err := decimal.NewFromString(schedule.MaintenanceFee.Amount)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid maintenance fee amount", ErrFeeValidation)
	}
	currency := strings.TrimSpace(schedule.MaintenanceFee.Currency)
	if currency == "" {
		currency = account.Currency
	}

	reference := fmt.Sprintf("maintenance:%s:%s", account.ID.String(), normalizeUsageDate(scheduledFor).Format("2006-01-02"))
	if existing, err := s.feeRepository.GetByReference(ctx, reference); err == nil {
		return existing, nil
	} else if !postgres.IsNotFoundError(err) && !errors.Is(err, postgres.ErrNotFound) {
		return nil, err
	}

	posting := &models.FeePosting{
		ID:              uuid.New(),
		AccountID:       account.ID,
		EventType:       "maintenance",
		Reference:       reference,
		LinkedReference: reference,
		Amount:          amount,
		Currency:        currency,
		Status:          models.FeePostingStatusPendingRecovery,
		Metadata: map[string]any{
			"scheduled_for": normalizeUsageDate(scheduledFor).Format("2006-01-02"),
		},
	}
	if err := s.feeRepository.Create(ctx, posting); err != nil {
		return nil, err
	}
	return posting, nil
}

func (s *DefaultFeeService) MarkPosted(ctx context.Context, reference string) (*models.FeePosting, error) {
	posting, err := s.feeRepository.GetByReference(ctx, reference)
	if err != nil {
		return nil, resolveFeeRepositoryError(err)
	}
	posting.Status = models.FeePostingStatusPosted
	if err := s.feeRepository.Update(ctx, posting); err != nil {
		return nil, err
	}
	return posting, nil
}

func (s *DefaultFeeService) MarkPendingRecovery(ctx context.Context, reference string, metadata map[string]any) (*models.FeePosting, error) {
	posting, err := s.feeRepository.GetByReference(ctx, reference)
	if err != nil {
		return nil, resolveFeeRepositoryError(err)
	}
	posting.Status = models.FeePostingStatusPendingRecovery
	if posting.Metadata == nil {
		posting.Metadata = map[string]any{}
	}
	for key, value := range metadata {
		posting.Metadata[key] = value
	}
	if err := s.feeRepository.Update(ctx, posting); err != nil {
		return nil, err
	}
	return posting, nil
}

func (s *DefaultFeeService) loadAccountAndProduct(ctx context.Context, id uuid.UUID) (*models.Account, *models.Product, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, nil, resolveAccountRepositoryError(err)
	}
	product, err := s.productRepository.Get(ctx, account.ProductID)
	if err != nil {
		return nil, nil, resolveProductRepositoryError(err)
	}
	return account, product, nil
}

func (s *DefaultFeeService) calculateTransactionFeeAmount(accountCurrency string, rule models.TransactionFee, amountMinorUnits int64) (decimal.Decimal, string, error) {
	currency := strings.TrimSpace(rule.Currency)
	if currency == "" {
		currency = accountCurrency
	}
	amountMajor := minorUnitsToDecimal(amountMinorUnits, accountCurrency)

	var feeAmount decimal.Decimal
	switch rule.Type {
	case "flat":
		parsed, err := decimal.NewFromString(rule.Value)
		if err != nil {
			return decimal.Zero, "", fmt.Errorf("%w: invalid flat fee value", ErrFeeValidation)
		}
		feeAmount = parsed
	case "percentage":
		percentage, err := decimal.NewFromString(rule.Value)
		if err != nil {
			return decimal.Zero, "", fmt.Errorf("%w: invalid percentage fee value", ErrFeeValidation)
		}
		feeAmount = amountMajor.Mul(percentage).Div(decimal.NewFromInt(100))
	default:
		return decimal.Zero, "", fmt.Errorf("%w: unsupported fee type %s", ErrFeeValidation, rule.Type)
	}

	if rule.Min != nil {
		minimum, err := decimal.NewFromString(*rule.Min)
		if err != nil {
			return decimal.Zero, "", fmt.Errorf("%w: invalid fee min bound", ErrFeeValidation)
		}
		if feeAmount.LessThan(minimum) {
			feeAmount = minimum
		}
	}
	if rule.Max != nil {
		maximum, err := decimal.NewFromString(*rule.Max)
		if err != nil {
			return decimal.Zero, "", fmt.Errorf("%w: invalid fee max bound", ErrFeeValidation)
		}
		if feeAmount.GreaterThan(maximum) {
			feeAmount = maximum
		}
	}
	return feeAmount.RoundBank(8), currency, nil
}

func isMaintenanceBoundary(frequency string, scheduledFor time.Time) bool {
	if !isMonthEnd(normalizeUsageDate(scheduledFor)) {
		return false
	}
	switch frequency {
	case "monthly":
		return true
	case "annually":
		return scheduledFor.Month() == time.December
	default:
		return false
	}
}

func resolveFeeRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrFeeNotApplicable
	default:
		return err
	}
}
