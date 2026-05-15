package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

var (
	ErrInterestValidation    = errors.New("interest validation failed")
	ErrInterestNotApplicable = errors.New("interest is not applicable")
)

type InterestPostingPreview struct {
	AccountID      uuid.UUID
	Currency       string
	AccrualIDs     []uuid.UUID
	TotalAccrued   decimal.Decimal
	PostableAmount int64
	Remainder      decimal.Decimal
}

type InterestService interface {
	Accrue(context.Context, uuid.UUID, int64, time.Time) (*models.InterestAccrual, error)
	IsPostingDue(context.Context, uuid.UUID, time.Time) (bool, error)
	PreviewPosting(context.Context, uuid.UUID) (*InterestPostingPreview, error)
	MarkPosted(context.Context, InterestPostingPreview, string) error
}

type DefaultInterestService struct {
	accountRepository  repositories.AccountRepository
	productRepository  repositories.ProductRepository
	interestRepository repositories.InterestAccrualRepository
}

func NewInterestService(
	accountRepository repositories.AccountRepository,
	productRepository repositories.ProductRepository,
	interestRepository repositories.InterestAccrualRepository,
) InterestService {
	return &DefaultInterestService{
		accountRepository:  accountRepository,
		productRepository:  productRepository,
		interestRepository: interestRepository,
	}
}

func (s *DefaultInterestService) Accrue(ctx context.Context, accountID uuid.UUID, balanceMinorUnits int64, accrualDate time.Time) (*models.InterestAccrual, error) {
	account, product, err := s.loadAccountAndProduct(ctx, accountID)
	if err != nil {
		return nil, err
	}
	config := product.InterestConfig
	if account.Status != models.AccountStatusActive || config == nil || config.Type == "" || config.Type == "none" {
		return nil, ErrInterestNotApplicable
	}

	accrualDate = normalizeUsageDate(accrualDate)
	if existing, ok, err := s.findExistingAccrual(ctx, accountID, accrualDate); err != nil {
		return nil, err
	} else if ok {
		return existing, nil
	}

	balanceBasis := minorUnitsToDecimal(balanceMinorUnits, account.Currency)
	if config.Type == "compound" {
		balanceBasis = balanceBasis.Add(account.InterestAccrued)
	}
	if balanceBasis.LessThanOrEqual(decimal.Zero) {
		return nil, ErrInterestNotApplicable
	}

	rate, err := resolveInterestRate(config, balanceBasis)
	if err != nil {
		return nil, err
	}
	amount, shouldAccrue, err := calculateAccrualAmount(config, balanceBasis, rate, accrualDate)
	if err != nil {
		return nil, err
	}
	if !shouldAccrue || amount.LessThanOrEqual(decimal.Zero) {
		return nil, ErrInterestNotApplicable
	}

	accrual := &models.InterestAccrual{
		ID:           uuid.New(),
		AccountID:    account.ID,
		AccrualDate:  accrualDate,
		BalanceBasis: balanceBasis,
		Rate:         rate,
		Amount:       amount,
		Posted:       false,
		Metadata: map[string]any{
			"currency":          account.Currency,
			"interest_type":     config.Type,
			"accrual_frequency": config.AccrualFrequency,
			"posting_frequency": config.PostingFrequency,
		},
	}
	if err := s.interestRepository.Create(ctx, accrual); err != nil {
		return nil, err
	}

	account.InterestAccrued = account.InterestAccrued.Add(amount)
	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return accrual, nil
}

func (s *DefaultInterestService) IsPostingDue(ctx context.Context, accountID uuid.UUID, when time.Time) (bool, error) {
	_, product, err := s.loadAccountAndProduct(ctx, accountID)
	if err != nil {
		return false, err
	}
	config := product.InterestConfig
	if config == nil || config.Type == "" || config.Type == "none" {
		return false, nil
	}
	when = normalizeUsageDate(when)
	if isPostingBoundary(config.PostingFrequency, when) {
		return true, nil
	}

	accruals, err := s.interestRepository.ListByAccount(ctx, accountID)
	if err != nil {
		return false, err
	}
	for _, accrual := range accruals {
		if accrual.Posted {
			continue
		}
		if isPostingBoundary(config.PostingFrequency, normalizeUsageDate(accrual.AccrualDate)) {
			return true, nil
		}
	}
	return false, nil
}

func (s *DefaultInterestService) PreviewPosting(ctx context.Context, accountID uuid.UUID) (*InterestPostingPreview, error) {
	account, _, err := s.loadAccountAndProduct(ctx, accountID)
	if err != nil {
		return nil, err
	}
	accruals, err := s.interestRepository.ListByAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}

	total := decimal.Zero
	ids := make([]uuid.UUID, 0)
	for _, accrual := range accruals {
		if accrual.Posted {
			continue
		}
		total = total.Add(accrual.Amount)
		ids = append(ids, accrual.ID)
	}
	if len(ids) == 0 {
		return nil, ErrInterestNotApplicable
	}

	postableMinor, postedMajor, err := decimalToMinorHalfUp(total, account.Currency)
	if err != nil {
		return nil, err
	}
	return &InterestPostingPreview{
		AccountID:      account.ID,
		Currency:       account.Currency,
		AccrualIDs:     ids,
		TotalAccrued:   total,
		PostableAmount: postableMinor,
		Remainder:      total.Sub(postedMajor),
	}, nil
}

func (s *DefaultInterestService) MarkPosted(ctx context.Context, preview InterestPostingPreview, postedReference string) error {
	account, _, err := s.loadAccountAndProduct(ctx, preview.AccountID)
	if err != nil {
		return err
	}
	postedMajor := minorUnitsToDecimal(preview.PostableAmount, account.Currency)

	accruals, err := s.interestRepository.ListByAccount(ctx, preview.AccountID)
	if err != nil {
		return err
	}
	remaining := map[uuid.UUID]struct{}{}
	for _, id := range preview.AccrualIDs {
		remaining[id] = struct{}{}
	}
	for _, accrual := range accruals {
		if _, ok := remaining[accrual.ID]; !ok {
			continue
		}
		accrual.Posted = true
		accrual.PostedReference = postedReference
		if err := s.interestRepository.Update(ctx, &accrual); err != nil {
			return err
		}
	}

	account.InterestAccrued = account.InterestAccrued.Sub(postedMajor)
	if account.InterestAccrued.IsNegative() {
		account.InterestAccrued = decimal.Zero
	}
	return resolveAccountRepositoryError(s.accountRepository.Update(ctx, account))
}

func (s *DefaultInterestService) findExistingAccrual(ctx context.Context, accountID uuid.UUID, accrualDate time.Time) (*models.InterestAccrual, bool, error) {
	accruals, err := s.interestRepository.ListByAccount(ctx, accountID)
	if err != nil {
		return nil, false, err
	}
	for i := range accruals {
		if normalizeUsageDate(accruals[i].AccrualDate).Equal(accrualDate) {
			return &accruals[i], true, nil
		}
	}
	return nil, false, nil
}

func (s *DefaultInterestService) loadAccountAndProduct(ctx context.Context, id uuid.UUID) (*models.Account, *models.Product, error) {
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

func resolveInterestRate(config *models.InterestConfig, balanceBasis decimal.Decimal) (decimal.Decimal, error) {
	if config.Type == "tiered" {
		for _, tier := range config.Tiers {
			minBalance, err := decimal.NewFromString(tier.MinBalance)
			if err != nil {
				return decimal.Zero, fmt.Errorf("%w: invalid tier min_balance", ErrInterestValidation)
			}
			if balanceBasis.LessThan(minBalance) {
				continue
			}
			if tier.MaxBalance != nil {
				maxBalance, err := decimal.NewFromString(*tier.MaxBalance)
				if err != nil {
					return decimal.Zero, fmt.Errorf("%w: invalid tier max_balance", ErrInterestValidation)
				}
				if balanceBasis.GreaterThan(maxBalance) {
					continue
				}
			}
			return decimal.NewFromString(tier.Rate)
		}
		return decimal.Zero, ErrInterestNotApplicable
	}
	rate, err := decimal.NewFromString(config.Rate)
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: invalid interest rate", ErrInterestValidation)
	}
	return rate, nil
}

func calculateAccrualAmount(config *models.InterestConfig, balanceBasis, annualRate decimal.Decimal, when time.Time) (decimal.Decimal, bool, error) {
	rateFactor := annualRate.Div(decimal.NewFromInt(100))
	switch config.AccrualFrequency {
	case "daily":
		return balanceBasis.Mul(rateFactor).Div(decimal.NewFromInt(365)).Round(12), true, nil
	case "monthly":
		if !isMonthEnd(when) {
			return decimal.Zero, false, nil
		}
		return balanceBasis.Mul(rateFactor).Div(decimal.NewFromInt(12)).Round(12), true, nil
	default:
		return decimal.Zero, false, fmt.Errorf("%w: unsupported accrual frequency %s", ErrInterestValidation, config.AccrualFrequency)
	}
}

func isPostingBoundary(frequency string, when time.Time) bool {
	if !isMonthEnd(when) {
		return false
	}
	switch frequency {
	case "monthly":
		return true
	case "quarterly":
		switch when.Month() {
		case time.March, time.June, time.September, time.December:
			return true
		default:
			return false
		}
	case "annually":
		return when.Month() == time.December
	case "maturity":
		return false
	default:
		return false
	}
}

func isMonthEnd(when time.Time) bool {
	return when.AddDate(0, 0, 1).Month() != when.Month()
}

func minorUnitsToDecimal(amount int64, currency string) decimal.Decimal {
	definition, ok := currencyregistry.Lookup(currency)
	if !ok {
		definition = currencyregistry.Definition{Precision: 2, Enabled: true}
	}
	return decimal.NewFromInt(amount).Shift(int32(-definition.Precision))
}

func decimalToMinorHalfUp(amount decimal.Decimal, currency string) (int64, decimal.Decimal, error) {
	definition, ok := currencyregistry.Lookup(currency)
	if !ok {
		definition = currencyregistry.Definition{Precision: 2, Enabled: true}
	}
	minor := amount.Shift(int32(definition.Precision)).Round(0)
	return minor.IntPart(), minor.Shift(int32(-definition.Precision)), nil
}

func resolveInterestRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrInterestNotApplicable
	default:
		return err
	}
}
