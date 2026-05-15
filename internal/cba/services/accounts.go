package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

var (
	ErrAccountValidation             = errors.New("account validation failed")
	ErrAccountNotFound               = errors.New("account not found")
	ErrAccountAlreadyExists          = errors.New("account already exists")
	ErrAccountInvalidStateTransition = errors.New("account state transition is invalid")
)

type AccountService interface {
	Open(context.Context, OpenAccountInput) (*models.Account, error)
	List(context.Context, repositories.AccountFilter) ([]models.Account, error)
	Get(context.Context, uuid.UUID) (*models.Account, error)
	Activate(context.Context, uuid.UUID) (*models.Account, error)
	Suspend(context.Context, uuid.UUID) (*models.Account, error)
	Freeze(context.Context, uuid.UUID) (*models.Account, error)
	Dormant(context.Context, uuid.UUID) (*models.Account, error)
	Reactivate(context.Context, uuid.UUID) (*models.Account, error)
	Close(context.Context, uuid.UUID, int64) (*models.Account, error)
	ValidateCredit(context.Context, uuid.UUID, int64, int64, time.Time) (*models.Account, error)
	ValidateDebit(context.Context, uuid.UUID, int64, int64, time.Time) (*models.Account, error)
	ValidateLien(context.Context, uuid.UUID, int64, int64, time.Time) (*models.Account, error)
	ValidateRelease(context.Context, uuid.UUID, string) (*models.Account, error)
	RecordCreditUsage(context.Context, uuid.UUID, int64, string, time.Time) error
	RecordDebitUsage(context.Context, uuid.UUID, int64, string, time.Time) error
	TouchActivity(context.Context, uuid.UUID, time.Time) (*models.Account, error)
}

type OpenAccountInput struct {
	ClientID       uuid.UUID      `json:"client_id"`
	ProductID      uuid.UUID      `json:"product_id"`
	OpeningDeposit json.Number    `json:"opening_deposit"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type DefaultAccountService struct {
	accountRepository repositories.AccountRepository
	clientRepository  repositories.ClientRepository
	productRepository repositories.ProductRepository
	dailyUsageRepo    repositories.DailyUsageRepository
}

func NewAccountService(
	accountRepository repositories.AccountRepository,
	clientRepository repositories.ClientRepository,
	productRepository repositories.ProductRepository,
	dailyUsageRepo repositories.DailyUsageRepository,
) AccountService {
	return &DefaultAccountService{
		accountRepository: accountRepository,
		clientRepository:  clientRepository,
		productRepository: productRepository,
		dailyUsageRepo:    dailyUsageRepo,
	}
}

func (s *DefaultAccountService) Open(ctx context.Context, input OpenAccountInput) (*models.Account, error) {
	if input.ClientID == uuid.Nil {
		return nil, fmt.Errorf("%w: client_id is required", ErrAccountValidation)
	}
	if input.ProductID == uuid.Nil {
		return nil, fmt.Errorf("%w: product_id is required", ErrAccountValidation)
	}

	client, err := s.clientRepository.Get(ctx, input.ClientID)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}
	product, err := s.productRepository.Get(ctx, input.ProductID)
	if err != nil {
		return nil, resolveProductRepositoryError(err)
	}

	if client.Status != models.ClientStatusActive {
		return nil, fmt.Errorf("%w: client must be active before opening an account", ErrAccountValidation)
	}
	if product.Status != models.ProductStatusActive {
		return nil, fmt.Errorf("%w: product must be active before opening an account", ErrAccountValidation)
	}
	if client.KYCLevel < product.Rules.RequiresKYCLevel {
		return nil, fmt.Errorf("%w: client kyc level does not satisfy product requirements", ErrAccountValidation)
	}
	if len(product.Rules.EligibleClientTypes) > 0 && !containsString(product.Rules.EligibleClientTypes, client.Type) {
		return nil, fmt.Errorf("%w: client type %s is not eligible for this product", ErrAccountValidation, client.Type)
	}

	openingDeposit, err := parseAccountDecimal(input.OpeningDeposit)
	if err != nil {
		return nil, err
	}
	if openingDeposit.IsNegative() {
		return nil, fmt.Errorf("%w: opening_deposit cannot be negative", ErrAccountValidation)
	}

	minOpening, err := decimal.NewFromString(strings.TrimSpace(product.Rules.MinOpeningBalance))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid product min_opening_balance", ErrAccountValidation)
	}
	if openingDeposit.LessThan(minOpening) {
		return nil, fmt.Errorf("%w: opening_deposit is below product min_opening_balance", ErrAccountValidation)
	}
	if product.Rules.MaxBalance != nil {
		maxBalance, err := decimal.NewFromString(strings.TrimSpace(*product.Rules.MaxBalance))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid product max_balance", ErrAccountValidation)
		}
		if openingDeposit.GreaterThan(maxBalance) {
			return nil, fmt.Errorf("%w: opening_deposit exceeds product max_balance", ErrAccountValidation)
		}
	}

	walletID := fmt.Sprintf("client-%s-%s", strings.TrimSpace(client.ClientNumber), strings.TrimSpace(product.Code))
	if _, err := s.accountRepository.GetByWalletID(ctx, walletID); err == nil {
		return nil, ErrAccountAlreadyExists
	} else if !postgres.IsNotFoundError(err) && !errors.Is(err, postgres.ErrNotFound) {
		return nil, resolveAccountRepositoryError(err)
	}

	accountNumber, err := s.generateAccountNumber(ctx)
	if err != nil {
		return nil, err
	}

	account := &models.Account{
		AccountNumber:   accountNumber,
		ClientID:        client.ID,
		ProductID:       product.ID,
		Currency:        product.Currency,
		Status:          models.AccountStatusPending,
		WalletID:        walletID,
		FreezeDebits:    false,
		OpenedAt:        time.Now().UTC(),
		InterestAccrued: decimal.Zero,
		Metadata:        normalizeAccountMetadata(input.Metadata),
	}

	if !openingDeposit.IsZero() {
		account.Metadata["opening_deposit"] = input.OpeningDeposit.String()
	}

	if err := s.accountRepository.Create(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	return account, nil
}

func (s *DefaultAccountService) List(ctx context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
	return s.accountRepository.List(ctx, filter)
}

func (s *DefaultAccountService) Get(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) Activate(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	switch account.Status {
	case models.AccountStatusActive:
		return account, nil
	case models.AccountStatusPending, models.AccountStatusDormant:
		now := time.Now().UTC()
		account.Status = models.AccountStatusActive
		account.ActivatedAt = &now
	default:
		return nil, fmt.Errorf("%w: cannot activate account in status %s", ErrAccountInvalidStateTransition, account.Status)
	}

	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) Suspend(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	switch account.Status {
	case models.AccountStatusSuspended:
		return account, nil
	case models.AccountStatusPending, models.AccountStatusActive, models.AccountStatusDormant:
		account.Status = models.AccountStatusSuspended
	default:
		return nil, fmt.Errorf("%w: cannot suspend account in status %s", ErrAccountInvalidStateTransition, account.Status)
	}

	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) Freeze(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	switch account.Status {
	case models.AccountStatusActive, models.AccountStatusDormant:
		account.FreezeDebits = true
	default:
		return nil, fmt.Errorf("%w: only active or dormant accounts can be frozen", ErrAccountInvalidStateTransition)
	}

	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) Dormant(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	switch account.Status {
	case models.AccountStatusDormant:
		return account, nil
	case models.AccountStatusActive:
		account.Status = models.AccountStatusDormant
	default:
		return nil, fmt.Errorf("%w: only active accounts can become dormant", ErrAccountInvalidStateTransition)
	}

	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) Reactivate(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	switch account.Status {
	case models.AccountStatusActive:
		if !account.FreezeDebits {
			return account, nil
		}
	case models.AccountStatusDormant, models.AccountStatusSuspended:
		now := time.Now().UTC()
		account.Status = models.AccountStatusActive
		account.ActivatedAt = &now
	default:
		return nil, fmt.Errorf("%w: cannot reactivate account in status %s", ErrAccountInvalidStateTransition, account.Status)
	}

	account.FreezeDebits = false

	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) Close(ctx context.Context, id uuid.UUID, currentBalance int64) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}

	switch account.Status {
	case models.AccountStatusClosed:
		return account, nil
	case models.AccountStatusPending, models.AccountStatusActive, models.AccountStatusDormant, models.AccountStatusSuspended:
	default:
		return nil, fmt.Errorf("%w: cannot close account in status %s", ErrAccountInvalidStateTransition, account.Status)
	}

	if currentBalance != 0 {
		return nil, fmt.Errorf("%w: account balance must be zero before close", ErrAccountValidation)
	}

	now := time.Now().UTC()
	account.Status = models.AccountStatusClosed
	account.ClosedAt = &now

	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) ValidateCredit(ctx context.Context, id uuid.UUID, amount, currentBalance int64, usageAt time.Time) (*models.Account, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrAccountValidation)
	}

	account, product, err := s.loadAccountAndProduct(ctx, id)
	if err != nil {
		return nil, err
	}

	switch account.Status {
	case models.AccountStatusActive, models.AccountStatusDormant:
	case models.AccountStatusSuspended:
		return nil, fmt.Errorf("%w: suspended accounts cannot be credited", ErrAccountValidation)
	default:
		return nil, fmt.Errorf("%w: account must be active or dormant for credits", ErrAccountValidation)
	}

	if !product.Rules.AllowCredits {
		return nil, fmt.Errorf("%w: product does not allow credits", ErrAccountValidation)
	}

	if product.Rules.TransactionLimits != nil && product.Rules.TransactionLimits.SingleCreditLimit != nil {
		limit, err := ruleAmountToAtomic(*product.Rules.TransactionLimits.SingleCreditLimit, account.Currency)
		if err != nil {
			return nil, err
		}
		if amount > limit {
			return nil, fmt.Errorf("%w: amount exceeds single_credit_limit", ErrAccountValidation)
		}
	}

	if product.Rules.MaxBalance != nil {
		maxBalance, err := ruleAmountToAtomic(*product.Rules.MaxBalance, account.Currency)
		if err != nil {
			return nil, err
		}
		if currentBalance+amount > maxBalance {
			return nil, fmt.Errorf("%w: credit would exceed product max_balance", ErrAccountValidation)
		}
	}

	if product.Rules.TransactionLimits != nil && product.Rules.TransactionLimits.DailyCreditLimit != nil {
		dailyLimit, err := ruleAmountToAtomic(*product.Rules.TransactionLimits.DailyCreditLimit, account.Currency)
		if err != nil {
			return nil, err
		}
		usage, err := s.getDailyUsage(ctx, account.ID, usageAt)
		if err != nil {
			return nil, err
		}
		if usage.CreditAmount.IntPart()+amount > dailyLimit {
			return nil, fmt.Errorf("%w: amount exceeds daily_credit_limit", ErrAccountValidation)
		}
	}

	return account, nil
}

func (s *DefaultAccountService) ValidateDebit(ctx context.Context, id uuid.UUID, amount, currentBalance int64, usageAt time.Time) (*models.Account, error) {
	return s.validateDebitLike(ctx, id, amount, currentBalance, "debit", usageAt)
}

func (s *DefaultAccountService) ValidateLien(ctx context.Context, id uuid.UUID, amount, currentBalance int64, usageAt time.Time) (*models.Account, error) {
	return s.validateDebitLike(ctx, id, amount, currentBalance, "lien", usageAt)
}

func (s *DefaultAccountService) ValidateRelease(ctx context.Context, id uuid.UUID, mode string) (*models.Account, error) {
	account, _, err := s.loadAccountAndProduct(ctx, id)
	if err != nil {
		return nil, err
	}

	switch account.Status {
	case models.AccountStatusActive, models.AccountStatusDormant:
	default:
		return nil, fmt.Errorf("%w: account must be active or dormant for lien release", ErrAccountValidation)
	}

	mode = strings.TrimSpace(mode)
	if mode != "" && mode != "PAY" && mode != "RELEASE" {
		return nil, fmt.Errorf("%w: mode must be PAY or RELEASE", ErrAccountValidation)
	}

	return account, nil
}

func (s *DefaultAccountService) TouchActivity(ctx context.Context, id uuid.UUID, at time.Time) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	at = at.UTC()
	account.LastActivityAt = &at
	if err := s.accountRepository.Update(ctx, account); err != nil {
		return nil, resolveAccountRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultAccountService) RecordCreditUsage(ctx context.Context, id uuid.UUID, amount int64, reference string, usageAt time.Time) error {
	return s.recordUsage(ctx, id, amount, reference, usageAt, false)
}

func (s *DefaultAccountService) RecordDebitUsage(ctx context.Context, id uuid.UUID, amount int64, reference string, usageAt time.Time) error {
	return s.recordUsage(ctx, id, amount, reference, usageAt, true)
}

func normalizeAccountMetadata(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	ret := make(map[string]any, len(input))
	for key, value := range input {
		ret[strings.TrimSpace(key)] = value
	}
	return ret
}

func (s *DefaultAccountService) validateDebitLike(ctx context.Context, id uuid.UUID, amount, currentBalance int64, operation string, usageAt time.Time) (*models.Account, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrAccountValidation)
	}

	account, product, err := s.loadAccountAndProduct(ctx, id)
	if err != nil {
		return nil, err
	}

	if account.Status != models.AccountStatusActive {
		return nil, fmt.Errorf("%w: account must be active for %s", ErrAccountValidation, operation)
	}
	if account.FreezeDebits {
		return nil, fmt.Errorf("%w: account debits are frozen", ErrAccountValidation)
	}
	if !product.Rules.AllowDebits {
		return nil, fmt.Errorf("%w: product does not allow debits", ErrAccountValidation)
	}

	if product.Rules.TransactionLimits != nil && product.Rules.TransactionLimits.SingleDebitLimit != nil {
		limit, err := ruleAmountToAtomic(*product.Rules.TransactionLimits.SingleDebitLimit, account.Currency)
		if err != nil {
			return nil, err
		}
		if amount > limit {
			return nil, fmt.Errorf("%w: amount exceeds single_debit_limit", ErrAccountValidation)
		}
	}

	if product.Rules.TransactionLimits != nil && product.Rules.TransactionLimits.DailyDebitLimit != nil {
		dailyLimit, err := ruleAmountToAtomic(*product.Rules.TransactionLimits.DailyDebitLimit, account.Currency)
		if err != nil {
			return nil, err
		}
		usage, err := s.getDailyUsage(ctx, account.ID, usageAt)
		if err != nil {
			return nil, err
		}
		if usage.DebitAmount.IntPart()+amount > dailyLimit {
			return nil, fmt.Errorf("%w: amount exceeds daily_debit_limit", ErrAccountValidation)
		}
	}

	resultingBalance := currentBalance - amount
	if !product.Rules.AllowNegativeBalance {
		if resultingBalance < 0 {
			return nil, fmt.Errorf("%w: %s would overdraw the account", ErrAccountValidation, operation)
		}

		minBalance, err := ruleAmountToAtomic(product.Rules.MinBalance, account.Currency)
		if err != nil {
			return nil, err
		}
		if resultingBalance < minBalance {
			return nil, fmt.Errorf("%w: %s would breach product min_balance", ErrAccountValidation, operation)
		}
	}

	return account, nil
}

func (s *DefaultAccountService) getDailyUsage(ctx context.Context, accountID uuid.UUID, usageAt time.Time) (*models.AccountDailyUsage, error) {
	if s.dailyUsageRepo == nil {
		return &models.AccountDailyUsage{
			AccountID:    accountID,
			UsageDate:    normalizeUsageDate(usageAt),
			DebitAmount:  decimal.Zero,
			CreditAmount: decimal.Zero,
		}, nil
	}

	usage, err := s.dailyUsageRepo.GetForDate(ctx, accountID, usageAt)
	switch {
	case err == nil:
		return usage, nil
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return &models.AccountDailyUsage{
			AccountID:    accountID,
			UsageDate:    normalizeUsageDate(usageAt),
			DebitAmount:  decimal.Zero,
			CreditAmount: decimal.Zero,
		}, nil
	default:
		return nil, err
	}
}

func (s *DefaultAccountService) recordUsage(ctx context.Context, accountID uuid.UUID, amount int64, reference string, usageAt time.Time, isDebit bool) error {
	if s.dailyUsageRepo == nil {
		return nil
	}

	usage, err := s.getDailyUsage(ctx, accountID, usageAt)
	if err != nil {
		return err
	}

	amountDecimal := decimal.NewFromInt(amount)
	usage.AccountID = accountID
	usage.UsageDate = normalizeUsageDate(usageAt)
	usage.LastReference = strings.TrimSpace(reference)
	if isDebit {
		usage.DebitAmount = usage.DebitAmount.Add(amountDecimal)
		usage.DebitCount++
	} else {
		usage.CreditAmount = usage.CreditAmount.Add(amountDecimal)
		usage.CreditCount++
	}

	if usage.ID == uuid.Nil {
		return s.dailyUsageRepo.Create(ctx, usage)
	}
	return s.dailyUsageRepo.Update(ctx, usage)
}

func parseAccountDecimal(value json.Number) (decimal.Decimal, error) {
	if value == "" {
		return decimal.Zero, nil
	}
	ret, err := decimal.NewFromString(strings.TrimSpace(value.String()))
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: opening_deposit must be numeric", ErrAccountValidation)
	}
	return ret, nil
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func (s *DefaultAccountService) loadAccountAndProduct(ctx context.Context, id uuid.UUID) (*models.Account, *models.Product, error) {
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

func ruleAmountToAtomic(value string, currency string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	if !strings.Contains(value, ".") {
		ret, err := decimal.NewFromString(value)
		if err != nil {
			return 0, fmt.Errorf("%w: invalid product rule amount %q", ErrAccountValidation, value)
		}
		return ret.IntPart(), nil
	}
	dec, err := decimal.NewFromString(value)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid product rule amount %q", ErrAccountValidation, value)
	}
	definition, ok := currencyregistry.Lookup(currency)
	if !ok {
		definition = currencyregistry.Definition{Precision: 2, Enabled: true}
	}
	scaled := dec.Shift(int32(definition.Precision)).Round(0)
	return scaled.IntPart(), nil
}

func normalizeUsageDate(usageAt time.Time) time.Time {
	return usageAt.UTC().Truncate(24 * time.Hour)
}

func (s *DefaultAccountService) generateAccountNumber(ctx context.Context) (string, error) {
	for range 10 {
		accountNumber, err := randomDigits(10)
		if err != nil {
			return "", fmt.Errorf("%w: unable to generate account number", ErrAccountValidation)
		}
		_, err = s.accountRepository.GetByNumber(ctx, accountNumber)
		switch {
		case err == nil:
			continue
		case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
			return accountNumber, nil
		default:
			return "", resolveAccountRepositoryError(err)
		}
	}
	return "", fmt.Errorf("%w: unable to generate unique account number", ErrAccountValidation)
}

func resolveAccountRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrAccountNotFound
	case errors.Is(err, postgres.ErrConstraintsFailed{}):
		return ErrAccountAlreadyExists
	default:
		return err
	}
}
