package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/feequote"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

var (
	ErrFeeConfigValidation = errors.New("fee configuration validation failed")
	ErrFeeConfigNotFound   = errors.New("fee configuration not found")
	ErrFeeConfigConflict   = errors.New("fee configuration overlaps an existing one")
	// ErrTaxCountryUnknown is returned when tax rules exist for the event but the client's
	// country cannot be determined. Charging without tax would be silently wrong.
	ErrTaxCountryUnknown = errors.New("client country is unknown and tax rules are configured")
	// ErrTaxAccountMissing is returned when a tax rule applies but has no payable account for the
	// account's currency.
	ErrTaxAccountMissing = errors.New("tax rule has no payable account for the currency")
)

var (
	taxCodePattern = regexp.MustCompile(`^[A-Z0-9_]{1,32}$`)
	countryPattern = regexp.MustCompile(`^[A-Z]{2}$`)
)

type CreateFeeOverrideInput struct {
	Scope         string                  `json:"scope"`
	ClientID      *uuid.UUID              `json:"client_id,omitempty"`
	AccountID     *uuid.UUID              `json:"account_id,omitempty"`
	ProductID     *uuid.UUID              `json:"product_id,omitempty"`
	Currency      string                  `json:"currency,omitempty"`
	Event         string                  `json:"event"`
	Rules         []models.TransactionFee `json:"rules"`
	EffectiveFrom *time.Time              `json:"effective_from,omitempty"`
	EffectiveTo   *time.Time              `json:"effective_to,omitempty"`
	Metadata      map[string]any          `json:"metadata,omitempty"`
}

type CreateTaxRuleInput struct {
	Country         string            `json:"country"`
	Code            string            `json:"code"`
	Name            string            `json:"name"`
	Event           string            `json:"event"`
	Base            string            `json:"base"`
	Rate            string            `json:"rate"`
	Min             *string           `json:"min,omitempty"`
	Max             *string           `json:"max,omitempty"`
	PayableAccounts map[string]string `json:"payable_accounts"`
	EffectiveFrom   *time.Time        `json:"effective_from,omitempty"`
	EffectiveTo     *time.Time        `json:"effective_to,omitempty"`
	Metadata        map[string]any    `json:"metadata,omitempty"`
}

type FeeQuoteRequest struct {
	AccountID   uuid.UUID
	Event       string
	AmountMinor int64
	// At is the moment the quote is for. Zero means now.
	At time.Time
}

type FeeQuoteLine struct {
	Type   string          `json:"type"`
	Amount decimal.Decimal `json:"amount"`
}

type TaxQuoteLine struct {
	RuleID             uuid.UUID       `json:"rule_id"`
	Code               string          `json:"code"`
	Base               string          `json:"base"`
	Rate               string          `json:"rate"`
	Amount             decimal.Decimal `json:"amount"`
	PayableAccountCode string          `json:"payable_account_code"`
}

// FeeQuote is the fee and tax to charge on top of a transaction, in major units and in minor units.
// Total is what leaves the customer's wallet: amount plus fee plus tax.
type FeeQuote struct {
	AccountID uuid.UUID `json:"account_id"`
	Event     string    `json:"event"`
	Currency  string    `json:"currency"`

	Amount decimal.Decimal `json:"amount"`
	Fee    decimal.Decimal `json:"fee"`
	Tax    decimal.Decimal `json:"tax"`
	Gross  decimal.Decimal `json:"gross"`
	Total  decimal.Decimal `json:"total"`

	AmountMinor int64 `json:"amount_minor"`
	FeeMinor    int64 `json:"fee_minor"`
	TaxMinor    int64 `json:"tax_minor"`
	TotalMinor  int64 `json:"total_minor"`

	// FeeSource says which configuration decided the fee: "account", "client", "product" or "none".
	FeeSource     string         `json:"fee_source"`
	FeeOverrideID *uuid.UUID     `json:"fee_override_id,omitempty"`
	FeeLines      []FeeQuoteLine `json:"fee_lines"`
	TaxLines      []TaxQuoteLine `json:"tax_lines"`
}

type FeeConfigService interface {
	CreateOverride(context.Context, CreateFeeOverrideInput) (*models.FeeOverride, error)
	GetOverride(context.Context, uuid.UUID) (*models.FeeOverride, error)
	ListOverrides(context.Context, repositories.FeeOverrideFilter) ([]models.FeeOverride, error)
	// RevokeOverride ends an override now. Quotes already made are not affected.
	RevokeOverride(context.Context, uuid.UUID) (*models.FeeOverride, error)

	CreateTaxRule(context.Context, CreateTaxRuleInput) (*models.TaxRule, error)
	GetTaxRule(context.Context, uuid.UUID) (*models.TaxRule, error)
	ListTaxRules(context.Context, repositories.TaxRuleFilter) ([]models.TaxRule, error)
	RevokeTaxRule(context.Context, uuid.UUID) (*models.TaxRule, error)

	Quote(context.Context, FeeQuoteRequest) (*FeeQuote, error)
}

type DefaultFeeConfigService struct {
	repository repositories.FeeConfigRepository
	accounts   repositories.AccountRepository
	products   repositories.ProductRepository
	clients    repositories.ClientRepository
	glAccounts repositories.GLAccountRepository
	now        func() time.Time
}

func NewFeeConfigService(
	repository repositories.FeeConfigRepository,
	accounts repositories.AccountRepository,
	products repositories.ProductRepository,
	clients repositories.ClientRepository,
	glAccounts repositories.GLAccountRepository,
) *DefaultFeeConfigService {
	return &DefaultFeeConfigService{
		repository: repository,
		accounts:   accounts,
		products:   products,
		clients:    clients,
		glAccounts: glAccounts,
		now:        func() time.Time { return time.Now().UTC() },
	}
}

func feeConfigInvalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrFeeConfigValidation, fmt.Sprintf(format, args...))
}

func validFeeEvent(event string) bool {
	return event == "debit" || event == "credit" || event == "transfer"
}

func notFound(err error, what string) error {
	if errors.Is(err, postgres.ErrNotFound) || postgres.IsNotFoundError(err) {
		return fmt.Errorf("%w: %s", ErrFeeConfigNotFound, what)
	}
	return err
}

// period returns the effective window, defaulting the start to now.
func (s *DefaultFeeConfigService) period(from, to *time.Time) (time.Time, *time.Time, error) {
	start := s.now()
	if from != nil {
		start = from.UTC()
	}
	if to != nil {
		end := to.UTC()
		if !end.After(start) {
			return time.Time{}, nil, feeConfigInvalid("effective_to must be after effective_from")
		}
		return start, &end, nil
	}
	return start, nil, nil
}

func overlaps(aFrom time.Time, aTo *time.Time, bFrom time.Time, bTo *time.Time) bool {
	if aTo != nil && !aTo.After(bFrom) {
		return false
	}
	if bTo != nil && !bTo.After(aFrom) {
		return false
	}
	return true
}

func (s *DefaultFeeConfigService) CreateOverride(ctx context.Context, in CreateFeeOverrideInput) (*models.FeeOverride, error) {
	in.Event = strings.ToLower(strings.TrimSpace(in.Event))
	in.Scope = strings.ToLower(strings.TrimSpace(in.Scope))
	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	if !validFeeEvent(in.Event) {
		return nil, feeConfigInvalid("event must be one of debit, credit, transfer")
	}
	if len(in.Rules) == 0 {
		return nil, feeConfigInvalid("rules must contain at least one fee rule")
	}
	rules := make([]models.TransactionFee, len(in.Rules))
	for i, rule := range in.Rules {
		if rule.Event != "" && rule.Event != in.Event {
			return nil, feeConfigInvalid("rule %d has event %q, expected %q", i+1, rule.Event, in.Event)
		}
		if rule.Currency != "" && strings.ToUpper(rule.Currency) != in.Currency && in.Scope == models.FeeOverrideScopeClient {
			return nil, feeConfigInvalid("rule %d charges in %s, which differs from the override currency", i+1, rule.Currency)
		}
		rule.Event = in.Event
		if err := feequote.ValidateFeeRule(rule); err != nil {
			return nil, feeConfigInvalid("rule %d: %v", i+1, err)
		}
		rules[i] = rule
	}
	from, to, err := s.period(in.EffectiveFrom, in.EffectiveTo)
	if err != nil {
		return nil, err
	}

	override := &models.FeeOverride{
		Scope: in.Scope, Event: in.Event, Rules: rules,
		EffectiveFrom: from, EffectiveTo: to,
		Status: models.FeeConfigStatusActive, Metadata: in.Metadata,
	}
	if override.Metadata == nil {
		override.Metadata = map[string]any{}
	}

	switch in.Scope {
	case models.FeeOverrideScopeAccount:
		if in.AccountID == nil || in.ClientID != nil || in.ProductID != nil {
			return nil, feeConfigInvalid("an account override needs account_id and no client_id or product_id")
		}
		account, err := s.accounts.Get(ctx, *in.AccountID)
		if err != nil {
			return nil, notFound(err, "account")
		}
		if in.Currency != "" && in.Currency != account.Currency {
			return nil, feeConfigInvalid("currency %s does not match the account currency %s", in.Currency, account.Currency)
		}
		override.AccountID = in.AccountID
		override.Currency = account.Currency
	case models.FeeOverrideScopeClient:
		if in.ClientID == nil || in.AccountID != nil {
			return nil, feeConfigInvalid("a client override needs client_id and no account_id")
		}
		if definition, ok := currencyregistry.Lookup(in.Currency); !ok || !definition.Enabled {
			return nil, feeConfigInvalid("currency %q is not enabled", in.Currency)
		}
		if _, err := s.clients.Get(ctx, *in.ClientID); err != nil {
			return nil, notFound(err, "client")
		}
		if in.ProductID != nil {
			product, err := s.products.Get(ctx, *in.ProductID)
			if err != nil {
				return nil, notFound(err, "product")
			}
			if product.Currency != in.Currency {
				return nil, feeConfigInvalid("product %s is in %s, not %s", product.Code, product.Currency, in.Currency)
			}
		}
		override.ClientID = in.ClientID
		override.ProductID = in.ProductID
		override.Currency = in.Currency
	default:
		return nil, feeConfigInvalid("scope must be client or account")
	}

	// Two active overrides for the same target must not cover the same moment.
	active := models.FeeConfigStatusActive
	filter := repositories.FeeOverrideFilter{Event: &in.Event, Status: &active, ClientID: override.ClientID, AccountID: override.AccountID}
	existing, err := s.repository.ListOverrides(ctx, filter)
	if err != nil {
		return nil, err
	}
	for _, other := range existing {
		if other.Currency != override.Currency || !sameUUID(other.ProductID, override.ProductID) {
			continue
		}
		if overlaps(from, to, other.EffectiveFrom, other.EffectiveTo) {
			return nil, fmt.Errorf("%w: override %s already covers this target for part of that period", ErrFeeConfigConflict, other.ID)
		}
	}
	if err := s.repository.CreateOverride(ctx, override); err != nil {
		return nil, err
	}
	return override, nil
}

func sameUUID(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (s *DefaultFeeConfigService) GetOverride(ctx context.Context, id uuid.UUID) (*models.FeeOverride, error) {
	o, err := s.repository.GetOverride(ctx, id)
	if err != nil {
		return nil, notFound(err, "fee override")
	}
	return o, nil
}

func (s *DefaultFeeConfigService) ListOverrides(ctx context.Context, filter repositories.FeeOverrideFilter) ([]models.FeeOverride, error) {
	return s.repository.ListOverrides(ctx, filter)
}

func (s *DefaultFeeConfigService) RevokeOverride(ctx context.Context, id uuid.UUID) (*models.FeeOverride, error) {
	o, err := s.GetOverride(ctx, id)
	if err != nil {
		return nil, err
	}
	if o.Status == models.FeeConfigStatusRevoked {
		return o, nil
	}
	o.Status = models.FeeConfigStatusRevoked
	if now := s.now(); o.EffectiveTo == nil || o.EffectiveTo.After(now) {
		if now.After(o.EffectiveFrom) {
			o.EffectiveTo = &now
		}
	}
	if err := s.repository.UpdateOverride(ctx, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (s *DefaultFeeConfigService) CreateTaxRule(ctx context.Context, in CreateTaxRuleInput) (*models.TaxRule, error) {
	in.Country = strings.ToUpper(strings.TrimSpace(in.Country))
	in.Code = strings.ToUpper(strings.TrimSpace(in.Code))
	in.Event = strings.ToLower(strings.TrimSpace(in.Event))
	in.Base = strings.ToLower(strings.TrimSpace(in.Base))
	in.Name = strings.TrimSpace(in.Name)
	if !countryPattern.MatchString(in.Country) {
		return nil, feeConfigInvalid("country must be a two-letter ISO code")
	}
	if !taxCodePattern.MatchString(in.Code) {
		return nil, feeConfigInvalid("code must be 1 to 32 characters of A-Z, 0-9 and _")
	}
	if in.Name == "" || len(in.Name) > 255 {
		return nil, feeConfigInvalid("name is required and must be at most 255 characters")
	}
	if !validFeeEvent(in.Event) {
		return nil, feeConfigInvalid("event must be one of debit, credit, transfer")
	}
	if err := feequote.ValidateTax(feequote.Tax{Base: in.Base, Rate: in.Rate, Min: in.Min, Max: in.Max}); err != nil {
		return nil, feeConfigInvalid("%v", err)
	}
	if len(in.PayableAccounts) == 0 {
		return nil, feeConfigInvalid("payable_accounts needs at least one currency")
	}
	payable := make(map[string]string, len(in.PayableAccounts))
	for currency, code := range in.PayableAccounts {
		currency = strings.ToUpper(strings.TrimSpace(currency))
		account, err := s.glAccounts.GetByCode(ctx, strings.TrimSpace(code))
		if err != nil {
			if postgres.IsNotFoundError(err) || errors.Is(err, postgres.ErrNotFound) {
				return nil, feeConfigInvalid("payable account %q does not exist", code)
			}
			return nil, err
		}
		switch {
		case account.Currency != currency:
			return nil, feeConfigInvalid("payable account %s is in %s, not %s", account.Code, account.Currency, currency)
		case account.Type != models.GLAccountTypeLiability:
			return nil, feeConfigInvalid("payable account %s must be a liability account", account.Code)
		case account.Status != models.GLAccountStatusActive:
			return nil, feeConfigInvalid("payable account %s is not active", account.Code)
		}
		payable[currency] = account.Code
	}
	from, to, err := s.period(in.EffectiveFrom, in.EffectiveTo)
	if err != nil {
		return nil, err
	}

	active := models.FeeConfigStatusActive
	existing, err := s.repository.ListTaxRules(ctx, repositories.TaxRuleFilter{Country: &in.Country, Event: &in.Event, Status: &active})
	if err != nil {
		return nil, err
	}
	for _, other := range existing {
		if other.Code == in.Code && overlaps(from, to, other.EffectiveFrom, other.EffectiveTo) {
			return nil, fmt.Errorf("%w: tax rule %s already covers %s %s for part of that period", ErrFeeConfigConflict, other.ID, in.Country, in.Code)
		}
	}

	rule := &models.TaxRule{
		Country: in.Country, Code: in.Code, Name: in.Name, Event: in.Event,
		Base: in.Base, Rate: in.Rate, MinAmount: in.Min, MaxAmount: in.Max,
		PayableAccounts: payable, EffectiveFrom: from, EffectiveTo: to,
		Status: models.FeeConfigStatusActive, Metadata: in.Metadata,
	}
	if rule.Metadata == nil {
		rule.Metadata = map[string]any{}
	}
	if err := s.repository.CreateTaxRule(ctx, rule); err != nil {
		return nil, err
	}
	return rule, nil
}

func (s *DefaultFeeConfigService) GetTaxRule(ctx context.Context, id uuid.UUID) (*models.TaxRule, error) {
	t, err := s.repository.GetTaxRule(ctx, id)
	if err != nil {
		return nil, notFound(err, "tax rule")
	}
	return t, nil
}

func (s *DefaultFeeConfigService) ListTaxRules(ctx context.Context, filter repositories.TaxRuleFilter) ([]models.TaxRule, error) {
	return s.repository.ListTaxRules(ctx, filter)
}

func (s *DefaultFeeConfigService) RevokeTaxRule(ctx context.Context, id uuid.UUID) (*models.TaxRule, error) {
	t, err := s.GetTaxRule(ctx, id)
	if err != nil {
		return nil, err
	}
	if t.Status == models.FeeConfigStatusRevoked {
		return t, nil
	}
	t.Status = models.FeeConfigStatusRevoked
	if now := s.now(); t.EffectiveTo == nil || t.EffectiveTo.After(now) {
		if now.After(t.EffectiveFrom) {
			t.EffectiveTo = &now
		}
	}
	if err := s.repository.UpdateTaxRule(ctx, t); err != nil {
		return nil, err
	}
	return t, nil
}

// ClientCountry returns the client's ISO country: the incorporation country of a corporate
// client, otherwise the country of the contact address. Empty when neither is a valid code.
func ClientCountry(client *models.Client) string {
	candidates := make([]string, 0, 2)
	if client.CorporateData != nil {
		candidates = append(candidates, client.CorporateData.IncorporationCountry)
	}
	if client.Contact.Address != nil {
		candidates = append(candidates, client.Contact.Address.Country)
	}
	for _, candidate := range candidates {
		if c := strings.ToUpper(strings.TrimSpace(candidate)); countryPattern.MatchString(c) {
			return c
		}
	}
	return ""
}

func (s *DefaultFeeConfigService) Quote(ctx context.Context, req FeeQuoteRequest) (*FeeQuote, error) {
	if !validFeeEvent(req.Event) {
		return nil, feeConfigInvalid("event must be one of debit, credit, transfer")
	}
	if req.AmountMinor <= 0 {
		return nil, feeConfigInvalid("amount must be positive")
	}
	at := req.At.UTC()
	if req.At.IsZero() {
		at = s.now()
	}
	account, err := s.accounts.Get(ctx, req.AccountID)
	if err != nil {
		return nil, notFound(err, "account")
	}
	product, err := s.products.Get(ctx, account.ProductID)
	if err != nil {
		return nil, notFound(err, "product")
	}
	client, err := s.clients.Get(ctx, account.ClientID)
	if err != nil {
		return nil, notFound(err, "client")
	}

	rules, source, overrideID, err := s.resolveFeeRules(ctx, account, product, req.Event, at)
	if err != nil {
		return nil, err
	}

	taxRules, err := s.resolveTaxRules(ctx, client, account.Currency, req.Event, at)
	if err != nil {
		return nil, err
	}
	taxes := make([]feequote.Tax, len(taxRules))
	for i, rule := range taxRules {
		taxes[i] = feequote.Tax{Base: rule.Base, Rate: rule.Rate, Min: rule.MinAmount, Max: rule.MaxAmount}
	}

	precision := currencyPrecision(account.Currency)
	result, err := feequote.Quote(feequote.Input{
		Amount:    minorUnitsToDecimal(req.AmountMinor, account.Currency),
		Precision: precision,
		Fees:      rules,
		Taxes:     taxes,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFeeConfigValidation, err)
	}

	quote := &FeeQuote{
		AccountID: account.ID, Event: req.Event, Currency: account.Currency,
		Amount: result.Amount, Fee: result.Fee, Tax: result.Tax, Gross: result.Gross, Total: result.Total,
		AmountMinor: req.AmountMinor,
		FeeMinor:    minorOf(result.Fee, precision),
		TaxMinor:    minorOf(result.Tax, precision),
		TotalMinor:  minorOf(result.Total, precision),
		FeeSource:   source, FeeOverrideID: overrideID,
		FeeLines: make([]FeeQuoteLine, len(rules)),
		TaxLines: make([]TaxQuoteLine, len(taxRules)),
	}
	for i, rule := range rules {
		quote.FeeLines[i] = FeeQuoteLine{Type: rule.Type, Amount: result.FeeLines[i]}
	}
	for i, rule := range taxRules {
		quote.TaxLines[i] = TaxQuoteLine{
			RuleID: rule.ID, Code: rule.Code, Base: rule.Base, Rate: rule.Rate,
			Amount: result.TaxLines[i], PayableAccountCode: rule.PayableAccounts[account.Currency],
		}
	}
	return quote, nil
}

func minorOf(amount decimal.Decimal, precision int32) int64 {
	return amount.Shift(precision).Round(0).IntPart()
}

// resolveFeeRules picks the fee rules for an account and event: account override, then client
// override for the account's product, then client override for the currency, then the product's
// own schedule. An override replaces the product's rules for that event.
func (s *DefaultFeeConfigService) resolveFeeRules(ctx context.Context, account *models.Account, product *models.Product, event string, at time.Time) ([]models.TransactionFee, string, *uuid.UUID, error) {
	overrides, err := s.repository.ActiveOverridesFor(ctx, account.ID, account.ClientID, event, at)
	if err != nil {
		return nil, "", nil, err
	}
	var best *models.FeeOverride
	bestRank := 0
	for i := range overrides {
		o := &overrides[i]
		if o.Currency != account.Currency {
			continue
		}
		rank := 0
		switch {
		case o.AccountID != nil && *o.AccountID == account.ID:
			rank = 3
		case o.ClientID != nil && *o.ClientID == account.ClientID && o.ProductID != nil && *o.ProductID == account.ProductID:
			rank = 2
		case o.ClientID != nil && *o.ClientID == account.ClientID && o.ProductID == nil:
			rank = 1
		}
		if rank == 0 {
			continue
		}
		if rank > bestRank || (rank == bestRank && o.EffectiveFrom.After(best.EffectiveFrom)) {
			best, bestRank = o, rank
		}
	}
	if best != nil {
		source := models.FeeOverrideScopeClient
		if bestRank == 3 {
			source = models.FeeOverrideScopeAccount
		}
		id := best.ID
		return best.Rules, source, &id, nil
	}

	var rules []models.TransactionFee
	if product.FeeSchedule != nil {
		for _, rule := range product.FeeSchedule.TransactionFees {
			if rule.Event != event {
				continue
			}
			if rule.Currency != "" && !strings.EqualFold(rule.Currency, account.Currency) {
				return nil, "", nil, feeConfigInvalid("product fee rule charges in %s, which differs from the account currency %s", rule.Currency, account.Currency)
			}
			rules = append(rules, rule)
		}
	}
	if len(rules) == 0 {
		return nil, "none", nil, nil
	}
	return rules, "product", nil, nil
}

func (s *DefaultFeeConfigService) resolveTaxRules(ctx context.Context, client *models.Client, currency, event string, at time.Time) ([]models.TaxRule, error) {
	country := ClientCountry(client)
	if country == "" {
		configured, err := s.repository.HasActiveTaxRules(ctx, event, at)
		if err != nil {
			return nil, err
		}
		if configured {
			return nil, fmt.Errorf("%w: client %s", ErrTaxCountryUnknown, client.ClientNumber)
		}
		return nil, nil
	}
	rules, err := s.repository.ActiveTaxRules(ctx, country, event, at)
	if err != nil {
		return nil, err
	}
	for _, rule := range rules {
		if _, ok := rule.PayableAccounts[currency]; !ok {
			return nil, fmt.Errorf("%w: %s %s in %s", ErrTaxAccountMissing, country, rule.Code, currency)
		}
	}
	return rules, nil
}
