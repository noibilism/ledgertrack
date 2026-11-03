package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
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
	ErrReserveValidation = errors.New("reserve rule validation failed")
	ErrReserveNotFound   = errors.New("reserve rule not found")
	ErrReserveConflict   = errors.New("reserve rule overlaps an existing one")
)

const (
	// MaxReserveHoldHours is one year, the longest hold a rule can set.
	MaxReserveHoldHours = 8760

	ReserveSourceAccount = "account"
	ReserveSourceClient  = "client"
	ReserveSourceNone    = "none"
)

var oneHundred = decimal.NewFromInt(100)

type CreateReserveRuleInput struct {
	Scope      string     `json:"scope"`
	ClientID   *uuid.UUID `json:"client_id,omitempty"`
	AccountID  *uuid.UUID `json:"account_id,omitempty"`
	ProductID  *uuid.UUID `json:"product_id,omitempty"`
	Currency   string     `json:"currency,omitempty"`
	Percentage string     `json:"percentage"`
	HoldHours  int        `json:"hold_hours"`
	// HoldMode is "calendar" (default) or "business". Business needs CalendarID.
	HoldMode   string     `json:"hold_mode,omitempty"`
	CalendarID *uuid.UUID `json:"calendar_id,omitempty"`
	// CreditTypes limits the rule to these credit types. Empty covers every credit.
	CreditTypes   []string       `json:"credit_types,omitempty"`
	EffectiveFrom *time.Time     `json:"effective_from,omitempty"`
	EffectiveTo   *time.Time     `json:"effective_to,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type ReservePreviewRequest struct {
	AccountID   uuid.UUID
	AmountMinor int64
	// CreditType is the type of the credit, matched against rules that list credit types. Empty
	// matches only rules that cover every credit.
	CreditType string
	// At is the moment of the credit. Zero means now.
	At time.Time
}

// ReservePreview says how a credit would be split: ReserveMinor goes to the ledger balance (held) until
// ReleaseAt, AvailableMinor is available at once. Nothing is posted.
type ReservePreview struct {
	AccountID      uuid.UUID  `json:"account_id"`
	Currency       string     `json:"currency"`
	AmountMinor    int64      `json:"amount_minor"`
	ReserveMinor   int64      `json:"reserve_minor"`
	AvailableMinor int64      `json:"available_minor"`
	Percentage     string     `json:"percentage,omitempty"`
	HoldHours      int        `json:"hold_hours,omitempty"`
	HoldMode       string     `json:"hold_mode,omitempty"`
	CalendarID     *uuid.UUID `json:"calendar_id,omitempty"`
	CreditType     string     `json:"credit_type,omitempty"`
	ReleaseAt      *time.Time `json:"release_at,omitempty"`
	// RuleSource is "account", "client" or "none" when no rule applies.
	RuleSource string     `json:"rule_source"`
	RuleID     *uuid.UUID `json:"rule_id,omitempty"`
}

type ReserveRuleService interface {
	CreateRule(context.Context, CreateReserveRuleInput) (*models.ReserveRule, error)
	GetRule(context.Context, uuid.UUID) (*models.ReserveRule, error)
	ListRules(context.Context, repositories.ReserveRuleFilter) ([]models.ReserveRule, error)
	// RevokeRule ends a rule now. Holds already made keep their release time.
	RevokeRule(context.Context, uuid.UUID) (*models.ReserveRule, error)
	Preview(context.Context, ReservePreviewRequest) (*ReservePreview, error)
}

type DefaultReserveRuleService struct {
	repository repositories.ReserveRepository
	accounts   repositories.AccountRepository
	products   repositories.ProductRepository
	clients    repositories.ClientRepository
	calendars  repositories.CalendarRepository
	now        func() time.Time
}

func NewReserveRuleService(
	repository repositories.ReserveRepository,
	accounts repositories.AccountRepository,
	products repositories.ProductRepository,
	clients repositories.ClientRepository,
	calendars repositories.CalendarRepository,
) *DefaultReserveRuleService {
	return &DefaultReserveRuleService{
		repository: repository, accounts: accounts, products: products, clients: clients, calendars: calendars,
		now: func() time.Time { return time.Now().UTC() },
	}
}

func reserveInvalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrReserveValidation, fmt.Sprintf(format, args...))
}

func reserveNotFound(err error, what string) error {
	if errors.Is(err, postgres.ErrNotFound) || postgres.IsNotFoundError(err) {
		return fmt.Errorf("%w: %s", ErrReserveNotFound, what)
	}
	return err
}

// validateReservePercentage accepts a decimal above 0 and up to 100 with at most 4 decimals.
func validateReservePercentage(raw string) (decimal.Decimal, error) {
	value, err := decimal.NewFromString(strings.TrimSpace(raw))
	if err != nil {
		return decimal.Zero, reserveInvalid("percentage %q is not a number", raw)
	}
	if !value.IsPositive() || value.GreaterThan(oneHundred) {
		return decimal.Zero, reserveInvalid("percentage must be above 0 and at most 100")
	}
	if value.Exponent() < -4 {
		return decimal.Zero, reserveInvalid("percentage can have at most 4 decimals")
	}
	return value, nil
}

var creditTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

const maxCreditTypes = 20

// normalizeCreditTypes lowercases, validates, de-duplicates and sorts the list.
func normalizeCreditTypes(in []string) ([]string, error) {
	if len(in) > maxCreditTypes {
		return nil, reserveInvalid("at most %d credit types per rule", maxCreditTypes)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.ToLower(strings.TrimSpace(raw))
		if !creditTypePattern.MatchString(v) {
			return nil, reserveInvalid("credit type %q must be lowercase letters, digits and underscores, starting with a letter", raw)
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out, nil
}

// creditTypesIntersect says whether two rules compete for the same credits. Two general rules do, and
// two typed rules sharing a type do. A typed rule and a general one do not: the typed rule wins for its
// own types, so a general rule can sit beside it.
func creditTypesIntersect(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

func (s *DefaultReserveRuleService) CreateRule(ctx context.Context, in CreateReserveRuleInput) (*models.ReserveRule, error) {
	in.Scope = strings.ToLower(strings.TrimSpace(in.Scope))
	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	percentage, err := validateReservePercentage(in.Percentage)
	if err != nil {
		return nil, err
	}
	if in.HoldHours < 1 || in.HoldHours > MaxReserveHoldHours {
		return nil, reserveInvalid("hold_hours must be between 1 and %d", MaxReserveHoldHours)
	}

	types, err := normalizeCreditTypes(in.CreditTypes)
	if err != nil {
		return nil, err
	}
	mode := strings.ToLower(strings.TrimSpace(in.HoldMode))
	if mode == "" {
		mode = models.HoldModeCalendar
	}
	switch mode {
	case models.HoldModeCalendar:
		if in.CalendarID != nil {
			return nil, reserveInvalid("calendar_id is only for hold_mode business")
		}
	case models.HoldModeBusiness:
		if in.CalendarID == nil {
			return nil, reserveInvalid("hold_mode business needs a calendar_id")
		}
		if _, err := s.calendars.GetCalendar(ctx, *in.CalendarID); err != nil {
			return nil, reserveNotFound(err, "business calendar")
		}
	default:
		return nil, reserveInvalid("hold_mode must be calendar or business")
	}

	from := s.now()
	if in.EffectiveFrom != nil {
		from = in.EffectiveFrom.UTC()
	}
	var to *time.Time
	if in.EffectiveTo != nil {
		end := in.EffectiveTo.UTC()
		if !end.After(from) {
			return nil, reserveInvalid("effective_to must be after effective_from")
		}
		to = &end
	}

	rule := &models.ReserveRule{
		Scope: in.Scope, Percentage: percentage.String(), HoldHours: in.HoldHours,
		HoldMode: mode, CalendarID: in.CalendarID, CreditTypes: types,
		EffectiveFrom: from, EffectiveTo: to, Status: models.ReserveStatusActive, Metadata: in.Metadata,
	}
	if rule.Metadata == nil {
		rule.Metadata = map[string]any{}
	}

	switch in.Scope {
	case models.ReserveScopeAccount:
		if in.AccountID == nil || in.ClientID != nil || in.ProductID != nil {
			return nil, reserveInvalid("an account rule needs account_id and no client_id or product_id")
		}
		account, err := s.accounts.Get(ctx, *in.AccountID)
		if err != nil {
			return nil, reserveNotFound(err, "account")
		}
		if in.Currency != "" && in.Currency != account.Currency {
			return nil, reserveInvalid("currency %s does not match the account currency %s", in.Currency, account.Currency)
		}
		rule.AccountID = in.AccountID
		rule.Currency = account.Currency
	case models.ReserveScopeClient:
		if in.ClientID == nil || in.AccountID != nil {
			return nil, reserveInvalid("a client rule needs client_id and no account_id")
		}
		if definition, ok := currencyregistry.Lookup(in.Currency); !ok || !definition.Enabled {
			return nil, reserveInvalid("currency %q is not enabled", in.Currency)
		}
		if _, err := s.clients.Get(ctx, *in.ClientID); err != nil {
			return nil, reserveNotFound(err, "client")
		}
		if in.ProductID != nil {
			product, err := s.products.Get(ctx, *in.ProductID)
			if err != nil {
				return nil, reserveNotFound(err, "product")
			}
			if product.Currency != in.Currency {
				return nil, reserveInvalid("product %s is in %s, not %s", product.Code, product.Currency, in.Currency)
			}
		}
		rule.ClientID = in.ClientID
		rule.ProductID = in.ProductID
		rule.Currency = in.Currency
	default:
		return nil, reserveInvalid("scope must be client or account")
	}

	// Two active rules for the same target must not cover the same moment.
	active := models.ReserveStatusActive
	existing, err := s.repository.ListRules(ctx, repositories.ReserveRuleFilter{Status: &active, ClientID: rule.ClientID, AccountID: rule.AccountID})
	if err != nil {
		return nil, err
	}
	for _, other := range existing {
		if other.Currency != rule.Currency || !sameUUID(other.ProductID, rule.ProductID) {
			continue
		}
		if creditTypesIntersect(types, other.CreditTypes) && overlaps(from, to, other.EffectiveFrom, other.EffectiveTo) {
			return nil, fmt.Errorf("%w: rule %s already covers this target for part of that period", ErrReserveConflict, other.ID)
		}
	}
	if err := s.repository.CreateRule(ctx, rule); err != nil {
		return nil, err
	}
	return rule, nil
}

func (s *DefaultReserveRuleService) GetRule(ctx context.Context, id uuid.UUID) (*models.ReserveRule, error) {
	rule, err := s.repository.GetRule(ctx, id)
	if err != nil {
		return nil, reserveNotFound(err, "reserve rule")
	}
	return rule, nil
}

func (s *DefaultReserveRuleService) ListRules(ctx context.Context, filter repositories.ReserveRuleFilter) ([]models.ReserveRule, error) {
	return s.repository.ListRules(ctx, filter)
}

func (s *DefaultReserveRuleService) RevokeRule(ctx context.Context, id uuid.UUID) (*models.ReserveRule, error) {
	rule, err := s.GetRule(ctx, id)
	if err != nil {
		return nil, err
	}
	if rule.Status == models.ReserveStatusRevoked {
		return rule, nil
	}
	rule.Status = models.ReserveStatusRevoked
	if now := s.now(); rule.EffectiveTo == nil || rule.EffectiveTo.After(now) {
		if now.After(rule.EffectiveFrom) {
			rule.EffectiveTo = &now
		}
	}
	if err := s.repository.UpdateRule(ctx, rule); err != nil {
		return nil, err
	}
	return rule, nil
}

// resolveRule picks the most specific active rule: the account's own, then the client's rule for the
// account's product, then the client's rule for the currency.
func (s *DefaultReserveRuleService) resolveRule(ctx context.Context, account *models.Account, creditType string, at time.Time) (*models.ReserveRule, string, error) {
	rules, err := s.repository.ActiveRulesFor(ctx, account.ID, account.ClientID, at)
	if err != nil {
		return nil, "", err
	}
	var best *models.ReserveRule
	bestRank := 0
	for i := range rules {
		r := &rules[i]
		if r.Currency != account.Currency {
			continue
		}
		typed := len(r.CreditTypes) > 0
		if typed && !slices.Contains(r.CreditTypes, creditType) {
			continue
		}
		rank := 0
		switch {
		case r.AccountID != nil && *r.AccountID == account.ID:
			rank = 3
		case r.ClientID != nil && *r.ClientID == account.ClientID && r.ProductID != nil && *r.ProductID == account.ProductID:
			rank = 2
		case r.ClientID != nil && *r.ClientID == account.ClientID && r.ProductID == nil:
			rank = 1
		}
		if rank == 0 {
			continue
		}
		// a rule that names the credit type beats a general one at the same level
		score := rank * 2
		if typed {
			score++
		}
		if score > bestRank {
			best, bestRank = r, score
		}
	}
	switch {
	case best == nil:
		return nil, ReserveSourceNone, nil
	case bestRank >= 6:
		return best, ReserveSourceAccount, nil
	default:
		return best, ReserveSourceClient, nil
	}
}

func (s *DefaultReserveRuleService) Preview(ctx context.Context, req ReservePreviewRequest) (*ReservePreview, error) {
	if req.AmountMinor <= 0 {
		return nil, reserveInvalid("amount must be positive")
	}
	at := req.At.UTC()
	if req.At.IsZero() {
		at = s.now()
	}
	account, err := s.accounts.Get(ctx, req.AccountID)
	if err != nil {
		return nil, reserveNotFound(err, "account")
	}
	preview := &ReservePreview{
		AccountID: account.ID, Currency: account.Currency, AmountMinor: req.AmountMinor,
		AvailableMinor: req.AmountMinor, RuleSource: ReserveSourceNone,
	}
	creditType := strings.ToLower(strings.TrimSpace(req.CreditType))
	if creditType != "" && !creditTypePattern.MatchString(creditType) {
		return nil, reserveInvalid("credit_type %q is not valid", req.CreditType)
	}
	rule, source, err := s.resolveRule(ctx, account, creditType, at)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return preview, nil
	}
	reserve := ReserveAmountMinor(req.AmountMinor, decimal.RequireFromString(rule.Percentage))
	release := at.Add(time.Duration(rule.HoldHours) * time.Hour)
	if rule.HoldMode == models.HoldModeBusiness && rule.CalendarID != nil {
		calendar, err := s.calendars.GetCalendar(ctx, *rule.CalendarID)
		if err != nil {
			return nil, reserveNotFound(err, "business calendar")
		}
		if release, err = AddBusinessHours(at, rule.HoldHours, calendar); err != nil {
			return nil, err
		}
	}
	preview.HoldMode = rule.HoldMode
	preview.CalendarID = rule.CalendarID
	preview.CreditType = creditType
	preview.ReserveMinor = reserve
	preview.AvailableMinor = req.AmountMinor - reserve
	preview.Percentage = rule.Percentage
	preview.HoldHours = rule.HoldHours
	preview.ReleaseAt = &release
	preview.RuleSource = source
	preview.RuleID = &rule.ID
	return preview, nil
}

// ReserveAmountMinor is the share of a credit that is held: percentage of the amount in minor units,
// rounded half-even to a whole minor unit. It never exceeds the amount.
func ReserveAmountMinor(amountMinor int64, percentage decimal.Decimal) int64 {
	held := decimal.NewFromInt(amountMinor).Mul(percentage).Div(oneHundred).RoundBank(0).IntPart()
	if held > amountMinor {
		return amountMinor
	}
	return held
}
