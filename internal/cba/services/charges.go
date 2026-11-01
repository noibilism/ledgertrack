package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

// ErrChargeConfig means the fee or tax configuration cannot be posted: a fee rule has no income
// account, a tax rule has no payable account for the currency, or an account is closed, a header, in
// the wrong currency, or in another ledger than the transaction. It is a setup problem, not a
// problem with the request.
var ErrChargeConfig = errors.New("fee or tax configuration cannot be posted")

const (
	ChargeKindFee = "fee"
	ChargeKindTax = "tax"
)

type ChargeRequest struct {
	AccountID   uuid.UUID
	Event       string
	AmountMinor int64
	// Ledger is the ledger the transaction will be posted in. The GL accounts must live there so the
	// customer leg, the fee and the tax commit together.
	Ledger string
	At     time.Time
}

// ChargeLine is one posting of a charge: a fee to an income account or a tax to a payable account.
type ChargeLine struct {
	Kind        string `json:"kind"`
	Code        string `json:"code"`
	Address     string `json:"address"`
	AmountMinor int64  `json:"amount_minor"`
	// Label is the fee type or the tax code.
	Label string `json:"label"`
}

// ChargePlan is what a transaction charges on top of its amount, resolved to ledger addresses.
type ChargePlan struct {
	Event      string       `json:"event"`
	Currency   string       `json:"currency"`
	FeeMinor   int64        `json:"fee_minor"`
	TaxMinor   int64        `json:"tax_minor"`
	GrossMinor int64        `json:"gross_minor"`
	FeeSource  string       `json:"fee_source"`
	Lines      []ChargeLine `json:"lines"`
}

// ChargeService turns the fee quote of a transaction into postings. It reads configuration only.
type ChargeService interface {
	// Plan returns nil when the transaction carries no fee and no tax.
	Plan(context.Context, ChargeRequest) (*ChargePlan, error)
}

type DefaultChargeService struct {
	fees FeeConfigService
	gl   repositories.GLAccountRepository
}

func NewChargeService(fees FeeConfigService, gl repositories.GLAccountRepository) *DefaultChargeService {
	return &DefaultChargeService{fees: fees, gl: gl}
}

func chargeConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrChargeConfig, fmt.Sprintf(format, args...))
}

func (s *DefaultChargeService) Plan(ctx context.Context, req ChargeRequest) (*ChargePlan, error) {
	quote, err := s.fees.Quote(ctx, FeeQuoteRequest{AccountID: req.AccountID, Event: req.Event, AmountMinor: req.AmountMinor, At: req.At})
	if err != nil {
		return nil, err
	}
	if quote.FeeMinor+quote.TaxMinor == 0 {
		return nil, nil
	}
	precision := currencyPrecision(quote.Currency)
	plan := &ChargePlan{
		Event: quote.Event, Currency: quote.Currency, FeeSource: quote.FeeSource,
		FeeMinor: quote.FeeMinor, TaxMinor: quote.TaxMinor, GrossMinor: quote.FeeMinor + quote.TaxMinor,
	}
	var feeSum, taxSum int64
	for _, line := range quote.FeeLines {
		minor := minorOf(line.Amount, precision)
		if minor == 0 {
			continue
		}
		if line.IncomeAccountCode == "" {
			return nil, chargeConfig("the %s fee has no income account; set income_account on the fee rule", line.Type)
		}
		account, err := s.account(ctx, line.IncomeAccountCode, quote.Currency, req.Ledger, models.GLAccountTypeIncome, "income")
		if err != nil {
			return nil, err
		}
		plan.Lines = append(plan.Lines, ChargeLine{Kind: ChargeKindFee, Code: account.Code, Address: account.Address, AmountMinor: minor, Label: line.Type})
		feeSum += minor
	}
	for _, line := range quote.TaxLines {
		minor := minorOf(line.Amount, precision)
		if minor == 0 {
			continue
		}
		if line.PayableAccountCode == "" {
			return nil, chargeConfig("tax %s has no payable account in %s", line.Code, quote.Currency)
		}
		account, err := s.account(ctx, line.PayableAccountCode, quote.Currency, req.Ledger, models.GLAccountTypeLiability, "payable")
		if err != nil {
			return nil, err
		}
		plan.Lines = append(plan.Lines, ChargeLine{Kind: ChargeKindTax, Code: account.Code, Address: account.Address, AmountMinor: minor, Label: line.Code})
		taxSum += minor
	}
	if feeSum != quote.FeeMinor || taxSum != quote.TaxMinor {
		// The lines are each rounded to the currency precision, so they must add up to the totals.
		return nil, chargeConfig("fee and tax lines (%d, %d) do not add up to the quote (%d, %d)", feeSum, taxSum, quote.FeeMinor, quote.TaxMinor)
	}
	return plan, nil
}

func (s *DefaultChargeService) account(ctx context.Context, code, currency, ledger, accountType, role string) (*models.GLAccount, error) {
	account, err := s.gl.GetByCode(ctx, strings.TrimSpace(code))
	if err != nil {
		if postgres.IsNotFoundError(err) || errors.Is(err, postgres.ErrNotFound) {
			return nil, chargeConfig("%s account %q does not exist", role, code)
		}
		return nil, err
	}
	switch {
	case account.Currency != currency:
		return nil, chargeConfig("%s account %s is in %s, not %s", role, account.Code, account.Currency, currency)
	case account.Type != accountType:
		return nil, chargeConfig("%s account %s is a %s account", role, account.Code, account.Type)
	case account.Status != models.GLAccountStatusActive:
		return nil, chargeConfig("%s account %s is not active", role, account.Code)
	case !glAccountPosts(account):
		return nil, chargeConfig("%s account %s is a header and takes no postings", role, account.Code)
	case account.Ledger != ledger:
		return nil, chargeConfig("%s account %s is in ledger %s, the transaction is in %s", role, account.Code, account.Ledger, ledger)
	}
	return account, nil
}
