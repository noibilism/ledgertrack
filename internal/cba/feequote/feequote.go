// Package feequote computes the fee and the tax that go with a transaction. It is pure:
// no storage and no ledger. Callers resolve which fee rule and which tax rule apply and
// pass them in; the package returns amounts rounded to the currency precision.
package feequote

import (
	"errors"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/formancehq/ledger/internal/cba/models"
)

const (
	FeeTypeFlat       = "flat"
	FeeTypePercentage = "percentage"
	FeeTypeTiered     = "tiered"

	// TaxBaseFee taxes the (rounded) fee. TaxBaseAmount taxes the transaction amount.
	TaxBaseFee    = "fee"
	TaxBaseAmount = "amount"
)

var (
	ErrInvalidRule  = errors.New("invalid fee or tax rule")
	ErrInvalidInput = errors.New("invalid quote input")
)

var hundred = decimal.NewFromInt(100)

// Tax is the calculation part of a tax rule. Rate is a percentage, so "7.5" means 7.5%.
type Tax struct {
	Base string
	Rate string
	Min  *string
	Max  *string
}

// Input describes one quote. Amount is in major units. Fee and Tax may be nil.
type Input struct {
	Amount    decimal.Decimal
	Precision int32
	Fee       *models.TransactionFee
	Tax       *Tax
}

// Result holds the amounts of one quote, all rounded to the currency precision.
// Gross is fee plus tax, what is charged on top of the amount. Total is amount plus Gross,
// what leaves the customer's wallet.
type Result struct {
	Amount decimal.Decimal
	Fee    decimal.Decimal
	Tax    decimal.Decimal
	Gross  decimal.Decimal
	Total  decimal.Decimal
}

// Quote computes the fee, then the tax on the fee or on the amount, rounding each line
// half-even to the currency precision on its own. Tax on the fee uses the rounded fee.
func Quote(in Input) (Result, error) {
	if !in.Amount.IsPositive() {
		return Result{}, fmt.Errorf("%w: amount must be positive", ErrInvalidInput)
	}
	if in.Precision < 0 || in.Precision > 18 {
		return Result{}, fmt.Errorf("%w: precision %d", ErrInvalidInput, in.Precision)
	}
	out := Result{Amount: in.Amount.RoundBank(in.Precision)}
	if in.Fee != nil {
		fee, err := FeeAmount(*in.Fee, in.Amount)
		if err != nil {
			return Result{}, err
		}
		out.Fee = fee.RoundBank(in.Precision)
	}
	if in.Tax != nil {
		base := in.Amount
		if in.Tax.Base == TaxBaseFee {
			base = out.Fee
		}
		tax, err := TaxAmount(*in.Tax, base)
		if err != nil {
			return Result{}, err
		}
		out.Tax = tax.RoundBank(in.Precision)
	}
	out.Gross = out.Fee.Add(out.Tax)
	out.Total = out.Amount.Add(out.Gross)
	return out, nil
}

// FeeAmount returns the unrounded fee for an amount, with min and max applied.
func FeeAmount(rule models.TransactionFee, amount decimal.Decimal) (decimal.Decimal, error) {
	if err := ValidateFeeRule(rule); err != nil {
		return decimal.Zero, err
	}
	fee, err := baseFee(rule.Type, rule.Value, rule.Tiers, amount)
	if err != nil {
		return decimal.Zero, err
	}
	return applyBounds(fee, rule.Min, rule.Max)
}

// TaxAmount returns the unrounded tax on base, with min and max applied. A zero base
// gives zero tax, even when a minimum is set.
func TaxAmount(rule Tax, base decimal.Decimal) (decimal.Decimal, error) {
	if err := ValidateTax(rule); err != nil {
		return decimal.Zero, err
	}
	if !base.IsPositive() {
		return decimal.Zero, nil
	}
	rate := decimal.RequireFromString(rule.Rate)
	return applyBounds(base.Mul(rate).Div(hundred), rule.Min, rule.Max)
}

func baseFee(kind, value string, tiers []models.FeeTier, amount decimal.Decimal) (decimal.Decimal, error) {
	switch kind {
	case FeeTypeFlat:
		return decimal.RequireFromString(value), nil
	case FeeTypePercentage:
		return amount.Mul(decimal.RequireFromString(value)).Div(hundred), nil
	case FeeTypeTiered:
		for _, tier := range tiers {
			if amount.LessThan(decimal.RequireFromString(tier.From)) {
				continue
			}
			if tier.To != nil && amount.GreaterThanOrEqual(decimal.RequireFromString(*tier.To)) {
				continue
			}
			return baseFee(tier.Type, tier.Value, nil, amount)
		}
		return decimal.Zero, fmt.Errorf("%w: no tier covers amount %s", ErrInvalidRule, amount)
	}
	return decimal.Zero, fmt.Errorf("%w: unsupported fee type %q", ErrInvalidRule, kind)
}

func applyBounds(value decimal.Decimal, minimum, maximum *string) (decimal.Decimal, error) {
	if minimum != nil {
		m, err := decimal.NewFromString(*minimum)
		if err != nil {
			return decimal.Zero, fmt.Errorf("%w: invalid min bound", ErrInvalidRule)
		}
		if value.LessThan(m) {
			value = m
		}
	}
	if maximum != nil {
		m, err := decimal.NewFromString(*maximum)
		if err != nil {
			return decimal.Zero, fmt.Errorf("%w: invalid max bound", ErrInvalidRule)
		}
		if value.GreaterThan(m) {
			value = m
		}
	}
	return value, nil
}

// ValidateFeeRule checks the calculation fields of a fee rule (not its event or currency).
// Tiered rules need bands that start at zero, touch each other and end open.
func ValidateFeeRule(rule models.TransactionFee) error {
	switch rule.Type {
	case FeeTypeFlat:
		if _, err := nonNegative("fee value", rule.Value); err != nil {
			return err
		}
	case FeeTypePercentage:
		if err := percent("fee value", rule.Value); err != nil {
			return err
		}
	case FeeTypeTiered:
		if err := validateTiers(rule.Tiers); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported fee type %q", ErrInvalidRule, rule.Type)
	}
	return validateBounds(rule.Min, rule.Max)
}

// ValidateTax checks a tax rule's calculation fields.
func ValidateTax(rule Tax) error {
	switch rule.Base {
	case TaxBaseFee, TaxBaseAmount:
	default:
		return fmt.Errorf("%w: tax base must be fee or amount, got %q", ErrInvalidRule, rule.Base)
	}
	if err := percent("tax rate", rule.Rate); err != nil {
		return err
	}
	return validateBounds(rule.Min, rule.Max)
}

func validateTiers(tiers []models.FeeTier) error {
	if len(tiers) == 0 {
		return fmt.Errorf("%w: tiered fee needs at least one tier", ErrInvalidRule)
	}
	expectedFrom := decimal.Zero
	for i, tier := range tiers {
		from, err := nonNegative("tier from", tier.From)
		if err != nil {
			return err
		}
		if !from.Equal(expectedFrom) {
			return fmt.Errorf("%w: tier %d starts at %s, expected %s (tiers must start at 0 and touch)", ErrInvalidRule, i+1, from, expectedFrom)
		}
		last := i == len(tiers)-1
		switch {
		case tier.To == nil && !last:
			return fmt.Errorf("%w: only the last tier may be open ended", ErrInvalidRule)
		case tier.To != nil && last:
			return fmt.Errorf("%w: the last tier must be open ended", ErrInvalidRule)
		case tier.To != nil:
			to, err := nonNegative("tier to", *tier.To)
			if err != nil {
				return err
			}
			if !to.GreaterThan(from) {
				return fmt.Errorf("%w: tier %d ends at or before its start", ErrInvalidRule, i+1)
			}
			expectedFrom = to
		}
		switch tier.Type {
		case FeeTypeFlat:
			if _, err := nonNegative("tier value", tier.Value); err != nil {
				return err
			}
		case FeeTypePercentage:
			if err := percent("tier value", tier.Value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: tier type must be flat or percentage, got %q", ErrInvalidRule, tier.Type)
		}
	}
	return nil
}

func validateBounds(minimum, maximum *string) error {
	var lo, hi *decimal.Decimal
	if minimum != nil {
		v, err := nonNegative("min bound", *minimum)
		if err != nil {
			return err
		}
		lo = &v
	}
	if maximum != nil {
		v, err := nonNegative("max bound", *maximum)
		if err != nil {
			return err
		}
		hi = &v
	}
	if lo != nil && hi != nil && lo.GreaterThan(*hi) {
		return fmt.Errorf("%w: min bound is above max bound", ErrInvalidRule)
	}
	return nil
}

func nonNegative(name, raw string) (decimal.Decimal, error) {
	v, err := decimal.NewFromString(raw)
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: %s %q is not a number", ErrInvalidRule, name, raw)
	}
	if v.IsNegative() {
		return decimal.Zero, fmt.Errorf("%w: %s must not be negative", ErrInvalidRule, name)
	}
	return v, nil
}

func percent(name, raw string) error {
	v, err := nonNegative(name, raw)
	if err != nil {
		return err
	}
	if v.GreaterThan(hundred) {
		return fmt.Errorf("%w: %s must not exceed 100", ErrInvalidRule, name)
	}
	return nil
}
