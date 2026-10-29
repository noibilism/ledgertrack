package currency

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

var (
	// ErrInvalidAmount is returned when an amount string is not a valid number.
	ErrInvalidAmount = errors.New("invalid amount")
	// ErrTooManyDecimals is returned when an amount has more significant fractional
	// digits than the currency precision allows. Amounts are never rounded.
	ErrTooManyDecimals = errors.New("too many decimal places")
	// ErrAmountOutOfRange is returned when an amount does not fit in an int64 of minor units.
	ErrAmountOutOfRange = errors.New("amount out of range")
)

const (
	maxAmountLen = 64
	maxExponent  = 30
	minExponent  = -100
)

// ParseAmount converts an API amount into minor units of the given currency.
//
// Two forms are accepted, matching the historical API behaviour:
//   - an integer literal (no '.') is already expressed in minor units and is
//     returned as is: "1050" is 1050 minor units (10.50 for a 2dp currency);
//   - a literal containing a '.' (or an exponent) is a decimal amount in major
//     units: "10.50" is 1050 minor units for a 2dp currency, 10500000 for USDT (6dp).
//
// Decimal amounts are parsed exactly (no floating point). If the amount has
// more fractional digits than the currency precision it is rejected with
// ErrTooManyDecimals, it is never rounded. Trailing zeros beyond the precision
// ("10.500" for a 2dp currency) are accepted because the value is exact.
// An unknown currency yields ErrUnknownCurrency, and an empty amount yields 0.
func ParseAmount(amount string, code string) (int64, error) {
	amount = strings.TrimSpace(amount)
	if amount == "" {
		return 0, nil
	}

	definition, ok := Lookup(code)
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownCurrency, strings.TrimSpace(code))
	}
	precision := definition.Precision
	if precision < 0 || precision > MaxPrecision {
		return 0, fmt.Errorf("%w: %s has precision %d", ErrInvalidPrecision, normalizeCode(code), precision)
	}

	if !strings.Contains(amount, ".") {
		minor, err := strconv.ParseInt(amount, 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return 0, fmt.Errorf("%w: %s", ErrAmountOutOfRange, amount)
			}
			return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
		}
		return minor, nil
	}

	if len(amount) > maxAmountLen {
		return 0, fmt.Errorf("%w: amount is too long", ErrInvalidAmount)
	}
	d, err := decimal.NewFromString(amount)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	if exp := d.Exponent(); exp > maxExponent || exp < minExponent {
		return 0, fmt.Errorf("%w: %q", ErrAmountOutOfRange, amount)
	}

	shifted := d.Shift(int32(precision))
	if !shifted.Equal(shifted.Truncate(0)) {
		return 0, fmt.Errorf("%w: %s allows at most %d decimal places", ErrTooManyDecimals, normalizeCode(code), precision)
	}
	bi := shifted.BigInt()
	if !bi.IsInt64() {
		return 0, fmt.Errorf("%w: %q", ErrAmountOutOfRange, amount)
	}
	return bi.Int64(), nil
}

// FormatMinor renders minor units as an exact decimal string in major units with
// exactly `precision` fractional digits, for example (1050, "USD") gives "10.50"
// and (10500000, "USDT") gives "10.500000". It is the inverse of ParseAmount for
// decimal inputs.
func FormatMinor(minor int64, code string) (string, error) {
	definition, ok := Lookup(code)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownCurrency, strings.TrimSpace(code))
	}
	if definition.Precision < 0 || definition.Precision > MaxPrecision {
		return "", fmt.Errorf("%w: %s has precision %d", ErrInvalidPrecision, normalizeCode(code), definition.Precision)
	}
	return decimal.NewFromInt(minor).Shift(int32(-definition.Precision)).StringFixed(int32(definition.Precision)), nil
}
