package currency

import (
	"errors"
	"fmt"
	"strings"
)

// ErrUnknownCurrency is returned when a currency code is not present in the registry.
var ErrUnknownCurrency = errors.New("unknown currency")

// ErrInvalidPrecision is returned when a registered currency has a precision outside the supported range.
var ErrInvalidPrecision = errors.New("invalid currency precision")

// MaxPrecision is the largest decimal precision accepted for a currency.
const MaxPrecision = 18

// AssetFor returns the ledger asset name ("CCY/precision") for a currency code,
// using the precision stored in the registry (for example "USD/2", "USDT/6", "JPY/0").
//
// The code is normalized (trimmed, upper-cased). If the currency is not in the
// registry, ErrUnknownCurrency is returned: callers must not fall back to a
// default precision, because a wrong scale silently mis-prices every amount.
// The enabled flag is not checked here; use Lookup for that.
func AssetFor(code string) (string, error) {
	normalized := normalizeCode(code)
	definition, ok := Lookup(normalized)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownCurrency, strings.TrimSpace(code))
	}
	if definition.Precision < 0 || definition.Precision > MaxPrecision {
		return "", fmt.Errorf("%w: %s has precision %d", ErrInvalidPrecision, normalized, definition.Precision)
	}
	return fmt.Sprintf("%s/%d", normalized, definition.Precision), nil
}
