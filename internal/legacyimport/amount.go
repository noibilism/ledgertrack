// Package legacyimport implements the Bankman to LedgerTrack migration tool.
//
// It only ever talks to a running LedgerTrack over its HTTP API. Nothing in
// this package touches the database directly.
package legacyimport

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

// amountRe accepts plain non-negative decimals only: no sign, no exponent, no
// thousands separators, no leading zeros (except a single "0" integer part).
var amountRe = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// AmountOptions tunes ParseAmount.
type AmountOptions struct {
	// TrimZeros tolerates fractional digits beyond the currency precision
	// when every excess digit is "0" (for example "10.5000" for a 2dp currency).
	TrimZeros bool
}

// ParseAmount converts a decimal string in major units to an integer in minor
// units using the given precision. It never uses floating point and rejects
// anything that cannot be represented exactly, including values that do not
// fit an int64 (the wallet layer of LedgerTrack reads balances as int64).
func ParseAmount(s string, precision int, opt AmountOptions) (*big.Int, error) {
	if precision < 0 || precision > 18 {
		return nil, fmt.Errorf("unsupported precision %d", precision)
	}
	if !amountRe.MatchString(s) {
		return nil, fmt.Errorf("invalid amount %q: expected a plain non-negative decimal (no sign, exponent, spaces or separators)", s)
	}
	intPart, frac, _ := strings.Cut(s, ".")
	if len(frac) > precision {
		extra := frac[precision:]
		if opt.TrimZeros && strings.Trim(extra, "0") == "" {
			frac = frac[:precision]
		} else {
			return nil, fmt.Errorf("invalid amount %q: %d fractional digits exceed currency precision %d", s, len(frac), precision)
		}
	}
	frac += strings.Repeat("0", precision-len(frac))
	n, ok := new(big.Int).SetString(intPart+frac, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount %q", s)
	}
	if !n.IsInt64() {
		return nil, fmt.Errorf("amount %q exceeds the int64 range of minor units", s)
	}
	return n, nil
}

// FormatAmount renders minor units as a decimal string in major units with
// exactly `precision` fractional digits.
func FormatAmount(minor *big.Int, precision int) string {
	if minor == nil {
		return ""
	}
	neg := minor.Sign() < 0
	digits := new(big.Int).Abs(minor).String()
	if precision > 0 {
		if len(digits) <= precision {
			digits = strings.Repeat("0", precision-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-precision] + "." + digits[len(digits)-precision:]
	}
	if neg {
		return "-" + digits
	}
	return digits
}
