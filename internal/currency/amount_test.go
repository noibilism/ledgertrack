package currency

import (
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func setTestRegistry(t *testing.T) {
	t.Helper()
	SetDefinitions(map[string]Definition{
		"USD":  {Precision: 2, Enabled: true},
		"USDT": {Precision: 6, Enabled: true},
		"JPY":  {Precision: 0, Enabled: true},
		"BTC":  {Precision: 8, Enabled: true},
		"BAD":  {Precision: 40, Enabled: true},
		"NEG":  {Precision: -1, Enabled: true},
	})
	t.Cleanup(func() { SetDefinitions(nil) })
}

func TestAssetFor(t *testing.T) {
	setTestRegistry(t)

	for code, expected := range map[string]string{
		"USD":    "USD/2",
		"usd":    "USD/2",
		" usdt ": "USDT/6",
		"JPY":    "JPY/0",
		"BTC":    "BTC/8",
	} {
		asset, err := AssetFor(code)
		require.NoError(t, err, code)
		require.Equal(t, expected, asset, code)
	}

	_, err := AssetFor("XYZ")
	require.ErrorIs(t, err, ErrUnknownCurrency)
	_, err = AssetFor("")
	require.ErrorIs(t, err, ErrUnknownCurrency)
	_, err = AssetFor("BAD")
	require.ErrorIs(t, err, ErrInvalidPrecision)
	_, err = AssetFor("NEG")
	require.ErrorIs(t, err, ErrInvalidPrecision)
}

func TestAssetForEmptyRegistry(t *testing.T) {
	SetDefinitions(nil)
	_, err := AssetFor("USD")
	require.ErrorIs(t, err, ErrUnknownCurrency)
}

func TestParseAmount(t *testing.T) {
	setTestRegistry(t)

	type testCase struct {
		name     string
		amount   string
		currency string
		expected int64
		err      error
	}

	for _, tc := range []testCase{
		// Empty amount
		{name: "empty", amount: "", currency: "USD", expected: 0},

		// Integers are minor units, whatever the currency
		{name: "usd integer", amount: "1050", currency: "USD", expected: 1050},
		{name: "usdt integer", amount: "1050", currency: "USDT", expected: 1050},
		{name: "jpy integer", amount: "1050", currency: "JPY", expected: 1050},
		{name: "zero", amount: "0", currency: "USD", expected: 0},
		{name: "negative integer", amount: "-5", currency: "USD", expected: -5},
		{name: "int64 max", amount: "9223372036854775807", currency: "USD", expected: math.MaxInt64},
		{name: "int64 min", amount: "-9223372036854775808", currency: "USD", expected: math.MinInt64},
		{name: "int64 overflow", amount: "9223372036854775808", currency: "USD", err: ErrAmountOutOfRange},
		{name: "exponent without dot is not an integer", amount: "1e3", currency: "USD", err: ErrInvalidAmount},
		{name: "garbage", amount: "abc", currency: "USD", err: ErrInvalidAmount},
		{name: "garbage decimal", amount: "1.2.3", currency: "USD", err: ErrInvalidAmount},
		{name: "lone dot", amount: ".", currency: "USD", err: ErrInvalidAmount},

		// 2dp decimals are major units
		{name: "usd 10.50", amount: "10.50", currency: "USD", expected: 1050},
		{name: "usd 10.5", amount: "10.5", currency: "USD", expected: 1050},
		{name: "usd 0.01", amount: "0.01", currency: "USD", expected: 1},
		{name: "usd 0.00", amount: "0.00", currency: "USD", expected: 0},
		{name: "usd 0.29 is exact (float64 would give 28.999..)", amount: "0.29", currency: "USD", expected: 29},
		{name: "usd 1.15", amount: "1.15", currency: "USD", expected: 115},
		{name: "usd 4.35", amount: "4.35", currency: "USD", expected: 435},
		{name: "usd negative", amount: "-10.50", currency: "USD", expected: -1050},
		{name: "usd lowercase code", amount: "10.50", currency: "usd", expected: 1050},
		{name: "usd 3 decimals rejected", amount: "10.505", currency: "USD", err: ErrTooManyDecimals},
		{name: "usd 3 decimals rejected, would round down", amount: "10.501", currency: "USD", err: ErrTooManyDecimals},
		{name: "usd 3 decimals rejected, would round up", amount: "10.499", currency: "USD", err: ErrTooManyDecimals},
		{name: "usd half a minor unit rejected", amount: "0.005", currency: "USD", err: ErrTooManyDecimals},
		{name: "usd trailing zeros beyond precision are exact", amount: "10.500", currency: "USD", expected: 1050},
		{name: "usd many trailing zeros", amount: "10.5000000000", currency: "USD", expected: 1050},
		{name: "usd exponent form", amount: "1.05e1", currency: "USD", expected: 1050},
		{name: "usd exponent form too precise", amount: "1.0005e1", currency: "USD", err: ErrTooManyDecimals},
		{name: "usd max major units", amount: "92233720368547758.07", currency: "USD", expected: math.MaxInt64},
		{name: "usd overflow", amount: "92233720368547758.08", currency: "USD", err: ErrAmountOutOfRange},
		{name: "usd huge exponent", amount: "1.0e999999999", currency: "USD", err: ErrAmountOutOfRange},
		{name: "usd tiny exponent", amount: "1.0e-999999999", currency: "USD", err: ErrAmountOutOfRange},
		{name: "usd too long", amount: "0." + strings.Repeat("0", 80), currency: "USD", err: ErrInvalidAmount},

		// 6dp (USDT)
		{name: "usdt 1.000000", amount: "1.000000", currency: "USDT", expected: 1_000_000},
		{name: "usdt 1.5", amount: "1.5", currency: "USDT", expected: 1_500_000},
		{name: "usdt smallest unit", amount: "0.000001", currency: "USDT", expected: 1},
		{name: "usdt 123456.123456", amount: "123456.123456", currency: "USDT", expected: 123_456_123_456},
		{name: "usdt 0.1+ float trap", amount: "0.1", currency: "USDT", expected: 100_000},
		{name: "usdt 7 decimals rejected", amount: "0.0000001", currency: "USDT", err: ErrTooManyDecimals},
		{name: "usdt 7 decimals trailing zero ok", amount: "1.2345670", currency: "USDT", expected: 1_234_567},
		{name: "usdt 7 decimals non zero rejected", amount: "1.2345671", currency: "USDT", err: ErrTooManyDecimals},
		{name: "usdt 2 decimals fine", amount: "10.50", currency: "USDT", expected: 10_500_000},

		// 0dp
		{name: "jpy 5.0", amount: "5.0", currency: "JPY", expected: 5},
		{name: "jpy 5.00", amount: "5.00", currency: "JPY", expected: 5},
		{name: "jpy 5.5 rejected", amount: "5.5", currency: "JPY", err: ErrTooManyDecimals},
		{name: "jpy 0.1 rejected", amount: "0.1", currency: "JPY", err: ErrTooManyDecimals},
		{name: "jpy 1234.0", amount: "1234.0", currency: "JPY", expected: 1234},

		// 8dp
		{name: "btc 1 satoshi", amount: "0.00000001", currency: "BTC", expected: 1},
		{name: "btc 9 decimals rejected", amount: "0.000000001", currency: "BTC", err: ErrTooManyDecimals},

		// Unknown or broken currencies never fall back to a default precision
		{name: "unknown currency decimal", amount: "10.50", currency: "XYZ", err: ErrUnknownCurrency},
		{name: "unknown currency integer", amount: "1050", currency: "XYZ", err: ErrUnknownCurrency},
		{name: "empty currency", amount: "1050", currency: "", err: ErrUnknownCurrency},
		{name: "invalid precision", amount: "1050", currency: "BAD", err: ErrInvalidPrecision},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseAmount(tc.amount, tc.currency)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestParseAmountUSDTLarge(t *testing.T) {
	setTestRegistry(t)

	// 9,223,372,036,854.775807 USDT is the largest representable amount.
	got, err := ParseAmount("9223372036854.775807", "USDT")
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), got)

	_, err = ParseAmount("9223372036854.775808", "USDT")
	require.ErrorIs(t, err, ErrAmountOutOfRange)
}

func TestFormatMinor(t *testing.T) {
	setTestRegistry(t)

	for _, tc := range []struct {
		minor    int64
		currency string
		expected string
	}{
		{1050, "USD", "10.50"},
		{1, "USD", "0.01"},
		{0, "USD", "0.00"},
		{-1050, "USD", "-10.50"},
		{10_500_000, "USDT", "10.500000"},
		{1, "USDT", "0.000001"},
		{1050, "JPY", "1050"},
		{math.MaxInt64, "USD", "92233720368547758.07"},
		{math.MinInt64, "USDT", "-9223372036854.775808"},
	} {
		got, err := FormatMinor(tc.minor, tc.currency)
		require.NoError(t, err)
		require.Equal(t, tc.expected, got)
	}

	_, err := FormatMinor(1, "XYZ")
	require.ErrorIs(t, err, ErrUnknownCurrency)
}

// Property: for every currency, minor units -> decimal string -> minor units is
// the identity, and decimal string -> minor units -> decimal string is the
// identity on canonical strings (exactly `precision` fractional digits).
func TestAmountRoundTripProperty(t *testing.T) {
	setTestRegistry(t)

	rng := rand.New(rand.NewSource(42))
	boundaries := []int64{0, 1, -1, 9, 10, 99, 100, 101, 999_999, 1_000_000, 1_000_001, math.MaxInt64, math.MaxInt64 - 1, math.MinInt64 + 1}

	for _, code := range []string{"USD", "USDT", "JPY", "BTC"} {
		definition, _ := Lookup(code)
		t.Run(code, func(t *testing.T) {
			check := func(minor int64) {
				formatted, err := FormatMinor(minor, code)
				require.NoError(t, err)
				if definition.Precision > 0 {
					// Decimal strings carry a dot and so are read as major units.
					parsed, err := ParseAmount(formatted, code)
					require.NoError(t, err, formatted)
					require.Equal(t, minor, parsed, formatted)

					again, err := FormatMinor(parsed, code)
					require.NoError(t, err)
					require.Equal(t, formatted, again)

					// One more digit of precision must be rejected unless it is a zero.
					_, err = ParseAmount(formatted+"1", code)
					require.ErrorIs(t, err, ErrTooManyDecimals, formatted+"1")
					extra, err := ParseAmount(formatted+"0", code)
					require.NoError(t, err)
					require.Equal(t, minor, extra)
				} else {
					// 0dp: formatted value has no dot, integers are minor units.
					parsed, err := ParseAmount(formatted, code)
					require.NoError(t, err)
					require.Equal(t, minor, parsed)

					parsed, err = ParseAmount(formatted+".0", code)
					require.NoError(t, err)
					require.Equal(t, minor, parsed)
					_, err = ParseAmount(formatted+".1", code)
					require.ErrorIs(t, err, ErrTooManyDecimals)
				}
				// Integer literal of minor units is always the identity.
				parsedInt, err := ParseAmount(strconv.FormatInt(minor, 10), code)
				require.NoError(t, err)
				require.Equal(t, minor, parsedInt)
			}

			for _, b := range boundaries {
				check(b)
			}
			for i := 0; i < 20000; i++ {
				// Mix of magnitudes so small and huge values are both covered.
				shift := uint(rng.Intn(63))
				check(rng.Int63() >> shift)
				check(-(rng.Int63() >> shift))
			}
		})
	}
}

// Property: a random decimal string with at most `precision` fractional digits
// is parsed to exactly digits*10^(precision-frac), with no floating point error.
func TestParseAmountDecimalStringsExact(t *testing.T) {
	setTestRegistry(t)

	rng := rand.New(rand.NewSource(7))
	for _, code := range []string{"USD", "USDT", "BTC"} {
		definition, _ := Lookup(code)
		for i := 0; i < 20000; i++ {
			frac := rng.Intn(definition.Precision + 1)
			whole := rng.Int63n(10_000_000_000)
			var fracDigits strings.Builder
			for j := 0; j < frac; j++ {
				fracDigits.WriteByte(byte('0' + rng.Intn(10)))
			}
			s := strconv.FormatInt(whole, 10)
			expected := whole
			for j := 0; j < definition.Precision; j++ {
				expected *= 10
			}
			if frac > 0 {
				s += "." + fracDigits.String()
				fracValue, err := strconv.ParseInt(fracDigits.String(), 10, 64)
				require.NoError(t, err)
				for j := frac; j < definition.Precision; j++ {
					fracValue *= 10
				}
				expected += fracValue
			} else {
				// Without a dot the string would be read as minor units: force major units.
				s += ".0"
			}
			got, err := ParseAmount(s, code)
			require.NoError(t, err, s)
			require.Equal(t, expected, got, "%s %s", code, s)
		}
	}
}
