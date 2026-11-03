package legacyimport

import (
	"math/big"
	"testing"
)

func TestParseAmount(t *testing.T) {
	cases := []struct {
		in   string
		prec int
		opt  AmountOptions
		want string // minor units, "" means error
	}{
		{"0", 2, AmountOptions{}, "0"},
		{"0.00", 2, AmountOptions{}, "0"},
		{"1", 2, AmountOptions{}, "100"},
		{"1.5", 2, AmountOptions{}, "150"},
		{"1250000.50", 2, AmountOptions{}, "125000050"},
		{"0.01", 2, AmountOptions{}, "1"},
		{"5000.123456", 6, AmountOptions{}, "5000123456"},
		{"0.000001", 6, AmountOptions{}, "1"},
		{"12", 6, AmountOptions{}, "12000000"},
		{"7", 0, AmountOptions{}, "7"},
		// A float64 would round this: 0.1 + 0.2 style values must stay exact.
		{"9007199254740993.01", 2, AmountOptions{}, "900719925474099301"},
		{"92233720368547758.07", 2, AmountOptions{}, "9223372036854775807"},
		// Rejections.
		{"1.005", 2, AmountOptions{}, ""},
		{"1.500", 2, AmountOptions{}, ""},
		{"1.0000001", 6, AmountOptions{}, ""},
		{"1.0", 0, AmountOptions{}, ""},
		{"92233720368547758.08", 2, AmountOptions{}, ""}, // overflows int64
		{"-1.00", 2, AmountOptions{}, ""},
		{"+1.00", 2, AmountOptions{}, ""},
		{"1e3", 2, AmountOptions{}, ""},
		{"1,000.00", 2, AmountOptions{}, ""},
		{" 1.00", 2, AmountOptions{}, ""},
		{"1.", 2, AmountOptions{}, ""},
		{".5", 2, AmountOptions{}, ""},
		{"01.00", 2, AmountOptions{}, ""},
		{"", 2, AmountOptions{}, ""},
		{"abc", 2, AmountOptions{}, ""},
		// Tolerated only with TrimZeros and only for zero digits.
		{"1.500", 2, AmountOptions{TrimZeros: true}, "150"},
		{"10.50000000", 2, AmountOptions{TrimZeros: true}, "1050"},
		{"1.505", 2, AmountOptions{TrimZeros: true}, ""},
		{"1.0", 0, AmountOptions{TrimZeros: true}, "1"},
	}
	for _, tc := range cases {
		got, err := ParseAmount(tc.in, tc.prec, tc.opt)
		if tc.want == "" {
			if err == nil {
				t.Errorf("ParseAmount(%q, %d) = %s, want error", tc.in, tc.prec, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAmount(%q, %d): unexpected error %v", tc.in, tc.prec, err)
			continue
		}
		if got.String() != tc.want {
			t.Errorf("ParseAmount(%q, %d) = %s, want %s", tc.in, tc.prec, got, tc.want)
		}
	}
}

func TestFormatAmountRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		minor string
		prec  int
		want  string
	}{
		{"0", 2, "0.00"},
		{"1", 2, "0.01"},
		{"125000050", 2, "1250000.50"},
		{"5000123456", 6, "5000.123456"},
		{"1", 6, "0.000001"},
		{"7", 0, "7"},
		{"-150", 2, "-1.50"},
	} {
		n, _ := new(big.Int).SetString(tc.minor, 10)
		if got := FormatAmount(n, tc.prec); got != tc.want {
			t.Errorf("FormatAmount(%s, %d) = %q, want %q", tc.minor, tc.prec, got, tc.want)
		}
		if tc.minor[0] != '-' {
			back, err := ParseAmount(tc.want, tc.prec, AmountOptions{})
			if err != nil || back.Cmp(n) != 0 {
				t.Errorf("round trip %q: %v %v", tc.want, back, err)
			}
		}
	}
}
