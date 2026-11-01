package feequote

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/cba/models"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }
func sp(s string) *string        { return &s }

func TestQuoteWorkedExamples(t *testing.T) {
	t.Run("A: tax on the fee", func(t *testing.T) {
		r, err := Quote(Input{
			Amount:    d("100000"),
			Precision: 2,
			Fees:      []models.TransactionFee{{Type: FeeTypeFlat, Value: "1400"}},
			Taxes:     []Tax{{Base: TaxBaseFee, Rate: "7.5"}},
		})
		require.NoError(t, err)
		require.Equal(t, "1400", r.Fee.String())
		require.Equal(t, "105", r.Tax.String())
		require.Equal(t, "1505", r.Gross.String())
		require.Equal(t, "101505", r.Total.String())
	})
	t.Run("B: tax on the amount", func(t *testing.T) {
		r, err := Quote(Input{
			Amount:    d("100000"),
			Precision: 2,
			Fees:      []models.TransactionFee{{Type: FeeTypePercentage, Value: "1"}},
			Taxes:     []Tax{{Base: TaxBaseAmount, Rate: "0.5"}},
		})
		require.NoError(t, err)
		require.Equal(t, "1000", r.Fee.String())
		require.Equal(t, "500", r.Tax.String())
		require.Equal(t, "101500", r.Total.String())
	})
}

func TestQuoteRoundsEachLineHalfEven(t *testing.T) {
	// 0.125% of 100.00 is 0.125, which rounds half-even to 0.12 (not 0.13).
	r, err := Quote(Input{Amount: d("100"), Precision: 2, Fees: []models.TransactionFee{{Type: FeeTypePercentage, Value: "0.125"}}})
	require.NoError(t, err)
	require.Equal(t, "0.12", r.Fee.String())

	// Tax is computed on the rounded fee, never on the unrounded one: 0.12 * 10% = 0.012 -> 0.01.
	r, err = Quote(Input{
		Amount: d("100"), Precision: 2,
		Fees:  []models.TransactionFee{{Type: FeeTypePercentage, Value: "0.125"}},
		Taxes: []Tax{{Base: TaxBaseFee, Rate: "10"}},
	})
	require.NoError(t, err)
	require.Equal(t, "0.01", r.Tax.String())
	require.Equal(t, "0.13", r.Gross.String())
	require.Equal(t, "100.13", r.Total.String())
}

func TestQuoteZeroDecimalPrecision(t *testing.T) {
	r, err := Quote(Input{Amount: d("1000"), Precision: 0, Fees: []models.TransactionFee{{Type: FeeTypePercentage, Value: "0.25"}}})
	require.NoError(t, err)
	require.Equal(t, "2", r.Fee.String()) // 2.5 rounds half-even to 2
}

func TestQuoteCombinedRulesRoundEachLine(t *testing.T) {
	// Flat 10 plus 0.125% of 100.00 (0.125 -> 0.12): lines 10 and 0.12, fee 10.12.
	r, err := Quote(Input{Amount: d("100"), Precision: 2, Fees: []models.TransactionFee{
		{Type: FeeTypeFlat, Value: "10"},
		{Type: FeeTypePercentage, Value: "0.125"},
	}, Taxes: []Tax{{Base: TaxBaseFee, Rate: "10"}}})
	require.NoError(t, err)
	require.Len(t, r.FeeLines, 2)
	require.Equal(t, "10", r.FeeLines[0].String())
	require.Equal(t, "0.12", r.FeeLines[1].String())
	require.Equal(t, "10.12", r.Fee.String())
	require.Equal(t, "1.01", r.Tax.String()) // 10% of 10.12 = 1.012
	require.Equal(t, "111.13", r.Total.String())
}

func TestQuoteSeveralTaxesAreSummedAfterRoundingEach(t *testing.T) {
	r, err := Quote(Input{
		Amount: d("100"), Precision: 2,
		Fees:  []models.TransactionFee{{Type: FeeTypeFlat, Value: "10.05"}},
		Taxes: []Tax{{Base: TaxBaseFee, Rate: "7.5"}, {Base: TaxBaseAmount, Rate: "0.125"}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"0.75", "0.12"}, []string{r.TaxLines[0].String(), r.TaxLines[1].String()}) // 0.75375 and 0.125
	require.Equal(t, "0.87", r.Tax.String())
	require.Equal(t, "10.92", r.Gross.String())
}

func TestQuoteNoFeeNoTax(t *testing.T) {
	r, err := Quote(Input{Amount: d("50"), Precision: 2})
	require.NoError(t, err)
	require.True(t, r.Fee.IsZero())
	require.True(t, r.Tax.IsZero())
	require.Equal(t, "50", r.Total.String())
}

func TestQuoteTaxOnAmountWithoutFee(t *testing.T) {
	r, err := Quote(Input{Amount: d("200"), Precision: 2, Taxes: []Tax{{Base: TaxBaseAmount, Rate: "1.5"}}})
	require.NoError(t, err)
	require.Equal(t, "3", r.Tax.String())
	require.Equal(t, "203", r.Total.String())
}

func TestQuoteTaxOnFeeIsZeroWhenThereIsNoFee(t *testing.T) {
	r, err := Quote(Input{Amount: d("200"), Precision: 2, Taxes: []Tax{{Base: TaxBaseFee, Rate: "7.5", Min: sp("5")}}})
	require.NoError(t, err)
	require.True(t, r.Tax.IsZero(), "a minimum must not create tax on a zero fee")
}

func TestFeeBounds(t *testing.T) {
	rule := models.TransactionFee{Type: FeeTypePercentage, Value: "1", Min: sp("10"), Max: sp("50")}
	for amount, want := range map[string]string{"100": "10", "2000": "20", "100000": "50"} {
		got, err := FeeAmount(rule, d(amount))
		require.NoError(t, err)
		require.Equal(t, want, got.String(), "amount %s", amount)
	}
}

func TestTieredFeeUsesTheBandOfTheWholeAmount(t *testing.T) {
	rule := models.TransactionFee{Type: FeeTypeTiered, Tiers: []models.FeeTier{
		{From: "0", To: sp("5000"), Type: FeeTypeFlat, Value: "10"},
		{From: "5000", To: sp("50000"), Type: FeeTypeFlat, Value: "25"},
		{From: "50000", Type: FeeTypePercentage, Value: "0.5"},
	}}
	cases := map[string]string{
		"0.01":    "10",
		"4999.99": "10",
		"5000":    "25", // lower bound is inclusive
		"49999":   "25",
		"50000":   "250", // 0.5% of the whole amount
		"200000":  "1000",
	}
	for amount, want := range cases {
		got, err := FeeAmount(rule, d(amount))
		require.NoError(t, err)
		require.Equal(t, want, got.String(), "amount %s", amount)
	}
}

func TestQuoteRejectsBadInput(t *testing.T) {
	_, err := Quote(Input{Amount: decimal.Zero, Precision: 2})
	require.ErrorIs(t, err, ErrInvalidInput)
	_, err = Quote(Input{Amount: d("-1"), Precision: 2})
	require.ErrorIs(t, err, ErrInvalidInput)
	_, err = Quote(Input{Amount: d("1"), Precision: 40})
	require.ErrorIs(t, err, ErrInvalidInput)
	_, err = Quote(Input{Amount: d("1"), Precision: 2, Taxes: []Tax{{Base: "gross", Rate: "5"}}})
	require.ErrorIs(t, err, ErrInvalidRule)
}

func TestValidateFeeRule(t *testing.T) {
	good := []models.TransactionFee{
		{Type: FeeTypeFlat, Value: "0"},
		{Type: FeeTypePercentage, Value: "100"},
		{Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", Type: FeeTypeFlat, Value: "1"}}},
	}
	for _, r := range good {
		require.NoError(t, ValidateFeeRule(r), "%+v", r)
	}

	bad := map[string]models.TransactionFee{
		"unknown type":            {Type: "bracket", Value: "1"},
		"negative flat":           {Type: FeeTypeFlat, Value: "-1"},
		"not a number":            {Type: FeeTypeFlat, Value: "abc"},
		"percentage over 100":     {Type: FeeTypePercentage, Value: "100.01"},
		"min above max":           {Type: FeeTypeFlat, Value: "1", Min: sp("5"), Max: sp("2")},
		"negative min":            {Type: FeeTypeFlat, Value: "1", Min: sp("-1")},
		"tiered without tiers":    {Type: FeeTypeTiered},
		"tier not starting at 0":  {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "1", Type: FeeTypeFlat, Value: "1"}}},
		"tier gap":                {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", To: sp("10"), Type: FeeTypeFlat, Value: "1"}, {From: "11", Type: FeeTypeFlat, Value: "2"}}},
		"tier overlap":            {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", To: sp("10"), Type: FeeTypeFlat, Value: "1"}, {From: "9", Type: FeeTypeFlat, Value: "2"}}},
		"last tier closed":        {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", To: sp("10"), Type: FeeTypeFlat, Value: "1"}}},
		"open tier in the middle": {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", Type: FeeTypeFlat, Value: "1"}, {From: "10", Type: FeeTypeFlat, Value: "2"}}},
		"tier ends before start":  {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", To: sp("0"), Type: FeeTypeFlat, Value: "1"}, {From: "0", Type: FeeTypeFlat, Value: "2"}}},
		"nested tiered":           {Type: FeeTypeTiered, Tiers: []models.FeeTier{{From: "0", Type: FeeTypeTiered, Value: "1"}}},
	}
	for name, r := range bad {
		require.ErrorIs(t, ValidateFeeRule(r), ErrInvalidRule, name)
	}
}

func TestValidateTax(t *testing.T) {
	require.NoError(t, ValidateTax(Tax{Base: TaxBaseFee, Rate: "7.5"}))
	require.NoError(t, ValidateTax(Tax{Base: TaxBaseAmount, Rate: "0", Min: sp("1"), Max: sp("10")}))
	require.ErrorIs(t, ValidateTax(Tax{Base: "", Rate: "7.5"}), ErrInvalidRule)
	require.ErrorIs(t, ValidateTax(Tax{Base: TaxBaseFee, Rate: "-1"}), ErrInvalidRule)
	require.ErrorIs(t, ValidateTax(Tax{Base: TaxBaseFee, Rate: "101"}), ErrInvalidRule)
	require.ErrorIs(t, ValidateTax(Tax{Base: TaxBaseFee, Rate: "x"}), ErrInvalidRule)
	require.ErrorIs(t, ValidateTax(Tax{Base: TaxBaseFee, Rate: "5", Min: sp("9"), Max: sp("2")}), ErrInvalidRule)
}
