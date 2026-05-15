package services

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/channels/models"
)

func TestComputeFeeAmountFlat(t *testing.T) {
	flat := "10.00"
	fee, err := computeFeeAmount(models.FeeStructure{
		Type: "flat",
		Flat: &flat,
	}, 10000, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 1000, fee)
}

func TestComputeFeeAmountPercentage(t *testing.T) {
	p := "2.5"
	fee, err := computeFeeAmount(models.FeeStructure{
		Type:       "percentage",
		Percentage: &p,
	}, 10000, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 250, fee)
}

func TestComputeFeeAmountCombined(t *testing.T) {
	flat := "1.00"
	p := "1"
	fee, err := computeFeeAmount(models.FeeStructure{
		Type:       "combined",
		Flat:       &flat,
		Percentage: &p,
	}, 10000, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 200, fee)
}

func TestComputeFeeAmountTiered(t *testing.T) {
	to100 := "100.00"
	to200 := "200.00"
	flat1 := "1.00"
	flat2 := "2.00"

	fee, err := computeFeeAmount(models.FeeStructure{
		Type: "tiered",
		Layers: []models.FeeLayer{
			{To: &to100, Flat: &flat1},
			{From: &to100, To: &to200, Flat: &flat2},
		},
	}, 15000, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 200, fee)
}

func TestComputeFeeAmountLayered(t *testing.T) {
	to100 := "100.00"
	p1 := "1"
	p2 := "0.5"

	fee, err := computeFeeAmount(models.FeeStructure{
		Type: "layered",
		Layers: []models.FeeLayer{
			{To: &to100, Percentage: &p1},
			{From: &to100, Percentage: &p2},
		},
	}, 15000, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 125, fee)
}

func TestComputeFeeAmountClampMinMax(t *testing.T) {
	p := "1"
	min := "5.00"
	max := "7.00"

	fee, err := computeFeeAmount(models.FeeStructure{
		Type:       "percentage",
		Percentage: &p,
		Min:        &min,
		Max:        &max,
	}, 100000, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 700, fee)
}
