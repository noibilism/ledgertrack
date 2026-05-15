package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/cba/models"
)

func TestFeeServicePrepareTransactionFeesPercentageWithBounds(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	productRepo := newProductRepositoryStub()
	feeRepo := newFeePostingRepositoryStub()
	service := NewFeeService(accountRepo, productRepo, feeRepo)

	productID := uuid.New()
	min := "1.50"
	max := "5.00"
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "CUR-USD-FEE",
		Name:     "Current With Fees",
		Category: "current",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		FeeSchedule: &models.FeeSchedule{
			TransactionFees: []models.TransactionFee{
				{
					Event: "debit",
					Type:  "percentage",
					Value: "2.5",
					Min:   &min,
					Max:   &max,
				},
			},
		},
	}))
	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "3000000001",
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-fee-1",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	fees, err := service.PrepareTransactionFees(context.Background(), account.ID, "debit", 10000, "txn-ref-1")
	require.NoError(t, err)
	require.Len(t, fees, 1)
	require.Equal(t, "2.5", fees[0].Amount.String())
	require.Equal(t, models.FeePostingStatusPendingRecovery, fees[0].Status)
	require.Equal(t, "fee:debit:txn-ref-1:1", fees[0].Reference)
}

func TestFeeServicePrepareMaintenanceFee(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	productRepo := newProductRepositoryStub()
	feeRepo := newFeePostingRepositoryStub()
	service := NewFeeService(accountRepo, productRepo, feeRepo)

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-MAINT",
		Name:     "Savings Maintenance",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		FeeSchedule: &models.FeeSchedule{
			MaintenanceFee: &models.MaintenanceFee{
				Amount:    "3.25",
				Frequency: "monthly",
			},
		},
	}))
	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "3000000002",
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-fee-2",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	posting, err := service.PrepareMaintenanceFee(context.Background(), account.ID, time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, "3.25", posting.Amount.String())
	require.Equal(t, "USD", posting.Currency)
	require.Equal(t, models.FeePostingStatusPendingRecovery, posting.Status)
}

func TestFeeServiceMarkPosted(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	productRepo := newProductRepositoryStub()
	feeRepo := newFeePostingRepositoryStub()
	service := NewFeeService(accountRepo, productRepo, feeRepo)

	require.NoError(t, feeRepo.Create(context.Background(), &models.FeePosting{
		ID:        uuid.New(),
		AccountID: uuid.New(),
		Reference: "fee-ref-1",
		Status:    models.FeePostingStatusPendingRecovery,
	}))

	posting, err := service.MarkPosted(context.Background(), "fee-ref-1")
	require.NoError(t, err)
	require.Equal(t, models.FeePostingStatusPosted, posting.Status)
}
