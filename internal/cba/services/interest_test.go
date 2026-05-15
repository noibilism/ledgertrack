package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/cba/models"
)

func TestInterestServiceAccrueSimpleDaily(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	productRepo := newProductRepositoryStub()
	interestRepo := newInterestAccrualRepositoryStub()
	service := NewInterestService(accountRepo, productRepo, interestRepo)

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-INT",
		Name:     "Interest Savings",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		InterestConfig: &models.InterestConfig{
			Type:             "simple",
			Rate:             "12",
			AccrualFrequency: "daily",
			PostingFrequency: "monthly",
		},
	}))
	account := &models.Account{
		ID:              uuid.New(),
		AccountNumber:   "2000000001",
		ProductID:       productID,
		Currency:        "USD",
		Status:          models.AccountStatusActive,
		WalletID:        "wallet-int-1",
		InterestAccrued: decimal.Zero,
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	accrual, err := service.Accrue(context.Background(), account.ID, 10000, time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, "100", accrual.BalanceBasis.String())
	require.Equal(t, "12", accrual.Rate.String())
	require.Equal(t, "0.032876712329", accrual.Amount.String())

	stored, err := accountRepo.Get(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, "0.032876712329", stored.InterestAccrued.String())
}

func TestInterestServicePreviewPostingRoundsHalfUp(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	productRepo := newProductRepositoryStub()
	interestRepo := newInterestAccrualRepositoryStub()
	service := NewInterestService(accountRepo, productRepo, interestRepo)

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-INT2",
		Name:     "Interest Savings 2",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		InterestConfig: &models.InterestConfig{
			Type:             "simple",
			Rate:             "10",
			AccrualFrequency: "daily",
			PostingFrequency: "monthly",
		},
	}))
	account := &models.Account{
		ID:              uuid.New(),
		AccountNumber:   "2000000002",
		ProductID:       productID,
		Currency:        "USD",
		Status:          models.AccountStatusActive,
		WalletID:        "wallet-int-2",
		InterestAccrued: decimal.RequireFromString("0.034"),
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))
	require.NoError(t, interestRepo.Create(context.Background(), &models.InterestAccrual{
		ID:           uuid.New(),
		AccountID:    account.ID,
		AccrualDate:  time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		BalanceBasis: decimal.RequireFromString("100"),
		Rate:         decimal.RequireFromString("10"),
		Amount:       decimal.RequireFromString("0.034"),
	}))

	preview, err := service.PreviewPosting(context.Background(), account.ID)
	require.NoError(t, err)
	require.EqualValues(t, 3, preview.PostableAmount)
	require.Equal(t, "0.034", preview.TotalAccrued.String())
	require.Equal(t, "0.004", preview.Remainder.String())

	require.NoError(t, service.MarkPosted(context.Background(), *preview, "interest-post-1"))
	updated, err := accountRepo.Get(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, "0.004", updated.InterestAccrued.String())
}

func TestInterestServicePostingBoundary(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	productRepo := newProductRepositoryStub()
	interestRepo := newInterestAccrualRepositoryStub()
	service := NewInterestService(accountRepo, productRepo, interestRepo)

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "SAV-USD-INT3",
		Name:     "Interest Savings 3",
		Category: "savings",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		InterestConfig: &models.InterestConfig{
			Type:             "simple",
			Rate:             "10",
			AccrualFrequency: "daily",
			PostingFrequency: "quarterly",
		},
	}))
	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "2000000003",
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-int-3",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	due, err := service.IsPostingDue(context.Background(), account.ID, time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, due)

	due, err = service.IsPostingDue(context.Background(), account.ID, time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.False(t, due)
}
