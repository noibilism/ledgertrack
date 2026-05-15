package services

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/cba/models"
)

func TestAccountServiceOpen(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	clientRepo := newClientRepositoryStub()
	productRepo := newProductRepositoryStub()
	service := NewAccountService(accountRepo, clientRepo, productRepo, newDailyUsageRepositoryStub())

	client := &models.Client{
		ID:           uuid.New(),
		ClientNumber: "CL-2026-000001",
		Type:         models.ClientTypeIndividual,
		Status:       models.ClientStatusActive,
		KYCLevel:     1,
		KYCStatus:    models.KYCStatusVerified,
		IndividualData: &models.IndividualData{
			FirstName: "Ada",
			LastName:  "Lovelace",
		},
	}
	require.NoError(t, clientRepo.Create(context.Background(), client))

	product := &models.Product{
		ID:       uuid.New(),
		Code:     "SAV-NGN-001",
		Name:     "Personal Savings NGN",
		Category: "savings",
		Currency: "NGN",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			MinOpeningBalance: "100",
			MinBalance:        "0",
			RequiresKYCLevel:  1,
		},
	}
	require.NoError(t, productRepo.Create(context.Background(), product))

	account, err := service.Open(context.Background(), OpenAccountInput{
		ClientID:       client.ID,
		ProductID:      product.ID,
		OpeningDeposit: json.Number("100"),
	})
	require.NoError(t, err)
	require.Equal(t, models.AccountStatusPending, account.Status)
	require.Equal(t, "NGN", account.Currency)
	require.Equal(t, "client-CL-2026-000001-SAV-NGN-001", account.WalletID)
	require.Len(t, account.AccountNumber, 10)
}

func TestAccountServiceOpenRequiresActiveClient(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	clientRepo := newClientRepositoryStub()
	productRepo := newProductRepositoryStub()
	service := NewAccountService(accountRepo, clientRepo, productRepo, newDailyUsageRepositoryStub())

	client := &models.Client{
		ID:           uuid.New(),
		ClientNumber: "CL-2026-000002",
		Type:         models.ClientTypeCorporate,
		Status:       models.ClientStatusPending,
		KYCLevel:     1,
		KYCStatus:    models.KYCStatusVerified,
		CorporateData: &models.CorporateData{
			LegalName: "Formance Ltd",
		},
	}
	require.NoError(t, clientRepo.Create(context.Background(), client))

	product := &models.Product{
		ID:       uuid.New(),
		Code:     "CUR-USD-001",
		Name:     "Corporate Current USD",
		Category: "current",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			MinOpeningBalance: "0",
			MinBalance:        "0",
		},
	}
	require.NoError(t, productRepo.Create(context.Background(), product))

	_, err := service.Open(context.Background(), OpenAccountInput{
		ClientID:  client.ID,
		ProductID: product.ID,
	})
	require.ErrorIs(t, err, ErrAccountValidation)
}

func TestAccountServiceCloseRequiresZeroBalance(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	service := NewAccountService(accountRepo, newClientRepositoryStub(), newProductRepositoryStub(), newDailyUsageRepositoryStub())

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0123456789",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000003-SAV-NGN-001",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	_, err := service.Close(context.Background(), account.ID, 1)
	require.ErrorIs(t, err, ErrAccountValidation)

	closed, err := service.Close(context.Background(), account.ID, 0)
	require.NoError(t, err)
	require.Equal(t, models.AccountStatusClosed, closed.Status)
	require.NotNil(t, closed.ClosedAt)
}

func TestAccountServiceReactivateClearsDebitFreeze(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	service := NewAccountService(accountRepo, newClientRepositoryStub(), newProductRepositoryStub(), newDailyUsageRepositoryStub())

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "0123456790",
		Status:        models.AccountStatusActive,
		WalletID:      "client-CL-2026-000004-SAV-USD-001",
		FreezeDebits:  true,
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))

	reactivated, err := service.Reactivate(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, models.AccountStatusActive, reactivated.Status)
	require.False(t, reactivated.FreezeDebits)
}

func TestAccountServiceValidateDebitRespectsDailyLimit(t *testing.T) {
	t.Parallel()

	accountRepo := newAccountRepositoryStub()
	clientRepo := newClientRepositoryStub()
	productRepo := newProductRepositoryStub()
	dailyUsageRepo := newDailyUsageRepositoryStub()
	service := NewAccountService(accountRepo, clientRepo, productRepo, dailyUsageRepo)

	productID := uuid.New()
	require.NoError(t, productRepo.Create(context.Background(), &models.Product{
		ID:       productID,
		Code:     "CUR-USD-010",
		Name:     "Current USD",
		Category: "current",
		Currency: "USD",
		Status:   models.ProductStatusActive,
		Rules: models.ProductRules{
			AllowDebits: true,
			MinBalance:  "0",
			TransactionLimits: &models.TransactionLimits{
				DailyDebitLimit: strPtr("100"),
			},
		},
	}))

	account := &models.Account{
		ID:            uuid.New(),
		AccountNumber: "9999999991",
		ProductID:     productID,
		Currency:      "USD",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-1",
	}
	require.NoError(t, accountRepo.Create(context.Background(), account))
	require.NoError(t, dailyUsageRepo.Create(context.Background(), &models.AccountDailyUsage{
		AccountID:    account.ID,
		UsageDate:    time.Now().UTC(),
		DebitAmount:  decimal.RequireFromString("90"),
		CreditAmount: decimal.Zero,
	}))

	_, err := service.ValidateDebit(context.Background(), account.ID, 20, 200, time.Now().UTC())
	require.ErrorIs(t, err, ErrAccountValidation)
}

func TestAccountServiceRecordCreditUsage(t *testing.T) {
	t.Parallel()

	service := NewAccountService(newAccountRepositoryStub(), newClientRepositoryStub(), newProductRepositoryStub(), newDailyUsageRepositoryStub())
	accountID := uuid.New()
	now := time.Now().UTC()

	require.NoError(t, service.RecordCreditUsage(context.Background(), accountID, 55, "ref-1", now))

	usage, err := service.(*DefaultAccountService).dailyUsageRepo.GetForDate(context.Background(), accountID, now)
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.CreditCount)
	require.Equal(t, "55", usage.CreditAmount.String())
	require.Equal(t, "ref-1", usage.LastReference)
}

func strPtr(v string) *string {
	return &v
}
