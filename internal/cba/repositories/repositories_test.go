//go:build it

package repositories

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"

	"github.com/formancehq/ledger/internal/cba/models"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

func TestRepositories(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgServer := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{
		DatabaseSourceName: pgServer.GetDSN(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	require.NoError(t, systemstore.GetMigrator(db).Up(ctx))

	productRepo := NewProductRepository(db)
	clientRepo := NewClientRepository(db)
	accountRepo := NewAccountRepository(db)
	kycRepo := NewKYCRepository(db)
	accrualRepo := NewInterestAccrualRepository(db)
	feeRepo := NewFeePostingRepository(db)
	dailyUsageRepo := NewDailyUsageRepository(db)

	product := &models.Product{
		Code:        "SAV-NGN-001",
		Name:        "Personal Savings NGN",
		Category:    "savings",
		Currency:    "NGN",
		Status:      models.ProductStatusDraft,
		Description: "Intro savings product",
		Rules: models.ProductRules{
			MinOpeningBalance: "100.00",
			MinBalance:        "0",
			AllowDebits:       true,
			AllowCredits:      true,
			RequiresKYCLevel:  1,
		},
	}
	require.NoError(t, productRepo.Create(ctx, product))
	require.NotEqual(t, uuid.Nil, product.ID)

	storedProduct, err := productRepo.GetByCode(ctx, product.Code)
	require.NoError(t, err)
	require.Equal(t, product.Name, storedProduct.Name)

	client := &models.Client{
		ClientNumber: "CL-2026-000001",
		Type:         models.ClientTypeIndividual,
		Status:       models.ClientStatusPending,
		KYCLevel:     0,
		KYCStatus:    models.KYCStatusPending,
		Contact: models.ClientContact{
			Email: "jane@example.com",
			Phone: "+2348000000000",
		},
		IndividualData: &models.IndividualData{
			FirstName:   "Jane",
			LastName:    "Doe",
			Nationality: "NG",
		},
	}
	require.NoError(t, clientRepo.Create(ctx, client))

	storedClient, err := clientRepo.GetByNumber(ctx, client.ClientNumber)
	require.NoError(t, err)
	require.Equal(t, client.Contact.Email, storedClient.Contact.Email)

	now := time.Now().UTC()
	account := &models.Account{
		AccountNumber:   "0123456789",
		ClientID:        client.ID,
		ProductID:       product.ID,
		Currency:        product.Currency,
		Status:          models.AccountStatusPending,
		WalletID:        "client-CL-2026-000001-SAV-NGN-001",
		FreezeDebits:    false,
		InterestAccrued: decimal.RequireFromString("0"),
		Metadata: map[string]any{
			"source": "integration-test",
		},
		LastActivityAt: &now,
	}
	require.NoError(t, accountRepo.Create(ctx, account))

	storedAccount, err := accountRepo.GetByWalletID(ctx, account.WalletID)
	require.NoError(t, err)
	require.Equal(t, account.AccountNumber, storedAccount.AccountNumber)

	kycRecord := &models.KYCRecord{
		ClientID:    client.ID,
		Level:       1,
		Status:      models.KYCStatusPending,
		SubmittedAt: now,
		Documents: []models.KYCDocument{
			{
				Type:      "national_id",
				Reference: "doc-1",
				Provider:  "s3",
			},
		},
		Payload: map[string]any{
			"national_id_number": "A12345",
		},
	}
	require.NoError(t, kycRepo.Create(ctx, kycRecord))

	clientKYCRecords, err := kycRepo.ListByClient(ctx, client.ID)
	require.NoError(t, err)
	require.Len(t, clientKYCRecords, 1)

	accrualDate := time.Date(2026, time.May, 14, 0, 0, 0, 0, time.UTC)
	accrual := &models.InterestAccrual{
		AccountID:    account.ID,
		AccrualDate:  accrualDate,
		BalanceBasis: decimal.RequireFromString("1000.00000000"),
		Rate:         decimal.RequireFromString("0.05000000"),
		Amount:       decimal.RequireFromString("0.13698630"),
		Posted:       false,
		Metadata: map[string]any{
			"policy": "daily-simple",
		},
	}
	require.NoError(t, accrualRepo.Create(ctx, accrual))

	accruals, err := accrualRepo.ListByAccount(ctx, account.ID)
	require.NoError(t, err)
	require.Len(t, accruals, 1)

	feePosting := &models.FeePosting{
		AccountID:       account.ID,
		EventType:       "debit",
		Reference:       "ref-001:fee:debit",
		LinkedReference: "ref-001",
		Amount:          decimal.RequireFromString("5.00000000"),
		Currency:        "NGN",
		Status:          models.FeePostingStatusPendingRecovery,
		Metadata: map[string]any{
			"reason": "simulated",
		},
	}
	require.NoError(t, feeRepo.Create(ctx, feePosting))

	storedFeePosting, err := feeRepo.GetByReference(ctx, feePosting.Reference)
	require.NoError(t, err)
	require.Equal(t, feePosting.LinkedReference, storedFeePosting.LinkedReference)

	usageDate := time.Date(2026, time.May, 14, 0, 0, 0, 0, time.UTC)
	dailyUsage := &models.AccountDailyUsage{
		AccountID:     account.ID,
		UsageDate:     usageDate,
		DebitAmount:   decimal.RequireFromString("50.00000000"),
		CreditAmount:  decimal.RequireFromString("100.00000000"),
		DebitCount:    1,
		CreditCount:   1,
		LastReference: "ref-001",
	}
	require.NoError(t, dailyUsageRepo.Create(ctx, dailyUsage))

	storedUsage, err := dailyUsageRepo.GetForDate(ctx, account.ID, usageDate)
	require.NoError(t, err)
	require.True(t, storedUsage.DebitAmount.Equal(dailyUsage.DebitAmount))
}
