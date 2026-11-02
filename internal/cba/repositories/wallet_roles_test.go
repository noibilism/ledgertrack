//go:build it

package repositories

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"

	"github.com/formancehq/ledger/internal/cba/models"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

func TestWalletRoles(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	pgServer := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{DatabaseSourceName: pgServer.GetDSN()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, systemstore.GetMigrator(db).Up(ctx))
	runWalletRoleTests(t, db)
}

func strp(s string) *string { return &s }

type roleFixture struct {
	db      *bun.DB
	repo    *BunWalletRoleRepository
	client  *models.Client
	product *models.Product
	n       int
}

func newRoleFixture(t *testing.T, db *bun.DB) *roleFixture {
	ctx := logging.TestingContext()
	product := &models.Product{
		Code: "MAIN-NGN-" + uuid.NewString()[:6], Name: "Main", Category: "wallet", Currency: "NGN",
		Status: models.ProductStatusActive,
		Rules:  models.ProductRules{MinOpeningBalance: "0", MinBalance: "0", AllowDebits: true, AllowCredits: true},
	}
	require.NoError(t, NewProductRepository(db).Create(ctx, product))
	client := &models.Client{
		ClientNumber: "CL-" + uuid.NewString()[:8], Type: models.ClientTypeIndividual, Status: models.ClientStatusActive,
		KYCStatus: models.KYCStatusVerified, Contact: models.ClientContact{Email: "a@example.com", Phone: "+234800"},
		IndividualData: &models.IndividualData{FirstName: "A", LastName: "B", Nationality: "NG"},
	}
	require.NoError(t, NewClientRepository(db).Create(ctx, client))
	return &roleFixture{db: db, repo: NewWalletRoleRepository(db), client: client, product: product}
}

func (f *roleFixture) account(t *testing.T, client *models.Client, status, accountType, currency string) *models.Account {
	f.n++
	a := &models.Account{
		AccountNumber: uuid.NewString()[:10], ClientID: client.ID, ProductID: f.product.ID, Currency: currency,
		Status: status, WalletID: "w-" + uuid.NewString(), InterestAccrued: decimal.Zero, Metadata: map[string]any{},
		AccountType: strp(accountType),
	}
	require.NoError(t, NewAccountRepository(f.db).Create(logging.TestingContext(), a))
	return a
}

func (f *roleFixture) audits(t *testing.T, id uuid.UUID) []models.AccountWalletRoleAudit {
	var rows []models.AccountWalletRoleAudit
	require.NoError(t, f.db.NewSelect().Model(&rows).Where("account_id = ?", id).OrderExpr("created_at, id").Scan(logging.TestingContext()))
	return rows
}

func runWalletRoleTests(t *testing.T, db *bun.DB) {
	ctx := logging.TestingContext()

	t.Run("classify is audited, replayable and protected", func(t *testing.T) {
		f := newRoleFixture(t, db)
		acct := f.account(t, f.client, models.AccountStatusActive, "main", "NGN")
		rel1, rel2 := "rel-"+uuid.NewString(), "rel-"+uuid.NewString()

		got, replayed, err := f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleMerchantMain, Actor: "svc", Evidence: "e1"})
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, models.WalletRoleMerchantMain, *got.WalletRole)
		require.Len(t, f.audits(t, acct.ID), 1)
		require.Equal(t, "classified", f.audits(t, acct.ID)[0].Action)

		_, replayed, err = f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleMerchantMain, Actor: "svc", Evidence: "e2"})
		require.NoError(t, err)
		require.True(t, replayed)
		require.Len(t, f.audits(t, acct.ID), 1, "a replay must not add an audit row")

		// merchant_main -> r2 needs an explicit correction
		_, _, err = f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleR2Subaccount, Relationship: &rel1, Actor: "svc", Evidence: "e3"})
		require.ErrorIs(t, err, ErrWalletRoleConflict)
		got, replayed, err = f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleR2Subaccount, Relationship: &rel1, Correction: true, Actor: "svc", Evidence: "e3"})
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, rel1, *got.R2RelationshipPublicID)
		require.Equal(t, "corrected", f.audits(t, acct.ID)[1].Action)

		// R2 ownership is immutable, even with a correction flag
		_, _, err = f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleMerchantMain, Correction: true, Actor: "svc", Evidence: "e4"})
		require.ErrorIs(t, err, ErrWalletRoleConflict)
		_, _, err = f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleR2Subaccount, Relationship: &rel2, Correction: true, Actor: "svc", Evidence: "e4"})
		require.ErrorIs(t, err, ErrWalletRoleConflict)
		_, replayed, err = f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: f.client.ID, Role: models.WalletRoleR2Subaccount, Relationship: &rel1, Actor: "svc", Evidence: "e5"})
		require.NoError(t, err)
		require.True(t, replayed)
	})

	t.Run("a relationship binds one wallet per currency", func(t *testing.T) {
		f := newRoleFixture(t, db)
		a1 := f.account(t, f.client, models.AccountStatusActive, "main", "NGN")
		a2 := f.account(t, f.client, models.AccountStatusActive, "main", "NGN")
		rel := "rel-" + uuid.NewString()
		_, _, err := f.repo.Classify(ctx, ClassifyInput{AccountID: a1.ID, ClientID: f.client.ID, Role: models.WalletRoleR2Subaccount, Relationship: &rel, Actor: "svc", Evidence: "e"})
		require.NoError(t, err)
		_, _, err = f.repo.Classify(ctx, ClassifyInput{AccountID: a2.ID, ClientID: f.client.ID, Role: models.WalletRoleR2Subaccount, Relationship: &rel, Actor: "svc", Evidence: "e"})
		require.ErrorIs(t, err, ErrWalletRoleConflict)
	})

	t.Run("another client's account is not found", func(t *testing.T) {
		f := newRoleFixture(t, db)
		other := newRoleFixture(t, db)
		acct := f.account(t, f.client, models.AccountStatusActive, "main", "NGN")
		_, _, err := f.repo.Classify(ctx, ClassifyInput{AccountID: acct.ID, ClientID: other.client.ID, Role: models.WalletRoleMerchantMain, Actor: "svc", Evidence: "e"})
		require.ErrorIs(t, err, ErrWalletNotFound)
		_, _, err = f.repo.Classify(ctx, ClassifyInput{AccountID: uuid.New(), ClientID: f.client.ID, Role: models.WalletRoleMerchantMain, Actor: "svc", Evidence: "e"})
		require.ErrorIs(t, err, ErrWalletNotFound)
	})

	t.Run("the database refuses an inconsistent role", func(t *testing.T) {
		f := newRoleFixture(t, db)
		acct := f.account(t, f.client, models.AccountStatusActive, "main", "NGN")
		_, err := db.ExecContext(ctx, `update _system.accounts set wallet_role = 'r2_subaccount' where id = ?`, acct.ID)
		require.Error(t, err, "r2_subaccount without a relationship")
		_, err = db.ExecContext(ctx, `update _system.accounts set r2_relationship_public_id = 'x' where id = ?`, acct.ID)
		require.Error(t, err, "relationship without the r2 role")
		_, err = db.ExecContext(ctx, `update _system.accounts set wallet_role = 'other' where id = ?`, acct.ID)
		require.Error(t, err, "unknown role")
	})

	t.Run("create R2 sub-account keeps the allocated id and replays", func(t *testing.T) {
		f := newRoleFixture(t, db)
		in := CreateR2SubaccountInput{
			AccountID: uuid.New(), AccountNumber: uuid.NewString()[:10], WalletID: "w-" + uuid.NewString(),
			Client: f.client, Product: f.product, AccountName: "R2 NGN", AccountType: "main", CountryCode: strp("ng"),
			Relationship: "rel-" + uuid.NewString(), Actor: "svc", Evidence: "ev",
		}
		got, replayed, err := f.repo.CreateR2Subaccount(ctx, in)
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, in.AccountID, got.ID)
		require.Equal(t, models.AccountStatusActive, got.Status)
		require.Equal(t, models.WalletRoleR2Subaccount, *got.WalletRole)
		require.Equal(t, "created", f.audits(t, in.AccountID)[0].Action)

		again := in
		again.AccountNumber, again.WalletID = uuid.NewString()[:10], "w-"+uuid.NewString()
		got, replayed, err = f.repo.CreateR2Subaccount(ctx, again)
		require.NoError(t, err)
		require.True(t, replayed)
		require.Equal(t, in.AccountID, got.ID)
		require.Len(t, f.audits(t, in.AccountID), 1)

		clash := again
		clash.AccountName = "x"
		clash.CountryCode = strp("gh")
		_, _, err = f.repo.CreateR2Subaccount(ctx, clash)
		require.ErrorIs(t, err, ErrWalletRoleConflict, "same id, different identity")

		other := again
		other.AccountID = uuid.New()
		_, _, err = f.repo.CreateR2Subaccount(ctx, other)
		require.ErrorIs(t, err, ErrWalletRoleConflict, "same relationship, different account id")
	})

	t.Run("concurrent creation of the same wallet creates it once", func(t *testing.T) {
		f := newRoleFixture(t, db)
		id, rel := uuid.New(), "rel-"+uuid.NewString()
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			created int
			errs    []error
		)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, replayed, err := f.repo.CreateR2Subaccount(ctx, CreateR2SubaccountInput{
					AccountID: id, AccountNumber: uuid.NewString()[:10], WalletID: "w-" + uuid.NewString(),
					Client: f.client, Product: f.product, AccountName: "R2", AccountType: "main",
					Relationship: rel, Actor: "svc", Evidence: "ev",
				})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
				} else if !replayed {
					created++
				}
			}()
		}
		wg.Wait()
		require.Empty(t, errs)
		require.Equal(t, 1, created)
		require.Len(t, f.audits(t, id), 1)
	})

	t.Run("eligible wallets are active main wallets in the currency", func(t *testing.T) {
		f := newRoleFixture(t, db)
		want := f.account(t, f.client, models.AccountStatusActive, "main", "NGN")
		f.account(t, f.client, models.AccountStatusSuspended, "main", "NGN")
		f.account(t, f.client, models.AccountStatusActive, "reserve", "NGN")
		f.account(t, f.client, models.AccountStatusActive, "main", "GHS")
		got, err := f.repo.EligibleWallets(ctx, f.client.ID, "NGN")
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, want.ID, got[0].ID)
		all, err := f.repo.ListByClient(ctx, f.client.ID)
		require.NoError(t, err)
		require.Len(t, all, 4)
	})
}
