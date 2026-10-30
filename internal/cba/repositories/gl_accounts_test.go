//go:build it

package repositories

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"

	"github.com/formancehq/ledger/internal/cba/models"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

func TestGLAccountRepository(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgServer := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{DatabaseSourceName: pgServer.GetDSN()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, systemstore.GetMigrator(db).Up(ctx))

	runGLAccountRepositoryChecks(t, ctx, NewGLAccountRepository(db))
}

func runGLAccountRepositoryChecks(t *testing.T, ctx context.Context, repo GLAccountRepository) {
	t.Helper()

	newAccount := func(code, address string, parent *string) *models.GLAccount {
		return &models.GLAccount{
			Code: code, Name: "acct " + code, Type: models.GLAccountTypeAsset, NormalSide: models.GLSideDebit,
			Currency: "NGN", ParentCode: parent, Ledger: "ledgertrack", Address: address,
			Status: models.GLAccountStatusActive, Metadata: map[string]any{"k": "v"},
		}
	}

	root := newAccount("1000", "gl:assets", nil)
	require.NoError(t, repo.Create(ctx, root))
	require.NotEmpty(t, root.ID)
	require.False(t, root.CreatedAt.IsZero())

	parent := "1000"
	child := newAccount("1010", "gl:assets:cash", &parent)
	require.NoError(t, repo.Create(ctx, child))

	t.Run("lookups", func(t *testing.T) {
		got, err := repo.GetByCode(ctx, "1010")
		require.NoError(t, err)
		require.Equal(t, "gl:assets:cash", got.Address)
		require.Equal(t, "1000", *got.ParentCode)
		require.Equal(t, "v", got.Metadata["k"])

		got, err = repo.GetByAddress(ctx, "ledgertrack", "gl:assets:cash")
		require.NoError(t, err)
		require.Equal(t, "1010", got.Code)

		_, err = repo.GetByCode(ctx, "nope")
		require.True(t, postgres.IsNotFoundError(err))
	})

	t.Run("constraints", func(t *testing.T) {
		err := repo.Create(ctx, newAccount("1000", "gl:other", nil))
		require.True(t, errors.Is(err, postgres.ErrConstraintsFailed{}), "unique code")

		err = repo.Create(ctx, newAccount("1001", "gl:assets", nil))
		require.True(t, errors.Is(err, postgres.ErrConstraintsFailed{}), "unique ledger+address")

		other := newAccount("1002", "gl:assets", nil)
		other.Ledger = "channels-NGN"
		require.NoError(t, repo.Create(ctx, other), "same address in another ledger is allowed")

		missing := "does-not-exist"
		require.Error(t, repo.Create(ctx, newAccount("1003", "gl:x", &missing)), "parent foreign key")

		require.Error(t, repo.Create(ctx, newAccount("1004", "assets:no_prefix", nil)), "gl: prefix check")

		bad := newAccount("1005", "gl:bad_type", nil)
		bad.Type = "revenue"
		require.Error(t, repo.Create(ctx, bad), "type check")
	})

	t.Run("update only touches mutable columns", func(t *testing.T) {
		got, err := repo.GetByCode(ctx, "1010")
		require.NoError(t, err)
		got.Name = "Cash"
		got.Status = models.GLAccountStatusClosed
		got.Metadata = map[string]any{"closed": true}
		got.Address = "gl:must:not:change"
		require.NoError(t, repo.Update(ctx, got))

		again, err := repo.GetByCode(ctx, "1010")
		require.NoError(t, err)
		require.Equal(t, "Cash", again.Name)
		require.Equal(t, models.GLAccountStatusClosed, again.Status)
		require.Equal(t, "gl:assets:cash", again.Address)
		require.Equal(t, true, again.Metadata["closed"])
	})

	t.Run("list filters", func(t *testing.T) {
		active, closed, ngn, usd, liability := models.GLAccountStatusActive, models.GLAccountStatusClosed, "NGN", "USD", models.GLAccountTypeLiability

		all, err := repo.List(ctx, GLAccountFilter{})
		require.NoError(t, err)
		require.Len(t, all, 3)

		got, err := repo.List(ctx, GLAccountFilter{Status: &closed})
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "1010", got[0].Code)

		got, err = repo.List(ctx, GLAccountFilter{Status: &active, Currency: &ngn})
		require.NoError(t, err)
		require.Len(t, got, 2)

		got, err = repo.List(ctx, GLAccountFilter{Currency: &usd})
		require.NoError(t, err)
		require.Empty(t, got)

		got, err = repo.List(ctx, GLAccountFilter{ParentCode: &parent})
		require.NoError(t, err)
		require.Len(t, got, 1)

		got, err = repo.List(ctx, GLAccountFilter{Type: &liability})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("RunExclusive commits or rolls back as one unit", func(t *testing.T) {
		boom := errors.New("boom")
		err := repo.RunExclusive(ctx, func(ctx context.Context, tx GLAccountRepository) error {
			require.NoError(t, tx.Create(ctx, newAccount("2000", "gl:liabilities", nil)))
			return boom
		})
		require.ErrorIs(t, err, boom)
		_, err = repo.GetByCode(ctx, "2000")
		require.True(t, postgres.IsNotFoundError(err), "rolled back")

		err = repo.RunExclusive(ctx, func(ctx context.Context, tx GLAccountRepository) error {
			if err := tx.Create(ctx, newAccount("2000", "gl:liabilities", nil)); err != nil {
				return err
			}
			// visible inside the transaction
			_, err := tx.GetByCode(ctx, "2000")
			return err
		})
		require.NoError(t, err)
		_, err = repo.GetByCode(ctx, "2000")
		require.NoError(t, err)
	})
}
