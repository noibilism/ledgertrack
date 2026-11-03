package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

type GLAccountFilter struct {
	Type       *string
	Currency   *string
	ParentCode *string
	Status     *string
}

// GLAccountRepository persists the GL registry.
type GLAccountRepository interface {
	Create(context.Context, *models.GLAccount) error
	// Update persists the mutable fields only (name, status, metadata).
	Update(context.Context, *models.GLAccount) error
	GetByCode(context.Context, string) (*models.GLAccount, error)
	GetByAddress(ctx context.Context, ledger, address string) (*models.GLAccount, error)
	List(context.Context, GLAccountFilter) ([]models.GLAccount, error)
	// RunExclusive runs fn in a single database transaction while holding a table lock that blocks
	// concurrent writers (readers are not blocked). The repository handed to fn is bound to that transaction.
	RunExclusive(ctx context.Context, fn func(ctx context.Context, repo GLAccountRepository) error) error
}

type BunGLAccountRepository struct {
	db bun.IDB
}

func NewGLAccountRepository(db bun.IDB) *BunGLAccountRepository {
	return &BunGLAccountRepository{db: db}
}

func (r *BunGLAccountRepository) Create(ctx context.Context, account *models.GLAccount) error {
	setUUID(&account.ID)
	_, err := r.db.NewInsert().Model(account).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunGLAccountRepository) Update(ctx context.Context, account *models.GLAccount) error {
	account.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().
		Model(account).
		Column("name", "status", "metadata", "updated_at").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunGLAccountRepository) GetByCode(ctx context.Context, code string) (*models.GLAccount, error) {
	account := &models.GLAccount{}
	err := r.db.NewSelect().Model(account).Where("code = ?", code).Scan(ctx)
	return account, postgres.ResolveError(err)
}

func (r *BunGLAccountRepository) GetByAddress(ctx context.Context, ledger, address string) (*models.GLAccount, error) {
	account := &models.GLAccount{}
	err := r.db.NewSelect().Model(account).Where("ledger = ?", ledger).Where("address = ?", address).Scan(ctx)
	return account, postgres.ResolveError(err)
}

func (r *BunGLAccountRepository) List(ctx context.Context, filter GLAccountFilter) ([]models.GLAccount, error) {
	accounts := make([]models.GLAccount, 0)
	query := r.db.NewSelect().Model(&accounts).OrderExpr("code asc")
	if filter.Type != nil {
		query = query.Where("type = ?", *filter.Type)
	}
	if filter.Currency != nil {
		query = query.Where("currency = ?", *filter.Currency)
	}
	if filter.ParentCode != nil {
		query = query.Where("parent_code = ?", *filter.ParentCode)
	}
	if filter.Status != nil {
		query = query.Where("status = ?", *filter.Status)
	}
	err := query.Scan(ctx)
	return accounts, postgres.ResolveError(err)
}

func (r *BunGLAccountRepository) RunExclusive(ctx context.Context, fn func(ctx context.Context, repo GLAccountRepository) error) error {
	return r.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		// SHARE ROW EXCLUSIVE conflicts with itself and with every write lock, but not with plain reads.
		if _, err := tx.ExecContext(ctx, "lock table _system.gl_accounts in share row exclusive mode"); err != nil {
			return postgres.ResolveError(err)
		}
		return fn(ctx, &BunGLAccountRepository{db: tx})
	})
}
