package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

type ReserveRuleFilter struct {
	ClientID  *uuid.UUID
	AccountID *uuid.UUID
	Status    *string
}

// ReserveRepository persists rolling reserve rules.
type ReserveRepository interface {
	CreateRule(context.Context, *models.ReserveRule) error
	GetRule(context.Context, uuid.UUID) (*models.ReserveRule, error)
	ListRules(context.Context, ReserveRuleFilter) ([]models.ReserveRule, error)
	// UpdateRule persists status, effective_to, metadata and updated_at.
	UpdateRule(context.Context, *models.ReserveRule) error
	// ActiveRulesFor returns the active rules in force at `at` that cover the account directly or
	// through its client. The caller picks the most specific.
	ActiveRulesFor(ctx context.Context, accountID, clientID uuid.UUID, at time.Time) ([]models.ReserveRule, error)
}

type BunReserveRepository struct {
	db bun.IDB
}

func NewReserveRepository(db bun.IDB) *BunReserveRepository {
	return &BunReserveRepository{db: db}
}

func (r *BunReserveRepository) CreateRule(ctx context.Context, rule *models.ReserveRule) error {
	setUUID(&rule.ID)
	_, err := r.db.NewInsert().Model(rule).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunReserveRepository) GetRule(ctx context.Context, id uuid.UUID) (*models.ReserveRule, error) {
	rule := &models.ReserveRule{}
	err := r.db.NewSelect().Model(rule).Where("id = ?", id).Scan(ctx)
	return rule, postgres.ResolveError(err)
}

func (r *BunReserveRepository) ListRules(ctx context.Context, f ReserveRuleFilter) ([]models.ReserveRule, error) {
	out := make([]models.ReserveRule, 0)
	q := r.db.NewSelect().Model(&out).OrderExpr("created_at desc")
	if f.ClientID != nil {
		q = q.Where("client_id = ?", *f.ClientID)
	}
	if f.AccountID != nil {
		q = q.Where("account_id = ?", *f.AccountID)
	}
	if f.Status != nil {
		q = q.Where("status = ?", *f.Status)
	}
	return out, postgres.ResolveError(q.Scan(ctx))
}

func (r *BunReserveRepository) UpdateRule(ctx context.Context, rule *models.ReserveRule) error {
	rule.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().Model(rule).Column("status", "effective_to", "metadata", "updated_at").WherePK().Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunReserveRepository) ActiveRulesFor(ctx context.Context, accountID, clientID uuid.UUID, at time.Time) ([]models.ReserveRule, error) {
	out := make([]models.ReserveRule, 0)
	err := r.db.NewSelect().Model(&out).
		Where("status = ?", models.ReserveStatusActive).
		Where("effective_from <= ?", at).
		Where("(effective_to is null or effective_to > ?)", at).
		Where("(account_id = ? or client_id = ?)", accountID, clientID).
		Scan(ctx)
	return out, postgres.ResolveError(err)
}
