package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

type FeeOverrideFilter struct {
	ClientID  *uuid.UUID
	AccountID *uuid.UUID
	Event     *string
	Status    *string
}

type TaxRuleFilter struct {
	Country *string
	Event   *string
	Status  *string
}

// FeeConfigRepository persists fee overrides and tax rules.
type FeeConfigRepository interface {
	CreateOverride(context.Context, *models.FeeOverride) error
	GetOverride(context.Context, uuid.UUID) (*models.FeeOverride, error)
	ListOverrides(context.Context, FeeOverrideFilter) ([]models.FeeOverride, error)
	// UpdateOverride persists status, effective_to, metadata and updated_at.
	UpdateOverride(context.Context, *models.FeeOverride) error
	// ActiveOverridesFor returns the active overrides for an event that are in force at `at` and
	// that cover the account directly or through its client. The caller picks the most specific.
	ActiveOverridesFor(ctx context.Context, accountID, clientID uuid.UUID, event string, at time.Time) ([]models.FeeOverride, error)

	CreateTaxRule(context.Context, *models.TaxRule) error
	GetTaxRule(context.Context, uuid.UUID) (*models.TaxRule, error)
	ListTaxRules(context.Context, TaxRuleFilter) ([]models.TaxRule, error)
	// UpdateTaxRule persists status, effective_to, metadata and updated_at.
	UpdateTaxRule(context.Context, *models.TaxRule) error
	// ActiveTaxRules returns the active rules for a country and event that are in force at `at`.
	ActiveTaxRules(ctx context.Context, country, event string, at time.Time) ([]models.TaxRule, error)
	// HasActiveTaxRules reports whether any country has an active rule for the event at `at`.
	HasActiveTaxRules(ctx context.Context, event string, at time.Time) (bool, error)
}

type BunFeeConfigRepository struct {
	db bun.IDB
}

func NewFeeConfigRepository(db bun.IDB) *BunFeeConfigRepository {
	return &BunFeeConfigRepository{db: db}
}

func (r *BunFeeConfigRepository) CreateOverride(ctx context.Context, o *models.FeeOverride) error {
	setUUID(&o.ID)
	_, err := r.db.NewInsert().Model(o).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) GetOverride(ctx context.Context, id uuid.UUID) (*models.FeeOverride, error) {
	o := &models.FeeOverride{}
	err := r.db.NewSelect().Model(o).Where("id = ?", id).Scan(ctx)
	return o, postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) ListOverrides(ctx context.Context, f FeeOverrideFilter) ([]models.FeeOverride, error) {
	out := make([]models.FeeOverride, 0)
	q := r.db.NewSelect().Model(&out).OrderExpr("created_at desc")
	if f.ClientID != nil {
		q = q.Where("client_id = ?", *f.ClientID)
	}
	if f.AccountID != nil {
		q = q.Where("account_id = ?", *f.AccountID)
	}
	if f.Event != nil {
		q = q.Where("event = ?", *f.Event)
	}
	if f.Status != nil {
		q = q.Where("status = ?", *f.Status)
	}
	return out, postgres.ResolveError(q.Scan(ctx))
}

func (r *BunFeeConfigRepository) UpdateOverride(ctx context.Context, o *models.FeeOverride) error {
	o.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().Model(o).Column("status", "effective_to", "metadata", "updated_at").WherePK().Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) ActiveOverridesFor(ctx context.Context, accountID, clientID uuid.UUID, event string, at time.Time) ([]models.FeeOverride, error) {
	out := make([]models.FeeOverride, 0)
	err := r.db.NewSelect().Model(&out).
		Where("status = ?", models.FeeConfigStatusActive).
		Where("event = ?", event).
		Where("effective_from <= ?", at).
		Where("(effective_to is null or effective_to > ?)", at).
		Where("(account_id = ? or client_id = ?)", accountID, clientID).
		Scan(ctx)
	return out, postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) CreateTaxRule(ctx context.Context, t *models.TaxRule) error {
	setUUID(&t.ID)
	_, err := r.db.NewInsert().Model(t).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) GetTaxRule(ctx context.Context, id uuid.UUID) (*models.TaxRule, error) {
	t := &models.TaxRule{}
	err := r.db.NewSelect().Model(t).Where("id = ?", id).Scan(ctx)
	return t, postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) ListTaxRules(ctx context.Context, f TaxRuleFilter) ([]models.TaxRule, error) {
	out := make([]models.TaxRule, 0)
	q := r.db.NewSelect().Model(&out).OrderExpr("country asc, code asc, effective_from desc")
	if f.Country != nil {
		q = q.Where("country = ?", *f.Country)
	}
	if f.Event != nil {
		q = q.Where("event = ?", *f.Event)
	}
	if f.Status != nil {
		q = q.Where("status = ?", *f.Status)
	}
	return out, postgres.ResolveError(q.Scan(ctx))
}

func (r *BunFeeConfigRepository) UpdateTaxRule(ctx context.Context, t *models.TaxRule) error {
	t.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().Model(t).Column("status", "effective_to", "metadata", "updated_at").WherePK().Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) ActiveTaxRules(ctx context.Context, country, event string, at time.Time) ([]models.TaxRule, error) {
	out := make([]models.TaxRule, 0)
	err := r.db.NewSelect().Model(&out).
		Where("country = ?", country).
		Where("event = ?", event).
		Where("status = ?", models.FeeConfigStatusActive).
		Where("effective_from <= ?", at).
		Where("(effective_to is null or effective_to > ?)", at).
		OrderExpr("code asc").
		Scan(ctx)
	return out, postgres.ResolveError(err)
}

func (r *BunFeeConfigRepository) HasActiveTaxRules(ctx context.Context, event string, at time.Time) (bool, error) {
	exists, err := r.db.NewSelect().Model((*models.TaxRule)(nil)).
		Where("event = ?", event).
		Where("status = ?", models.FeeConfigStatusActive).
		Where("effective_from <= ?", at).
		Where("(effective_to is null or effective_to > ?)", at).
		Exists(ctx)
	return exists, postgres.ResolveError(err)
}
