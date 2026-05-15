package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/channels/models"
)

type ChannelFeeConfigRepository interface {
	Upsert(context.Context, *models.ChannelFeeConfig) error
	GetByChannelID(context.Context, string) (*models.ChannelFeeConfig, error)
	List(context.Context, ChannelFeeConfigFilter) ([]models.ChannelFeeConfig, error)
}

type ChannelFeeConfigAuditRepository interface {
	Create(context.Context, *models.ChannelFeeConfigAudit) error
	ListByChannelID(context.Context, string, int) ([]models.ChannelFeeConfigAudit, error)
}

type ChannelFeeRecordRepository interface {
	Create(context.Context, *models.ChannelFeeRecord) error
	List(context.Context, ChannelFeeRecordFilter) ([]models.ChannelFeeRecord, error)
}

type ChannelFeeConfigFilter struct {
	Currency *string
	Enabled  *bool
}

type ChannelFeeRecordFilter struct {
	ChannelID  *string
	Currency   *string
	StartTime  *time.Time
	EndTime    *time.Time
	Limit      int
	Offset     int
}

type BunChannelFeeConfigRepository struct {
	db bun.IDB
}

func NewChannelFeeConfigRepository(db bun.IDB) *BunChannelFeeConfigRepository {
	return &BunChannelFeeConfigRepository{db: db}
}

func (r *BunChannelFeeConfigRepository) Upsert(ctx context.Context, cfg *models.ChannelFeeConfig) error {
	if cfg.ID == uuid.Nil {
		cfg.ID = uuid.New()
	}
	now := time.Now().UTC()
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = now
	}
	cfg.UpdatedAt = now

	_, err := r.db.NewInsert().
		Model(cfg).
		On("conflict (channel_id) do update").
		Set("currency = excluded.currency").
		Set("enabled = excluded.enabled").
		Set("user_fee = excluded.user_fee").
		Set("processing_fee = excluded.processing_fee").
		Set("updated_at = excluded.updated_at").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunChannelFeeConfigRepository) GetByChannelID(ctx context.Context, channelID string) (*models.ChannelFeeConfig, error) {
	cfg := &models.ChannelFeeConfig{}
	err := r.db.NewSelect().
		Model(cfg).
		Where("channel_id = ?", channelID).
		Limit(1).
		Scan(ctx)
	return cfg, postgres.ResolveError(err)
}

func (r *BunChannelFeeConfigRepository) List(ctx context.Context, filter ChannelFeeConfigFilter) ([]models.ChannelFeeConfig, error) {
	out := make([]models.ChannelFeeConfig, 0)
	q := r.db.NewSelect().Model(&out)
	if filter.Currency != nil {
		q = q.Where("currency = ?", *filter.Currency)
	}
	if filter.Enabled != nil {
		q = q.Where("enabled = ?", *filter.Enabled)
	}
	q = q.OrderExpr("updated_at desc")
	err := q.Scan(ctx)
	return out, postgres.ResolveError(err)
}

type BunChannelFeeConfigAuditRepository struct {
	db bun.IDB
}

func NewChannelFeeConfigAuditRepository(db bun.IDB) *BunChannelFeeConfigAuditRepository {
	return &BunChannelFeeConfigAuditRepository{db: db}
}

func (r *BunChannelFeeConfigAuditRepository) Create(ctx context.Context, audit *models.ChannelFeeConfigAudit) error {
	if audit.ID == uuid.Nil {
		audit.ID = uuid.New()
	}
	if audit.CreatedAt.IsZero() {
		audit.CreatedAt = time.Now().UTC()
	}
	_, err := r.db.NewInsert().Model(audit).Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunChannelFeeConfigAuditRepository) ListByChannelID(ctx context.Context, channelID string, limit int) ([]models.ChannelFeeConfigAudit, error) {
	out := make([]models.ChannelFeeConfigAudit, 0)
	q := r.db.NewSelect().
		Model(&out).
		Where("channel_id = ?", channelID).
		OrderExpr("created_at desc")
	if limit > 0 {
		q = q.Limit(limit)
	}
	err := q.Scan(ctx)
	return out, postgres.ResolveError(err)
}

type BunChannelFeeRecordRepository struct {
	db bun.IDB
}

func NewChannelFeeRecordRepository(db bun.IDB) *BunChannelFeeRecordRepository {
	return &BunChannelFeeRecordRepository{db: db}
}

func (r *BunChannelFeeRecordRepository) Create(ctx context.Context, rec *models.ChannelFeeRecord) error {
	if rec.ID == uuid.Nil {
		rec.ID = uuid.New()
	}
	if rec.OccurredAt.IsZero() {
		rec.OccurredAt = time.Now().UTC()
	}
	_, err := r.db.NewInsert().Model(rec).Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunChannelFeeRecordRepository) List(ctx context.Context, filter ChannelFeeRecordFilter) ([]models.ChannelFeeRecord, error) {
	out := make([]models.ChannelFeeRecord, 0)
	q := r.db.NewSelect().Model(&out)
	if filter.ChannelID != nil {
		q = q.Where("channel_id = ?", *filter.ChannelID)
	}
	if filter.Currency != nil {
		q = q.Where("currency = ?", *filter.Currency)
	}
	if filter.StartTime != nil {
		q = q.Where("occurred_at >= ?", *filter.StartTime)
	}
	if filter.EndTime != nil {
		q = q.Where("occurred_at <= ?", *filter.EndTime)
	}
	q = q.OrderExpr("occurred_at desc")
	if filter.Limit > 0 {
		q = q.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		q = q.Offset(filter.Offset)
	}
	err := q.Scan(ctx)
	return out, postgres.ResolveError(err)
}

