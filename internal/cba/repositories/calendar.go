package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

// CalendarRepository persists business calendars.
type CalendarRepository interface {
	CreateCalendar(context.Context, *models.BusinessCalendar) error
	GetCalendar(context.Context, uuid.UUID) (*models.BusinessCalendar, error)
	ListCalendars(context.Context) ([]models.BusinessCalendar, error)
	// UpdateCalendar persists name, timezone, working days, hours and holidays.
	UpdateCalendar(context.Context, *models.BusinessCalendar) error
}

type BunCalendarRepository struct {
	db bun.IDB
}

func NewCalendarRepository(db bun.IDB) *BunCalendarRepository {
	return &BunCalendarRepository{db: db}
}

func (r *BunCalendarRepository) CreateCalendar(ctx context.Context, c *models.BusinessCalendar) error {
	setUUID(&c.ID)
	_, err := r.db.NewInsert().Model(c).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunCalendarRepository) GetCalendar(ctx context.Context, id uuid.UUID) (*models.BusinessCalendar, error) {
	c := &models.BusinessCalendar{}
	err := r.db.NewSelect().Model(c).Where("id = ?", id).Scan(ctx)
	return c, postgres.ResolveError(err)
}

func (r *BunCalendarRepository) ListCalendars(ctx context.Context) ([]models.BusinessCalendar, error) {
	out := make([]models.BusinessCalendar, 0)
	err := r.db.NewSelect().Model(&out).OrderExpr("code asc").Scan(ctx)
	return out, postgres.ResolveError(err)
}

func (r *BunCalendarRepository) UpdateCalendar(ctx context.Context, c *models.BusinessCalendar) error {
	c.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().Model(c).Column("name", "timezone", "working_days", "open_time", "close_time", "holidays", "updated_at").WherePK().Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}
