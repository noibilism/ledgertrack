package transfers

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	channelmodels "github.com/formancehq/ledger/internal/channels/models"
)

// BunFeeRecorder writes _system.channel_fee_records rows exactly once per
// reference. The table has no unique index on reference (historical data may
// hold duplicates, so adding one could fail a migration); a transaction level
// advisory lock plus an existence check gives the same guarantee for writers
// that go through this recorder.
type BunFeeRecorder struct {
	db *bun.DB
}

var _ FeeRecorder = (*BunFeeRecorder)(nil)

func NewBunFeeRecorder(db *bun.DB) *BunFeeRecorder {
	return &BunFeeRecorder{db: db}
}

func (r *BunFeeRecorder) Record(ctx context.Context, rec *channelmodels.ChannelFeeRecord) error {
	err := r.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, `select pg_advisory_xact_lock(hashtextextended(?, 0))`, "channel_fee_record:"+rec.Reference); err != nil {
			return err
		}
		exists, err := tx.NewSelect().
			Model((*channelmodels.ChannelFeeRecord)(nil)).
			Where("reference = ?", rec.Reference).
			Exists(ctx)
		if err != nil || exists {
			return err
		}
		if rec.ID == uuid.Nil {
			rec.ID = uuid.New()
		}
		if rec.OccurredAt.IsZero() {
			rec.OccurredAt = time.Now().UTC()
		}
		_, err = tx.NewInsert().Model(rec).Exec(ctx)
		return err
	})
	return postgres.ResolveError(err)
}
