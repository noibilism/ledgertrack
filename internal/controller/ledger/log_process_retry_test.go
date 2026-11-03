package ledger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/platform/postgres"

	ledger "github.com/formancehq/ledger/internal"
)

func TestForgeLogGivesUpAfterTooManyDeadlocks(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	gc := gomock.NewController(t)
	store := NewMockStore(gc)

	store.EXPECT().BeginTX(gomock.Any(), gomock.Any()).Return(store, &bun.Tx{}, nil).Times(maxTxRetries)
	store.EXPECT().FindLatestSchemaVersion(gomock.Any()).Return(nil, nil).Times(maxTxRetries)
	store.EXPECT().Rollback(gomock.Any()).Return(nil).Times(maxTxRetries)

	attempts := 0
	lp := newLogProcessor[RunScript, ledger.CreatedTransaction]("foo", noop.Int64Counter{}, SchemaEnforcementAudit)
	_, _, _, err := lp.forgeLog(ctx, store, Parameters[RunScript]{}, func(context.Context, Store, *ledger.Schema, Parameters[RunScript]) (*ledger.CreatedTransaction, error) {
		attempts++
		return nil, postgres.ErrDeadlockDetected
	})
	require.ErrorIs(t, err, postgres.ErrDeadlockDetected)
	require.Equal(t, maxTxRetries, attempts)
}

func TestForgeLogStopsRetryingWhenContextIsDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(logging.TestingContext())
	gc := gomock.NewController(t)
	store := NewMockStore(gc)

	store.EXPECT().BeginTX(gomock.Any(), gomock.Any()).Return(store, &bun.Tx{}, nil)
	store.EXPECT().FindLatestSchemaVersion(gomock.Any()).Return(nil, nil)
	store.EXPECT().Rollback(gomock.Any()).Return(nil)

	lp := newLogProcessor[RunScript, ledger.CreatedTransaction]("foo", noop.Int64Counter{}, SchemaEnforcementAudit)
	_, _, _, err := lp.forgeLog(ctx, store, Parameters[RunScript]{}, func(context.Context, Store, *ledger.Schema, Parameters[RunScript]) (*ledger.CreatedTransaction, error) {
		cancel()
		return nil, postgres.ErrSerialization
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryBackoffIsBounded(t *testing.T) {
	t.Parallel()

	for attempt := 0; attempt < 40; attempt++ {
		for i := 0; i < 50; i++ {
			d := retryBackoff(attempt)
			require.Positive(t, d)
			require.LessOrEqual(t, d, retryMaxDelay)
		}
	}
}
