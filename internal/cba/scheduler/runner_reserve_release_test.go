package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/cba/services"
)

type releaserStub struct {
	passes []services.ReleaseStats
	err    error
	calls  int
	last   services.ReleaseOptions
}

func (s *releaserStub) RunRelease(_ context.Context, opts services.ReleaseOptions) (services.ReleaseStats, error) {
	s.last = opts
	i := s.calls
	s.calls++
	if i < len(s.passes) {
		return s.passes[i], s.err
	}
	return services.ReleaseStats{}, s.err
}

func newReleaseRunner(stub ReserveReleaser, batch int) *ReserveReleaseRunner {
	return NewReserveReleaseRunner(logging.Testing(), stub, ReserveReleaseRunnerConfig{Schedule: cron.Every(time.Minute), BatchSize: batch})
}

func TestReserveReleaseRunnerDrainsBacklogThenStops(t *testing.T) {
	t.Parallel()
	stub := &releaserStub{passes: []services.ReleaseStats{{Claimed: 10}, {Claimed: 10}, {Claimed: 4}}}
	require.NoError(t, newReleaseRunner(stub, 10).run(context.Background()))
	require.Equal(t, 3, stub.calls, "keeps going while batches are full, stops on a partial one")
}

func TestReserveReleaseRunnerBoundsATickAndSurfacesErrors(t *testing.T) {
	t.Parallel()
	always := &releaserStub{}
	for i := 0; i < maxReleasePassesPerTick+10; i++ {
		always.passes = append(always.passes, services.ReleaseStats{Claimed: 5})
	}
	require.NoError(t, newReleaseRunner(always, 5).run(context.Background()))
	require.Equal(t, maxReleasePassesPerTick, always.calls)

	failing := &releaserStub{err: errors.New("db down")}
	require.ErrorContains(t, newReleaseRunner(failing, 5).run(context.Background()), "db down")
}

func TestReserveReleaseRunnerDefaultsAndOwner(t *testing.T) {
	t.Parallel()
	stub := &releaserStub{}
	r := NewReserveReleaseRunner(logging.Testing(), stub, ReserveReleaseRunnerConfig{Schedule: cron.Every(time.Minute)})
	require.NoError(t, r.run(context.Background()))
	require.Equal(t, 200, stub.last.Batch)
	require.Equal(t, 2*time.Minute, stub.last.Lease)
	require.Equal(t, 10*time.Minute, stub.last.PendingGrace)
	require.Equal(t, 10, stub.last.MaxAttempts)
	require.Contains(t, stub.last.Owner, "reserve-release:")

	other := NewReserveReleaseRunner(logging.Testing(), stub, ReserveReleaseRunnerConfig{Schedule: cron.Every(time.Minute)})
	require.NotEqual(t, r.owner, other.owner, "every pod, and every start, has its own lease owner")
}
