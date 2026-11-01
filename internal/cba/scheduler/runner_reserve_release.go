package scheduler

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/cba/services"
)

// maxReleasePassesPerTick bounds one tick when many holds fall due together (for example at the
// start of a business day); the next tick continues.
const maxReleasePassesPerTick = 50

// ReserveReleaser is what the runner needs from the hold service.
type ReserveReleaser interface {
	RunRelease(ctx context.Context, opts services.ReleaseOptions) (services.ReleaseStats, error)
}

// ReserveReleaseRunner moves held shares from the wallet lien to the available balance once their SLA
// has passed, and settles credits that were interrupted between recording the hold and posting it.
// It is safe on several pods: rows are claimed with FOR UPDATE SKIP LOCKED and a lease, and the
// ledger release is idempotent on the hold id.
type ReserveReleaseRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	releaser    ReserveReleaser
	cfg         ReserveReleaseRunnerConfig
	owner       string
}

func NewReserveReleaseRunner(logger logging.Logger, releaser ReserveReleaser, cfg ReserveReleaseRunnerConfig) *ReserveReleaseRunner {
	host, _ := os.Hostname()
	return &ReserveReleaseRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		releaser:    releaser,
		cfg:         cfg.withDefaults(),
		owner:       "reserve-release:" + host + ":" + uuid.NewString(),
	}
}

func (c ReserveReleaseRunnerConfig) withDefaults() ReserveReleaseRunnerConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.Lease <= 0 {
		c.Lease = 2 * time.Minute
	}
	if c.PendingGrace <= 0 {
		c.PendingGrace = 10 * time.Minute
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	return c
}

func (r *ReserveReleaseRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running reserve release: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *ReserveReleaseRunner) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.stopChannel <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
	return nil
}

func (r *ReserveReleaseRunner) run(ctx context.Context) error {
	opts := services.ReleaseOptions{
		Owner: r.owner, Batch: r.cfg.BatchSize, Lease: r.cfg.Lease,
		PendingGrace: r.cfg.PendingGrace, MaxAttempts: r.cfg.MaxAttempts,
	}
	for pass := 0; pass < maxReleasePassesPerTick; pass++ {
		stats, err := r.releaser.RunRelease(ctx, opts)
		if stats.Claimed > 0 {
			r.logger.Infof(
				"reserve release pass: claimed=%d released=%d failed=%d needs_review=%d recovered=%d voided=%d",
				stats.Claimed, stats.Released, stats.Failed, stats.Escalated, stats.Recovered, stats.Voided,
			)
		}
		if err != nil {
			return err
		}
		if stats.Claimed < r.cfg.BatchSize {
			return nil
		}
	}
	return nil
}

func NewReserveReleaseRunnerModule(cfg ReserveReleaseRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger, service services.ReserveHoldService) *ReserveReleaseRunner {
			return NewReserveReleaseRunner(logger, service, cfg)
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *ReserveReleaseRunner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := runner.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()
					return nil
				},
				OnStop: runner.Stop,
			})
		}),
	)
}
