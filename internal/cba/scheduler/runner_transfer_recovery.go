package scheduler

import (
	"context"
	"time"

	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/transfers"
)

// maxRecoveryPassesPerTick bounds how long one tick can run when there is a
// large backlog; the next tick continues.
const maxRecoveryPassesPerTick = 50

// TransferRecoverer is what the runner needs from the transfers service.
type TransferRecoverer interface {
	RecoverBatch(ctx context.Context) (transfers.RecoveryStats, error)
}

// TransferRecoveryRunner finishes multi-ledger wallet transfers (debit and lien
// release with pay) that were interrupted after the principal was posted. It is
// safe to run on several pods: the store claims rows with FOR UPDATE SKIP LOCKED
// and a lease, and every leg is idempotent on the business reference.
type TransferRecoveryRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	recoverer   TransferRecoverer
	cfg         TransferRecoveryRunnerConfig
}

func NewTransferRecoveryRunner(
	logger logging.Logger,
	recoverer TransferRecoverer,
	cfg TransferRecoveryRunnerConfig,
) *TransferRecoveryRunner {
	return &TransferRecoveryRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		recoverer:   recoverer,
		cfg:         cfg,
	}
}

func (r *TransferRecoveryRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running transfer recovery: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *TransferRecoveryRunner) Stop(ctx context.Context) error {
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

func (r *TransferRecoveryRunner) run(ctx context.Context) error {
	for pass := 0; pass < maxRecoveryPassesPerTick; pass++ {
		stats, err := r.recoverer.RecoverBatch(ctx)
		if stats.Claimed > 0 {
			r.logger.Infof(
				"transfer recovery pass: claimed=%d completed=%d still_pending=%d needs_review=%d errored=%d",
				stats.Claimed, stats.Completed, stats.Pending, stats.Escalated, stats.Failed,
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

func NewTransferRecoveryRunnerModule(cfg TransferRecoveryRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			service *transfers.Service,
		) *TransferRecoveryRunner {
			return NewTransferRecoveryRunner(logger, service, cfg)
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *TransferRecoveryRunner) {
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
