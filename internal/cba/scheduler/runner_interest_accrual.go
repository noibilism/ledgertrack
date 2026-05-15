package scheduler

import (
	"context"
	"errors"
	"time"

	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

type InterestAccrualRunner struct {
	stopChannel       chan chan struct{}
	logger            logging.Logger
	accountRepository repositories.AccountRepository
	interestService   services.InterestService
	engine            PostingEngine
	cfg               InterestAccrualRunnerConfig
}

func NewInterestAccrualRunner(
	logger logging.Logger,
	accountRepository repositories.AccountRepository,
	interestService services.InterestService,
	engine PostingEngine,
	cfg InterestAccrualRunnerConfig,
) *InterestAccrualRunner {
	return &InterestAccrualRunner{
		stopChannel:       make(chan chan struct{}),
		logger:            logger,
		accountRepository: accountRepository,
		interestService:   interestService,
		engine:            engine,
		cfg:               cfg,
	}
}

func (r *InterestAccrualRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx, time.Now().UTC()); err != nil {
				r.logger.Errorf("error running interest accrual: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *InterestAccrualRunner) Stop(ctx context.Context) error {
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

func (r *InterestAccrualRunner) run(ctx context.Context, when time.Time) error {
	status := models.AccountStatusActive
	accounts, err := r.accountRepository.List(ctx, repositories.AccountFilter{Status: &status})
	if err != nil {
		return err
	}

	for _, account := range accounts {
		balance, err := r.engine.AvailableBalance(ctx, account)
		if err != nil {
			r.logger.Errorf("reading available balance for account %s: %v", account.ID, err)
			continue
		}
		if _, err := r.interestService.Accrue(ctx, account.ID, balance, when); err != nil && !errors.Is(err, services.ErrInterestNotApplicable) {
			r.logger.Errorf("accruing interest for account %s: %v", account.ID, err)
		}
	}

	return nil
}

func NewInterestAccrualRunnerModule(cfg InterestAccrualRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			accountRepository repositories.AccountRepository,
			interestService services.InterestService,
			engine PostingEngine,
		) *InterestAccrualRunner {
			return NewInterestAccrualRunner(logger, accountRepository, interestService, engine, cfg)
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *InterestAccrualRunner) {
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
