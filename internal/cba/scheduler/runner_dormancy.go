package scheduler

import (
	"context"
	"time"

	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

type DormancyRunner struct {
	stopChannel       chan chan struct{}
	logger            logging.Logger
	accountRepository repositories.AccountRepository
	productRepository repositories.ProductRepository
	accountService    services.AccountService
	cfg               DormancyRunnerConfig
}

func NewDormancyRunner(
	logger logging.Logger,
	accountRepository repositories.AccountRepository,
	productRepository repositories.ProductRepository,
	accountService services.AccountService,
	cfg DormancyRunnerConfig,
) *DormancyRunner {
	return &DormancyRunner{
		stopChannel:       make(chan chan struct{}),
		logger:            logger,
		accountRepository: accountRepository,
		productRepository: productRepository,
		accountService:    accountService,
		cfg:               cfg,
	}
}

func (r *DormancyRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx, time.Now().UTC()); err != nil {
				r.logger.Errorf("error running dormancy detection: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *DormancyRunner) Stop(ctx context.Context) error {
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

func (r *DormancyRunner) run(ctx context.Context, when time.Time) error {
	status := models.AccountStatusActive
	accounts, err := r.accountRepository.List(ctx, repositories.AccountFilter{Status: &status})
	if err != nil {
		return err
	}

	for _, account := range accounts {
		product, err := r.productRepository.Get(ctx, account.ProductID)
		if err != nil {
			r.logger.Errorf("loading product for account %s: %v", account.ID, err)
			continue
		}
		if product.Rules.DormancyDays == nil || *product.Rules.DormancyDays <= 0 {
			continue
		}

		lastActivity := account.OpenedAt
		if account.LastActivityAt != nil {
			lastActivity = *account.LastActivityAt
		}
		if normalizeScheduleDate(lastActivity).AddDate(0, 0, *product.Rules.DormancyDays).After(normalizeScheduleDate(when)) {
			continue
		}
		if _, err := r.accountService.Dormant(ctx, account.ID); err != nil {
			r.logger.Errorf("marking account %s dormant: %v", account.ID, err)
		}
	}

	return nil
}

func NewDormancyRunnerModule(cfg DormancyRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			accountRepository repositories.AccountRepository,
			productRepository repositories.ProductRepository,
			accountService services.AccountService,
		) *DormancyRunner {
			return NewDormancyRunner(logger, accountRepository, productRepository, accountService, cfg)
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *DormancyRunner) {
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
