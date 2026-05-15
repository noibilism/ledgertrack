package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

type InterestPostingRunner struct {
	stopChannel       chan chan struct{}
	logger            logging.Logger
	accountRepository repositories.AccountRepository
	interestService   services.InterestService
	engine            PostingEngine
	cfg               InterestPostingRunnerConfig
}

func NewInterestPostingRunner(
	logger logging.Logger,
	accountRepository repositories.AccountRepository,
	interestService services.InterestService,
	engine PostingEngine,
	cfg InterestPostingRunnerConfig,
) *InterestPostingRunner {
	return &InterestPostingRunner{
		stopChannel:       make(chan chan struct{}),
		logger:            logger,
		accountRepository: accountRepository,
		interestService:   interestService,
		engine:            engine,
		cfg:               cfg,
	}
}

func (r *InterestPostingRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx, time.Now().UTC()); err != nil {
				r.logger.Errorf("error running interest posting: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *InterestPostingRunner) Stop(ctx context.Context) error {
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

func (r *InterestPostingRunner) run(ctx context.Context, when time.Time) error {
	accounts, err := r.accountRepository.List(ctx, repositories.AccountFilter{})
	if err != nil {
		return err
	}

	for _, account := range accounts {
		due, err := r.interestService.IsPostingDue(ctx, account.ID, when)
		if err != nil {
			r.logger.Errorf("checking interest posting due for account %s: %v", account.ID, err)
			continue
		}
		if !due {
			continue
		}

		preview, err := r.interestService.PreviewPosting(ctx, account.ID)
		if err != nil {
			if errors.Is(err, services.ErrInterestNotApplicable) {
				continue
			}
			r.logger.Errorf("previewing interest posting for account %s: %v", account.ID, err)
			continue
		}
		if preview.PostableAmount <= 0 {
			continue
		}

		reference := fmt.Sprintf("interest:%s:%s", account.ID.String(), normalizeScheduleDate(when).Format("2006-01-02"))
		txnMetadata := map[string]string{
			"cba_operation": "interest_posting",
			"account_id":    account.ID.String(),
			"wallet_id":     account.WalletID,
		}

		if err := r.engine.Credit(ctx, account, preview.PostableAmount, reference, txnMetadata); err != nil {
			r.logger.Errorf("posting wallet interest for account %s: %v", account.ID, err)
			continue
		}
		if err := r.engine.RecordInterestExpense(ctx, account.Currency, reference, preview.PostableAmount, txnMetadata); err != nil {
			r.logger.Errorf("recording interest expense for account %s: %v", account.ID, err)
			continue
		}
		if err := r.interestService.MarkPosted(ctx, *preview, reference); err != nil {
			r.logger.Errorf("marking interest as posted for account %s: %v", account.ID, err)
		}
	}

	return nil
}

func NewInterestPostingRunnerModule(cfg InterestPostingRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			accountRepository repositories.AccountRepository,
			interestService services.InterestService,
			engine PostingEngine,
		) *InterestPostingRunner {
			return NewInterestPostingRunner(logger, accountRepository, interestService, engine, cfg)
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *InterestPostingRunner) {
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
