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

type MaintenanceFeeRunner struct {
	stopChannel       chan chan struct{}
	logger            logging.Logger
	accountRepository repositories.AccountRepository
	feeRepository     repositories.FeePostingRepository
	accountService    services.AccountService
	feeService        services.FeeService
	engine            PostingEngine
	cfg               MaintenanceFeeRunnerConfig
}

func NewMaintenanceFeeRunner(
	logger logging.Logger,
	accountRepository repositories.AccountRepository,
	feeRepository repositories.FeePostingRepository,
	accountService services.AccountService,
	feeService services.FeeService,
	engine PostingEngine,
	cfg MaintenanceFeeRunnerConfig,
) *MaintenanceFeeRunner {
	return &MaintenanceFeeRunner{
		stopChannel:       make(chan chan struct{}),
		logger:            logger,
		accountRepository: accountRepository,
		feeRepository:     feeRepository,
		accountService:    accountService,
		feeService:        feeService,
		engine:            engine,
		cfg:               cfg,
	}
}

func (r *MaintenanceFeeRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx, time.Now().UTC()); err != nil {
				r.logger.Errorf("error running maintenance fees: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *MaintenanceFeeRunner) Stop(ctx context.Context) error {
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

func (r *MaintenanceFeeRunner) run(ctx context.Context, when time.Time) error {
	if err := r.prepareDueFees(ctx, when); err != nil {
		return err
	}

	postings, err := r.feeRepository.ListPendingRecovery(ctx)
	if err != nil {
		return err
	}

	for _, posting := range postings {
		if err := r.processPendingFee(ctx, posting, when); err != nil {
			r.logger.Errorf("processing fee posting %s: %v", posting.Reference, err)
		}
	}

	return nil
}

func (r *MaintenanceFeeRunner) prepareDueFees(ctx context.Context, when time.Time) error {
	status := models.AccountStatusActive
	accounts, err := r.accountRepository.List(ctx, repositories.AccountFilter{Status: &status})
	if err != nil {
		return err
	}

	for _, account := range accounts {
		if _, err := r.feeService.PrepareMaintenanceFee(ctx, account.ID, when); err != nil && !errors.Is(err, services.ErrFeeNotApplicable) {
			r.logger.Errorf("preparing maintenance fee for account %s: %v", account.ID, err)
		}
	}

	return nil
}

func (r *MaintenanceFeeRunner) processPendingFee(ctx context.Context, posting models.FeePosting, when time.Time) error {
	account, err := r.accountService.Get(ctx, posting.AccountID)
	if err != nil {
		return err
	}

	amountMinor, err := decimalToMinorHalfUp(posting.Amount, posting.Currency)
	if err != nil {
		return err
	}
	if amountMinor <= 0 {
		return nil
	}

	walletPosted := metadataBool(posting.Metadata, "wallet_posted")
	if !walletPosted {
		currentBalance, err := r.engine.AvailableBalance(ctx, *account)
		if err != nil {
			r.markPending(ctx, posting.Reference, false, err)
			return err
		}
		if _, err := r.accountService.ValidateDebit(ctx, account.ID, amountMinor, currentBalance, when); err != nil {
			r.markPending(ctx, posting.Reference, false, err)
			return err
		}
		if err := r.engine.Debit(ctx, *account, amountMinor, posting.Reference, feePostingMetadata(posting, account)); err != nil {
			r.markPending(ctx, posting.Reference, false, err)
			return err
		}
		walletPosted = true
	}

	if err := r.engine.RecordFeeIncome(ctx, posting.Currency, posting.Reference, amountMinor, feePostingMetadata(posting, account)); err != nil {
		r.markPending(ctx, posting.Reference, walletPosted, err)
		return err
	}

	_, err = r.feeService.MarkPosted(ctx, posting.Reference)
	return err
}

func (r *MaintenanceFeeRunner) markPending(ctx context.Context, reference string, walletPosted bool, reason error) {
	if _, err := r.feeService.MarkPendingRecovery(ctx, reference, map[string]any{
		"wallet_posted":      walletPosted,
		"recovery_error":     reason.Error(),
		"recovery_attempted": normalizeScheduleDate(time.Now().UTC()).Format(time.RFC3339),
	}); err != nil {
		r.logger.Errorf("marking fee %s pending recovery: %v", reference, err)
	}
}

func feePostingMetadata(posting models.FeePosting, account *models.Account) map[string]string {
	return map[string]string{
		"cba_operation":    "fee_posting",
		"account_id":       account.ID.String(),
		"wallet_id":        account.WalletID,
		"fee_event_type":   posting.EventType,
		"linked_reference": posting.LinkedReference,
	}
}

func metadataBool(values map[string]any, key string) bool {
	raw, ok := values[key]
	if !ok {
		return false
	}
	ret, ok := raw.(bool)
	return ok && ret
}

func NewMaintenanceFeeRunnerModule(cfg MaintenanceFeeRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			accountRepository repositories.AccountRepository,
			feeRepository repositories.FeePostingRepository,
			accountService services.AccountService,
			feeService services.FeeService,
			engine PostingEngine,
		) *MaintenanceFeeRunner {
			return NewMaintenanceFeeRunner(logger, accountRepository, feeRepository, accountService, feeService, engine, cfg)
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *MaintenanceFeeRunner) {
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
