package scheduler

import (
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

type LedgerPostingConfig struct {
	LedgerName             string
	FeeIncomeAccount       string
	InterestExpenseAccount string
}

type InterestAccrualRunnerConfig struct {
	Schedule cron.Schedule
}

type InterestPostingRunnerConfig struct {
	Schedule cron.Schedule
}

type MaintenanceFeeRunnerConfig struct {
	Schedule cron.Schedule
}

type DormancyRunnerConfig struct {
	Schedule cron.Schedule
}

// TransferRecoveryRunnerConfig configures the runner that finishes interrupted
// multi-ledger wallet transfers (see docs/design/atomic-transfers.md).
type TransferRecoveryRunnerConfig struct {
	Schedule cron.Schedule
	// BatchSize is the number of transfers claimed per pass.
	BatchSize int
}

type ModuleConfig struct {
	LedgerPostingConfig          LedgerPostingConfig
	InterestAccrualRunnerConfig  InterestAccrualRunnerConfig
	InterestPostingRunnerConfig  InterestPostingRunnerConfig
	MaintenanceFeeRunnerConfig   MaintenanceFeeRunnerConfig
	DormancyRunnerConfig         DormancyRunnerConfig
	TransferRecoveryRunnerConfig TransferRecoveryRunnerConfig
}

func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(system systemcontroller.Controller) PostingEngine {
			return NewLedgerPostingEngine(system, cfg.LedgerPostingConfig)
		}),
		NewInterestAccrualRunnerModule(cfg.InterestAccrualRunnerConfig),
		NewInterestPostingRunnerModule(cfg.InterestPostingRunnerConfig),
		NewMaintenanceFeeRunnerModule(cfg.MaintenanceFeeRunnerConfig),
		NewDormancyRunnerModule(cfg.DormancyRunnerConfig),
		NewTransferRecoveryRunnerModule(cfg.TransferRecoveryRunnerConfig),
	)
}
