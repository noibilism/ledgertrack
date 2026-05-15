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

type ModuleConfig struct {
	LedgerPostingConfig         LedgerPostingConfig
	InterestAccrualRunnerConfig InterestAccrualRunnerConfig
	InterestPostingRunnerConfig InterestPostingRunnerConfig
	MaintenanceFeeRunnerConfig  MaintenanceFeeRunnerConfig
	DormancyRunnerConfig        DormancyRunnerConfig
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
	)
}
