package scheduler

import (
	"time"

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

// ReserveReleaseRunnerConfig configures the runner that releases held reserve shares to the
// available balance when their SLA has passed.
type ReserveReleaseRunnerConfig struct {
	Schedule cron.Schedule
	// BatchSize is the number of holds claimed per pass.
	BatchSize int
	// Lease is how long a pod owns a claimed hold before another may take it over.
	Lease time.Duration
	// PendingGrace is the age after which a hold whose credit was interrupted is settled.
	PendingGrace time.Duration
	// MaxAttempts is the number of failed releases before a hold goes to needs_review.
	MaxAttempts int
}

type ModuleConfig struct {
	LedgerPostingConfig          LedgerPostingConfig
	InterestAccrualRunnerConfig  InterestAccrualRunnerConfig
	InterestPostingRunnerConfig  InterestPostingRunnerConfig
	MaintenanceFeeRunnerConfig   MaintenanceFeeRunnerConfig
	DormancyRunnerConfig         DormancyRunnerConfig
	TransferRecoveryRunnerConfig TransferRecoveryRunnerConfig
	ReserveReleaseRunnerConfig   ReserveReleaseRunnerConfig
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
		NewReserveReleaseRunnerModule(cfg.ReserveReleaseRunnerConfig),
	)
}
