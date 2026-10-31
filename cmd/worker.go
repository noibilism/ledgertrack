package cmd

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/publish"
	"github.com/formancehq/go-libs/v3/service"

	"github.com/formancehq/ledger/internal/bus"
	"github.com/formancehq/ledger/internal/cba"
	"github.com/formancehq/ledger/internal/cba/scheduler"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/alldrivers"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/loghash"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/transfers"
	"github.com/formancehq/ledger/internal/worker"
)

const (
	WorkerPipelinesPullIntervalFlag    = "worker-pipelines-pull-interval"
	WorkerPipelinesPushRetryPeriodFlag = "worker-pipelines-push-retry-period"
	WorkerPipelinesSyncPeriod          = "worker-pipelines-sync-period"
	WorkerPipelinesLogsPageSize        = "worker-pipelines-logs-page-size"

	WorkerAsyncBlockHasherMaxBlockSizeFlag = "worker-async-block-hasher-max-block-size"
	WorkerAsyncBlockHasherScheduleFlag     = "worker-async-block-hasher-schedule"

	WorkerBucketCleanupRetentionPeriodFlag = "worker-bucket-cleanup-retention-period"
	WorkerBucketCleanupScheduleFlag        = "worker-bucket-cleanup-schedule"

	WorkerCBAInterestAccrualScheduleFlag     = "worker-cba-interest-accrual-schedule"
	WorkerCBAInterestPostingScheduleFlag     = "worker-cba-interest-posting-schedule"
	WorkerCBAMaintenanceFeeScheduleFlag      = "worker-cba-maintenance-fee-schedule"
	WorkerCBADormancyScheduleFlag            = "worker-cba-dormancy-schedule"
	WorkerCBATransferRecoveryScheduleFlag    = "worker-cba-transfer-recovery-schedule"
	WorkerCBATransferRecoveryBatchSizeFlag   = "worker-cba-transfer-recovery-batch-size"
	WorkerCBATransferRecoveryMaxAttemptsFlag = "worker-cba-transfer-recovery-max-attempts"
	WorkerCBATransferRecoveryGraceFlag       = "worker-cba-transfer-recovery-grace"
	WorkerCBATransferRecoveryLeaseFlag       = "worker-cba-transfer-recovery-lease"
	WorkerCBATransferRecoveryMaxBackoffFlag  = "worker-cba-transfer-recovery-max-backoff"
	WorkerCBALedgerNameFlag                  = "worker-cba-ledger-name"
	WorkerCBAFeeIncomeAccountFlag            = "worker-cba-fee-income-account"
	WorkerCBAInterestExpenseAccountFlag      = "worker-cba-interest-expense-account"

	WorkerGRPCAddressFlag = "worker-grpc-address"
)

type WorkerGRPCConfig struct {
	Address string `mapstructure:"worker-grpc-address"`
}

type WorkerConfiguration struct {
	HashLogsBlockMaxSize  int           `mapstructure:"worker-async-block-hasher-max-block-size"`
	HashLogsBlockCRONSpec cron.Schedule `mapstructure:"worker-async-block-hasher-schedule"`
	// HashLogsBlockDisabled turns the legacy block hasher off, see docs/design/async-log-hashing.md.
	HashLogsBlockDisabled bool `mapstructure:"worker-async-block-hasher-disabled"`

	LogHasherInterval  time.Duration `mapstructure:"worker-log-hasher-interval"`
	LogHasherBatchSize int           `mapstructure:"worker-log-hasher-batch-size"`
	LogHasherGapGrace  time.Duration `mapstructure:"worker-log-hasher-gap-grace"`
	LogHasherAlertLag  time.Duration `mapstructure:"worker-log-hasher-alert-lag"`

	PushRetryPeriod time.Duration `mapstructure:"worker-pipelines-push-retry-period"`
	PullInterval    time.Duration `mapstructure:"worker-pipelines-pull-interval"`
	SyncPeriod      time.Duration `mapstructure:"worker-pipelines-sync-period"`
	LogsPageSize    uint64        `mapstructure:"worker-pipelines-logs-page-size"`

	BucketCleanupRetentionPeriod time.Duration `mapstructure:"worker-bucket-cleanup-retention-period"`
	BucketCleanupCRONSpec        cron.Schedule `mapstructure:"worker-bucket-cleanup-schedule"`

	CBAInterestAccrualCRONSpec     cron.Schedule `mapstructure:"worker-cba-interest-accrual-schedule"`
	CBAInterestPostingCRONSpec     cron.Schedule `mapstructure:"worker-cba-interest-posting-schedule"`
	CBAMaintenanceFeeCRONSpec      cron.Schedule `mapstructure:"worker-cba-maintenance-fee-schedule"`
	CBADormancyCRONSpec            cron.Schedule `mapstructure:"worker-cba-dormancy-schedule"`
	CBATransferRecoveryCRONSpec    cron.Schedule `mapstructure:"worker-cba-transfer-recovery-schedule"`
	CBATransferRecoveryBatchSize   int           `mapstructure:"worker-cba-transfer-recovery-batch-size"`
	CBATransferRecoveryMaxAttempts int           `mapstructure:"worker-cba-transfer-recovery-max-attempts"`
	CBATransferRecoveryGrace       time.Duration `mapstructure:"worker-cba-transfer-recovery-grace"`
	CBATransferRecoveryLease       time.Duration `mapstructure:"worker-cba-transfer-recovery-lease"`
	CBATransferRecoveryMaxBackoff  time.Duration `mapstructure:"worker-cba-transfer-recovery-max-backoff"`
	CBALedgerName                  string        `mapstructure:"worker-cba-ledger-name"`
	CBAFeeIncomeAccount            string        `mapstructure:"worker-cba-fee-income-account"`
	CBAInterestExpenseAccount      string        `mapstructure:"worker-cba-interest-expense-account"`
}

func (cfg WorkerConfiguration) Validate() error {
	if cfg.BucketCleanupRetentionPeriod <= 0 {
		return fmt.Errorf("bucket cleanup retention period must be greater than zero")
	}
	if cfg.BucketCleanupCRONSpec == nil {
		return fmt.Errorf("bucket cleanup schedule must be set")
	}
	if cfg.CBAInterestAccrualCRONSpec == nil {
		return fmt.Errorf("cba interest accrual schedule must be set")
	}
	if cfg.CBAInterestPostingCRONSpec == nil {
		return fmt.Errorf("cba interest posting schedule must be set")
	}
	if cfg.CBAMaintenanceFeeCRONSpec == nil {
		return fmt.Errorf("cba maintenance fee schedule must be set")
	}
	if cfg.CBADormancyCRONSpec == nil {
		return fmt.Errorf("cba dormancy schedule must be set")
	}
	if cfg.CBATransferRecoveryCRONSpec == nil {
		return fmt.Errorf("cba transfer recovery schedule must be set")
	}
	if cfg.CBATransferRecoveryBatchSize <= 0 {
		return fmt.Errorf("cba transfer recovery batch size must be greater than zero")
	}
	if cfg.CBATransferRecoveryMaxAttempts <= 0 {
		return fmt.Errorf("cba transfer recovery max attempts must be greater than zero")
	}
	if cfg.CBATransferRecoveryGrace <= 0 || cfg.CBATransferRecoveryLease <= 0 || cfg.CBATransferRecoveryMaxBackoff <= 0 {
		return fmt.Errorf("cba transfer recovery grace, lease and max backoff must be greater than zero")
	}
	if cfg.CBALedgerName == "" {
		return fmt.Errorf("cba ledger name must be set")
	}
	if cfg.CBAFeeIncomeAccount == "" {
		return fmt.Errorf("cba fee income account must be set")
	}
	if cfg.CBAInterestExpenseAccount == "" {
		return fmt.Errorf("cba interest expense account must be set")
	}

	return nil
}

type WorkerCommandConfiguration struct {
	WorkerConfiguration `mapstructure:",squash"`
	commonConfig        `mapstructure:",squash"`
	WorkerGRPCConfig    `mapstructure:",squash"`
}

// addWorkerFlags adds command-line flags to cmd to configure worker runtime behavior.
// The flags control async block hashing, pipeline pull/push/sync behavior and pagination, and bucket cleanup retention and schedule.
func addWorkerFlags(cmd *cobra.Command) {
	cmd.Flags().Int(WorkerAsyncBlockHasherMaxBlockSizeFlag, 1000, "Max block size")
	cmd.Flags().String(WorkerAsyncBlockHasherScheduleFlag, "0 * * * * *", "Schedule")
	cmd.Flags().Bool("worker-async-block-hasher-disabled", false, "Disable the legacy block hasher (it chains nothing per log and is superseded by the log hasher for HASH_LOGS=ASYNC ledgers)")
	cmd.Flags().Duration("worker-log-hasher-interval", loghash.DefaultInterval, "Interval between two runs of the log hasher (HASH_LOGS=ASYNC ledgers)")
	cmd.Flags().Int("worker-log-hasher-batch-size", loghash.DefaultBatchSize, "Max logs chained per database transaction by the log hasher")
	cmd.Flags().Duration("worker-log-hasher-gap-grace", loghash.DefaultGapGrace, "Time a hole in the log ids must have been observed before the log hasher considers it final")
	cmd.Flags().Duration("worker-log-hasher-alert-lag", loghash.DefaultAlertLag, "Age of the oldest unchained log above which the log hasher logs an ALERT")
	cmd.Flags().Duration(WorkerPipelinesPullIntervalFlag, 5*time.Second, "Pipelines pull interval")
	cmd.Flags().Duration(WorkerPipelinesPushRetryPeriodFlag, 10*time.Second, "Pipelines push retry period")
	cmd.Flags().Duration(WorkerPipelinesSyncPeriod, time.Minute, "Pipelines sync period")
	cmd.Flags().Uint64(WorkerPipelinesLogsPageSize, 100, "Pipelines logs page size")
	cmd.Flags().Duration(WorkerBucketCleanupRetentionPeriodFlag, 30*24*time.Hour, "Retention period for deleted buckets before hard delete")
	cmd.Flags().String(WorkerBucketCleanupScheduleFlag, "0 0 * * * *", "Schedule for bucket cleanup (cron format)")
	cmd.Flags().String(WorkerCBAInterestAccrualScheduleFlag, "0 5 0 * * *", "Schedule for CBA interest accrual (cron format)")
	cmd.Flags().String(WorkerCBAInterestPostingScheduleFlag, "0 10 0 * * *", "Schedule for CBA interest posting (cron format)")
	cmd.Flags().String(WorkerCBAMaintenanceFeeScheduleFlag, "0 15 0 * * *", "Schedule for CBA maintenance fee processing (cron format)")
	cmd.Flags().String(WorkerCBADormancyScheduleFlag, "0 20 0 * * *", "Schedule for CBA dormancy detection (cron format)")
	cmd.Flags().String(WorkerCBATransferRecoveryScheduleFlag, "0 * * * * *", "Schedule for recovery of interrupted multi-ledger wallet transfers (cron format)")
	cmd.Flags().Int(WorkerCBATransferRecoveryBatchSizeFlag, 100, "Transfers claimed per recovery pass")
	cmd.Flags().Int(WorkerCBATransferRecoveryMaxAttemptsFlag, 10, "Drive attempts before a transfer is moved to failed_needs_review")
	cmd.Flags().Duration(WorkerCBATransferRecoveryGraceFlag, 2*time.Minute, "Minimum age of an unfinished transfer before recovery picks it up (also the base of the retry backoff)")
	cmd.Flags().Duration(WorkerCBATransferRecoveryLeaseFlag, 2*time.Minute, "How long a driver owns a transfer before another may take it over")
	cmd.Flags().Duration(WorkerCBATransferRecoveryMaxBackoffFlag, 30*time.Minute, "Upper bound of the exponential retry backoff of one transfer")
	cmd.Flags().String(WorkerCBALedgerNameFlag, "ledgertrack", "Ledger name used for CBA account wallet postings")
	cmd.Flags().String(WorkerCBAFeeIncomeAccountFlag, "revenue:fee_income", "Revenue account used for CBA fee income postings")
	cmd.Flags().String(WorkerCBAInterestExpenseAccountFlag, "revenue:interest_expense", "Revenue account used for CBA interest expense postings")
}

// NewWorkerCommand constructs the "worker" Cobra command which initializes and runs the worker service using loaded configuration and composed FX modules.
// The command registers worker-specific flags via addWorkerFlags and common service, bunconnect, and OTLP flags, and exposes the --worker-grpc-address flag (default ":8081").
// When executed it loads configuration and starts the service with the configured modules and a gRPC server.
func NewWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "worker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			cfg, err := LoadConfig[WorkerCommandConfiguration](cmd)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			if err := cfg.Validate(); err != nil {
				return err
			}

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlpModule(cmd, cfg.commonConfig),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{}),
				drivers.NewFXModule(),
				fx.Invoke(alldrivers.Register),
				systemcontroller.NewFXModule(systemcontroller.ModuleConfiguration{
					NumscriptInterpreter:      cfg.NumscriptInterpreter,
					NumscriptInterpreterFlags: cfg.NumscriptInterpreterFlags,
					NSCacheConfiguration: ledgercontroller.CacheConfiguration{
						MaxCount: 1024,
					},
					DatabaseRetryConfiguration: systemcontroller.DatabaseRetryConfiguration{
						MaxRetry: 10,
						Delay:    100 * time.Millisecond,
					},
					EnableFeatures:        cfg.ExperimentalFeaturesEnabled,
					SchemaEnforcementMode: cfg.commonConfig.SchemaEnforcementMode,
					WalletLedgerHashMode:  cfg.WalletLedgerHashMode,
				}),
				bus.NewFxModule(),
				currency.NewFXModule(),
				cba.NewFXModule(),
				transfers.NewFXModule(transferConfig(cfg.WorkerConfiguration)),
				newWorkerModule(cfg.WorkerConfiguration),
				// The system controller needs a ReplicationBackend. The worker hosts the
				// replication manager itself, so use it directly (same as `serve --worker`).
				replication.NewFXEmbeddedClientModule(),
				worker.NewGRPCServerFXModule(worker.GRPCServerModuleConfig{
					Address: cfg.Address,
					ServerOptions: []grpc.ServerOption{
						grpc.Creds(insecure.NewCredentials()),
					},
				}),
			).Run(cmd)
		},
	}

	cmd.Flags().String(WorkerGRPCAddressFlag, ":8081", "GRPC address")

	addWorkerFlags(cmd)
	addWalletLedgerHashModeFlag(cmd)
	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.SchemaSystem
	})

	return cmd
}

// newWorkerModule creates an fx.Option that configures the worker module using the provided WorkerConfiguration.
// It maps the configuration into AsyncBlockRunnerConfig, ReplicationConfig, and BucketCleanupRunnerConfig for the worker.
func newWorkerModule(configuration WorkerConfiguration) fx.Option {
	return worker.NewFXModule(worker.ModuleConfig{
		AsyncBlockRunnerConfig: storage.AsyncBlockRunnerConfig{
			MaxBlockSize: configuration.HashLogsBlockMaxSize,
			Schedule:     configuration.HashLogsBlockCRONSpec,
		},
		AsyncBlockRunnerDisabled: configuration.HashLogsBlockDisabled,
		LogHasherConfig: loghash.Config{
			Interval:  configuration.LogHasherInterval,
			BatchSize: configuration.LogHasherBatchSize,
			GapGrace:  configuration.LogHasherGapGrace,
			AlertLag:  configuration.LogHasherAlertLag,
		},
		ReplicationConfig: replication.WorkerModuleConfig{
			PushRetryPeriod: configuration.PushRetryPeriod,
			PullInterval:    configuration.PullInterval,
			SyncPeriod:      configuration.SyncPeriod,
			LogsPageSize:    configuration.LogsPageSize,
		},
		BucketCleanupRunnerConfig: storage.BucketCleanupRunnerConfig{
			RetentionPeriod: configuration.BucketCleanupRetentionPeriod,
			Schedule:        configuration.BucketCleanupCRONSpec,
		},
		CBASchedulerConfig: scheduler.ModuleConfig{
			LedgerPostingConfig: scheduler.LedgerPostingConfig{
				LedgerName:             configuration.CBALedgerName,
				FeeIncomeAccount:       configuration.CBAFeeIncomeAccount,
				InterestExpenseAccount: configuration.CBAInterestExpenseAccount,
			},
			InterestAccrualRunnerConfig: scheduler.InterestAccrualRunnerConfig{
				Schedule: configuration.CBAInterestAccrualCRONSpec,
			},
			InterestPostingRunnerConfig: scheduler.InterestPostingRunnerConfig{
				Schedule: configuration.CBAInterestPostingCRONSpec,
			},
			MaintenanceFeeRunnerConfig: scheduler.MaintenanceFeeRunnerConfig{
				Schedule: configuration.CBAMaintenanceFeeCRONSpec,
			},
			DormancyRunnerConfig: scheduler.DormancyRunnerConfig{
				Schedule: configuration.CBADormancyCRONSpec,
			},
			TransferRecoveryRunnerConfig: scheduler.TransferRecoveryRunnerConfig{
				Schedule:  configuration.CBATransferRecoveryCRONSpec,
				BatchSize: configuration.CBATransferRecoveryBatchSize,
			},
		},
	})
}

// transferConfig maps the worker flags to the transfers service configuration.
// The same values drive inline transfers in `serve` and recovery in `worker`.
func transferConfig(cfg WorkerConfiguration) transfers.Config {
	c := transfers.DefaultConfig()
	c.BatchSize = cfg.CBATransferRecoveryBatchSize
	c.MaxAttempts = cfg.CBATransferRecoveryMaxAttempts
	c.Grace = cfg.CBATransferRecoveryGrace
	c.Lease = cfg.CBATransferRecoveryLease
	c.MaxBackoff = cfg.CBATransferRecoveryMaxBackoff
	return c
}
