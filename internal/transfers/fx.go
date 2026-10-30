package transfers

import (
	"github.com/uptrace/bun"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

// NewService wires the production store, ledger access and fee recorder.
func NewProductionService(db *bun.DB, system systemcontroller.Controller, logger logging.Logger, cfg Config) *Service {
	return NewService(
		NewBunStore(db),
		NewLedgerExecutor(NewSystemLedgers(system), NewBunFeeRecorder(db)),
		cfg,
		logger,
	)
}

// NewFXModule provides *Service. It is included by both `serve` (inline
// driving) and `worker` (recovery); when the worker is embedded in `serve` the
// module is included once.
func NewFXModule(cfg Config) fx.Option {
	return fx.Provide(func(db *bun.DB, system systemcontroller.Controller, logger logging.Logger) *Service {
		return NewProductionService(db, system, logger, cfg)
	})
}
