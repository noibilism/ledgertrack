package system

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/driver"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

func newModuleController(t *testing.T, configuration ModuleConfiguration) *DefaultController {
	t.Helper()

	var ctrl *DefaultController
	app := fxtest.New(t,
		fx.NopLogger,
		NewFXModule(configuration),
		fx.Provide(
			func() *driver.Driver { return nil },
			func() *systemstore.DefaultStore { return nil },
			func() ledgercontroller.Listener { return nil },
			func() ReplicationBackend { return nil },
			func() metric.MeterProvider { return metricnoop.NewMeterProvider() },
			func() trace.TracerProvider { return tracenoop.NewTracerProvider() },
		),
		fx.Populate(&ctrl),
	)
	app.RequireStart().RequireStop()
	require.NotNil(t, ctrl)
	return ctrl
}

// The limiter built by cmd/serve must reach the ledger controllers. If
// NewFXModule stops passing WithWriteLimiter, writes through the system
// controller (transfers, bulk, non HTTP callers) are no longer bounded.
func TestModuleWiresWriteLimiter(t *testing.T) {
	limiter := ledgercontroller.NewWriteLimiter(ledgercontroller.WriteLimiterConfiguration{MaxInFlight: 3})
	require.NotNil(t, limiter)

	ctrl := newModuleController(t, ModuleConfiguration{WriteLimiter: limiter})
	require.Same(t, limiter, ctrl.writeLimiter, "ModuleConfiguration.WriteLimiter is not passed to the controller")
}

// The worker intentionally runs without a limiter.
func TestModuleWithoutWriteLimiter(t *testing.T) {
	ctrl := newModuleController(t, ModuleConfiguration{})
	require.Nil(t, ctrl.writeLimiter)
}
