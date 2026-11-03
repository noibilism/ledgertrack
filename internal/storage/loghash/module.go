package loghash

import (
	"context"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"
)

// NewFXModule runs the Hasher in the background. It does nothing until a ledger is configured with HASH_LOGS=ASYNC.
func NewFXModule(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger, db *bun.DB, tp trace.TracerProvider, mp metric.MeterProvider) *Hasher {
			return NewHasher(logger, db, cfg,
				WithTracer(tp.Tracer("loghash")),
				WithMeterProvider(mp),
			)
		}),
		fx.Invoke(func(lc fx.Lifecycle, h *Hasher) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := h.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()
					return nil
				},
				OnStop: h.Stop,
			})
		}),
	)
}
