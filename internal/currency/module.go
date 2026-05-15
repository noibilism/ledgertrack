package currency

import (
	"context"

	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

type Loader struct {
	db *bun.DB
}

func NewLoader(db *bun.DB) *Loader {
	return &Loader{db: db}
}

func (l *Loader) Load(ctx context.Context) error {
	return Load(ctx, l.db)
}

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(NewLoader),
		fx.Invoke(func(lc fx.Lifecycle, loader *Loader) {
			lc.Append(fx.Hook{
				OnStart: loader.Load,
			})
		}),
	)
}
