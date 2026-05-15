package channels

import (
	"github.com/uptrace/bun"
	"go.uber.org/fx"

	"github.com/formancehq/ledger/internal/channels/repositories"
	"github.com/formancehq/ledger/internal/channels/services"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(
			func(db *bun.DB) repositories.ChannelFeeConfigRepository {
				return repositories.NewChannelFeeConfigRepository(db)
			},
			func(db *bun.DB) repositories.ChannelFeeConfigAuditRepository {
				return repositories.NewChannelFeeConfigAuditRepository(db)
			},
			func(db *bun.DB) repositories.ChannelFeeRecordRepository {
				return repositories.NewChannelFeeRecordRepository(db)
			},
			func(
				configRepo repositories.ChannelFeeConfigRepository,
				auditRepo repositories.ChannelFeeConfigAuditRepository,
				recordRepo repositories.ChannelFeeRecordRepository,
			) services.ChannelFeeConfigService {
				return services.NewChannelFeeConfigService(configRepo, auditRepo, recordRepo)
			},
			func(
				db *bun.DB,
				recordRepo repositories.ChannelFeeRecordRepository,
			) services.ChannelRevenueReportingService {
				return services.NewChannelRevenueReportingService(db, recordRepo)
			},
		),
	)
}

