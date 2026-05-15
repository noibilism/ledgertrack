package api

import (
	_ "embed"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/health"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/services"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
	"github.com/formancehq/ledger/internal/controller/system"
)

type BulkConfig struct {
	MaxSize  int
	Parallel int
}

type Config struct {
	Version    string
	Debug      bool
	Bulk       BulkConfig
	Pagination common.PaginationConfig
	Exporters  bool
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(
			backend system.Controller,
			authenticator auth.Authenticator,
			tracerProvider trace.TracerProvider,
			productService services.ProductService,
			clientService services.ClientService,
			kycService services.KYCService,
			accountService services.AccountService,
			reportingService services.ReportingService,
			financeReportingService services.FinanceReportingService,
			channelFeeConfigService channelservices.ChannelFeeConfigService,
			channelRevenueReportingService channelservices.ChannelRevenueReportingService,
		) chi.Router {
			return NewRouter(
				backend,
				authenticator,
				cfg.Version,
				cfg.Debug,
				WithTracer(tracerProvider.Tracer("api")),
				WithBulkMaxSize(cfg.Bulk.MaxSize),
				WithBulkerFactory(bulking.NewDefaultBulkerFactory(
					bulking.WithParallelism(cfg.Bulk.Parallel),
					bulking.WithTracer(tracerProvider.Tracer("api.bulking")),
				)),
				WithPaginationConfiguration(cfg.Pagination),
				WithExporters(cfg.Exporters),
				WithProductService(productService),
				WithClientService(clientService),
				WithKYCService(kycService),
				WithAccountService(accountService),
				WithReportingService(reportingService),
				WithFinanceReportingService(financeReportingService),
				WithChannelFeeConfigService(channelFeeConfigService),
				WithChannelRevenueReportingService(channelRevenueReportingService),
			)
		}),
		health.Module(),
	)
}
