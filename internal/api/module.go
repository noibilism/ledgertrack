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
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/transfers"
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
	// WriteLimiter bounds the write requests in flight, shared with the ledger controllers (nil: no bound).
	WriteLimiter *ledgercontroller.WriteLimiter
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
			glAccountService services.GLAccountService,
			feeConfigService services.FeeConfigService,
			glReportingService services.GLReportingService,
			channelFeeConfigService channelservices.ChannelFeeConfigService,
			channelRevenueReportingService channelservices.ChannelRevenueReportingService,
			transferService *transfers.Service,
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
				WithWriteLimiter(cfg.WriteLimiter),
				WithProductService(productService),
				WithClientService(clientService),
				WithKYCService(kycService),
				WithAccountService(accountService),
				WithReportingService(reportingService),
				WithFinanceReportingService(financeReportingService),
				WithGLAccountService(glAccountService),
				WithFeeConfigService(feeConfigService),
				WithGLReportingService(glReportingService),
				WithChannelFeeConfigService(channelFeeConfigService),
				WithChannelRevenueReportingService(channelRevenueReportingService),
				WithTransferService(transferService),
			)
		}),
		health.Module(),
	)
}
