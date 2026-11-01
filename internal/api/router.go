package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	otelchimetric "github.com/riandyrn/otelchi/metric"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/service"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/cba/services"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/transfers"
)

// todo: refine textual errors
func NewRouter(
	systemController system.Controller,
	authenticator auth.Authenticator,
	version string,
	debug bool,
	opts ...RouterOption,
) chi.Router {

	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	baseCfg := otelchimetric.NewBaseConfig(
		"ledger",
		otelchimetric.WithMeterProvider(routerOptions.meterProvider),
	)

	mux := chi.NewRouter()
	mux.Use(
		cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
			AllowedHeaders:   []string{"*"},
			ExposedHeaders:   []string{"Count"},
		}).Handler,
		common.LogID(),
		middleware.RequestLogger(api.NewLogFormatter()),
		service.OTLPMiddleware("ledger", debug),
		otelchimetric.NewRequestDurationMillis(baseCfg),
		otelchimetric.NewRequestInFlight(baseCfg),
		otelchimetric.NewResponseSizeBytes(baseCfg),
		func(next http.Handler) http.Handler {
			fn := func(w http.ResponseWriter, r *http.Request) {
				defer func() {
					if rvr := recover(); rvr != nil {
						if rvr == http.ErrAbortHandler {
							// we don't recover http.ErrAbortHandler so the response
							// to the client is aborted, this should not be logged
							panic(rvr)
						}

						if debug {
							middleware.PrintPrettyStack(rvr)
						}

						otlp.RecordError(r.Context(), fmt.Errorf("%s", rvr))

						w.WriteHeader(http.StatusInternalServerError)
					}
				}()

				next.ServeHTTP(w, r)
			}

			return http.HandlerFunc(fn)
		},
	)

	v2Router := v2.NewRouter(
		systemController,
		authenticator,
		version,
		v2.WithTracer(routerOptions.tracer),
		v2.WithBulkerFactory(routerOptions.bulkerFactory),
		v2.WithDefaultBulkHandlerFactories(routerOptions.bulkMaxSize),
		v2.WithPaginationConfig(routerOptions.paginationConfig),
		v2.WithExporters(routerOptions.exporters),
		v2.WithProductService(routerOptions.productService),
		v2.WithClientService(routerOptions.clientService),
		v2.WithKYCService(routerOptions.kycService),
		v2.WithAccountService(routerOptions.accountService),
		v2.WithReportingService(routerOptions.reportingService),
		v2.WithFinanceReportingService(routerOptions.financeReportingService),
		v2.WithGLAccountService(routerOptions.glAccountService),
		v2.WithFeeConfigService(routerOptions.feeConfigService),
		v2.WithReserveRuleService(routerOptions.reserveRuleService),
		v2.WithBusinessCalendarService(routerOptions.calendarService),
		v2.WithReserveHoldService(routerOptions.reserveHoldService),
		v2.WithGLReportingService(routerOptions.glReportingService),
		v2.WithChannelFeeConfigService(routerOptions.channelFeeConfigService),
		v2.WithChannelRevenueReportingService(routerOptions.channelRevenueReportingService),
		v2.WithTransferService(routerOptions.transferService),
	)
	// Write requests are admitted (or rejected with 503 + Retry-After) before they touch the database.
	admitWrites := common.WriteAdmission(routerOptions.writeLimiter)
	mux.Handle("/v2*", admitWrites(http.StripPrefix("/v2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chi.RouteContext(r.Context()).Reset()
		v2Router.ServeHTTP(w, r)
	}))))
	mux.Handle("/*", admitWrites(v1.NewRouter(
		systemController,
		authenticator,
		version,
		debug,
		v1.WithTracer(routerOptions.tracer),
	)))

	return mux
}

type routerOptions struct {
	tracer                         trace.Tracer
	meterProvider                  metric.MeterProvider
	bulkMaxSize                    int
	bulkerFactory                  bulking.BulkerFactory
	paginationConfig               common.PaginationConfig
	exporters                      bool
	productService                 services.ProductService
	clientService                  services.ClientService
	kycService                     services.KYCService
	accountService                 services.AccountService
	reportingService               services.ReportingService
	financeReportingService        services.FinanceReportingService
	glAccountService               services.GLAccountService
	feeConfigService               services.FeeConfigService
	reserveRuleService             services.ReserveRuleService
	calendarService                services.BusinessCalendarService
	reserveHoldService             services.ReserveHoldService
	glReportingService             services.GLReportingService
	channelFeeConfigService        channelservices.ChannelFeeConfigService
	channelRevenueReportingService channelservices.ChannelRevenueReportingService
	transferService                *transfers.Service
	writeLimiter                   *ledgercontroller.WriteLimiter
}

type RouterOption func(ro *routerOptions)

// WithWriteLimiter bounds the write requests in flight (nil: no bound).
func WithWriteLimiter(limiter *ledgercontroller.WriteLimiter) RouterOption {
	return func(ro *routerOptions) {
		ro.writeLimiter = limiter
	}
}

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithBulkMaxSize(bulkMaxSize int) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkMaxSize = bulkMaxSize
	}
}

func WithBulkerFactory(bf bulking.BulkerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkerFactory = bf
	}
}

func WithPaginationConfiguration(paginationConfig common.PaginationConfig) RouterOption {
	return func(ro *routerOptions) {
		ro.paginationConfig = paginationConfig
	}
}

func WithExporters(v bool) RouterOption {
	return func(ro *routerOptions) {
		ro.exporters = v
	}
}

func WithProductService(productService services.ProductService) RouterOption {
	return func(ro *routerOptions) {
		ro.productService = productService
	}
}

func WithClientService(clientService services.ClientService) RouterOption {
	return func(ro *routerOptions) {
		ro.clientService = clientService
	}
}

func WithKYCService(kycService services.KYCService) RouterOption {
	return func(ro *routerOptions) {
		ro.kycService = kycService
	}
}

func WithAccountService(accountService services.AccountService) RouterOption {
	return func(ro *routerOptions) {
		ro.accountService = accountService
	}
}

func WithReportingService(reportingService services.ReportingService) RouterOption {
	return func(ro *routerOptions) {
		ro.reportingService = reportingService
	}
}

func WithFinanceReportingService(financeReportingService services.FinanceReportingService) RouterOption {
	return func(ro *routerOptions) {
		ro.financeReportingService = financeReportingService
	}
}

func WithReserveHoldService(reserveHoldService services.ReserveHoldService) RouterOption {
	return func(ro *routerOptions) {
		ro.reserveHoldService = reserveHoldService
	}
}

func WithBusinessCalendarService(calendarService services.BusinessCalendarService) RouterOption {
	return func(ro *routerOptions) {
		ro.calendarService = calendarService
	}
}

func WithReserveRuleService(reserveRuleService services.ReserveRuleService) RouterOption {
	return func(ro *routerOptions) {
		ro.reserveRuleService = reserveRuleService
	}
}

func WithFeeConfigService(feeConfigService services.FeeConfigService) RouterOption {
	return func(ro *routerOptions) {
		ro.feeConfigService = feeConfigService
	}
}

func WithGLAccountService(glAccountService services.GLAccountService) RouterOption {
	return func(ro *routerOptions) {
		ro.glAccountService = glAccountService
	}
}

func WithGLReportingService(glReportingService services.GLReportingService) RouterOption {
	return func(ro *routerOptions) {
		ro.glReportingService = glReportingService
	}
}

func WithChannelFeeConfigService(channelFeeConfigService channelservices.ChannelFeeConfigService) RouterOption {
	return func(ro *routerOptions) {
		ro.channelFeeConfigService = channelFeeConfigService
	}
}

func WithTransferService(transferService *transfers.Service) RouterOption {
	return func(ro *routerOptions) {
		ro.transferService = transferService
	}
}

func WithChannelRevenueReportingService(channelRevenueReportingService channelservices.ChannelRevenueReportingService) RouterOption {
	return func(ro *routerOptions) {
		ro.channelRevenueReportingService = channelRevenueReportingService
	}
}

func WithMeterProvider(mp metric.MeterProvider) RouterOption {
	return func(ro *routerOptions) {
		ro.meterProvider = mp
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithMeterProvider(noopmetrics.MeterProvider{}),
	WithBulkMaxSize(DefaultBulkMaxSize),
	WithPaginationConfiguration(common.PaginationConfig{
		MaxPageSize:     bunpaginate.MaxPageSize,
		DefaultPageSize: bunpaginate.QueryDefaultPageSize,
	}),
}

const DefaultBulkMaxSize = 100
