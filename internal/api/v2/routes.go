package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	"github.com/formancehq/ledger/internal/cba/services"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

// NewRouter creates a chi.Router configured with the v2 HTTP API routes for the ledger service.
// It registers authentication-protected top-level endpoints (including /_info), an "/_" group
// that may expose exporter management and bucket operations, ledger-scoped routes (ledger creation,
// metadata, and nested ledger subroutes such as bulk operations, info, stats, pipelines when
// enabled, logs, accounts, transactions, aggregated balances, and volumes), and applies tracing
// attributes for the selected ledger on ledger-scoped requests.
// The behavior of tracing, bulking, bulk handler factories, pagination, and whether exporter-related
// endpoints are mounted is controlled via RouterOption arguments.
func NewRouter(
	systemController systemcontroller.Controller,
	authenticator auth.Authenticator,
	version string,
	opts ...RouterOption,
) chi.Router {
	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	router := chi.NewMux()

	router.Group(func(router chi.Router) {
		router.Use(auth.Middleware(authenticator))

		router.Get("/_info", v1.GetInfo(systemController, version))

		router.Route("/_", func(router chi.Router) {
			if routerOptions.exporters {
				router.Route("/exporters", func(router chi.Router) {
					router.Get("/", listExporters(systemController))
					router.Get("/{exporterID}", getExporter(systemController))
					router.Put("/{exporterID}", updateExporter(systemController))
					router.Delete("/{exporterID}", deleteExporter(systemController))
					router.Post("/", createExporter(systemController))
				})
			}
			router.Route("/buckets", func(router chi.Router) {
				router.Delete("/{bucket}", deleteBucket(systemController))
				router.Post("/{bucket}/restore", restoreBucket(systemController))
			})
		})
		router.Get("/", listLedgers(systemController, routerOptions.paginationConfig))
		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					trace.
						SpanFromContext(r.Context()).
						SetAttributes(attribute.String("ledger", chi.URLParam(r, "ledger")))
					handler.ServeHTTP(w, r)
				})
			})
			router.Post("/", createLedger(systemController))
			router.Get("/", readLedger(systemController))
			router.Put("/metadata", updateLedgerMetadata(systemController))
			router.Delete("/metadata/{key}", deleteLedgerMetadata(systemController))

			router.With(common.LedgerMiddleware(systemController, func(r *http.Request) string {
				return chi.URLParam(r, "ledger")
			}, routerOptions.tracer, "/_info")).Group(func(router chi.Router) {
				router.Post("/_bulk", bulkHandler(
					routerOptions.bulkerFactory,
					routerOptions.bulkHandlerFactories,
				))
				router.Get("/_info", getLedgerInfo)
				router.Get("/stats", readStats)
				router.Post("/schema/{version}", insertSchema)
				router.Get("/schema/{version}", readSchema)
				router.Get("/schema", listSchemas(routerOptions.paginationConfig))

				if routerOptions.exporters {
					router.Route("/pipelines", func(router chi.Router) {
						router.Get("/", listPipelines(systemController))
						router.Post("/", createPipeline(systemController))
						router.Route("/{pipelineID}", func(router chi.Router) {
							router.Get("/", readPipeline(systemController))
							router.Delete("/", deletePipeline(systemController))
							router.Post("/start", startPipeline(systemController))
							router.Post("/stop", stopPipeline(systemController))
							router.Post("/reset", resetPipeline(systemController))
						})
					})
				}

				router.Route("/logs", func(router chi.Router) {
					router.Get("/", listLogs(routerOptions.paginationConfig))
					router.Post("/import", importLogs)
					router.Post("/export", exportLogs)
				})

				router.Route("/accounts", func(router chi.Router) {
					if routerOptions.accountService != nil {
						router.Get("/", ledgerAwareAccountList(routerOptions))
					} else {
						router.Get("/", listAccounts(routerOptions.paginationConfig))
					}
					router.Head("/", countAccounts)
					if routerOptions.accountService != nil {
						router.Post("/", ledgertrackOnly(openAccount(routerOptions.accountService)))
					}
					router.Route("/{address}", func(router chi.Router) {
						if routerOptions.accountService != nil {
							router.Get("/", ledgerAwareAccountRead(routerOptions.accountService))
							router.Get("/balance", ledgertrackOnly(getAccountBalance(routerOptions.accountService)))
							router.Get("/history", ledgertrackOnly(getAccountHistory(routerOptions.accountService)))
							router.Get("/statement", ledgertrackOnly(getAccountStatement(routerOptions.accountService)))
							router.Post("/credit", ledgertrackOnly(creditAccount(routerOptions.accountService)))
							router.Post("/debit", ledgertrackOnly(debitAccount(routerOptions.accountService, systemController)))
							router.Post("/lien", ledgertrackOnly(lienAccount(routerOptions.accountService)))
							router.Post("/lien/release", ledgertrackOnly(releaseAccountLien(routerOptions.accountService, systemController)))
							router.Post("/activate", ledgertrackOnly(activateAccount(routerOptions.accountService)))
							router.Post("/suspend", ledgertrackOnly(suspendAccount(routerOptions.accountService)))
							router.Post("/freeze", ledgertrackOnly(freezeAccount(routerOptions.accountService)))
							router.Post("/dormant", ledgertrackOnly(dormantAccount(routerOptions.accountService)))
							router.Post("/reactivate", ledgertrackOnly(reactivateAccount(routerOptions.accountService)))
							router.Post("/close", ledgertrackOnly(closeAccount(routerOptions.accountService)))
						} else {
							router.Get("/", readAccount)
						}
						router.Post("/metadata", addAccountMetadata)
						router.Delete("/metadata/{key}", deleteAccountMetadata)
					})
				})

				router.Route("/transactions", func(router chi.Router) {
					router.Get("/", listTransactions(routerOptions.paginationConfig))
					router.Head("/", countTransactions)
					router.Post("/", createTransaction)
					router.Get("/{id}", readTransaction)
					router.Post("/{id}/revert", revertTransaction)
					router.Post("/{id}/metadata", addTransactionMetadata)
					router.Delete("/{id}/metadata/{key}", deleteTransactionMetadata)
				})

				router.Get("/aggregate/balances", readBalancesAggregated)

				router.Get("/volumes", readVolumes(routerOptions.paginationConfig))
				router.Get("/currencies", listCurrencies())

				router.Route("/wallets", func(router chi.Router) {
					router.Post("/", createWallet(systemController))
					router.Get("/balances", getWalletBalances(systemController))
					router.Route("/{walletID}", func(router chi.Router) {
						router.Post("/credit", creditWallet(systemController))
						router.Post("/debit", debitWallet(systemController, routerOptions.channelFeeConfigService))
						router.Post("/lien", lienWallet(systemController))
						router.Post("/lien/release", releaseLien(systemController, routerOptions.channelFeeConfigService))
						router.Get("/statement", getWalletStatement(systemController))
						router.Get("/history", getWalletHistory(systemController))
					})
				})

				router.Route("/channels", func(router chi.Router) {
					router.Post("/", createChannel(systemController))
					router.Route("/{channelID}", func(router chi.Router) {
						router.Post("/credit", creditChannel(systemController))
						router.Get("/", readChannel(systemController))
						router.Get("/history", getChannelHistory(systemController, routerOptions.paginationConfig))
						if routerOptions.channelFeeConfigService != nil {
							router.Route("/fees", func(router chi.Router) {
								router.Get("/config", getChannelFeeConfig(routerOptions.channelFeeConfigService))
								router.Put("/config", upsertChannelFeeConfig(routerOptions.channelFeeConfigService))
								router.Get("/audits", listChannelFeeConfigAudits(routerOptions.channelFeeConfigService))
							})
						}
					})
					if routerOptions.channelFeeConfigService != nil {
						router.Get("/fees/configs", listChannelFeeConfigs(routerOptions.channelFeeConfigService))
					}
				})

				if routerOptions.productService != nil {
					router.Route("/products", func(router chi.Router) {
						router.Post("/", createProduct(routerOptions.productService))
						router.Get("/", listProducts(routerOptions.productService))
						router.Route("/{productID}", func(router chi.Router) {
							router.Get("/", readProduct(routerOptions.productService))
							router.Patch("/", patchProduct(routerOptions.productService))
							router.Post("/activate", activateProduct(routerOptions.productService))
							router.Post("/retire", retireProduct(routerOptions.productService))
						})
					})
				}

				if routerOptions.clientService != nil && routerOptions.kycService != nil {
					router.Route("/clients", func(router chi.Router) {
						router.Post("/", createClient(routerOptions.clientService))
						router.Get("/", listClients(routerOptions.clientService))
						router.Route("/{clientID}", func(router chi.Router) {
							router.Get("/", readClient(routerOptions.clientService))
							if routerOptions.accountService != nil {
								router.Get("/accounts", listClientAccounts(routerOptions.clientService, routerOptions.accountService))
							}
							router.Patch("/", patchClient(routerOptions.clientService))
							router.Post("/activate", activateClient(routerOptions.clientService))
							router.Post("/suspend", suspendClient(routerOptions.clientService))
							router.Post("/reactivate", reactivateClient(routerOptions.clientService))
							router.Post("/close", closeClient(routerOptions.clientService))
							router.Route("/kyc", func(router chi.Router) {
								router.Post("/", submitKYC(routerOptions.kycService))
								router.Get("/", listClientKYC(routerOptions.kycService))
								router.Route("/{kycID}", func(router chi.Router) {
									router.Post("/verify", verifyKYC(routerOptions.kycService))
									router.Post("/reject", rejectKYC(routerOptions.kycService))
								})
							})
						})
					})
				}

				if routerOptions.reportingService != nil || routerOptions.financeReportingService != nil || routerOptions.channelRevenueReportingService != nil {
					router.Route("/reports", func(router chi.Router) {
						if routerOptions.reportingService != nil {
							router.Get("/clients/{clientID}/portfolio", ledgertrackOnly(getClientPortfolioReport(routerOptions.reportingService)))
							router.Get("/accounts/{address}/statement", ledgertrackOnly(getAccountStatementReport(routerOptions.reportingService)))
							router.Get("/transactions/daily", ledgertrackOnly(getDailyTransactionSummaryReport(routerOptions.reportingService)))
							router.Get("/interest-fees", ledgertrackOnly(getInterestFeeReport(routerOptions.reportingService)))
						}
						if routerOptions.financeReportingService != nil {
							router.Route("/finance", func(router chi.Router) {
								router.Get("/trial-balance", ledgertrackOnly(getFinanceTrialBalanceReport(systemController, routerOptions.financeReportingService)))
								router.Get("/balance-sheet", ledgertrackOnly(getFinanceBalanceSheetReport(systemController, routerOptions.financeReportingService)))
								router.Get("/pnl", ledgertrackOnly(getFinanceProfitAndLossReport(systemController, routerOptions.financeReportingService)))
								router.Get("/cash-flow", ledgertrackOnly(getFinanceCashFlowReport(systemController, routerOptions.financeReportingService)))
							})
						}
						if routerOptions.channelRevenueReportingService != nil {
							router.Route("/channels", func(router chi.Router) {
								router.Post("/revenue", ledgertrackOnly(getChannelRevenueSummaryReport(routerOptions.channelRevenueReportingService)))
								router.Post("/revenue/timeseries", ledgertrackOnly(getChannelRevenueTimeseriesReport(routerOptions.channelRevenueReportingService)))
								router.Post("/revenue/export", ledgertrackOnly(exportChannelRevenueReport(routerOptions.channelRevenueReportingService)))
								router.Post("/dashboard", ledgertrackOnly(getChannelRevenueDashboardMetrics(routerOptions.channelRevenueReportingService)))
							})
						}
					})
				}

			})
		})
	})

	return router
}

type routerOptions struct {
	tracer                         trace.Tracer
	bulkerFactory                  bulking.BulkerFactory
	bulkHandlerFactories           map[string]bulking.HandlerFactory
	paginationConfig               common.PaginationConfig
	exporters                      bool
	productService                 services.ProductService
	clientService                  services.ClientService
	kycService                     services.KYCService
	accountService                 services.AccountService
	reportingService               services.ReportingService
	financeReportingService        services.FinanceReportingService
	channelFeeConfigService        channelservices.ChannelFeeConfigService
	channelRevenueReportingService channelservices.ChannelRevenueReportingService
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithBulkHandlerFactories(bulkHandlerFactories map[string]bulking.HandlerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkHandlerFactories = bulkHandlerFactories
	}
}

func WithBulkerFactory(bulkerFactory bulking.BulkerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkerFactory = bulkerFactory
	}
}

func WithPaginationConfig(paginationConfig common.PaginationConfig) RouterOption {
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

func WithChannelFeeConfigService(channelFeeConfigService channelservices.ChannelFeeConfigService) RouterOption {
	return func(ro *routerOptions) {
		ro.channelFeeConfigService = channelFeeConfigService
	}
}

func WithChannelRevenueReportingService(channelRevenueReportingService channelservices.ChannelRevenueReportingService) RouterOption {
	return func(ro *routerOptions) {
		ro.channelRevenueReportingService = channelRevenueReportingService
	}
}

func WithDefaultBulkHandlerFactories(bulkMaxSize int) RouterOption {
	return WithBulkHandlerFactories(map[string]bulking.HandlerFactory{
		"application/json": bulking.NewJSONBulkHandlerFactory(bulkMaxSize),
		"application/vnd.formance.ledger.api.v2.bulk+script-stream": bulking.NewTextStreamBulkHandlerFactory(),
		"application/vnd.formance.ledger.api.v2.bulk+json-stream":   bulking.NewJSONStreamBulkHandlerFactory(),
	})
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithBulkerFactory(bulking.NewDefaultBulkerFactory()),
	WithDefaultBulkHandlerFactories(100),
	WithPaginationConfig(common.PaginationConfig{
		DefaultPageSize: bunpaginate.QueryDefaultPageSize,
		MaxPageSize:     bunpaginate.MaxPageSize,
	}),
}
