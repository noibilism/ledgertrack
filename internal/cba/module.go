package cba

import (
	"github.com/uptrace/bun"
	"go.uber.org/fx"

	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(
			func(db *bun.DB) repositories.ProductRepository {
				return repositories.NewProductRepository(db)
			},
			func(db *bun.DB) repositories.ClientRepository {
				return repositories.NewClientRepository(db)
			},
			func(db *bun.DB) repositories.AccountRepository {
				return repositories.NewAccountRepository(db)
			},
			func(db *bun.DB) repositories.KYCRepository {
				return repositories.NewKYCRepository(db)
			},
			func(db *bun.DB) repositories.DailyUsageRepository {
				return repositories.NewDailyUsageRepository(db)
			},
			func(db *bun.DB) repositories.InterestAccrualRepository {
				return repositories.NewInterestAccrualRepository(db)
			},
			func(db *bun.DB) repositories.FeePostingRepository {
				return repositories.NewFeePostingRepository(db)
			},
			func(productRepository repositories.ProductRepository) services.ProductService {
				return services.NewProductService(productRepository)
			},
			func(
				clientRepository repositories.ClientRepository,
				accountRepository repositories.AccountRepository,
			) services.ClientService {
				return services.NewClientService(clientRepository, accountRepository)
			},
			func(
				accountRepository repositories.AccountRepository,
				clientRepository repositories.ClientRepository,
				productRepository repositories.ProductRepository,
				dailyUsageRepository repositories.DailyUsageRepository,
			) services.AccountService {
				return services.NewAccountService(accountRepository, clientRepository, productRepository, dailyUsageRepository)
			},
			func(
				clientRepository repositories.ClientRepository,
				kycRepository repositories.KYCRepository,
			) services.KYCService {
				return services.NewKYCService(clientRepository, kycRepository)
			},
			func(
				accountRepository repositories.AccountRepository,
				productRepository repositories.ProductRepository,
				interestRepository repositories.InterestAccrualRepository,
			) services.InterestService {
				return services.NewInterestService(accountRepository, productRepository, interestRepository)
			},
			func(
				accountRepository repositories.AccountRepository,
				productRepository repositories.ProductRepository,
				feeRepository repositories.FeePostingRepository,
			) services.FeeService {
				return services.NewFeeService(accountRepository, productRepository, feeRepository)
			},
			func(
				clientRepository repositories.ClientRepository,
				accountRepository repositories.AccountRepository,
				interestRepository repositories.InterestAccrualRepository,
				feeRepository repositories.FeePostingRepository,
			) services.ReportingService {
				return services.NewReportingService(clientRepository, accountRepository, interestRepository, feeRepository)
			},
			func() services.FinanceReportingService {
				return services.NewFinanceReportingService()
			},
			func(db *bun.DB) repositories.GLAccountRepository {
				return repositories.NewGLAccountRepository(db)
			},
			func(glAccountRepository repositories.GLAccountRepository) services.GLAccountService {
				return services.NewGLAccountService(glAccountRepository)
			},
			func(db *bun.DB) repositories.FeeConfigRepository {
				return repositories.NewFeeConfigRepository(db)
			},
			func(
				feeConfigRepository repositories.FeeConfigRepository,
				accountRepository repositories.AccountRepository,
				productRepository repositories.ProductRepository,
				clientRepository repositories.ClientRepository,
				glAccountRepository repositories.GLAccountRepository,
			) services.FeeConfigService {
				return services.NewFeeConfigService(feeConfigRepository, accountRepository, productRepository, clientRepository, glAccountRepository)
			},
			func(db *bun.DB) repositories.ReserveRepository {
				return repositories.NewReserveRepository(db)
			},
			func(
				reserveRepository repositories.ReserveRepository,
				accountRepository repositories.AccountRepository,
				productRepository repositories.ProductRepository,
				clientRepository repositories.ClientRepository,
			) services.ReserveRuleService {
				return services.NewReserveRuleService(reserveRepository, accountRepository, productRepository, clientRepository)
			},
			func(glAccountRepository repositories.GLAccountRepository) services.GLReportingService {
				return services.NewGLReportingService(glAccountRepository)
			},
		),
	)
}
