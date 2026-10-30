package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/query"
	libtime "github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

var ErrGLReportingValidation = errors.New("gl reporting validation failed")

// GLReportingService reads GL registry accounts and their ledger balances.
//
// Sign convention: a ledger balance is input - output, where the destination of a posting receives the
// input. A GL account therefore carries a debit balance when its ledger balance is positive and a credit
// balance when it is negative (money moved *into* an account debits it, moved *out of* it credits it).
// Summed over every account of a currency the ledger balances are zero, so total debits == total credits
// is exactly the "no unmapped or one-sided movement" check. normal_side only decides which sign is
// "natural" for the account (natural_balance) and flags the opposite as abnormal.
type GLReportingService interface {
	AccountBalance(ctx context.Context, sys systemcontroller.Controller, code string, pit *libtime.Time) (*GLAccountBalance, error)
	TrialBalance(ctx context.Context, sys systemcontroller.Controller, filter GLTrialBalanceFilter) (*GLTrialBalanceReport, error)
}

type GLTrialBalanceFilter struct {
	Currency string
	AsOf     *libtime.Time
}

type GLAccountBalance struct {
	Code       string        `json:"code"`
	Name       string        `json:"name"`
	Type       string        `json:"type"`
	NormalSide string        `json:"normal_side"`
	Currency   string        `json:"currency"`
	ParentCode *string       `json:"parent_code"`
	Ledger     string        `json:"ledger"`
	Address    string        `json:"address"`
	Status     string        `json:"status"`
	Asset      string        `json:"asset"`
	PIT        *stdtime.Time `json:"pit,omitempty"`
	// Balance is the raw ledger balance (input - output) in minor units.
	Balance int64 `json:"balance"`
	Debit   int64 `json:"debit"`
	Credit  int64 `json:"credit"`
	// NaturalBalance is positive when the balance sits on the account's normal side.
	NaturalBalance int64 `json:"natural_balance"`
	Abnormal       bool  `json:"abnormal"`
}

type GLTrialBalanceTypeSubtotal struct {
	Type     string `json:"type"`
	Accounts int    `json:"accounts"`
	Debit    int64  `json:"debit"`
	Credit   int64  `json:"credit"`
	// Net is debit - credit.
	Net int64 `json:"net"`
}

type GLTrialBalanceReport struct {
	Currency     string                       `json:"currency"`
	Asset        string                       `json:"asset"`
	AsOf         *stdtime.Time                `json:"as_of,omitempty"`
	Accounts     []GLAccountBalance           `json:"accounts"`
	Subtotals    []GLTrialBalanceTypeSubtotal `json:"subtotals"`
	TotalDebits  int64                        `json:"total_debits"`
	TotalCredits int64                        `json:"total_credits"`
	// Difference is total_debits - total_credits.
	Difference int64 `json:"difference"`
	Balanced   bool  `json:"balanced"`
}

// glBalanceReader returns the ledger balance (input - output) of one address for one asset.
type glBalanceReader func(ctx context.Context, ledger, address, asset string, pit *libtime.Time) (int64, error)

type DefaultGLReportingService struct {
	repository repositories.GLAccountRepository
	// readerFor builds the balance reader for a request; replaced in unit tests.
	readerFor func(systemcontroller.Controller) glBalanceReader
}

func NewGLReportingService(repository repositories.GLAccountRepository) GLReportingService {
	return &DefaultGLReportingService{
		repository: repository,
		readerFor:  newSystemGLBalanceReader,
	}
}

func (s *DefaultGLReportingService) AccountBalance(ctx context.Context, sys systemcontroller.Controller, code string, pit *libtime.Time) (*GLAccountBalance, error) {
	account, err := s.repository.GetByCode(ctx, strings.TrimSpace(code))
	if err != nil {
		return nil, resolveGLRepositoryError(err)
	}
	asset, err := glAsset(account.Currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrGLReportingValidation, err)
	}
	balance, err := s.readerFor(sys)(ctx, account.Ledger, account.Address, asset, pit)
	if err != nil {
		return nil, err
	}
	line := buildGLAccountBalance(*account, asset, balance)
	line.PIT = toStdTimePtr(pit)
	return &line, nil
}

func (s *DefaultGLReportingService) TrialBalance(ctx context.Context, sys systemcontroller.Controller, filter GLTrialBalanceFilter) (*GLTrialBalanceReport, error) {
	currency := strings.ToUpper(strings.TrimSpace(filter.Currency))
	if currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrGLReportingValidation)
	}
	asset, err := glAsset(currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrGLReportingValidation, err)
	}

	active := models.GLAccountStatusActive
	accounts, err := s.repository.List(ctx, repositories.GLAccountFilter{Currency: &currency, Status: &active})
	if err != nil {
		return nil, err
	}

	read := s.readerFor(sys)
	report := &GLTrialBalanceReport{
		Currency: currency,
		Asset:    asset,
		AsOf:     toStdTimePtr(filter.AsOf),
		Accounts: make([]GLAccountBalance, 0, len(accounts)),
	}

	subtotals := make(map[string]*GLTrialBalanceTypeSubtotal, len(models.GLAccountTypes))
	report.Subtotals = make([]GLTrialBalanceTypeSubtotal, 0, len(models.GLAccountTypes))
	for _, accountType := range models.GLAccountTypes {
		report.Subtotals = append(report.Subtotals, GLTrialBalanceTypeSubtotal{Type: accountType})
		subtotals[accountType] = &report.Subtotals[len(report.Subtotals)-1]
	}

	for _, account := range accounts {
		balance, err := read(ctx, account.Ledger, account.Address, asset, filter.AsOf)
		if err != nil {
			return nil, fmt.Errorf("reading balance of gl account %s (%s/%s): %w", account.Code, account.Ledger, account.Address, err)
		}
		line := buildGLAccountBalance(account, asset, balance)
		report.Accounts = append(report.Accounts, line)

		report.TotalDebits += line.Debit
		report.TotalCredits += line.Credit
		if subtotal, ok := subtotals[line.Type]; ok {
			subtotal.Accounts++
			subtotal.Debit += line.Debit
			subtotal.Credit += line.Credit
			subtotal.Net += line.Debit - line.Credit
		}
	}

	report.Difference = report.TotalDebits - report.TotalCredits
	report.Balanced = report.Difference == 0
	return report, nil
}

func buildGLAccountBalance(account models.GLAccount, asset string, balance int64) GLAccountBalance {
	line := GLAccountBalance{
		Code:       account.Code,
		Name:       account.Name,
		Type:       account.Type,
		NormalSide: account.NormalSide,
		Currency:   account.Currency,
		ParentCode: account.ParentCode,
		Ledger:     account.Ledger,
		Address:    account.Address,
		Status:     account.Status,
		Asset:      asset,
		Balance:    balance,
	}
	if balance >= 0 {
		line.Debit = balance
	} else {
		line.Credit = -balance
	}
	if account.NormalSide == models.GLSideDebit {
		line.NaturalBalance = balance
	} else {
		line.NaturalBalance = -balance
	}
	line.Abnormal = line.NaturalBalance < 0
	return line
}

// glAsset returns the ledger asset (CCY/precision) for a currency using the currency registry.
// It is the single place the GL code builds an asset name, so it can later be swapped for a shared helper.
func glAsset(currency string) (string, error) {
	code := strings.ToUpper(strings.TrimSpace(currency))
	definition, ok := currencyregistry.Lookup(code)
	if !ok {
		return "", fmt.Errorf("currency %s is not in the currency registry", code)
	}
	return fmt.Sprintf("%s/%d", code, definition.Precision), nil
}

// newSystemGLBalanceReader reads exact-address balances through the ledger controllers. A ledger that does
// not exist yet has no postings, so its balances are zero.
func newSystemGLBalanceReader(sys systemcontroller.Controller) glBalanceReader {
	return func(ctx context.Context, ledgerName, address, asset string, pit *libtime.Time) (int64, error) {
		l, err := sys.GetLedgerController(ctx, ledgerName)
		if err != nil {
			if postgres.IsNotFoundError(err) {
				return 0, nil
			}
			return 0, err
		}

		balances, err := l.GetAggregatedBalances(ctx, storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{
			Builder: query.Match("address", address),
			PIT:     pit,
			Opts:    ledgerstore.GetAggregatedVolumesOptions{},
		})
		if err != nil {
			return 0, err
		}
		amount, ok := balances[asset]
		if !ok || amount == nil {
			return 0, nil
		}
		if !amount.IsInt64() {
			return 0, fmt.Errorf("balance of %s in ledger %s overflows int64", address, ledgerName)
		}
		return amount.Int64(), nil
	}
}
