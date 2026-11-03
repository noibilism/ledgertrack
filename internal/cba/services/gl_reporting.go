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
// input. A GL account therefore carries a credit balance when its ledger balance is positive and a debit
// balance when it is negative (money moved *into* an account credits it, moved *out of* it debits it).
// This is the only orientation that matches how Cray moves money: a customer wallet holds a positive
// balance (a liability, credit), system:control is the source of every wallet credit and so goes negative
// (the funding side, debit), and a fee or tax paid out of a wallet lands positive on its income or payable
// account (credit). Summed over every account of a currency the ledger balances are zero, so total debits
// == total credits is exactly the "no unmapped or one-sided movement" check. normal_side only decides which
// sign is "natural" for the account (natural_balance) and flags the opposite as abnormal.
//
// Customer wallets (users:*:wallets:CCY:available|lien) are not registered GL accounts. The trial balance
// rolls them up into GL 201100 Customer Wallets, and shows system:control as the unallocated funding
// side, so a wallet balance is visible in the GL and the report still proves out.
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
	// PostedBalance and WalletBalance split the balance of the Customer Wallets line into what was
	// posted to the GL account itself and what the wallets under it hold. Both are zero elsewhere.
	PostedBalance int64 `json:"posted_balance,omitempty"`
	WalletBalance int64 `json:"wallet_balance,omitempty"`
	// Synthetic marks a line that is not a registered GL account: the Customer Wallets line when 201100
	// is missing from the registry, and system:control.
	Synthetic bool `json:"synthetic,omitempty"`
}

// GLWalletRollup is how the wallets under GL 201100 add up.
type GLWalletRollup struct {
	Ledger string `json:"ledger"`
	// AccountCode is the GL account the wallets roll up into.
	AccountCode string `json:"account_code"`
	Available   int64  `json:"available"`
	Lien        int64  `json:"lien"`
	// Total is Available + Lien, a credit balance.
	Total int64 `json:"total"`
	// Control is the raw ledger balance of system:control:{CCY} and its shards. It is negative by the
	// amount of money credited to wallets from the control account.
	Control int64 `json:"control"`
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
	Wallets      GLWalletRollup               `json:"wallets"`
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

	posted := make([]int64, len(accounts))
	for i, account := range accounts {
		balance, err := read(ctx, account.Ledger, account.Address, asset, filter.AsOf)
		if err != nil {
			return nil, fmt.Errorf("reading balance of gl account %s (%s/%s): %w", account.Code, account.Ledger, account.Address, err)
		}
		posted[i] = balance
	}

	// The wallets live in the ledger of the Customer Wallets account when it is registered.
	walletLedger := walletLedgerName
	var walletAccount *models.GLAccount
	for i := range accounts {
		if isCustomerWalletsCode(accounts[i].Code, currency) {
			walletAccount = &accounts[i]
			walletLedger = accounts[i].Ledger
			break
		}
	}
	rollup, err := readWalletRollup(ctx, read, walletLedger, currency, asset, filter.AsOf)
	if err != nil {
		return nil, err
	}
	rollup.AccountCode = customerWalletsCode
	if walletAccount != nil {
		rollup.AccountCode = walletAccount.Code
	}
	report.Wallets = rollup

	add := func(line GLAccountBalance) {
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

	for i, account := range accounts {
		line := buildGLAccountBalance(account, asset, posted[i])
		if walletAccount != nil && account.Code == walletAccount.Code {
			line = buildGLAccountBalance(account, asset, posted[i]+rollup.Total)
			line.PostedBalance = posted[i]
			line.WalletBalance = rollup.Total
		}
		add(line)
	}

	if walletAccount == nil && rollup.Total != 0 {
		add(syntheticWalletsLine(walletLedger, currency, asset, rollup.Total))
	}
	if rollup.Control != 0 {
		add(syntheticControlLine(walletLedger, currency, asset, rollup.Control))
	}

	report.Difference = report.TotalDebits - report.TotalCredits
	report.Balanced = report.Difference == 0
	return report, nil
}

// walletLedgerName is the ledger that holds customer wallets and system:control.
const walletLedgerName = "ledgertrack"

// customerWalletsCode is the GL account the wallet balances roll up into. USD keeps the plain code, the
// other currencies use {code}.{CCY}.
const customerWalletsCode = "201100"

func isCustomerWalletsCode(code, currency string) bool {
	return code == customerWalletsCode || code == customerWalletsCode+"."+currency
}

// readWalletRollup adds up every wallet of a currency (available and lien) and the control account that
// funds them. The wallet patterns use the ledger's partial address match: an empty segment matches any id.
func readWalletRollup(ctx context.Context, read glBalanceReader, ledger, currency, asset string, pit *libtime.Time) (GLWalletRollup, error) {
	rollup := GLWalletRollup{Ledger: ledger}
	var err error
	if rollup.Available, err = read(ctx, ledger, fmt.Sprintf("users::wallets:%s:available", currency), asset, pit); err != nil {
		return rollup, fmt.Errorf("reading wallet balances (%s): %w", currency, err)
	}
	if rollup.Lien, err = read(ctx, ledger, fmt.Sprintf("users::wallets:%s:lien", currency), asset, pit); err != nil {
		return rollup, fmt.Errorf("reading wallet liens (%s): %w", currency, err)
	}
	rollup.Total = rollup.Available + rollup.Lien

	// system:control:{CCY} and its shards system:control:{CCY}:{n}. The trailing empty segment matches the
	// exact address and every deeper one, so a single read covers both (reading the exact address as well
	// would count it twice).
	control, err := read(ctx, ledger, fmt.Sprintf("system:control:%s:", currency), asset, pit)
	if err != nil {
		return rollup, fmt.Errorf("reading control balance (%s): %w", currency, err)
	}
	rollup.Control = control
	return rollup, nil
}

func syntheticWalletsLine(ledger, currency, asset string, total int64) GLAccountBalance {
	line := buildGLAccountBalance(models.GLAccount{
		Code:       customerWalletsCode,
		Name:       "Customer Wallets (wallet rollup)",
		Type:       models.GLAccountTypeLiability,
		NormalSide: models.GLSideCredit,
		Currency:   currency,
		Ledger:     ledger,
		Address:    fmt.Sprintf("users::wallets:%s", currency),
		Status:     models.GLAccountStatusActive,
	}, asset, total)
	line.WalletBalance = total
	line.Synthetic = true
	return line
}

func syntheticControlLine(ledger, currency, asset string, balance int64) GLAccountBalance {
	line := buildGLAccountBalance(models.GLAccount{
		Code:       "system:control",
		Name:       "Wallet funding control (unallocated)",
		Type:       models.GLAccountTypeAsset,
		NormalSide: models.GLSideDebit,
		Currency:   currency,
		Ledger:     ledger,
		Address:    fmt.Sprintf("system:control:%s", currency),
		Status:     models.GLAccountStatusActive,
	}, asset, balance)
	line.Synthetic = true
	return line
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
		line.Credit = balance
	} else {
		line.Debit = -balance
	}
	if account.NormalSide == models.GLSideCredit {
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
