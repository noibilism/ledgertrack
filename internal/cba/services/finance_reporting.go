package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	libtime "github.com/formancehq/go-libs/v3/time"

	ledgerinternal "github.com/formancehq/ledger/internal"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

var (
	ErrFinanceReportingValidation = errors.New("finance reporting validation failed")
)

type FinanceReportingService interface {
	TrialBalance(context.Context, systemcontroller.Controller, FinanceTrialBalanceFilter) (*FinanceTrialBalanceReport, error)
	BalanceSheet(context.Context, systemcontroller.Controller, FinanceBalanceSheetFilter) (*FinanceBalanceSheetReport, error)
	ProfitAndLoss(context.Context, systemcontroller.Controller, FinanceProfitAndLossFilter) (*FinanceProfitAndLossReport, error)
	CashFlow(context.Context, systemcontroller.Controller, FinanceCashFlowFilter) (*FinanceCashFlowReport, error)
}

type FinanceTrialBalanceFilter struct {
	Currency               string
	AsOf                   *libtime.Time
	FeeIncomeAccount       string
	InterestExpenseAccount string
}

type FinanceBalanceSheetFilter struct {
	Currency               string
	AsOf                   *libtime.Time
	FeeIncomeAccount       string
	InterestExpenseAccount string
}

type FinanceProfitAndLossFilter struct {
	Currency               string
	StartTime              *stdtime.Time
	EndTime                *stdtime.Time
	FeeIncomeAccount       string
	InterestExpenseAccount string
}

type FinanceCashFlowFilter struct {
	Currency               string
	StartTime              *libtime.Time
	EndTime                *libtime.Time
	FeeIncomeAccount       string
	InterestExpenseAccount string
}

type FinanceTrialBalanceLine struct {
	Ledger   string `json:"ledger"`
	Account  string `json:"account"`
	Balance  int64  `json:"balance"`
	Currency string `json:"currency"`
}

type FinanceTrialBalanceReport struct {
	Currency string                    `json:"currency"`
	AsOf     *stdtime.Time             `json:"as_of,omitempty"`
	Lines    []FinanceTrialBalanceLine `json:"lines"`
}

type FinanceBalanceSheetSectionLine struct {
	Name     string `json:"name"`
	Balance  int64  `json:"balance"`
	Currency string `json:"currency"`
}

type FinanceBalanceSheetReport struct {
	Currency    string                           `json:"currency"`
	AsOf        *stdtime.Time                    `json:"as_of,omitempty"`
	Assets      []FinanceBalanceSheetSectionLine `json:"assets"`
	Liabilities []FinanceBalanceSheetSectionLine `json:"liabilities"`
	NetPosition int64                            `json:"net_position"`
}

type FinanceProfitAndLossLine struct {
	Name     string `json:"name"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type FinanceProfitAndLossReport struct {
	Currency  string                     `json:"currency"`
	StartTime *stdtime.Time              `json:"start_time,omitempty"`
	EndTime   *stdtime.Time              `json:"end_time,omitempty"`
	Income    []FinanceProfitAndLossLine `json:"income"`
	Expenses  []FinanceProfitAndLossLine `json:"expenses"`
	NetProfit int64                      `json:"net_profit"`
}

type FinanceCashFlowLine struct {
	Name      string `json:"name"`
	Opening   int64  `json:"opening"`
	Closing   int64  `json:"closing"`
	NetChange int64  `json:"net_change"`
	Currency  string `json:"currency"`
}

type FinanceCashFlowReport struct {
	Currency  string                `json:"currency"`
	StartTime *stdtime.Time         `json:"start_time,omitempty"`
	EndTime   *stdtime.Time         `json:"end_time,omitempty"`
	Lines     []FinanceCashFlowLine `json:"lines"`
}

type DefaultFinanceReportingService struct{}

func NewFinanceReportingService() FinanceReportingService {
	return &DefaultFinanceReportingService{}
}

func (s *DefaultFinanceReportingService) TrialBalance(ctx context.Context, sys systemcontroller.Controller, filter FinanceTrialBalanceFilter) (*FinanceTrialBalanceReport, error) {
	currency := strings.ToUpper(strings.TrimSpace(filter.Currency))
	if currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrFinanceReportingValidation)
	}

	asOf := toStdTimePtr(filter.AsOf)

	feeIncomeAccount := normalizeOrDefaultAccount(filter.FeeIncomeAccount, "revenue:fee_income")
	interestExpenseAccount := normalizeOrDefaultAccount(filter.InterestExpenseAccount, "revenue:interest_expense")

	lines := make([]FinanceTrialBalanceLine, 0, 8)

	ledgertrackLine := func(account string, balance int64) {
		lines = append(lines, FinanceTrialBalanceLine{
			Ledger:   "ledgertrack",
			Account:  account,
			Balance:  balance,
			Currency: currency,
		})
	}
	channelsLine := func(account string, balance int64) {
		lines = append(lines, FinanceTrialBalanceLine{
			Ledger:   fmt.Sprintf("channels-%s", currency),
			Account:  account,
			Balance:  balance,
			Currency: currency,
		})
	}
	revenueLine := func(account string, balance int64) {
		lines = append(lines, FinanceTrialBalanceLine{
			Ledger:   fmt.Sprintf("revenue-%s", currency),
			Account:  account,
			Balance:  balance,
			Currency: currency,
		})
	}

	systemBalance, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("system:control:%s", currency), filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	ledgertrackLine(fmt.Sprintf("system:control:%s", currency), systemBalance)

	availableBalance, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("users::wallets:%s:available", currency), filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	ledgertrackLine(fmt.Sprintf("users::wallets:%s:available", currency), availableBalance)

	lienBalance, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("users::wallets:%s:lien", currency), filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	ledgertrackLine(fmt.Sprintf("users::wallets:%s:lien", currency), lienBalance)

	channelsBalance, err := aggregatedBalance(ctx, sys, fmt.Sprintf("channels-%s", currency), "channel:", filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	channelsLine("channel:", channelsBalance)

	feeIncomeBalance, err := aggregatedBalance(ctx, sys, fmt.Sprintf("revenue-%s", currency), feeIncomeAccount, filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	revenueLine(feeIncomeAccount, feeIncomeBalance)

	interestExpenseBalance, err := aggregatedBalance(ctx, sys, fmt.Sprintf("revenue-%s", currency), interestExpenseAccount, filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	revenueLine(interestExpenseAccount, interestExpenseBalance)

	revenueAccumulated, err := aggregatedBalance(ctx, sys, fmt.Sprintf("revenue-%s", currency), "revenue:accumulated", filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	revenueLine("revenue:accumulated", revenueAccumulated)

	return &FinanceTrialBalanceReport{
		Currency: currency,
		AsOf:     asOf,
		Lines:    lines,
	}, nil
}

func (s *DefaultFinanceReportingService) BalanceSheet(ctx context.Context, sys systemcontroller.Controller, filter FinanceBalanceSheetFilter) (*FinanceBalanceSheetReport, error) {
	currency := strings.ToUpper(strings.TrimSpace(filter.Currency))
	if currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrFinanceReportingValidation)
	}
	asOf := toStdTimePtr(filter.AsOf)

	systemBalance, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("system:control:%s", currency), filter.AsOf, currency)
	if err != nil {
		return nil, err
	}

	channelsBalance, err := aggregatedBalance(ctx, sys, fmt.Sprintf("channels-%s", currency), "channel:", filter.AsOf, currency)
	if err != nil {
		return nil, err
	}

	availableBalance, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("users::wallets:%s:available", currency), filter.AsOf, currency)
	if err != nil {
		return nil, err
	}
	lienBalance, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("users::wallets:%s:lien", currency), filter.AsOf, currency)
	if err != nil {
		return nil, err
	}

	assets := []FinanceBalanceSheetSectionLine{
		{Name: fmt.Sprintf("system:control:%s", currency), Balance: systemBalance, Currency: currency},
		{Name: "channels total (channel:)", Balance: channelsBalance, Currency: currency},
	}
	liabilities := []FinanceBalanceSheetSectionLine{
		{Name: "customer deposits available", Balance: availableBalance, Currency: currency},
		{Name: "customer deposits lien", Balance: lienBalance, Currency: currency},
	}

	net := int64(0)
	for _, l := range assets {
		net += l.Balance
	}
	for _, l := range liabilities {
		net -= l.Balance
	}

	return &FinanceBalanceSheetReport{
		Currency:    currency,
		AsOf:        asOf,
		Assets:      assets,
		Liabilities: liabilities,
		NetPosition: net,
	}, nil
}

func (s *DefaultFinanceReportingService) ProfitAndLoss(ctx context.Context, sys systemcontroller.Controller, filter FinanceProfitAndLossFilter) (*FinanceProfitAndLossReport, error) {
	currency := strings.ToUpper(strings.TrimSpace(filter.Currency))
	if currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrFinanceReportingValidation)
	}
	if filter.StartTime == nil || filter.EndTime == nil {
		return nil, fmt.Errorf("%w: startTime and endTime are required", ErrFinanceReportingValidation)
	}
	if filter.EndTime.Before(*filter.StartTime) {
		return nil, fmt.Errorf("%w: endTime must be after startTime", ErrFinanceReportingValidation)
	}

	feeIncomeAccount := normalizeOrDefaultAccount(filter.FeeIncomeAccount, "revenue:fee_income")
	interestExpenseAccount := normalizeOrDefaultAccount(filter.InterestExpenseAccount, "revenue:interest_expense")

	revenueLedgerName := fmt.Sprintf("revenue-%s", currency)
	revenueLedger, err := sys.GetLedgerController(ctx, revenueLedgerName)
	if err != nil {
		return nil, err
	}

	asset := fmt.Sprintf("%s/2", currency)
	qb := query.Or(
		query.Match("account", feeIncomeAccount),
		query.Match("account", interestExpenseAccount),
		query.Match("account", "revenue:accumulated"),
	)
	qb = query.And(qb,
		query.Gte("timestamp", filter.StartTime.UTC().Format(stdtime.RFC3339)),
		query.Lte("timestamp", filter.EndTime.UTC().Format(stdtime.RFC3339)),
	)

	var order bunpaginate.Order = bunpaginate.OrderAsc
	var (
		feeIncome       int64
		interestExpense int64
		walletRevenue   int64
	)
	err = storagecommon.Iterate(ctx, storagecommon.InitialPaginatedQuery[any]{
		PageSize: 200,
		Column:   "id",
		Order:    &order,
		Options: storagecommon.ResourceQuery[any]{
			Builder: qb,
		},
	}, revenueLedger.ListTransactions, func(cursor *bunpaginate.Cursor[ledgerinternal.Transaction]) error {
		for _, tx := range cursor.Data {
			for _, p := range tx.Postings {
				if p.Asset != asset {
					continue
				}
				if p.Destination == feeIncomeAccount {
					feeIncome += p.Amount.Int64()
				}
				if p.Destination == "revenue:accumulated" {
					walletRevenue += p.Amount.Int64()
				}
				if p.Source == interestExpenseAccount {
					interestExpense += p.Amount.Int64()
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	income := []FinanceProfitAndLossLine{
		{Name: "fee income", Amount: feeIncome, Currency: currency},
		{Name: "wallet revenue", Amount: walletRevenue, Currency: currency},
	}
	expenses := []FinanceProfitAndLossLine{
		{Name: "interest expense", Amount: interestExpense, Currency: currency},
	}

	return &FinanceProfitAndLossReport{
		Currency:  currency,
		StartTime: filter.StartTime,
		EndTime:   filter.EndTime,
		Income:    income,
		Expenses:  expenses,
		NetProfit: (feeIncome + walletRevenue) - interestExpense,
	}, nil
}

func (s *DefaultFinanceReportingService) CashFlow(ctx context.Context, sys systemcontroller.Controller, filter FinanceCashFlowFilter) (*FinanceCashFlowReport, error) {
	currency := strings.ToUpper(strings.TrimSpace(filter.Currency))
	if currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrFinanceReportingValidation)
	}
	if filter.StartTime == nil || filter.EndTime == nil {
		return nil, fmt.Errorf("%w: startTime and endTime are required", ErrFinanceReportingValidation)
	}
	if filter.EndTime.Time.Before(filter.StartTime.Time) {
		return nil, fmt.Errorf("%w: endTime must be after startTime", ErrFinanceReportingValidation)
	}

	openControl, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("system:control:%s", currency), filter.StartTime, currency)
	if err != nil {
		return nil, err
	}
	closeControl, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("system:control:%s", currency), filter.EndTime, currency)
	if err != nil {
		return nil, err
	}

	openDeposits, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("users::wallets:%s:available", currency), filter.StartTime, currency)
	if err != nil {
		return nil, err
	}
	closeDeposits, err := aggregatedBalance(ctx, sys, "ledgertrack", fmt.Sprintf("users::wallets:%s:available", currency), filter.EndTime, currency)
	if err != nil {
		return nil, err
	}

	openChannels, err := aggregatedBalance(ctx, sys, fmt.Sprintf("channels-%s", currency), "channel:", filter.StartTime, currency)
	if err != nil {
		return nil, err
	}
	closeChannels, err := aggregatedBalance(ctx, sys, fmt.Sprintf("channels-%s", currency), "channel:", filter.EndTime, currency)
	if err != nil {
		return nil, err
	}

	lines := []FinanceCashFlowLine{
		{
			Name:      fmt.Sprintf("system:control:%s", currency),
			Opening:   openControl,
			Closing:   closeControl,
			NetChange: closeControl - openControl,
			Currency:  currency,
		},
		{
			Name:      "customer deposits available",
			Opening:   openDeposits,
			Closing:   closeDeposits,
			NetChange: closeDeposits - openDeposits,
			Currency:  currency,
		},
		{
			Name:      "channels total (channel:)",
			Opening:   openChannels,
			Closing:   closeChannels,
			NetChange: closeChannels - openChannels,
			Currency:  currency,
		},
	}

	start := filter.StartTime.Time.UTC()
	end := filter.EndTime.Time.UTC()

	return &FinanceCashFlowReport{
		Currency:  currency,
		StartTime: &start,
		EndTime:   &end,
		Lines:     lines,
	}, nil
}

func normalizeOrDefaultAccount(value, defaultValue string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue
	}
	return value
}

func aggregatedBalance(ctx context.Context, sys systemcontroller.Controller, ledgerName, addressFilter string, pit *libtime.Time, currency string) (int64, error) {
	_ = sys.CreateLedger(ctx, ledgerName, ledgerinternal.Configuration{})

	l, err := sys.GetLedgerController(ctx, ledgerName)
	if err != nil {
		return 0, err
	}

	q := storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{
		Builder: query.Match("address", addressFilter),
		Opts:    ledgerstore.GetAggregatedVolumesOptions{},
	}
	if pit != nil {
		q.PIT = pit
	}
	balances, err := l.GetAggregatedBalances(ctx, q)
	if err != nil {
		return 0, err
	}
	asset := fmt.Sprintf("%s/2", currency)
	if amount, ok := balances[asset]; ok && amount != nil {
		return amount.Int64(), nil
	}
	return 0, nil
}

func toStdTimePtr(t *libtime.Time) *stdtime.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	x := t.Time.UTC()
	return &x
}
