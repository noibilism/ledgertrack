package services

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/query"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

var (
	ErrReportingValidation = errors.New("reporting validation failed")
	ErrReportingNotFound   = errors.New("reporting resource not found")
)

type StatementReportFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
}

type DailyTransactionSummaryFilter struct {
	Date      time.Time
	ClientID  *uuid.UUID
	AccountID *uuid.UUID
}

type InterestFeeReportFilter struct {
	ClientID  *uuid.UUID
	AccountID *uuid.UUID
	StartTime *time.Time
	EndTime   *time.Time
}

type ClientPortfolioAccount struct {
	models.Account
	AvailableBalance int64 `json:"available_balance"`
}

type ClientPortfolioCurrencySummary struct {
	Currency              string `json:"currency"`
	AccountCount          int    `json:"account_count"`
	ActiveCount           int    `json:"active_count"`
	DormantCount          int    `json:"dormant_count"`
	SuspendedCount        int    `json:"suspended_count"`
	PendingCount          int    `json:"pending_count"`
	ClosedCount           int    `json:"closed_count"`
	TotalAvailableBalance int64  `json:"total_available_balance"`
	TotalInterestAccrued  string `json:"total_interest_accrued"`
}

type ClientPortfolioReport struct {
	Client           models.Client                    `json:"client"`
	Accounts         []ClientPortfolioAccount         `json:"accounts"`
	TotalAccounts    int                              `json:"total_accounts"`
	TotalsByCurrency []ClientPortfolioCurrencySummary `json:"totals_by_currency"`
}

type AccountStatementReport struct {
	Account          models.Account               `json:"account"`
	StartTime        *time.Time                   `json:"start_time,omitempty"`
	EndTime          *time.Time                   `json:"end_time,omitempty"`
	OpeningBalance   int64                        `json:"opening_balance"`
	ClosingBalance   int64                        `json:"closing_balance"`
	TotalCredits     int64                        `json:"total_credits"`
	TotalDebits      int64                        `json:"total_debits"`
	TransactionCount int                          `json:"transaction_count"`
	Transactions     []ledgerinternal.Transaction `json:"-"`
}

type DailyTransactionAccountSummary struct {
	AccountID        uuid.UUID        `json:"account_id"`
	AccountNumber    string           `json:"account_number"`
	Currency         string           `json:"currency"`
	TransactionCount int64            `json:"transaction_count"`
	DebitCount       int64            `json:"debit_count"`
	CreditCount      int64            `json:"credit_count"`
	DebitAmount      int64            `json:"debit_amount"`
	CreditAmount     int64            `json:"credit_amount"`
	NetAmount        int64            `json:"net_amount"`
	OperationCounts  map[string]int64 `json:"operation_counts"`
}

type DailyTransactionSummaryReport struct {
	ReportDate       string                           `json:"report_date"`
	TransactionCount int64                            `json:"transaction_count"`
	DebitCount       int64                            `json:"debit_count"`
	CreditCount      int64                            `json:"credit_count"`
	DebitAmount      int64                            `json:"debit_amount"`
	CreditAmount     int64                            `json:"credit_amount"`
	NetAmount        int64                            `json:"net_amount"`
	OperationCounts  map[string]int64                 `json:"operation_counts"`
	Accounts         []DailyTransactionAccountSummary `json:"accounts"`
}

type InterestFeeAccountSummary struct {
	AccountID                uuid.UUID `json:"account_id"`
	AccountNumber            string    `json:"account_number"`
	Currency                 string    `json:"currency"`
	InterestAccrued          string    `json:"interest_accrued"`
	InterestPostedCount      int64     `json:"interest_posted_count"`
	InterestPendingCount     int64     `json:"interest_pending_count"`
	FeeTotal                 string    `json:"fee_total"`
	FeePostedAmount          string    `json:"fee_posted_amount"`
	FeePendingRecoveryAmount string    `json:"fee_pending_recovery_amount"`
	FeePostedCount           int64     `json:"fee_posted_count"`
	FeePendingRecoveryCount  int64     `json:"fee_pending_recovery_count"`
}

type InterestFeeTotals struct {
	InterestAccrued          string `json:"interest_accrued"`
	InterestPostedCount      int64  `json:"interest_posted_count"`
	InterestPendingCount     int64  `json:"interest_pending_count"`
	FeeTotal                 string `json:"fee_total"`
	FeePostedAmount          string `json:"fee_posted_amount"`
	FeePendingRecoveryAmount string `json:"fee_pending_recovery_amount"`
	FeePostedCount           int64  `json:"fee_posted_count"`
	FeePendingRecoveryCount  int64  `json:"fee_pending_recovery_count"`
}

type InterestFeeReport struct {
	Accounts []InterestFeeAccountSummary `json:"accounts"`
	Totals   InterestFeeTotals           `json:"totals"`
}

type ReportingService interface {
	ClientPortfolio(context.Context, ledgercontroller.Controller, uuid.UUID) (*ClientPortfolioReport, error)
	AccountStatement(context.Context, ledgercontroller.Controller, uuid.UUID, StatementReportFilter) (*AccountStatementReport, error)
	DailyTransactionSummary(context.Context, ledgercontroller.Controller, DailyTransactionSummaryFilter) (*DailyTransactionSummaryReport, error)
	InterestFeeSummary(context.Context, InterestFeeReportFilter) (*InterestFeeReport, error)
}

type DefaultReportingService struct {
	clientRepository   repositories.ClientRepository
	accountRepository  repositories.AccountRepository
	interestRepository repositories.InterestAccrualRepository
	feeRepository      repositories.FeePostingRepository
}

func NewReportingService(
	clientRepository repositories.ClientRepository,
	accountRepository repositories.AccountRepository,
	interestRepository repositories.InterestAccrualRepository,
	feeRepository repositories.FeePostingRepository,
) ReportingService {
	return &DefaultReportingService{
		clientRepository:   clientRepository,
		accountRepository:  accountRepository,
		interestRepository: interestRepository,
		feeRepository:      feeRepository,
	}
}

func (s *DefaultReportingService) ClientPortfolio(ctx context.Context, ledger ledgercontroller.Controller, clientID uuid.UUID) (*ClientPortfolioReport, error) {
	client, err := s.getClient(ctx, clientID)
	if err != nil {
		return nil, err
	}

	accounts, err := s.accountRepository.List(ctx, repositories.AccountFilter{ClientID: &clientID})
	if err != nil {
		return nil, err
	}

	accountReports := make([]ClientPortfolioAccount, 0, len(accounts))
	currencyTotals := map[string]*ClientPortfolioCurrencySummary{}
	for _, account := range accounts {
		balance, err := reportingAvailableBalance(ctx, ledger, account)
		if err != nil {
			return nil, err
		}

		accountReports = append(accountReports, ClientPortfolioAccount{
			Account:          account,
			AvailableBalance: balance,
		})

		summary, ok := currencyTotals[account.Currency]
		if !ok {
			summary = &ClientPortfolioCurrencySummary{
				Currency:             account.Currency,
				TotalInterestAccrued: decimal.Zero.String(),
			}
			currencyTotals[account.Currency] = summary
		}
		summary.AccountCount++
		summary.TotalAvailableBalance += balance
		summary.TotalInterestAccrued = decimal.RequireFromString(summary.TotalInterestAccrued).Add(account.InterestAccrued).String()
		switch account.Status {
		case models.AccountStatusActive:
			summary.ActiveCount++
		case models.AccountStatusDormant:
			summary.DormantCount++
		case models.AccountStatusSuspended:
			summary.SuspendedCount++
		case models.AccountStatusPending:
			summary.PendingCount++
		case models.AccountStatusClosed:
			summary.ClosedCount++
		}
	}

	totalsByCurrency := make([]ClientPortfolioCurrencySummary, 0, len(currencyTotals))
	for _, summary := range currencyTotals {
		totalsByCurrency = append(totalsByCurrency, *summary)
	}
	sort.Slice(totalsByCurrency, func(i, j int) bool {
		return totalsByCurrency[i].Currency < totalsByCurrency[j].Currency
	})

	return &ClientPortfolioReport{
		Client:           *client,
		Accounts:         accountReports,
		TotalAccounts:    len(accountReports),
		TotalsByCurrency: totalsByCurrency,
	}, nil
}

func (s *DefaultReportingService) AccountStatement(ctx context.Context, ledger ledgercontroller.Controller, accountID uuid.UUID, filter StatementReportFilter) (*AccountStatementReport, error) {
	account, err := s.getAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}

	transactions, err := listTransactionsForAccounts(ctx, ledger, []models.Account{*account}, filter.StartTime, filter.EndTime)
	if err != nil {
		return nil, err
	}

	report := &AccountStatementReport{
		Account:      *account,
		StartTime:    filter.StartTime,
		EndTime:      filter.EndTime,
		Transactions: transactions,
	}

	availableAddress := reportingAvailableAddress(account.WalletID, account.Currency)
	first := true
	for _, tx := range transactions {
		before, after, delta := reportingBalanceChange(tx, availableAddress, account.Currency)
		if first {
			report.OpeningBalance = before
			first = false
		}
		report.ClosingBalance = after
		if delta > 0 {
			report.TotalCredits += delta
		} else if delta < 0 {
			report.TotalDebits += -delta
		}
	}
	report.TransactionCount = len(transactions)

	return report, nil
}

func (s *DefaultReportingService) DailyTransactionSummary(ctx context.Context, ledger ledgercontroller.Controller, filter DailyTransactionSummaryFilter) (*DailyTransactionSummaryReport, error) {
	if filter.Date.IsZero() {
		return nil, fmt.Errorf("%w: date is required", ErrReportingValidation)
	}

	start := normalizeUsageDate(filter.Date)
	end := start.Add(24*time.Hour - time.Nanosecond)
	accounts, err := s.listAccountsForScope(ctx, filter.ClientID, filter.AccountID)
	if err != nil {
		return nil, err
	}

	report := &DailyTransactionSummaryReport{
		ReportDate:      start.Format("2006-01-02"),
		OperationCounts: map[string]int64{},
		Accounts:        make([]DailyTransactionAccountSummary, 0, len(accounts)),
	}
	if len(accounts) == 0 {
		return report, nil
	}

	accountSummaries := make(map[uuid.UUID]*DailyTransactionAccountSummary, len(accounts))
	accountByAvailableAddress := make(map[string]models.Account, len(accounts))
	for _, account := range accounts {
		accountSummaries[account.ID] = &DailyTransactionAccountSummary{
			AccountID:       account.ID,
			AccountNumber:   account.AccountNumber,
			Currency:        account.Currency,
			OperationCounts: map[string]int64{},
		}
		accountByAvailableAddress[reportingAvailableAddress(account.WalletID, account.Currency)] = account
	}

	transactions, err := listTransactionsForAccounts(ctx, ledger, accounts, &start, &end)
	if err != nil {
		return nil, err
	}

	for _, tx := range transactions {
		for address, account := range accountByAvailableAddress {
			_, _, delta := reportingBalanceChange(tx, address, account.Currency)
			if delta == 0 {
				continue
			}

			summary := accountSummaries[account.ID]
			summary.TransactionCount++
			report.TransactionCount++
			summary.NetAmount += delta
			report.NetAmount += delta

			operation := tx.Metadata["cba_operation"]
			if operation == "" {
				operation = "ledger"
			}
			summary.OperationCounts[operation]++
			report.OperationCounts[operation]++

			if delta > 0 {
				summary.CreditCount++
				summary.CreditAmount += delta
				report.CreditCount++
				report.CreditAmount += delta
			} else {
				amount := -delta
				summary.DebitCount++
				summary.DebitAmount += amount
				report.DebitCount++
				report.DebitAmount += amount
			}
			break
		}
	}

	for _, account := range accounts {
		report.Accounts = append(report.Accounts, *accountSummaries[account.ID])
	}
	sort.Slice(report.Accounts, func(i, j int) bool {
		return report.Accounts[i].AccountNumber < report.Accounts[j].AccountNumber
	})

	return report, nil
}

func (s *DefaultReportingService) InterestFeeSummary(ctx context.Context, filter InterestFeeReportFilter) (*InterestFeeReport, error) {
	accounts, err := s.listAccountsForScope(ctx, filter.ClientID, filter.AccountID)
	if err != nil {
		return nil, err
	}

	report := &InterestFeeReport{
		Accounts: make([]InterestFeeAccountSummary, 0, len(accounts)),
		Totals: InterestFeeTotals{
			InterestAccrued:          decimal.Zero.String(),
			FeeTotal:                 decimal.Zero.String(),
			FeePostedAmount:          decimal.Zero.String(),
			FeePendingRecoveryAmount: decimal.Zero.String(),
		},
	}

	totalInterestAccrued := decimal.Zero
	totalFeeAmount := decimal.Zero
	totalFeePosted := decimal.Zero
	totalFeePending := decimal.Zero

	for _, account := range accounts {
		accruals, err := s.interestRepository.ListByAccount(ctx, account.ID)
		if err != nil {
			return nil, err
		}
		postings, err := s.feeRepository.ListByAccount(ctx, account.ID)
		if err != nil {
			return nil, err
		}

		accountSummary := InterestFeeAccountSummary{
			AccountID:                account.ID,
			AccountNumber:            account.AccountNumber,
			Currency:                 account.Currency,
			InterestAccrued:          decimal.Zero.String(),
			FeeTotal:                 decimal.Zero.String(),
			FeePostedAmount:          decimal.Zero.String(),
			FeePendingRecoveryAmount: decimal.Zero.String(),
		}

		accountInterest := decimal.Zero
		accountFeeTotal := decimal.Zero
		accountFeePosted := decimal.Zero
		accountFeePending := decimal.Zero

		for _, accrual := range accruals {
			if !withinDateRange(accrual.AccrualDate, filter.StartTime, filter.EndTime) {
				continue
			}
			accountInterest = accountInterest.Add(accrual.Amount)
			if accrual.Posted {
				accountSummary.InterestPostedCount++
			} else {
				accountSummary.InterestPendingCount++
			}
		}

		for _, posting := range postings {
			if !withinDateRange(posting.CreatedAt, filter.StartTime, filter.EndTime) {
				continue
			}
			accountFeeTotal = accountFeeTotal.Add(posting.Amount)
			switch posting.Status {
			case models.FeePostingStatusPosted:
				accountSummary.FeePostedCount++
				accountFeePosted = accountFeePosted.Add(posting.Amount)
			case models.FeePostingStatusPendingRecovery:
				accountSummary.FeePendingRecoveryCount++
				accountFeePending = accountFeePending.Add(posting.Amount)
			}
		}

		accountSummary.InterestAccrued = accountInterest.String()
		accountSummary.FeeTotal = accountFeeTotal.String()
		accountSummary.FeePostedAmount = accountFeePosted.String()
		accountSummary.FeePendingRecoveryAmount = accountFeePending.String()
		report.Accounts = append(report.Accounts, accountSummary)

		totalInterestAccrued = totalInterestAccrued.Add(accountInterest)
		totalFeeAmount = totalFeeAmount.Add(accountFeeTotal)
		totalFeePosted = totalFeePosted.Add(accountFeePosted)
		totalFeePending = totalFeePending.Add(accountFeePending)
		report.Totals.InterestPostedCount += accountSummary.InterestPostedCount
		report.Totals.InterestPendingCount += accountSummary.InterestPendingCount
		report.Totals.FeePostedCount += accountSummary.FeePostedCount
		report.Totals.FeePendingRecoveryCount += accountSummary.FeePendingRecoveryCount
	}

	sort.Slice(report.Accounts, func(i, j int) bool {
		return report.Accounts[i].AccountNumber < report.Accounts[j].AccountNumber
	})
	report.Totals.InterestAccrued = totalInterestAccrued.String()
	report.Totals.FeeTotal = totalFeeAmount.String()
	report.Totals.FeePostedAmount = totalFeePosted.String()
	report.Totals.FeePendingRecoveryAmount = totalFeePending.String()

	return report, nil
}

func (s *DefaultReportingService) getClient(ctx context.Context, clientID uuid.UUID) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, clientID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, fmt.Errorf("%w: client", ErrReportingNotFound)
		}
		return nil, err
	}
	return client, nil
}

func (s *DefaultReportingService) getAccount(ctx context.Context, accountID uuid.UUID) (*models.Account, error) {
	account, err := s.accountRepository.Get(ctx, accountID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil, fmt.Errorf("%w: account", ErrReportingNotFound)
		}
		return nil, err
	}
	return account, nil
}

func (s *DefaultReportingService) listAccountsForScope(ctx context.Context, clientID, accountID *uuid.UUID) ([]models.Account, error) {
	if accountID != nil {
		account, err := s.getAccount(ctx, *accountID)
		if err != nil {
			return nil, err
		}
		if clientID != nil && account.ClientID != *clientID {
			return nil, fmt.Errorf("%w: account does not belong to client", ErrReportingValidation)
		}
		return []models.Account{*account}, nil
	}

	if clientID != nil {
		if _, err := s.getClient(ctx, *clientID); err != nil {
			return nil, err
		}
	}

	filter := repositories.AccountFilter{
		ClientID: clientID,
	}
	return s.accountRepository.List(ctx, filter)
}

func listTransactionsForAccounts(ctx context.Context, ledger ledgercontroller.Controller, accounts []models.Account, startTime, endTime *time.Time) ([]ledgerinternal.Transaction, error) {
	if len(accounts) == 0 {
		return nil, nil
	}

	builders := make([]query.Builder, 0, len(accounts)*2)
	for _, account := range accounts {
		builders = append(builders,
			query.Match("account", reportingAvailableAddress(account.WalletID, account.Currency)),
			query.Match("account", reportingLienAddress(account.WalletID, account.Currency)),
		)
	}

	qb := query.Or(builders...)
	if startTime != nil {
		qb = query.And(qb, query.Gte("timestamp", startTime.Format(time.RFC3339)))
	}
	if endTime != nil {
		qb = query.And(qb, query.Lte("timestamp", endTime.Format(time.RFC3339)))
	}

	var order bunpaginate.Order = bunpaginate.OrderAsc
	ret := make([]ledgerinternal.Transaction, 0)
	err := storagecommon.Iterate(ctx, storagecommon.InitialPaginatedQuery[any]{
		PageSize: 100,
		Column:   "id",
		Order:    &order,
		Options: storagecommon.ResourceQuery[any]{
			Builder: qb,
			Expand:  []string{"volumes"},
		},
	}, ledger.ListTransactions, func(cursor *bunpaginate.Cursor[ledgerinternal.Transaction]) error {
		ret = append(ret, cursor.Data...)
		return nil
	})
	return ret, err
}

func reportingBalanceChange(tx ledgerinternal.Transaction, trackedAccount, currency string) (int64, int64, int64) {
	var balanceBefore, balanceAfter int64
	assetName := fmt.Sprintf("%s/2", currency)
	if vol, ok := tx.PostCommitVolumes[trackedAccount]; ok {
		if v, ok := vol[assetName]; ok {
			balanceAfter = v.Balance().Int64()
		}
	}
	netChange := int64(0)
	for _, posting := range tx.Postings {
		if posting.Asset != assetName {
			continue
		}
		if posting.Destination == trackedAccount {
			netChange += posting.Amount.Int64()
		}
		if posting.Source == trackedAccount {
			netChange -= posting.Amount.Int64()
		}
	}
	balanceBefore = balanceAfter - netChange
	return balanceBefore, balanceAfter, netChange
}

func reportingAvailableBalance(ctx context.Context, ledger ledgercontroller.Controller, account models.Account) (int64, error) {
	balancesMap, err := ledger.GetAggregatedBalances(ctx, storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{
		Builder: query.Match("address", reportingAvailableAddress(account.WalletID, account.Currency)),
	})
	if err != nil {
		return 0, err
	}
	assetName := fmt.Sprintf("%s/2", account.Currency)
	if amount, ok := balancesMap[assetName]; ok && amount != nil {
		return amount.Int64(), nil
	}
	return 0, nil
}

func reportingAvailableAddress(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:available", walletID, currency)
}

func reportingLienAddress(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:lien", walletID, currency)
}

func withinDateRange(value time.Time, startTime, endTime *time.Time) bool {
	if startTime != nil && value.Before(*startTime) {
		return false
	}
	if endTime != nil && value.After(*endTime) {
		return false
	}
	return true
}
