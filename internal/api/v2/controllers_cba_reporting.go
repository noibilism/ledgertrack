package v2

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/services"
)

func getClientPortfolioReport(reportingService services.ReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := reportingService.ClientPortfolio(r.Context(), common.LedgerFromContext(r.Context()), clientID)
		if err != nil {
			handleReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func getAccountStatementReport(reportingService services.ReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		filter, err := extractStatementReportFilter(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := reportingService.AccountStatement(r.Context(), common.LedgerFromContext(r.Context()), accountID, filter)
		if err != nil {
			handleReportingError(w, r, err)
			return
		}

		transactions := make([]any, 0, len(report.Transactions))
		availableAddress := walletAvailableAddress(report.Account.WalletID, report.Account.Currency)
		for _, tx := range report.Transactions {
			transactions = append(transactions, accountTransactionResponse(tx, availableAddress, report.Account.Currency))
		}

		api.Ok(w, map[string]any{
			"account":           report.Account,
			"start_time":        report.StartTime,
			"end_time":          report.EndTime,
			"opening_balance":   report.OpeningBalance,
			"closing_balance":   report.ClosingBalance,
			"total_credits":     report.TotalCredits,
			"total_debits":      report.TotalDebits,
			"transaction_count": report.TransactionCount,
			"transactions":      transactions,
		})
	}
}

func getDailyTransactionSummaryReport(reportingService services.ReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := extractDailyTransactionSummaryFilter(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := reportingService.DailyTransactionSummary(r.Context(), common.LedgerFromContext(r.Context()), filter)
		if err != nil {
			handleReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func getInterestFeeReport(reportingService services.ReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := extractInterestFeeReportFilter(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := reportingService.InterestFeeSummary(r.Context(), filter)
		if err != nil {
			handleReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func extractStatementReportFilter(r *http.Request) (services.StatementReportFilter, error) {
	startTime, err := parseRFC3339Query(r, "startTime")
	if err != nil {
		return services.StatementReportFilter{}, err
	}
	endTime, err := parseRFC3339Query(r, "endTime")
	if err != nil {
		return services.StatementReportFilter{}, err
	}
	return services.StatementReportFilter{
		StartTime: startTime,
		EndTime:   endTime,
	}, nil
}

func extractDailyTransactionSummaryFilter(r *http.Request) (services.DailyTransactionSummaryFilter, error) {
	dateValue := strings.TrimSpace(r.URL.Query().Get("date"))
	if dateValue == "" {
		return services.DailyTransactionSummaryFilter{}, services.ErrReportingValidation
	}
	reportDate, err := time.Parse("2006-01-02", dateValue)
	if err != nil {
		return services.DailyTransactionSummaryFilter{}, err
	}

	clientID, err := parseUUIDQuery(r, "client_id")
	if err != nil {
		return services.DailyTransactionSummaryFilter{}, err
	}
	accountID, err := parseUUIDQuery(r, "account_id")
	if err != nil {
		return services.DailyTransactionSummaryFilter{}, err
	}

	return services.DailyTransactionSummaryFilter{
		Date:      reportDate,
		ClientID:  clientID,
		AccountID: accountID,
	}, nil
}

func extractInterestFeeReportFilter(r *http.Request) (services.InterestFeeReportFilter, error) {
	clientID, err := parseUUIDQuery(r, "client_id")
	if err != nil {
		return services.InterestFeeReportFilter{}, err
	}
	accountID, err := parseUUIDQuery(r, "account_id")
	if err != nil {
		return services.InterestFeeReportFilter{}, err
	}
	startTime, err := parseRFC3339Query(r, "startTime")
	if err != nil {
		return services.InterestFeeReportFilter{}, err
	}
	endTime, err := parseRFC3339Query(r, "endTime")
	if err != nil {
		return services.InterestFeeReportFilter{}, err
	}

	return services.InterestFeeReportFilter{
		ClientID:  clientID,
		AccountID: accountID,
		StartTime: startTime,
		EndTime:   endTime,
	}, nil
}

func parseUUIDQuery(r *http.Request, key string) (*uuid.UUID, error) {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return nil, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseRFC3339Query(r *http.Request, key string) (*time.Time, error) {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func handleReportingError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case strings.Contains(err.Error(), services.ErrReportingValidation.Error()):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrReportingValidation):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrReportingNotFound):
		api.NotFound(w, err)
	default:
		common.InternalServerError(w, r, err)
	}
}
