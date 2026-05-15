package v2

import (
	"errors"
	"net/http"
	"strings"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/api"
	libtime "github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/services"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func getFinanceTrialBalanceReport(sys systemcontroller.Controller, financeReportingService services.FinanceReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		asOf, err := getDate(r, "asOf")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := financeReportingService.TrialBalance(r.Context(), sys, services.FinanceTrialBalanceFilter{
			Currency:               r.URL.Query().Get("currency"),
			AsOf:                   asOf,
			FeeIncomeAccount:       r.URL.Query().Get("fee_income_account"),
			InterestExpenseAccount: r.URL.Query().Get("interest_expense_account"),
		})
		if err != nil {
			handleFinanceReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func getFinanceBalanceSheetReport(sys systemcontroller.Controller, financeReportingService services.FinanceReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		asOf, err := getDate(r, "asOf")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := financeReportingService.BalanceSheet(r.Context(), sys, services.FinanceBalanceSheetFilter{
			Currency:               r.URL.Query().Get("currency"),
			AsOf:                   asOf,
			FeeIncomeAccount:       r.URL.Query().Get("fee_income_account"),
			InterestExpenseAccount: r.URL.Query().Get("interest_expense_account"),
		})
		if err != nil {
			handleFinanceReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func getFinanceProfitAndLossReport(sys systemcontroller.Controller, financeReportingService services.FinanceReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime, err := getDate(r, "startTime")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		endTime, err := getDate(r, "endTime")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := financeReportingService.ProfitAndLoss(r.Context(), sys, services.FinanceProfitAndLossFilter{
			Currency:               r.URL.Query().Get("currency"),
			StartTime:              toStdTimePtr(startTime),
			EndTime:                toStdTimePtr(endTime),
			FeeIncomeAccount:       r.URL.Query().Get("fee_income_account"),
			InterestExpenseAccount: r.URL.Query().Get("interest_expense_account"),
		})
		if err != nil {
			handleFinanceReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func getFinanceCashFlowReport(sys systemcontroller.Controller, financeReportingService services.FinanceReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime, err := getDate(r, "startTime")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		endTime, err := getDate(r, "endTime")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := financeReportingService.CashFlow(r.Context(), sys, services.FinanceCashFlowFilter{
			Currency:               r.URL.Query().Get("currency"),
			StartTime:              startTime,
			EndTime:                endTime,
			FeeIncomeAccount:       r.URL.Query().Get("fee_income_account"),
			InterestExpenseAccount: r.URL.Query().Get("interest_expense_account"),
		})
		if err != nil {
			handleFinanceReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func handleFinanceReportingError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrFinanceReportingValidation):
		api.BadRequest(w, common.ErrValidation, err)
	default:
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "missing feature") {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		common.HandleCommonErrors(w, r, err)
	}
}

func toStdTimePtr(t *libtime.Time) *stdtime.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	x := t.Time.UTC()
	return &x
}
