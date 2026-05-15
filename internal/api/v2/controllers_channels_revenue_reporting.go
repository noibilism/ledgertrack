package v2

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/phpdave11/gofpdf"

	"github.com/formancehq/ledger/internal/api/common"
	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	channelrepos "github.com/formancehq/ledger/internal/channels/repositories"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
)

type channelRevenueSummaryRequest struct {
	Currency  string  `json:"currency"`
	ChannelID *string `json:"channel_id,omitempty"`
	StartTime *string `json:"start_time,omitempty"`
	EndTime   *string `json:"end_time,omitempty"`
	Limit     int     `json:"limit,omitempty"`
	Offset    int     `json:"offset,omitempty"`
}

type channelRevenueTimeseriesRequest struct {
	Currency  string  `json:"currency"`
	Interval  string  `json:"interval,omitempty"`
	ChannelID *string `json:"channel_id,omitempty"`
	StartTime string  `json:"start_time"`
	EndTime   string  `json:"end_time"`
}

type channelRevenueExportRequest struct {
	Format   string  `json:"format,omitempty"`
	Report   string  `json:"report,omitempty"`
	Currency string  `json:"currency"`

	ChannelID *string `json:"channel_id,omitempty"`
	StartTime *string `json:"start_time,omitempty"`
	EndTime   *string `json:"end_time,omitempty"`
	Interval  string  `json:"interval,omitempty"`
	Limit     int     `json:"limit,omitempty"`
	Offset    int     `json:"offset,omitempty"`
}

type channelRevenueDashboardRequest struct {
	Currency string `json:"currency"`
	TopN     int    `json:"top_n,omitempty"`
	Months   int    `json:"months,omitempty"`
}

func getChannelRevenueSummaryReport(reportingService channelservices.ChannelRevenueReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req channelRevenueSummaryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		startTime, err := parseRFC3339Ptr(req.StartTime)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		endTime, err := parseRFC3339Ptr(req.EndTime)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := reportingService.RevenueSummary(r.Context(), channelservices.ChannelRevenueSummaryFilter{
			Currency:  req.Currency,
			ChannelID: req.ChannelID,
			StartTime:  startTime,
			EndTime:    endTime,
			Limit:      req.Limit,
			Offset:     req.Offset,
		})
		if err != nil {
			handleChannelRevenueReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func getChannelRevenueTimeseriesReport(reportingService channelservices.ChannelRevenueReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req channelRevenueTimeseriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		startTime, err := time.Parse(time.RFC3339, strings.TrimSpace(req.StartTime))
		if err != nil {
			api.BadRequest(w, common.ErrValidation, errors.New("invalid start_time"))
			return
		}
		endTime, err := time.Parse(time.RFC3339, strings.TrimSpace(req.EndTime))
		if err != nil {
			api.BadRequest(w, common.ErrValidation, errors.New("invalid end_time"))
			return
		}

		report, err := reportingService.RevenueTimeseries(r.Context(), channelservices.ChannelRevenueTimeseriesFilter{
			Currency:  req.Currency,
			StartTime: startTime,
			EndTime:   endTime,
			Interval:  req.Interval,
			ChannelID: req.ChannelID,
		})
		if err != nil {
			handleChannelRevenueReportingError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

func exportChannelRevenueReport(reportingService channelservices.ChannelRevenueReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req channelRevenueExportRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		format := strings.ToLower(strings.TrimSpace(req.Format))
		if format == "" {
			format = "csv"
		}
		if format != "csv" && format != "pdf" {
			api.BadRequest(w, common.ErrValidation, errors.New("unsupported format"))
			return
		}

		reportType := strings.ToLower(strings.TrimSpace(req.Report))
		if reportType == "" {
			reportType = "summary"
		}
		if reportType != "summary" && reportType != "timeseries" && reportType != "records" {
			api.BadRequest(w, common.ErrValidation, errors.New("unsupported report"))
			return
		}

		startTime, err := parseRFC3339Ptr(req.StartTime)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		endTime, err := parseRFC3339Ptr(req.EndTime)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		switch {
		case reportType == "summary" && format == "csv":
			report, err := reportingService.RevenueSummary(r.Context(), channelservices.ChannelRevenueSummaryFilter{
				Currency:  req.Currency,
				ChannelID: req.ChannelID,
				StartTime:  startTime,
				EndTime:    endTime,
				Limit:      req.Limit,
				Offset:     req.Offset,
			})
			if err != nil {
				handleChannelRevenueReportingError(w, r, err)
				return
			}
			writeSummaryCSV(w, report)
			return

		case reportType == "summary" && format == "pdf":
			report, err := reportingService.RevenueSummary(r.Context(), channelservices.ChannelRevenueSummaryFilter{
				Currency:  req.Currency,
				ChannelID: req.ChannelID,
				StartTime:  startTime,
				EndTime:    endTime,
				Limit:      req.Limit,
				Offset:     req.Offset,
			})
			if err != nil {
				handleChannelRevenueReportingError(w, r, err)
				return
			}
			writeSummaryPDF(w, report)
			return

		case reportType == "timeseries":
			if startTime == nil || endTime == nil {
				api.BadRequest(w, common.ErrValidation, errors.New("start_time and end_time are required"))
				return
			}
			report, err := reportingService.RevenueTimeseries(r.Context(), channelservices.ChannelRevenueTimeseriesFilter{
				Currency:  req.Currency,
				StartTime: *startTime,
				EndTime:   *endTime,
				Interval:  req.Interval,
				ChannelID: req.ChannelID,
			})
			if err != nil {
				handleChannelRevenueReportingError(w, r, err)
				return
			}
			if format == "csv" {
				writeTimeseriesCSV(w, report)
			} else {
				writeTimeseriesPDF(w, report)
			}
			return

		default:
			filter := channelrepos.ChannelFeeRecordFilter{
				ChannelID: req.ChannelID,
				Limit:     req.Limit,
				Offset:    req.Offset,
			}
			if c := strings.TrimSpace(req.Currency); c != "" {
				cc := strings.ToUpper(c)
				filter.Currency = &cc
			}
			filter.StartTime = startTime
			filter.EndTime = endTime

			records, err := reportingService.ListFeeRecords(r.Context(), filter)
			if err != nil {
				handleChannelRevenueReportingError(w, r, err)
				return
			}
			if format == "csv" {
				writeRecordsCSV(w, records)
				return
			}
			api.WriteErrorResponse(w, http.StatusNotImplemented, api.ErrorInternal, errors.New("pdf export for records is not supported"))
			return
		}
	}
}

func getChannelRevenueDashboardMetrics(reportingService channelservices.ChannelRevenueReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req channelRevenueDashboardRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		if strings.TrimSpace(req.Currency) == "" {
			api.BadRequest(w, common.ErrValidation, errors.New("currency is required"))
			return
		}

		topN := req.TopN
		if topN <= 0 {
			topN = 5
		}
		months := req.Months
		if months <= 0 {
			months = 6
		}

		now := time.Now().UTC()
		currentStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		prevStart := currentStart.AddDate(0, -1, 0)
		prevEnd := currentStart.Add(-time.Nanosecond)

		currentAll, err := reportingService.RevenueSummary(r.Context(), channelservices.ChannelRevenueSummaryFilter{
			Currency:  req.Currency,
			StartTime:  &currentStart,
			EndTime:    &now,
		})
		if err != nil {
			handleChannelRevenueReportingError(w, r, err)
			return
		}

		currentTop, err := reportingService.RevenueSummary(r.Context(), channelservices.ChannelRevenueSummaryFilter{
			Currency:  req.Currency,
			StartTime:  &currentStart,
			EndTime:    &now,
			Limit:      topN,
			Offset:     0,
		})
		if err != nil {
			handleChannelRevenueReportingError(w, r, err)
			return
		}

		prevAll, err := reportingService.RevenueSummary(r.Context(), channelservices.ChannelRevenueSummaryFilter{
			Currency:  req.Currency,
			StartTime:  &prevStart,
			EndTime:    &prevEnd,
		})
		if err != nil {
			handleChannelRevenueReportingError(w, r, err)
			return
		}

		var momGrowthPercent *float64
		if prevAll.Totals.NetRevenue != 0 {
			p := (float64(currentAll.Totals.NetRevenue-prevAll.Totals.NetRevenue) / float64(prevAll.Totals.NetRevenue)) * 100
			momGrowthPercent = &p
		}

		trendStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -(months-1), 0)
		trend, err := reportingService.RevenueTimeseries(r.Context(), channelservices.ChannelRevenueTimeseriesFilter{
			Currency:  req.Currency,
			StartTime: trendStart,
			EndTime:   now,
			Interval:  "month",
		})
		if err != nil {
			handleChannelRevenueReportingError(w, r, err)
			return
		}

		api.Ok(w, map[string]any{
			"currency": req.Currency,
			"current_period": map[string]any{
				"start_time": currentStart,
				"end_time":   now,
			},
			"net_revenue_current":   currentAll.Totals.NetRevenue,
			"net_revenue_previous":  prevAll.Totals.NetRevenue,
			"mom_growth_percent":    momGrowthPercent,
			"top_channels_by_net":   currentTop.Rows,
			"month_over_month_trend": trend,
		})
	}
}

func parseRFC3339Ptr(v *string) (*time.Time, error) {
	if v == nil {
		return nil, nil
	}
	value := strings.TrimSpace(*v)
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func handleChannelRevenueReportingError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case strings.Contains(err.Error(), channelservices.ErrChannelRevenueValidation.Error()):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, channelservices.ErrChannelRevenueValidation):
		api.BadRequest(w, common.ErrValidation, err)
	default:
		common.InternalServerError(w, r, err)
	}
}

func writeSummaryCSV(w http.ResponseWriter, report *channelservices.ChannelRevenueSummaryReport) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	_ = writer.Write([]string{
		"channel_id",
		"transaction_count",
		"principal_amount",
		"gross_revenue",
		"processing_cost",
		"net_revenue",
		"average_net_revenue_per_transaction",
		"average_gross_revenue_per_transaction",
	})
	for _, row := range report.Rows {
		_ = writer.Write([]string{
			row.ChannelID,
			strconv.FormatInt(row.TransactionCount, 10),
			strconv.FormatInt(row.PrincipalAmount, 10),
			strconv.FormatInt(row.GrossRevenue, 10),
			strconv.FormatInt(row.ProcessingCost, 10),
			strconv.FormatInt(row.NetRevenue, 10),
			strconv.FormatInt(row.AverageNetPerTx, 10),
			strconv.FormatInt(row.AverageGrossPerTx, 10),
		})
	}
	_ = writer.Write([]string{
		"TOTALS",
		strconv.FormatInt(report.Totals.TransactionCount, 10),
		strconv.FormatInt(report.Totals.PrincipalAmount, 10),
		strconv.FormatInt(report.Totals.GrossRevenue, 10),
		strconv.FormatInt(report.Totals.ProcessingCost, 10),
		strconv.FormatInt(report.Totals.NetRevenue, 10),
		"",
		"",
	})
	writer.Flush()

	filename := fmt.Sprintf("channel_revenue_summary_%s.csv", strings.ToLower(report.Currency))
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func writeTimeseriesCSV(w http.ResponseWriter, report *channelservices.ChannelRevenueTimeseriesReport) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	_ = writer.Write([]string{
		"start_time",
		"gross_revenue",
		"processing_cost",
		"net_revenue",
		"transaction_count",
	})
	for _, p := range report.Points {
		_ = writer.Write([]string{
			p.StartTime.UTC().Format(time.RFC3339),
			strconv.FormatInt(p.GrossRevenue, 10),
			strconv.FormatInt(p.ProcessingCost, 10),
			strconv.FormatInt(p.NetRevenue, 10),
			strconv.FormatInt(p.TransactionCount, 10),
		})
	}
	writer.Flush()

	filename := fmt.Sprintf("channel_revenue_timeseries_%s_%s.csv", strings.ToLower(report.Currency), report.Interval)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func writeRecordsCSV(w http.ResponseWriter, records []channelmodels.ChannelFeeRecord) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	_ = writer.Write([]string{
		"occurred_at",
		"channel_id",
		"currency",
		"wallet_id",
		"reference",
		"total_amount",
		"principal_amount",
		"user_fee_amount",
		"processing_fee_amount",
		"net_revenue_amount",
		"ledger_tx_id",
		"channel_tx_id",
		"revenue_tx_id",
	})
	for _, rec := range records {
		var walletID string
		if rec.WalletID != nil {
			walletID = *rec.WalletID
		}
		var ledgerTxID string
		if rec.LedgerTxID != nil {
			ledgerTxID = strconv.FormatInt(*rec.LedgerTxID, 10)
		}
		var channelTxID string
		if rec.ChannelTxID != nil {
			channelTxID = strconv.FormatInt(*rec.ChannelTxID, 10)
		}
		var revenueTxID string
		if rec.RevenueTxID != nil {
			revenueTxID = strconv.FormatInt(*rec.RevenueTxID, 10)
		}

		_ = writer.Write([]string{
			rec.OccurredAt.UTC().Format(time.RFC3339),
			rec.ChannelID,
			rec.Currency,
			walletID,
			rec.Reference,
			strconv.FormatInt(rec.TotalAmount, 10),
			strconv.FormatInt(rec.PrincipalAmount, 10),
			strconv.FormatInt(rec.UserFeeAmount, 10),
			strconv.FormatInt(rec.ProcessingFeeAmount, 10),
			strconv.FormatInt(rec.NetRevenueAmount, 10),
			ledgerTxID,
			channelTxID,
			revenueTxID,
		})
	}
	writer.Flush()

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=\"channel_fee_records.csv\"")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func writeSummaryPDF(w http.ResponseWriter, report *channelservices.ChannelRevenueSummaryReport) {
	pdf := gofpdf.New("L", "mm", "A4", "")
	pdf.SetMargins(10, 10, 10)
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(0, 8, fmt.Sprintf("Channel Revenue Summary (%s)", report.Currency))
	pdf.Ln(10)

	pdf.SetFont("Arial", "", 10)
	pdf.Cell(0, 6, fmt.Sprintf("Totals: tx=%d gross=%d cost=%d net=%d", report.Totals.TransactionCount, report.Totals.GrossRevenue, report.Totals.ProcessingCost, report.Totals.NetRevenue))
	pdf.Ln(8)

	pdf.SetFont("Arial", "B", 9)
	headers := []string{"Channel", "Tx", "Principal", "Gross", "Cost", "Net", "AvgNet", "AvgGross"}
	widths := []float64{40, 18, 28, 28, 28, 28, 24, 24}
	for i, h := range headers {
		pdf.CellFormat(widths[i], 6, h, "1", 0, "C", false, 0, "")
	}
	pdf.Ln(-1)

	pdf.SetFont("Arial", "", 9)
	for _, row := range report.Rows {
		values := []string{
			row.ChannelID,
			strconv.FormatInt(row.TransactionCount, 10),
			strconv.FormatInt(row.PrincipalAmount, 10),
			strconv.FormatInt(row.GrossRevenue, 10),
			strconv.FormatInt(row.ProcessingCost, 10),
			strconv.FormatInt(row.NetRevenue, 10),
			strconv.FormatInt(row.AverageNetPerTx, 10),
			strconv.FormatInt(row.AverageGrossPerTx, 10),
		}
		for i, v := range values {
			pdf.CellFormat(widths[i], 6, v, "1", 0, "", false, 0, "")
		}
		pdf.Ln(-1)
	}

	filename := fmt.Sprintf("channel_revenue_summary_%s.pdf", strings.ToLower(report.Currency))
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.WriteHeader(http.StatusOK)
	_ = pdf.Output(w)
}

func writeTimeseriesPDF(w http.ResponseWriter, report *channelservices.ChannelRevenueTimeseriesReport) {
	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetMargins(10, 10, 10)
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(0, 8, fmt.Sprintf("Channel Revenue Timeseries (%s, %s)", report.Currency, report.Interval))
	pdf.Ln(10)

	pdf.SetFont("Arial", "B", 9)
	headers := []string{"StartTime", "Gross", "Cost", "Net", "Tx"}
	widths := []float64{50, 35, 35, 35, 20}
	for i, h := range headers {
		pdf.CellFormat(widths[i], 6, h, "1", 0, "C", false, 0, "")
	}
	pdf.Ln(-1)

	pdf.SetFont("Arial", "", 9)
	for _, p := range report.Points {
		values := []string{
			p.StartTime.UTC().Format(time.RFC3339),
			strconv.FormatInt(p.GrossRevenue, 10),
			strconv.FormatInt(p.ProcessingCost, 10),
			strconv.FormatInt(p.NetRevenue, 10),
			strconv.FormatInt(p.TransactionCount, 10),
		}
		for i, v := range values {
			pdf.CellFormat(widths[i], 6, v, "1", 0, "", false, 0, "")
		}
		pdf.Ln(-1)
	}

	filename := fmt.Sprintf("channel_revenue_timeseries_%s_%s.pdf", strings.ToLower(report.Currency), report.Interval)
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.WriteHeader(http.StatusOK)
	_ = pdf.Output(w)
}
