package v2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	channelrepos "github.com/formancehq/ledger/internal/channels/repositories"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
)

type channelRevenueReportingServiceForHTTPTests struct{}

func (s *channelRevenueReportingServiceForHTTPTests) RevenueSummary(context.Context, channelservices.ChannelRevenueSummaryFilter) (*channelservices.ChannelRevenueSummaryReport, error) {
	return &channelservices.ChannelRevenueSummaryReport{
		Currency: "USD",
		Rows: []channelservices.ChannelRevenueSummaryRow{
			{
				ChannelID:        "channel-1",
				TransactionCount: 2,
				PrincipalAmount:  2000,
				GrossRevenue:     40,
				ProcessingCost:   10,
				NetRevenue:       30,
				AverageNetPerTx:  15,
				AverageGrossPerTx: 20,
			},
		},
		Totals: channelservices.ChannelRevenueTotals{
			TransactionCount: 2,
			PrincipalAmount:  2000,
			GrossRevenue:     40,
			ProcessingCost:   10,
			NetRevenue:       30,
		},
	}, nil
}

func (s *channelRevenueReportingServiceForHTTPTests) RevenueTimeseries(context.Context, channelservices.ChannelRevenueTimeseriesFilter) (*channelservices.ChannelRevenueTimeseriesReport, error) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	return &channelservices.ChannelRevenueTimeseriesReport{
		Currency:  "USD",
		Interval:  "day",
		StartTime: start,
		EndTime:   end,
		Points: []channelservices.ChannelRevenueTimeseriesPoint{
			{
				StartTime:        start,
				GrossRevenue:     40,
				ProcessingCost:   10,
				NetRevenue:       30,
				TransactionCount: 2,
			},
		},
	}, nil
}

func (s *channelRevenueReportingServiceForHTTPTests) ListFeeRecords(context.Context, channelrepos.ChannelFeeRecordFilter) ([]channelmodels.ChannelFeeRecord, error) {
	return []channelmodels.ChannelFeeRecord{}, nil
}

func TestChannelRevenueSummaryHTTP(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := &channelRevenueReportingServiceForHTTPTests{}
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithChannelRevenueReportingService(svc))

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/reports/channels/revenue", strings.NewReader(`{"currency":"USD"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	resp, ok := api.DecodeSingleResponse[channelservices.ChannelRevenueSummaryReport](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "USD", resp.Currency)
	require.Len(t, resp.Rows, 1)
}

func TestChannelRevenueExportCSVHTTP(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := &channelRevenueReportingServiceForHTTPTests{}
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithChannelRevenueReportingService(svc))

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/reports/channels/revenue/export", strings.NewReader(`{"currency":"USD","format":"csv","report":"summary"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/csv", rec.Header().Get("Content-Type"))
	require.Contains(t, rec.Body.String(), "channel_id,transaction_count")
}

func TestChannelRevenueExportPDFHTTP(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := &channelRevenueReportingServiceForHTTPTests{}
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithChannelRevenueReportingService(svc))

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/reports/channels/revenue/export", strings.NewReader(`{"currency":"USD","format":"pdf","report":"summary"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/pdf", rec.Header().Get("Content-Type"))
	require.True(t, strings.HasPrefix(rec.Body.String(), "%PDF"))
}

func TestChannelRevenueDashboardHTTP(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := &channelRevenueReportingServiceForHTTPTests{}
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithChannelRevenueReportingService(svc))

	req := httptest.NewRequest(http.MethodPost, "/ledgertrack/reports/channels/dashboard", strings.NewReader(`{"currency":"USD"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	resp, ok := api.DecodeSingleResponse[map[string]any](t, rec.Body)
	require.True(t, ok)
	out = resp
	require.Equal(t, "USD", out["currency"])
}
