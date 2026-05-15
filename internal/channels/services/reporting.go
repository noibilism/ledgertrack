package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/formancehq/ledger/internal/channels/models"
	"github.com/formancehq/ledger/internal/channels/repositories"
)

var (
	ErrChannelRevenueValidation = errors.New("channel revenue validation failed")
)

type ChannelRevenueReportingService interface {
	RevenueSummary(context.Context, ChannelRevenueSummaryFilter) (*ChannelRevenueSummaryReport, error)
	RevenueTimeseries(context.Context, ChannelRevenueTimeseriesFilter) (*ChannelRevenueTimeseriesReport, error)
	ListFeeRecords(context.Context, repositories.ChannelFeeRecordFilter) ([]models.ChannelFeeRecord, error)
}

type ChannelRevenueSummaryFilter struct {
	Currency   string
	ChannelID  *string
	StartTime  *time.Time
	EndTime    *time.Time
	Limit      int
	Offset     int
}

type ChannelRevenueTimeseriesFilter struct {
	Currency  string
	StartTime time.Time
	EndTime   time.Time
	Interval  string
	ChannelID *string
}

type ChannelRevenueSummaryRow struct {
	ChannelID           string `json:"channel_id"`
	TransactionCount    int64  `json:"transaction_count"`
	PrincipalAmount     int64  `json:"principal_amount"`
	GrossRevenue        int64  `json:"gross_revenue"`
	ProcessingCost      int64  `json:"processing_cost"`
	NetRevenue          int64  `json:"net_revenue"`
	AverageNetPerTx     int64  `json:"average_net_revenue_per_transaction"`
	AverageGrossPerTx   int64  `json:"average_gross_revenue_per_transaction"`
}

type ChannelRevenueTotals struct {
	TransactionCount int64 `json:"transaction_count"`
	PrincipalAmount  int64 `json:"principal_amount"`
	GrossRevenue     int64 `json:"gross_revenue"`
	ProcessingCost   int64 `json:"processing_cost"`
	NetRevenue       int64 `json:"net_revenue"`
}

type ChannelRevenueSummaryReport struct {
	Currency   string                 `json:"currency"`
	StartTime  *time.Time             `json:"start_time,omitempty"`
	EndTime    *time.Time             `json:"end_time,omitempty"`
	Rows       []ChannelRevenueSummaryRow `json:"rows"`
	Totals     ChannelRevenueTotals   `json:"totals"`
}

type ChannelRevenueTimeseriesPoint struct {
	StartTime       time.Time `json:"start_time"`
	GrossRevenue    int64     `json:"gross_revenue"`
	ProcessingCost  int64     `json:"processing_cost"`
	NetRevenue      int64     `json:"net_revenue"`
	TransactionCount int64    `json:"transaction_count"`
}

type ChannelRevenueTimeseriesReport struct {
	Currency  string                      `json:"currency"`
	Interval  string                      `json:"interval"`
	StartTime time.Time                   `json:"start_time"`
	EndTime   time.Time                   `json:"end_time"`
	Points    []ChannelRevenueTimeseriesPoint `json:"points"`
}

type DefaultChannelRevenueReportingService struct {
	db        bun.IDB
	recordRepo repositories.ChannelFeeRecordRepository
}

func NewChannelRevenueReportingService(db bun.IDB, recordRepo repositories.ChannelFeeRecordRepository) ChannelRevenueReportingService {
	return &DefaultChannelRevenueReportingService{
		db:         db,
		recordRepo: recordRepo,
	}
}

func (s *DefaultChannelRevenueReportingService) RevenueSummary(ctx context.Context, filter ChannelRevenueSummaryFilter) (*ChannelRevenueSummaryReport, error) {
	filter.Currency = strings.ToUpper(strings.TrimSpace(filter.Currency))
	if filter.Currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrChannelRevenueValidation)
	}

	type row struct {
		ChannelID        string `bun:"channel_id"`
		TxCount          int64  `bun:"tx_count"`
		PrincipalAmount  int64  `bun:"principal_amount"`
		GrossRevenue     int64  `bun:"gross_revenue"`
		ProcessingCost   int64  `bun:"processing_cost"`
		NetRevenue       int64  `bun:"net_revenue"`
	}

	rows := make([]row, 0)
	q := s.db.NewSelect().
		TableExpr("_system.channel_fee_records").
		Column("channel_id").
		ColumnExpr("count(*) as tx_count").
		ColumnExpr("coalesce(sum(principal_amount), 0) as principal_amount").
		ColumnExpr("coalesce(sum(user_fee_amount), 0) as gross_revenue").
		ColumnExpr("coalesce(sum(processing_fee_amount), 0) as processing_cost").
		ColumnExpr("coalesce(sum(net_revenue_amount), 0) as net_revenue").
		Where("currency = ?", filter.Currency).
		GroupExpr("channel_id").
		OrderExpr("net_revenue desc")
	if filter.ChannelID != nil {
		q = q.Where("channel_id = ?", *filter.ChannelID)
	}
	if filter.StartTime != nil {
		q = q.Where("occurred_at >= ?", *filter.StartTime)
	}
	if filter.EndTime != nil {
		q = q.Where("occurred_at <= ?", *filter.EndTime)
	}
	if filter.Limit > 0 {
		q = q.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		q = q.Offset(filter.Offset)
	}

	if err := q.Scan(ctx, &rows); err != nil {
		return nil, err
	}

	reportRows := make([]ChannelRevenueSummaryRow, 0, len(rows))
	totals := ChannelRevenueTotals{}
	for _, r := range rows {
		avgNet := int64(0)
		avgGross := int64(0)
		if r.TxCount > 0 {
			avgNet = r.NetRevenue / r.TxCount
			avgGross = r.GrossRevenue / r.TxCount
		}
		reportRows = append(reportRows, ChannelRevenueSummaryRow{
			ChannelID:         r.ChannelID,
			TransactionCount:  r.TxCount,
			PrincipalAmount:   r.PrincipalAmount,
			GrossRevenue:      r.GrossRevenue,
			ProcessingCost:    r.ProcessingCost,
			NetRevenue:        r.NetRevenue,
			AverageNetPerTx:   avgNet,
			AverageGrossPerTx: avgGross,
		})
		totals.TransactionCount += r.TxCount
		totals.PrincipalAmount += r.PrincipalAmount
		totals.GrossRevenue += r.GrossRevenue
		totals.ProcessingCost += r.ProcessingCost
		totals.NetRevenue += r.NetRevenue
	}

	return &ChannelRevenueSummaryReport{
		Currency:  filter.Currency,
		StartTime: filter.StartTime,
		EndTime:   filter.EndTime,
		Rows:      reportRows,
		Totals:    totals,
	}, nil
}

func (s *DefaultChannelRevenueReportingService) RevenueTimeseries(ctx context.Context, filter ChannelRevenueTimeseriesFilter) (*ChannelRevenueTimeseriesReport, error) {
	filter.Currency = strings.ToUpper(strings.TrimSpace(filter.Currency))
	if filter.Currency == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrChannelRevenueValidation)
	}
	interval := strings.ToLower(strings.TrimSpace(filter.Interval))
	if interval == "" {
		interval = "day"
	}
	switch interval {
	case "day", "week", "month":
	default:
		return nil, fmt.Errorf("%w: unsupported interval", ErrChannelRevenueValidation)
	}

	type row struct {
		BucketStart time.Time `bun:"bucket_start"`
		Gross       int64     `bun:"gross_revenue"`
		Cost        int64     `bun:"processing_cost"`
		Net         int64     `bun:"net_revenue"`
		Count       int64     `bun:"tx_count"`
	}

	rows := make([]row, 0)
	q := s.db.NewSelect().
		TableExpr("_system.channel_fee_records").
		ColumnExpr("date_trunc(?, occurred_at) as bucket_start", interval).
		ColumnExpr("coalesce(sum(user_fee_amount), 0) as gross_revenue").
		ColumnExpr("coalesce(sum(processing_fee_amount), 0) as processing_cost").
		ColumnExpr("coalesce(sum(net_revenue_amount), 0) as net_revenue").
		ColumnExpr("count(*) as tx_count").
		Where("currency = ?", filter.Currency).
		Where("occurred_at >= ?", filter.StartTime).
		Where("occurred_at <= ?", filter.EndTime).
		GroupExpr("bucket_start").
		OrderExpr("bucket_start asc")
	if filter.ChannelID != nil {
		q = q.Where("channel_id = ?", *filter.ChannelID)
	}
	if err := q.Scan(ctx, &rows); err != nil {
		return nil, err
	}

	points := make([]ChannelRevenueTimeseriesPoint, 0, len(rows))
	for _, r := range rows {
		points = append(points, ChannelRevenueTimeseriesPoint{
			StartTime:        r.BucketStart.UTC(),
			GrossRevenue:     r.Gross,
			ProcessingCost:   r.Cost,
			NetRevenue:       r.Net,
			TransactionCount: r.Count,
		})
	}

	return &ChannelRevenueTimeseriesReport{
		Currency:  filter.Currency,
		Interval:  interval,
		StartTime: filter.StartTime.UTC(),
		EndTime:   filter.EndTime.UTC(),
		Points:    points,
	}, nil
}

func (s *DefaultChannelRevenueReportingService) ListFeeRecords(ctx context.Context, filter repositories.ChannelFeeRecordFilter) ([]models.ChannelFeeRecord, error) {
	return s.recordRepo.List(ctx, filter)
}

