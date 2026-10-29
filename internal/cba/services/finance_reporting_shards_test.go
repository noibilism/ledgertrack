package services

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/formancehq/go-libs/v3/query"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/stretchr/testify/require"

	ledgerinternal "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

// matchesStoreAddress mirrors the semantics of filterAccountAddress in
// internal/storage/ledger/utils.go for a single `$match: {address: ...}`:
//   - a pattern without an empty segment (or trailing "...") is an exact match;
//   - otherwise every non-empty segment must equal the account segment at the
//     same index, and the segment count must be equal unless the pattern ends
//     with an empty segment (prefix match of any length).
func matchesStoreAddress(pattern, address string) bool {
	src := strings.Split(pattern, ":")
	partial := false
	for i, s := range src {
		if s == "" || (s == "..." && i == len(src)-1) {
			partial = true
		}
	}
	if !partial {
		return pattern == address
	}
	acc := strings.Split(address, ":")
	if src[len(src)-1] != "" && len(acc) != len(src) {
		return false
	}
	for i, s := range src {
		if s == "" || s == "..." {
			continue
		}
		if i >= len(acc) || acc[i] != s {
			return false
		}
	}
	return true
}

type fakeReportLedger struct {
	ledgercontroller.Controller
	balances map[string]int64 // address -> balance
	currency string
}

func (f *fakeReportLedger) GetAggregatedBalances(_ context.Context, q storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]) (ledgerinternal.BalancesByAssets, error) {
	var patterns []string
	if err := q.Builder.Walk(func(_ string, key string, value any) error {
		if key == "address" {
			patterns = append(patterns, value.(string))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	total := new(big.Int)
	for addr, bal := range f.balances {
		for _, p := range patterns {
			if matchesStoreAddress(p, addr) {
				total.Add(total, big.NewInt(bal))
				break
			}
		}
	}
	return ledgerinternal.BalancesByAssets{f.currency + "/2": total}, nil
}

type fakeReportSystem struct {
	systemcontroller.Controller
	ledgers map[string]*fakeReportLedger
}

func (f *fakeReportSystem) CreateLedger(context.Context, string, ledgerinternal.Configuration) error {
	return nil
}

func (f *fakeReportSystem) GetLedgerController(_ context.Context, name string) (ledgercontroller.Controller, error) {
	if l, ok := f.ledgers[name]; ok {
		return l, nil
	}
	return &fakeReportLedger{balances: map[string]int64{}, currency: "USD"}, nil
}

func newFakeReportSystem(control map[string]int64) *fakeReportSystem {
	balances := map[string]int64{
		"users:u1:wallets:USD:available": 700,
		"users:u2:wallets:USD:available": 300,
		"users:u1:wallets:USD:lien":      50,
		// Other currency must never leak into USD reports.
		"system:control:EUR":   -999,
		"system:control:EUR:1": -999,
		// Same prefix but a different currency code must not match.
		"system:control:USDT": -111,
	}
	for k, v := range control {
		balances[k] = v
	}
	return &fakeReportSystem{ledgers: map[string]*fakeReportLedger{
		"ledgertrack":  {balances: balances, currency: "USD"},
		"channels-USD": {balances: map[string]int64{"channel:c1": 40, "channel:c2": 10}, currency: "USD"},
	}}
}

func TestSystemControlPatternMatchesBaseAndShardsOnly(t *testing.T) {
	t.Parallel()

	p := systemControlPattern("USD")
	require.True(t, matchesStoreAddress(p, "system:control:USD"))
	require.True(t, matchesStoreAddress(p, "system:control:USD:0"))
	require.True(t, matchesStoreAddress(p, "system:control:USD:31"))
	require.False(t, matchesStoreAddress(p, "system:control:EUR"))
	require.False(t, matchesStoreAddress(p, "system:control:EUR:1"))
	require.False(t, matchesStoreAddress(p, "system:control:USDT"))
	require.False(t, matchesStoreAddress(p, "users:u1:wallets:USD:available"))

	// Documents the original bug: the exact address never matched a shard.
	require.False(t, matchesStoreAddress("system:control:USD", "system:control:USD:3"))
}

func systemLine(t *testing.T, lines []FinanceTrialBalanceLine) FinanceTrialBalanceLine {
	t.Helper()
	for _, l := range lines {
		if l.Ledger == "ledgertrack" && strings.HasPrefix(l.Account, "system:control:") {
			return l
		}
	}
	t.Fatal("system control line not found")
	return FinanceTrialBalanceLine{}
}

func TestTrialBalanceSumsSystemControlShards(t *testing.T) {
	t.Parallel()

	svc := NewFinanceReportingService()
	filter := FinanceTrialBalanceFilter{Currency: "usd"}

	t.Run("unsharded output is unchanged", func(t *testing.T) {
		t.Parallel()
		sys := newFakeReportSystem(map[string]int64{"system:control:USD": -1000})
		report, err := svc.TrialBalance(context.Background(), sys, filter)
		require.NoError(t, err)
		line := systemLine(t, report.Lines)
		require.Equal(t, "system:control:USD", line.Account)
		require.EqualValues(t, -1000, line.Balance)
		require.Len(t, report.Lines, 7)
	})

	t.Run("sharded addresses are summed", func(t *testing.T) {
		t.Parallel()
		control := map[string]int64{}
		var sum int64
		for i := 0; i < 8; i++ {
			v := int64(-(i + 1) * 100)
			control["system:control:USD:"+string(rune('0'+i))] = v
			sum += v
		}
		sys := newFakeReportSystem(control)
		report, err := svc.TrialBalance(context.Background(), sys, filter)
		require.NoError(t, err)
		line := systemLine(t, report.Lines)
		require.Equal(t, "system:control:USD", line.Account, "label stays the logical account")
		require.EqualValues(t, sum, line.Balance)
	})

	t.Run("base plus shards after a shard count change", func(t *testing.T) {
		t.Parallel()
		sys := newFakeReportSystem(map[string]int64{
			"system:control:USD":   -400,
			"system:control:USD:0": -300,
			"system:control:USD:1": -300,
		})
		report, err := svc.TrialBalance(context.Background(), sys, filter)
		require.NoError(t, err)
		require.EqualValues(t, -1000, systemLine(t, report.Lines).Balance)
	})
}

func TestBalanceSheetSumsSystemControlShards(t *testing.T) {
	t.Parallel()

	svc := NewFinanceReportingService()
	sys := newFakeReportSystem(map[string]int64{
		"system:control:USD:0": -600,
		"system:control:USD:1": -400,
	})
	report, err := svc.BalanceSheet(context.Background(), sys, FinanceBalanceSheetFilter{Currency: "USD"})
	require.NoError(t, err)
	require.Equal(t, "system:control:USD", report.Assets[0].Name)
	require.EqualValues(t, -1000, report.Assets[0].Balance)
	// assets: -1000 control + 50 channels; liabilities: 1000 available + 50 lien
	require.EqualValues(t, -1000+50-1000-50, report.NetPosition)
}

func TestCashFlowSumsSystemControlShards(t *testing.T) {
	t.Parallel()

	svc := NewFinanceReportingService()
	sys := newFakeReportSystem(map[string]int64{
		"system:control:USD:0": -600,
		"system:control:USD:5": -400,
	})
	start := libtime.Now().Add(-1 * 3600 * 1e9)
	end := libtime.Now()
	report, err := svc.CashFlow(context.Background(), sys, FinanceCashFlowFilter{
		Currency:  "USD",
		StartTime: &start,
		EndTime:   &end,
	})
	require.NoError(t, err)
	require.Equal(t, "system:control:USD", report.Lines[0].Name)
	require.EqualValues(t, -1000, report.Lines[0].Opening)
	require.EqualValues(t, -1000, report.Lines[0].Closing)
}

// Guards against the query shape drifting: the control account must be queried
// through a single address match with the shard-aware pattern.
func TestAggregatedBalanceUsesAddressMatch(t *testing.T) {
	t.Parallel()

	var seen []string
	b := query.Match("address", systemControlPattern("NGN"))
	require.NoError(t, b.Walk(func(op string, key string, value any) error {
		seen = append(seen, op+" "+key+" "+value.(string))
		return nil
	}))
	require.Equal(t, []string{"$match address system:control:NGN:"}, seen)
}
