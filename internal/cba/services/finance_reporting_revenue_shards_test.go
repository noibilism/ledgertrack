package services

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/stretchr/testify/require"

	ledgerinternal "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/hotrows"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

// rvBalanceMatch mirrors filterAccountAddress (internal/storage/ledger/utils.go), the filter the
// aggregated balances endpoint applies to `$match: {address: ...}`:
//   - no empty segment and no trailing "...": exact comparison;
//   - otherwise every non empty segment must equal the account segment at the same index and the
//     segment count must be equal, unless the pattern ends with an empty segment (any length).
func rvBalanceMatch(pattern, address string) bool {
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

// rvTxMatch mirrors filterAccountAddressOnTransactions (internal/storage/ledger/transactions.go),
// the filter ListTransactions applies to `$match: {account: ...}`. It differs from the balances
// filter: for a partial pattern that does not end with "...", the exploded address must carry a
// null marker at index len(pattern segments), so the SEGMENT COUNT MUST EQUAL the pattern's, and
// the trailing empty segment counts. "revenue:accumulated:" therefore matches
// "revenue:accumulated:7" but not "revenue:accumulated".
func rvTxMatch(pattern, address string) bool {
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
	for i, s := range src {
		if s == "" {
			continue
		}
		if i == len(src)-1 && s == "..." {
			break
		}
		if i >= len(acc) || acc[i] != s {
			return false
		}
	}
	if src[len(src)-1] != "..." && len(acc) != len(src) {
		return false
	}
	return true
}

func rvPatterns(t *testing.T, b query.Builder, key string) []string {
	t.Helper()
	var ret []string
	if b == nil {
		return ret
	}
	require.NoError(t, b.Walk(func(_ string, k string, value any) error {
		if k == key {
			ret = append(ret, value.(string))
		}
		return nil
	}))
	return ret
}

type rvLedger struct {
	ledgercontroller.Controller
	t        *testing.T
	balances map[string]int64
	txs      []ledgerinternal.Transaction
	currency string
}

func (f *rvLedger) GetAggregatedBalances(_ context.Context, q storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]) (ledgerinternal.BalancesByAssets, error) {
	patterns := rvPatterns(f.t, q.Builder, "address")
	total := new(big.Int)
	for addr, bal := range f.balances {
		for _, p := range patterns {
			if rvBalanceMatch(p, addr) {
				total.Add(total, big.NewInt(bal))
				break
			}
		}
	}
	return ledgerinternal.BalancesByAssets{f.currency + "/2": total}, nil
}

func (f *rvLedger) ListTransactions(_ context.Context, q storagecommon.PaginatedQuery[any]) (*bunpaginate.Cursor[ledgerinternal.Transaction], error) {
	initial, ok := q.(storagecommon.InitialPaginatedQuery[any])
	require.True(f.t, ok, "unexpected paginated query %T", q)
	patterns := rvPatterns(f.t, initial.Options.Builder, "account")
	var data []ledgerinternal.Transaction
	for _, tx := range f.txs {
	txs:
		for _, p := range tx.Postings {
			for _, pat := range patterns {
				if rvTxMatch(pat, p.Source) || rvTxMatch(pat, p.Destination) {
					data = append(data, tx)
					break txs
				}
			}
		}
	}
	return &bunpaginate.Cursor[ledgerinternal.Transaction]{Data: data}, nil
}

type rvSystem struct {
	systemcontroller.Controller
	t       *testing.T
	ledgers map[string]*rvLedger
}

func (f *rvSystem) CreateLedger(context.Context, string, ledgerinternal.Configuration) error {
	return nil
}

func (f *rvSystem) GetLedgerController(_ context.Context, name string) (ledgercontroller.Controller, error) {
	if l, ok := f.ledgers[name]; ok {
		return l, nil
	}
	return &rvLedger{t: f.t, balances: map[string]int64{}, currency: "USD"}, nil
}

func rvTx(id uint64, ref string, asset string, source, destination string, amount int64) ledgerinternal.Transaction {
	tx := ledgerinternal.NewTransaction().
		WithPostings(ledgerinternal.NewPosting(source, destination, asset, big.NewInt(amount))).
		WithReference(ref).
		WithTimestamp(libtime.Now())
	tx.ID = &id
	return tx
}

// rvRevenueBook holds the same economic content in different layouts: one posting per transfer,
// 40 transfers of 10 and 40 of 5 plus 3 of 7 on the fee income account. Total accumulated: 600.
type rvRevenueBook struct {
	balances map[string]int64
	txs      []ledgerinternal.Transaction
}

func rvBuildBook(layout func(ref string) (source, accumulated string)) rvRevenueBook {
	b := rvRevenueBook{balances: map[string]int64{}}
	var id uint64
	post := func(ref string, amount int64) {
		src, dst := layout(ref)
		b.balances[dst] += amount
		b.balances[src] -= amount
		id++
		b.txs = append(b.txs, rvTx(id, ref+"-user-fee", "USD/2", src, dst, amount))
	}
	for i := 0; i < 40; i++ {
		post(fmt.Sprintf("ref-a-%d", i), 10)
	}
	for i := 0; i < 40; i++ {
		post(fmt.Sprintf("ref-b-%d", i), 5)
	}
	// Not accumulated: must stay out of the revenue figures.
	b.balances["revenue:fee_income"] += 21
	b.balances["revenue:channel_processing_cost"] += 99
	id++
	b.txs = append(b.txs, rvTx(id, "fee-income", "USD/2", "world", "revenue:fee_income", 21))
	return b
}

func rvSystemFor(t *testing.T, b rvRevenueBook) *rvSystem {
	return &rvSystem{t: t, ledgers: map[string]*rvLedger{
		"revenue-USD": {t: t, balances: b.balances, txs: b.txs, currency: "USD"},
	}}
}

func rvAccumulatedLine(t *testing.T, lines []FinanceTrialBalanceLine) FinanceTrialBalanceLine {
	t.Helper()
	for _, l := range lines {
		if l.Ledger == "revenue-USD" && l.Account == "revenue:accumulated" {
			return l
		}
	}
	t.Fatal("revenue:accumulated line not found")
	return FinanceTrialBalanceLine{}
}

func rvWalletRevenue(t *testing.T, r *FinanceProfitAndLossReport) int64 {
	t.Helper()
	for _, l := range r.Income {
		if l.Name == "wallet revenue" {
			return l.Amount
		}
	}
	t.Fatal("wallet revenue line not found")
	return 0
}

func TestRevenueAddressFiltersSemantics(t *testing.T) {
	t.Parallel()

	p := hotrows.Pattern(hotrows.AccumulatedBase)
	require.Equal(t, "revenue:accumulated:", p)

	// Balances filter: base and shards.
	require.True(t, rvBalanceMatch(p, "revenue:accumulated"))
	require.True(t, rvBalanceMatch(p, "revenue:accumulated:0"))
	require.True(t, rvBalanceMatch(p, "revenue:accumulated:15"))
	require.False(t, rvBalanceMatch(p, "revenue:fee_income"))
	require.False(t, rvBalanceMatch(p, "revenue:accumulated_old"))
	// The original bug: an exact match misses every shard.
	require.False(t, rvBalanceMatch(hotrows.AccumulatedBase, "revenue:accumulated:3"))

	// Transactions filter: the trailing empty segment counts, the base address is missed.
	require.True(t, rvTxMatch(p, "revenue:accumulated:7"))
	require.False(t, rvTxMatch(p, "revenue:accumulated"), "the transactions filter needs the base matched separately")
	require.True(t, rvTxMatch(hotrows.AccumulatedBase, "revenue:accumulated"))
	require.False(t, rvTxMatch(hotrows.AccumulatedBase, "revenue:accumulated:7"))
}

// The totals must not depend on how revenue:accumulated is laid out: unsharded, 16 shards, or a
// mix of both after --revenue-shards was turned on (or changed) mid-life.
func TestTrialBalanceAndProfitAndLossAreLayoutIndependent(t *testing.T) {
	t.Parallel()

	svc := NewFinanceReportingService()
	start := stdtime.Now().Add(-rvHour)
	end := stdtime.Now().Add(rvHour)

	layouts := map[string]func(ref string) (string, string){
		"unsharded": func(string) (string, string) { return "world", "revenue:accumulated" },
		"16 shards": func(ref string) (string, string) {
			s := hotrows.ShardFor(ref, 16)
			return fmt.Sprintf("world:%d", s), fmt.Sprintf("revenue:accumulated:%d", s)
		},
		"1 shard that is not 0": func(string) (string, string) { return "world:3", "revenue:accumulated:3" },
		"mixed: base before sharding, 4 then 16 shards": func(ref string) (string, string) {
			switch hotrows.ShardFor(ref, 3) {
			case 0:
				return "world", "revenue:accumulated"
			case 1:
				s := hotrows.ShardFor(ref, 4)
				return fmt.Sprintf("world:%d", s), fmt.Sprintf("revenue:accumulated:%d", s)
			default:
				s := hotrows.ShardFor(ref, 16)
				return fmt.Sprintf("world:%d", s), fmt.Sprintf("revenue:accumulated:%d", s)
			}
		},
	}

	for name, layout := range layouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			book := rvBuildBook(layout)
			sys := rvSystemFor(t, book)

			tb, err := svc.TrialBalance(context.Background(), sys, FinanceTrialBalanceFilter{Currency: "USD"})
			require.NoError(t, err)
			line := rvAccumulatedLine(t, tb.Lines)
			require.EqualValues(t, 600, line.Balance, "trial balance revenue:accumulated")
			require.Equal(t, "revenue:accumulated", line.Account, "the logical account label is unchanged")

			pl, err := svc.ProfitAndLoss(context.Background(), sys, FinanceProfitAndLossFilter{
				Currency:  "USD",
				StartTime: &start,
				EndTime:   &end,
			})
			require.NoError(t, err)
			require.EqualValues(t, 600, rvWalletRevenue(t, pl), "profit and loss wallet revenue")
			require.EqualValues(t, 621, pl.NetProfit, "wallet revenue + fee income")
		})
	}
}

// Charges the old code with the bug: reading only the exact address returns the unsharded part.
func TestExactAddressReadMissesShards(t *testing.T) {
	t.Parallel()

	book := rvBuildBook(func(ref string) (string, string) {
		s := hotrows.ShardFor(ref, 16)
		return fmt.Sprintf("world:%d", s), fmt.Sprintf("revenue:accumulated:%d", s)
	})
	l := &rvLedger{t: t, balances: book.balances, currency: "USD"}
	sys := &rvSystem{t: t, ledgers: map[string]*rvLedger{"revenue-USD": l}}

	exact, err := aggregatedBalance(context.Background(), sys, "revenue-USD", "revenue:accumulated", nil, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 0, exact, "an exact match sees none of the 16 shards")

	all, err := aggregatedBalance(context.Background(), sys, "revenue-USD", hotrows.Pattern(hotrows.AccumulatedBase), nil, "USD")
	require.NoError(t, err)
	require.EqualValues(t, 600, all)
}

const rvHour = stdtime.Hour
