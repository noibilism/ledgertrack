package v2

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/auth"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/hotrows"
)

var hrSendRe = regexp.MustCompile(`(?s)send \[USD/2 (\d+)\]\s*\(\s*source = @(\S+)( allowing unbounded overdraft)?\s*destination = @(\S+)\s*\)`)

type hrLeg struct {
	ledgerKind  string // user, channel, revenue
	reference   string
	source      string
	destination string
	amount      int64
	overdraft   bool
}

// hrRecorder plays the ledgers: it records every posting script, keeps one balance book per ledger
// kind and rejects a reused reference like the real ledger does (so a retry is a duplicate).
type hrRecorder struct {
	mu       sync.Mutex
	legs     []hrLeg
	refs     map[string]bool
	balances map[string]map[string]int64
	nextID   uint64
}

func newHRRecorder() *hrRecorder {
	return &hrRecorder{refs: map[string]bool{}, balances: map[string]map[string]int64{
		"user": {"users:u1:wallets:USD:available": 1_000_000_000},
	}}
}

func hrKind(script string) string {
	switch {
	case strings.Contains(script, "revenue:"):
		return "revenue"
	case strings.Contains(script, "channel:"):
		return "channel"
	default:
		return "user"
	}
}

func (r *hrRecorder) createTransaction(t *testing.T) func(context.Context, ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
	return func(_ context.Context, params ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
		script := params.Input.RunScript.Script.Plain
		m := hrSendRe.FindStringSubmatch(script)
		require.NotNil(t, m, "unexpected script %q", script)
		amount, err := strconv.ParseInt(m[1], 10, 64)
		require.NoError(t, err)
		leg := hrLeg{
			ledgerKind:  hrKind(script),
			reference:   params.Input.RunScript.Reference,
			source:      m[2],
			destination: m[4],
			amount:      amount,
			overdraft:   m[3] != "",
		}

		r.mu.Lock()
		defer r.mu.Unlock()
		key := leg.ledgerKind + "/" + leg.reference
		if r.refs[key] {
			return nil, nil, false, ledgercontroller.ErrTransactionReferenceConflict{}
		}
		// The real machine refuses to take more than the balance from any source that is neither
		// `world` nor explicitly allowed to overdraw.
		if leg.source != "world" && !leg.overdraft {
			require.GreaterOrEqual(t, r.balances[leg.ledgerKind][leg.source], leg.amount, "bounded source %s", leg.source)
		}
		r.refs[key] = true
		r.legs = append(r.legs, leg)
		if r.balances[leg.ledgerKind] == nil {
			r.balances[leg.ledgerKind] = map[string]int64{}
		}
		r.balances[leg.ledgerKind][leg.source] -= amount
		r.balances[leg.ledgerKind][leg.destination] += amount

		r.nextID++
		id := r.nextID
		tx := ledger.NewTransaction().WithPostings(ledger.NewPosting(leg.source, leg.destination, "USD/2", big.NewInt(amount)))
		tx.ID = &id
		return &ledger.Log{}, &ledger.CreatedTransaction{Transaction: tx}, false, nil
	}
}

func (r *hrRecorder) legsOf(kind string) []hrLeg {
	var ret []hrLeg
	for _, l := range r.legs {
		if l.ledgerKind == kind {
			ret = append(ret, l)
		}
	}
	return ret
}

func (r *hrRecorder) sumPrefix(kind, base string) int64 {
	var total int64
	for addr, bal := range r.balances[kind] {
		if hotrows.IsShardOf(base, addr) {
			total += bal
		}
	}
	return total
}

func (r *hrRecorder) sumAll(kind string) int64 {
	var total int64
	for _, bal := range r.balances[kind] {
		total += bal
	}
	return total
}

func hrDebit(t *testing.T, router http.Handler, walletID, ref string, amount, channelAmount int) int {
	t.Helper()
	body := fmt.Sprintf(`{"amount":%d,"reference":%q,"channelID":"chan-1","channelAmount":%d}`, amount, ref, channelAmount)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/%s/debit", walletID), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec.Code
}

func hrRouter(t *testing.T, rec *hrRecorder) http.Handler {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(rec.createTransaction(t)).
		AnyTimes()
	return NewRouter(systemController, auth.NewNoAuth(), "develop")
}

// Default configuration: the legs are what they have always been (this guards "defaults unchanged").
func TestChannelDebitLegsAreUnchangedByDefault(t *testing.T) {
	hotrows.SetRevenueShards(1)
	hotrows.SetChannelWorldShards(1)

	rec := newHRRecorder()
	router := hrRouter(t, rec)
	require.Equal(t, http.StatusCreated, hrDebit(t, router, "u1-USD", "ref-default", 1000, 900))

	require.Equal(t, []hrLeg{
		{ledgerKind: "user", reference: "ref-default", source: "users:u1:wallets:USD:available", destination: "system:control:USD", amount: 1000},
		{ledgerKind: "channel", reference: "ref-default", source: "channel:chan-1", destination: "world", amount: 900, overdraft: true},
		{ledgerKind: "revenue", reference: "ref-default-user-fee", source: "world", destination: "revenue:accumulated", amount: 100},
	}, rec.legs)
}

func TestChannelDebitSpreadsHotRowsAndKeepsBooksBalanced(t *testing.T) {
	hotrows.SetRevenueShards(16)
	hotrows.SetChannelWorldShards(16)
	defer hotrows.SetRevenueShards(1)
	defer hotrows.SetChannelWorldShards(1)

	rec := newHRRecorder()
	router := hrRouter(t, rec)

	const transfers = 300
	var wantFees, wantChannel int64
	for i := 0; i < transfers; i++ {
		ref := fmt.Sprintf("ref-%d", i)
		amount, channelAmount := 1000+i, 900
		require.Equal(t, http.StatusCreated, hrDebit(t, router, "u1-USD", ref, amount, channelAmount), ref)
		wantFees += int64(amount - channelAmount)
		wantChannel += int64(channelAmount)
	}

	// Per transfer: all counter accounts follow the shard of the business reference.
	byRef := map[string][]hrLeg{}
	for _, l := range rec.legs {
		base := strings.TrimSuffix(strings.TrimSuffix(l.reference, "-user-fee"), "-processing-fee")
		byRef[base] = append(byRef[base], l)
	}
	require.Len(t, byRef, transfers)
	for ref, legs := range byRef {
		shard := hotrows.ShardFor(ref, 16)
		for _, l := range legs {
			switch l.ledgerKind {
			case "channel":
				require.Equal(t, "channel:chan-1", l.source)
				require.Equal(t, fmt.Sprintf("world:%d", shard), l.destination)
			case "revenue":
				require.Equal(t, fmt.Sprintf("world:%d", shard), l.source)
				require.True(t, l.overdraft, "a sharded counter account is not `world`, it must be allowed to go negative")
				require.Equal(t, fmt.Sprintf("revenue:accumulated:%d", shard), l.destination)
			}
		}
	}

	// Hot rows are really spread: with 300 transfers over 16 shards no single address takes more than 10% of the writes.
	perAddress := map[string]int{}
	for _, l := range rec.legs {
		if l.ledgerKind != "user" {
			perAddress[l.ledgerKind+" "+l.destination]++
			perAddress[l.ledgerKind+" "+l.source]++
		}
	}
	for addr, n := range perAddress {
		if strings.Contains(addr, "chan-1") {
			continue // the channel account itself is not sharded
		}
		require.LessOrEqual(t, n, transfers/10+10, addr)
	}

	// Conservation: every ledger nets to zero, and the sum over the shards equals the unsharded totals.
	require.EqualValues(t, 0, rec.sumAll("revenue"))
	require.EqualValues(t, 0, rec.sumAll("channel"))
	require.EqualValues(t, wantFees, rec.sumPrefix("revenue", hotrows.AccumulatedBase))
	require.EqualValues(t, -wantChannel, rec.balances["channel"]["channel:chan-1"])
	require.EqualValues(t, -wantFees, rec.sumPrefix("revenue", "world"))
	require.EqualValues(t, wantChannel, rec.sumPrefix("channel", "world"))
	require.Zero(t, rec.balances["revenue"]["world"], "no posting touched the plain world account")
	require.Zero(t, rec.balances["channel"]["world"], "no posting touched the plain world account")
}

// A client retry, the recovery runner and a second process all re-post the same business reference.
// They must pick the same accounts so that the ledger recognises the duplicate reference, instead of
// posting the fee a second time on another shard.
func TestRetryOfARevenueLegLandsOnTheSameShard(t *testing.T) {
	hotrows.SetRevenueShards(16)
	defer hotrows.SetRevenueShards(1)

	rec := newHRRecorder()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().CreateTransaction(gomock.Any(), gomock.Any()).DoAndReturn(rec.createTransaction(t)).AnyTimes()

	for i := 0; i < 50; i++ {
		ref := fmt.Sprintf("retry-%d", i)
		_, firstTx, err := postRevenueEntries(context.Background(), systemController, "USD", ref, 70, 20)
		require.NoError(t, err)
		require.NotNil(t, firstTx)
		before := len(rec.legs)

		// Second attempt: the legs already exist, the ledger answers "duplicate reference" and nothing is posted.
		_, _, err = postRevenueEntries(context.Background(), systemController, "USD", ref, 70, 20)
		require.ErrorIs(t, err, ledgercontroller.ErrTransactionReferenceConflict{}, "a retry must hit the same reference on the same ledger, not create a new posting")
		require.Len(t, rec.legs, before)

		// And the addresses used are the ones derived from the reference alone.
		shard := hotrows.ShardFor(ref, 16)
		require.Equal(t, fmt.Sprintf("revenue:accumulated:%d", shard), rec.legs[before-2].destination)
		require.Equal(t, fmt.Sprintf("revenue:channel_processing_cost:%d", shard), rec.legs[before-1].destination)
	}
	require.EqualValues(t, 0, rec.sumAll("revenue"))
}
