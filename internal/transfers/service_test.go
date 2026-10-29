package transfers_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"

	currencyregistry "github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/transfers"
	"github.com/formancehq/ledger/internal/transfers/transferstest"
	"github.com/google/uuid"
)

func init() {
	currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{
		"NGN":  {Precision: 2, Enabled: true},
		"USDT": {Precision: 6, Enabled: true},
	})
}

const (
	userLedger    = "ledgertrack"
	channelLedger = "channels-NGN"
	revenueLedger = "revenue-NGN"
	asset         = "NGN/2"
	startFunds    = int64(10_000)
)

// ---- fault injection helpers -------------------------------------------------

type crash struct{ at string }

// runCrashing runs f and reports whether it "crashed" (panicked with crash).
// A crash skips every deferred-free cleanup in the service, so leases stay
// held exactly like after a killed process.
func runCrashing(t *testing.T, f func()) (crashed bool) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(crash); !ok {
				panic(r)
			}
			crashed = true
		}
	}()
	f()
	return false
}

// faultExec wraps a LegExecutor and can crash or fail around any leg.
type faultExec struct {
	inner transfers.LegExecutor
	mu    sync.Mutex
	// crashAfter panics after the named leg succeeded (before the orchestrator
	// persists the new state). Consumed on use.
	crashAfter map[string]bool
	// failAfter returns an error after the named leg succeeded (response lost).
	failAfter map[string]bool
	// failBefore returns an error before the named leg runs. Not consumed.
	failBefore map[string]bool
}

func newFaultExec(inner transfers.LegExecutor) *faultExec {
	return &faultExec{inner: inner, crashAfter: map[string]bool{}, failAfter: map[string]bool{}, failBefore: map[string]bool{}}
}

func (f *faultExec) take(m map[string]bool, leg string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m[leg] {
		delete(m, leg)
		return true
	}
	return false
}

func (f *faultExec) isSet(m map[string]bool, leg string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return m[leg]
}

func (f *faultExec) after(leg string) error {
	if f.take(f.crashAfter, leg) {
		panic(crash{at: "after " + leg})
	}
	if f.take(f.failAfter, leg) {
		return fmt.Errorf("injected failure after %s", leg)
	}
	return nil
}

func (f *faultExec) before(leg string) error {
	if f.isSet(f.failBefore, leg) {
		return fmt.Errorf("injected failure before %s", leg)
	}
	return nil
}

func (f *faultExec) PostUser(ctx context.Context, t *transfers.Transfer) (transfers.UserLeg, error) {
	if err := f.before("user"); err != nil {
		return transfers.UserLeg{}, err
	}
	leg, err := f.inner.PostUser(ctx, t)
	if err != nil {
		return leg, err
	}
	return leg, f.after("user")
}

func (f *faultExec) PostChannel(ctx context.Context, t *transfers.Transfer) (transfers.ChannelLeg, error) {
	if err := f.before("channel"); err != nil {
		return transfers.ChannelLeg{}, err
	}
	leg, err := f.inner.PostChannel(ctx, t)
	if err != nil {
		return leg, err
	}
	return leg, f.after("channel")
}

func (f *faultExec) PostRevenue(ctx context.Context, t *transfers.Transfer) (transfers.RevenueLeg, error) {
	if err := f.before("revenue"); err != nil {
		return transfers.RevenueLeg{}, err
	}
	leg, err := f.inner.PostRevenue(ctx, t)
	if err != nil {
		return leg, err
	}
	return leg, f.after("revenue")
}

func (f *faultExec) RecordFee(ctx context.Context, t *transfers.Transfer) error {
	if err := f.before("fee"); err != nil {
		return err
	}
	if err := f.inner.RecordFee(ctx, t); err != nil {
		return err
	}
	return f.after("fee")
}

// crashStore panics right after a successful store write, like a process dying
// between two statements.
type crashStore struct {
	transfers.Store
	mu          sync.Mutex
	afterCreate bool
	afterSave   map[transfers.State]bool
}

func (c *crashStore) Create(ctx context.Context, tr *transfers.Transfer, owner string, lease time.Duration) (*transfers.Transfer, bool, error) {
	got, created, err := c.Store.Create(ctx, tr, owner, lease)
	c.mu.Lock()
	crashNow := created && c.afterCreate
	c.afterCreate = false
	c.mu.Unlock()
	if crashNow {
		panic(crash{at: "after create"})
	}
	return got, created, err
}

func (c *crashStore) Save(ctx context.Context, tr *transfers.Transfer, from transfers.State, owner string) error {
	if err := c.Store.Save(ctx, tr, from, owner); err != nil {
		return err
	}
	c.mu.Lock()
	crashNow := c.afterSave[tr.State]
	delete(c.afterSave, tr.State)
	c.mu.Unlock()
	if crashNow {
		panic(crash{at: "after save " + string(tr.State)})
	}
	return nil
}

type recordingLogger struct {
	logging.Logger
	mu    *sync.Mutex
	lines *[]string
}

func (r recordingLogger) Errorf(f string, a ...any) {
	r.mu.Lock()
	*r.lines = append(*r.lines, fmt.Sprintf(f, a...))
	r.mu.Unlock()
	r.Logger.Errorf(f, a...)
}

func (r recordingLogger) WithFields(m map[string]any) logging.Logger {
	return recordingLogger{Logger: r.Logger.WithFields(m), mu: r.mu, lines: r.lines}
}

// ---- environment -------------------------------------------------------------

type env struct {
	t       *testing.T
	store   *transferstest.MemoryStore
	cstore  *crashStore
	ledgers *transferstest.FakeLedgers
	fees    *transferstest.FakeFees
	exec    *faultExec
	svc     *transfers.Service
	cfg     transfers.Config
	logs    *[]string
}

func newEnv(t *testing.T, mutate ...func(*transfers.Config)) *env {
	t.Helper()
	cfg := transfers.DefaultConfig()
	cfg.ClientRetryCooldown = 0
	for _, m := range mutate {
		m(&cfg)
	}
	e := &env{
		t:       t,
		store:   transferstest.NewMemoryStore(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)),
		ledgers: transferstest.NewFakeLedgers(),
		fees:    transferstest.NewFakeFees(),
		cfg:     cfg,
		logs:    &[]string{},
	}
	e.cstore = &crashStore{Store: e.store, afterSave: map[transfers.State]bool{}}
	e.exec = newFaultExec(transfers.NewLedgerExecutor(e.ledgers, e.fees))
	logger := recordingLogger{Logger: logging.Testing(), mu: &sync.Mutex{}, lines: e.logs}
	e.svc = transfers.NewService(e.cstore, e.exec, cfg, logger)
	e.ledgers.Fund(userLedger, "users:u1:wallets:NGN:available", asset, startFunds)
	e.ledgers.Fund(userLedger, "users:u1:wallets:NGN:lien", asset, startFunds)
	return e
}

func inputFor(kind transfers.Kind, ref string) transfers.Input {
	return transfers.Input{
		Reference: ref, UserLedger: userLedger, WalletID: "u1-NGN", Currency: "NGN", Kind: kind,
		RequestedAmount: 1000, Amount: 1000, ChannelID: "ch1", ChannelAmount: 900,
		UserFee: 100, ProcessingFee: 30, NetRevenue: 70,
		UserMetadata: map[string]string{"channel_id": "ch1"},
	}
}

var kinds = []transfers.Kind{transfers.KindDebit, transfers.KindReleasePay}

func (e *env) sourceAccount(kind transfers.Kind) string {
	if kind == transfers.KindReleasePay {
		return "users:u1:wallets:NGN:lien"
	}
	return "users:u1:wallets:NGN:available"
}

// expectBooks asserts the final balances and that no leg was applied twice.
func (e *env) expectBooks(kind transfers.Kind, ref string) {
	e.t.Helper()
	l := e.ledgers
	require.Equal(e.t, startFunds-1000, l.Balance(userLedger, e.sourceAccount(kind), asset), "user debited exactly once")
	if kind == transfers.KindDebit {
		require.Equal(e.t, int64(1000), l.Balance(userLedger, "system:control:NGN", asset))
	}
	require.Equal(e.t, int64(-900), l.Balance(channelLedger, "channel:ch1", asset), "channel debited exactly once")
	require.Equal(e.t, int64(100), l.Balance(revenueLedger, "revenue:accumulated", asset))
	require.Equal(e.t, int64(30), l.Balance(revenueLedger, "revenue:channel_processing_cost", asset))

	require.Equal(e.t, 1, l.Applied(userLedger, ref))
	require.Equal(e.t, 1, l.Applied(channelLedger, ref))
	require.Equal(e.t, 1, l.Applied(revenueLedger, ref+"-user-fee"))
	require.Equal(e.t, 1, l.Applied(revenueLedger, ref+"-processing-fee"))
	require.Equal(e.t, 1, e.fees.Rows(ref), "exactly one fee record")

	tr, err := e.store.GetByReference(context.Background(), ref)
	require.NoError(e.t, err)
	require.Equal(e.t, transfers.StateCompleted, tr.State)
	require.NotNil(e.t, tr.UserTxID)
	require.NotNil(e.t, tr.ChannelTxID)
	require.NotNil(e.t, tr.RevenueTxID)
	require.Nil(e.t, tr.LeaseOwner)
}

func (e *env) state(ref string) transfers.State {
	tr, err := e.store.GetByReference(context.Background(), ref)
	require.NoError(e.t, err)
	return tr.State
}

// ---- tests -------------------------------------------------------------------

func TestHappyPath(t *testing.T) {
	t.Parallel()
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			e := newEnv(t)
			out, err := e.svc.Execute(context.Background(), inputFor(kind, "ref-happy"))
			require.NoError(t, err)
			require.Equal(t, transfers.StatusCompleted, out.Status)
			require.False(t, out.Replayed)
			e.expectBooks(kind, "ref-happy")

			require.Equal(t, transfers.StateCompleted, out.Transfer.State)
		})
	}
}

// TestResponseContract pins the response body of the debit handler.
func TestResponseContract(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	out, err := e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, "ref-contract"))
	require.NoError(t, err)

	raw, err := json.Marshal(out.Transfer.Response(""))
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))

	for _, k := range []string{"txid", "timestamp", "postings", "metadata", "balance_before", "balance_after"} {
		require.Contains(t, body, k)
	}
	require.EqualValues(t, startFunds, body["balance_before"])
	require.EqualValues(t, startFunds-1000, body["balance_after"])
	md := body["metadata"].(map[string]any)
	require.Equal(t, "channels-NGN", md["channel_ledger"])
	require.Equal(t, "revenue-NGN", md["revenue_ledger"])
	require.Equal(t, "100", md["channel_user_fee_amount"])
	require.Equal(t, "30", md["channel_processing_fee_amount"])
	require.Equal(t, "70", md["channel_net_revenue_amount"])
	require.Equal(t, "ch1", md["channel_id"], "user tx metadata is merged like before")
	require.Regexp(t, `^\d+$`, md["channel_tx_id"], "real tx id, not a pointer address")
	require.Regexp(t, `^\d+$`, md["revenue_tx_id"])
	require.Equal(t, "Channel balance is negative: -900 NGN", body["warning"])
	postings := body["postings"].([]any)
	require.Len(t, postings, 1)
}

// Every crash point: recovery completes the transfer exactly once.
func TestCrashAtEveryPointThenRecover(t *testing.T) {
	t.Parallel()

	type point struct {
		name    string
		install func(e *env)
	}
	points := []point{
		{"after create before first leg", func(e *env) { e.cstore.afterCreate = true }},
		{"after user leg committed", func(e *env) { e.exec.crashAfter["user"] = true }},
		{"after user_posted saved", func(e *env) { e.cstore.afterSave[transfers.StateUserPosted] = true }},
		{"after channel leg committed", func(e *env) { e.exec.crashAfter["channel"] = true }},
		{"after channel_posted saved", func(e *env) { e.cstore.afterSave[transfers.StateChannelPosted] = true }},
		{"between the two revenue postings", func(e *env) {
			e.ledgers.AfterCommit = func(m transfers.Movement) error {
				if strings.HasSuffix(m.Reference, "-user-fee") {
					panic(crash{at: "mid revenue"})
				}
				return nil
			}
		}},
		{"after revenue leg committed", func(e *env) { e.exec.crashAfter["revenue"] = true }},
		{"after revenue_posted saved", func(e *env) { e.cstore.afterSave[transfers.StateRevenuePosted] = true }},
		{"after fee record written", func(e *env) { e.exec.crashAfter["fee"] = true }},
	}

	for _, kind := range kinds {
		for _, p := range points {
			t.Run(string(kind)+"/"+p.name, func(t *testing.T) {
				e := newEnv(t)
				ref := "ref-crash"
				in := inputFor(kind, ref)
				p.install(e)

				crashed := runCrashing(t, func() { _, _ = e.svc.Execute(context.Background(), in) })
				require.True(t, crashed, "crash point was not reached")
				e.ledgers.AfterCommit = nil

				// Right after the crash the lease is still held and the grace
				// period has not elapsed: recovery must not touch it.
				stats, err := e.svc.RecoverBatch(context.Background())
				require.NoError(t, err)
				require.Zero(t, stats.Claimed, "recovery must respect lease and grace")

				// Past lease and grace the runner finishes the job.
				e.store.Advance(e.cfg.Lease + e.cfg.Grace + time.Second)
				stats, err = e.svc.RecoverBatch(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, stats.Claimed)
				require.Equal(t, 1, stats.Completed)

				e.expectBooks(kind, ref)

				// Nothing left to do, and a second pass is a no-op.
				e.store.Advance(time.Hour)
				stats, err = e.svc.RecoverBatch(context.Background())
				require.NoError(t, err)
				require.Zero(t, stats.Claimed)

				// The client retrying afterwards gets the stored result.
				calls := e.ledgers.PostCalls()
				out, err := e.svc.Execute(context.Background(), in)
				require.NoError(t, err)
				require.Equal(t, transfers.StatusCompleted, out.Status)
				require.True(t, out.Replayed)
				require.Equal(t, calls, e.ledgers.PostCalls(), "replay must not touch any ledger")
				e.expectBooks(kind, ref)
			})
		}
	}
}

// A leg that fails (error, not crash) after committing: the client gets a
// pending outcome, the money is not lost or doubled, recovery finishes it.
func TestLegFailureThenRecover(t *testing.T) {
	t.Parallel()
	for _, leg := range []string{"user", "channel", "revenue"} {
		for _, mode := range []string{"response lost after commit", "failed before commit"} {
			t.Run(leg+"/"+mode, func(t *testing.T) {
				e := newEnv(t)
				ref := "ref-legfail"
				in := inputFor(transfers.KindDebit, ref)
				if mode == "response lost after commit" {
					e.exec.failAfter[leg] = true
				} else {
					e.exec.failBefore[leg] = true
				}

				out, err := e.svc.Execute(context.Background(), in)
				require.NoError(t, err)
				require.Equal(t, transfers.StatusPending, out.Status)
				require.Error(t, out.Cause)
				if leg != "user" {
					require.NotNil(t, out.Transfer.UserTxID, "principal is posted")
				}
				require.NotEqual(t, transfers.StateCompleted, e.state(ref))

				// Fix the fault. Recovery waits for grace.
				e.exec.mu.Lock()
				delete(e.exec.failBefore, leg)
				e.exec.mu.Unlock()
				stats, _ := e.svc.RecoverBatch(context.Background())
				require.Zero(t, stats.Claimed)

				e.store.Advance(e.cfg.Grace + time.Second)
				stats, err = e.svc.RecoverBatch(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, stats.Completed)
				e.expectBooks(transfers.KindDebit, ref)
			})
		}
	}
}

func TestFeeRecordFailureKeepsSuccessResponseAndIsRecovered(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	failing := true
	e.fees.Fail = func() bool { return failing }

	out, err := e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, "ref-fee"))
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status, "all money legs are done")
	require.Contains(t, out.Warning, "fee record write failed")
	require.Equal(t, transfers.StateRevenuePosted, e.state("ref-fee"))
	require.Contains(t, out.Transfer.Response(out.Warning)["warning"], "fee record write failed")

	failing = false
	e.store.Advance(e.cfg.Grace + time.Second)
	stats, err := e.svc.RecoverBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Completed)
	e.expectBooks(transfers.KindDebit, "ref-fee")
}

func TestDuplicateRequestMidFlightGetsInProgressAndPostsNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := inputFor(transfers.KindDebit, "ref-dup")

	var nested *transfers.Outcome
	var nestedErr error
	fired := false
	e.ledgers.AfterCommit = func(m transfers.Movement) error {
		if m.Ledger == channelLedger && !fired {
			fired = true
			// The same request arrives while the first one is between legs.
			nested, nestedErr = e.svc.Execute(context.Background(), in)
		}
		return nil
	}
	out, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status)

	require.NoError(t, nestedErr)
	require.Equal(t, transfers.StatusInProgress, nested.Status)
	e.expectBooks(transfers.KindDebit, "ref-dup")
}

func TestRetryStormDoesNotReDriveOrBurnAttempts(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(c *transfers.Config) { c.ClientRetryCooldown = 5 * time.Second })
	in := inputFor(transfers.KindDebit, "ref-storm")
	e.exec.failBefore["channel"] = true

	out, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusPending, out.Status)
	callsAfterFirst := e.ledgers.PostCalls()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, err := e.svc.Execute(context.Background(), in)
			require.NoError(t, err)
			require.NotEqual(t, transfers.StatusCompleted, o.Status)
		}()
	}
	wg.Wait()
	require.Equal(t, callsAfterFirst, e.ledgers.PostCalls(), "cooldown stops client retries from hammering ledgers")

	tr, _ := e.store.GetByReference(context.Background(), "ref-storm")
	require.Equal(t, 1, tr.Attempts)
	require.Equal(t, 1, e.ledgers.Applied(userLedger, "ref-storm"))

	// After the cooldown a retry drives again (and counts no attempt).
	e.store.Advance(10 * time.Second)
	delete(e.exec.failBefore, "channel")
	out, err = e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status)
	require.True(t, out.Replayed)
	e.expectBooks(transfers.KindDebit, "ref-storm")
}

// Recovery workers and live client retries hammer the same stalled transfer.
func TestConcurrentRecoveryAndRetryNeverDoublePost(t *testing.T) {
	t.Parallel()
	for _, crashAt := range []string{"user", "channel", "revenue"} {
		t.Run("stalled after "+crashAt, func(t *testing.T) {
			e := newEnv(t)
			ref := "ref-conc"
			in := inputFor(transfers.KindDebit, ref)
			e.exec.crashAfter[crashAt] = true
			require.True(t, runCrashing(t, func() { _, _ = e.svc.Execute(context.Background(), in) }))

			e.store.Advance(e.cfg.Lease + e.cfg.Grace + time.Second)
			// Widen the race window between check and commit.
			e.ledgers.BeforeCommit = func(transfers.Movement) error { time.Sleep(time.Millisecond); return nil }

			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < 8; i++ {
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					_, err := e.svc.RecoverBatch(context.Background())
					require.NoError(t, err)
				}()
				go func() {
					defer wg.Done()
					<-start
					out, err := e.svc.Execute(context.Background(), in)
					require.NoError(t, err)
					require.Contains(t, []transfers.Status{transfers.StatusCompleted, transfers.StatusInProgress}, out.Status)
				}()
			}
			close(start)
			wg.Wait()

			// Whatever interleaving happened, one more pass settles it.
			e.store.Advance(time.Hour)
			_, err := e.svc.RecoverBatch(context.Background())
			require.NoError(t, err)
			e.expectBooks(transfers.KindDebit, ref)
		})
	}
}

func TestMaxAttemptsMovesToNeedsReviewAndAlerts(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(c *transfers.Config) { c.MaxAttempts = 3 })
	ref := "ref-review"
	in := inputFor(transfers.KindDebit, ref)
	e.exec.failBefore["channel"] = true

	out, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusPending, out.Status) // attempt 1

	e.store.Advance(time.Hour)
	stats, _ := e.svc.RecoverBatch(context.Background()) // attempt 2
	require.Equal(t, 1, stats.Pending)
	e.store.Advance(time.Hour)
	stats, _ = e.svc.RecoverBatch(context.Background()) // attempt 3 -> review
	require.Equal(t, 1, stats.Escalated)

	require.Equal(t, transfers.StateNeedsReview, e.state(ref))
	tr, _ := e.store.GetByReference(context.Background(), ref)
	require.Contains(t, tr.LastError, "injected failure before channel")
	require.Equal(t, 3, tr.Attempts)

	found := false
	for _, l := range *e.logs {
		if strings.Contains(l, "failed_needs_review") && strings.Contains(l, "manual action required") {
			found = true
		}
	}
	require.True(t, found, "an alert-able log line is emitted: %v", *e.logs)

	// Automation leaves it alone from now on; a client retry is told so.
	e.store.Advance(24 * time.Hour)
	stats, _ = e.svc.RecoverBatch(context.Background())
	require.Zero(t, stats.Claimed)
	out, err = e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusNeedsReview, out.Status)
	require.Equal(t, 1, e.ledgers.Applied(userLedger, ref), "principal stays posted exactly once")
	require.Zero(t, e.ledgers.Applied(channelLedger, ref))
}

func TestRejectedFirstLegIsTerminalAndNeverRecovered(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ref := "ref-nsf"
	in := inputFor(transfers.KindDebit, ref)
	in.Amount, in.RequestedAmount = startFunds+1, startFunds+1
	in.ChannelAmount = 900

	out, err := e.svc.Execute(context.Background(), in)
	require.Nil(t, out)
	var rejected *transfers.RejectedError
	require.True(t, errors.As(err, &rejected))
	require.Contains(t, err.Error(), "insufficient fund")
	require.Equal(t, transfers.StateFailed, e.state(ref))

	e.store.Advance(24 * time.Hour)
	stats, _ := e.svc.RecoverBatch(context.Background())
	require.Zero(t, stats.Claimed, "a rejected request must never be turned into a debit later")
	require.Zero(t, e.ledgers.Applied(userLedger, ref))

	// The client may retry the reference after the cause is gone.
	e.ledgers.Fund(userLedger, "users:u1:wallets:NGN:available", asset, 5)
	out, err = e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, ref))
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status)
	require.False(t, out.Replayed)
	require.Equal(t, 1, e.ledgers.Applied(userLedger, ref))
}

func TestReferenceUsedByDifferentTransactionIsNeverAdopted(t *testing.T) {
	t.Parallel()

	t.Run("on the user ledger the request is rejected", func(t *testing.T) {
		e := newEnv(t)
		e.ledgers.Preexisting(context.Background(), transfers.Movement{
			Ledger: userLedger, Reference: "ref-x", Source: "world", Destination: "users:u1:wallets:NGN:available",
			Asset: asset, Amount: 5,
		})
		out, err := e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, "ref-x"))
		require.Nil(t, out)
		require.ErrorIs(t, err, transfers.ErrReferenceMismatch)
		require.Equal(t, transfers.StateFailed, e.state("ref-x"))
		require.Zero(t, e.ledgers.Applied(channelLedger, "ref-x"))
	})

	t.Run("on a later leg it needs review instead of silently continuing", func(t *testing.T) {
		e := newEnv(t)
		e.ledgers.Preexisting(context.Background(), transfers.Movement{
			Ledger: channelLedger, Reference: "ref-y", Source: "channel:other", Destination: "world",
			Asset: asset, Amount: 123, UnboundedOverdraft: true,
		})
		out, err := e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, "ref-y"))
		require.NoError(t, err)
		require.Equal(t, transfers.StatusNeedsReview, out.Status)
		require.Equal(t, transfers.StateNeedsReview, e.state("ref-y"))
		require.Equal(t, 1, e.ledgers.Applied(userLedger, "ref-y"))
		require.Zero(t, e.ledgers.Applied(revenueLedger, "ref-y-user-fee"))
	})
}

func TestRetryReturnsStoredResultAndRejectsDifferentParameters(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := inputFor(transfers.KindDebit, "ref-replay")

	first, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	want, _ := json.Marshal(first.Transfer.Response(""))
	calls := e.ledgers.PostCalls()

	for i := 0; i < 3; i++ {
		again, err := e.svc.Execute(context.Background(), in)
		require.NoError(t, err)
		require.True(t, again.Replayed)
		got, _ := json.Marshal(again.Transfer.Response(""))
		require.JSONEq(t, string(want), string(got))
	}
	require.Equal(t, calls, e.ledgers.PostCalls())

	other := in
	other.ChannelAmount = 800
	_, err = e.svc.Execute(context.Background(), other)
	require.ErrorIs(t, err, transfers.ErrReferenceConflict)

	// Changed computed fees (config change between attempts) still find the transfer.
	feeChange := in
	feeChange.UserFee, feeChange.NetRevenue = 5, 5
	again, err := e.svc.Execute(context.Background(), feeChange)
	require.NoError(t, err)
	require.True(t, again.Replayed)
	e.expectBooks(transfers.KindDebit, "ref-replay")
}

// A debit that succeeded (fully or partly) before this feature existed and is
// retried by the client is completed instead of failing with a conflict.
func TestLegacyPartialTransferIsHealedByRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ref := "ref-legacy"
	in := inputFor(transfers.KindDebit, ref)

	// Legacy handler committed leg 1 only, then failed.
	e.ledgers.Preexisting(context.Background(), transfers.Movement{
		Ledger: userLedger, Reference: ref, Source: "users:u1:wallets:NGN:available",
		Destination: "system:control:NGN", Asset: asset, Amount: 1000, Metadata: in.UserMetadata,
	})

	out, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status)
	e.expectBooks(transfers.KindDebit, ref)
}

func TestMovementsUseRegistryPrecisionAndExpectedAccounts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.ledgers.Fund(userLedger, "users:u9:wallets:USDT:available", "USDT/6", 5_000_000)
	in := transfers.Input{
		Reference: "ref-usdt", UserLedger: userLedger, WalletID: "u9-USDT", Currency: "USDT", Kind: transfers.KindDebit,
		RequestedAmount: 1_000_000, Amount: 1_000_000, ChannelID: "chx", ChannelAmount: 900_000, UserFee: 100_000, NetRevenue: 100_000,
	}
	out, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status)

	moves := e.ledgers.Movements()
	require.Len(t, moves, 3)
	require.Equal(t, transfers.Movement{
		Ledger: userLedger, Reference: "ref-usdt", Source: "users:u9:wallets:USDT:available",
		Destination: "system:control:USDT", Asset: "USDT/6", Amount: 1_000_000, Metadata: map[string]string(nil),
	}.Asset, moves[0].Asset)
	require.Equal(t, "system:control:USDT", moves[0].Destination)
	require.Equal(t, "channels-USDT", moves[1].Ledger)
	require.Equal(t, "channel:chx", moves[1].Source)
	require.True(t, moves[1].UnboundedOverdraft)
	require.Equal(t, "world", moves[1].Destination)
	require.Equal(t, "revenue-USDT", moves[2].Ledger)
	require.Equal(t, "ref-usdt-user-fee", moves[2].Reference)
	require.Equal(t, "revenue:accumulated", moves[2].Destination)
	for _, m := range moves {
		require.Equal(t, "USDT/6", m.Asset)
	}
	require.Equal(t, 1, e.fees.Rows("ref-usdt"))
}

func TestAssetNameFollowsRegistry(t *testing.T) {
	t.Parallel()
	require.Equal(t, "NGN/2", transfers.AssetName("NGN"))
	require.Equal(t, "USDT/6", transfers.AssetName("USDT"))
	require.Equal(t, "ZZZ/2", transfers.AssetName("ZZZ"), "unknown currency falls back to precision 2")
}

func TestTransferRowIsWrittenBeforeFirstLedgerWrite(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var seen transfers.State
	e.ledgers.BeforeCommit = func(m transfers.Movement) error {
		if m.Ledger == userLedger {
			tr, err := e.store.GetByReference(context.Background(), m.Reference)
			require.NoError(t, err, "durable record must exist before the first ledger write")
			seen = tr.State
			require.NotEqual(t, uuid.Nil, tr.ID)
		}
		return nil
	}
	_, err := e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, "ref-order"))
	require.NoError(t, err)
	require.Equal(t, transfers.StateCreated, seen)
}
