package transfers_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"

	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/transfers"
	"github.com/formancehq/ledger/internal/transfers/transferstest"
)

// storeHarness lets one contract run against the in-memory and SQL stores.
// wait advances the store clock (memory) or really sleeps (SQL).
type storeHarness struct {
	store transfers.Store
	wait  func(time.Duration)
}

const tick = 200 * time.Millisecond

func input(ref string) transfers.Input {
	return transfers.Input{
		Reference: ref, UserLedger: "ledgertrack", WalletID: "u1-NGN", Currency: "NGN",
		Kind: transfers.KindDebit, RequestedAmount: 1000, Amount: 1000,
		ChannelID: "ch1", ChannelAmount: 900, UserFee: 100, NetRevenue: 100,
		UserMetadata: map[string]string{"k": "v"},
	}
}

func TestStoreContractMemory(t *testing.T) {
	t.Parallel()
	ms := transferstest.NewMemoryStore(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	runStoreContract(t, func(*testing.T) storeHarness {
		return storeHarness{store: ms, wait: ms.Advance}
	})
}

// TestStoreContractPostgres runs against a real Postgres when
// TRANSFERS_TEST_DSN is set (for example postgres://postgres@127.0.0.1:5432/db?sslmode=disable).
func TestStoreContractPostgres(t *testing.T) {
	dsn := os.Getenv("TRANSFERS_TEST_DSN")
	if dsn == "" {
		t.Skip("TRANSFERS_TEST_DSN not set")
	}
	db := openDB(t, dsn)
	runStoreContract(t, func(*testing.T) storeHarness {
		return storeHarness{store: transfers.NewBunStore(db), wait: time.Sleep}
	})
}

func openDB(t *testing.T, dsn string) *bun.DB {
	t.Helper()
	ctx := logging.TestingContext()
	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{DatabaseSourceName: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, systemstore.GetMigrator(db).Up(ctx))
	_, err = db.ExecContext(ctx, "truncate _system.transfers")
	require.NoError(t, err)
	return db
}

func runStoreContract(t *testing.T, mk func(*testing.T) storeHarness) {
	ctx := context.Background()
	prefix := fmt.Sprintf("c%d-", time.Now().UnixNano())
	lease := 10 * tick

	t.Run("create is unique per reference", func(t *testing.T) {
		h := mk(t)
		a := transfers.NewTransfer(input(prefix + "create"))
		got, created, err := h.store.Create(ctx, a, "A", lease)
		require.NoError(t, err)
		require.True(t, created)
		require.Equal(t, transfers.StateCreated, got.State)
		require.Equal(t, 1, got.Attempts)
		require.NotNil(t, got.LeaseOwner)
		require.Equal(t, "A", *got.LeaseOwner)
		require.Equal(t, map[string]string{"k": "v"}, got.UserMetadata)

		b := transfers.NewTransfer(input(prefix + "create"))
		again, created, err := h.store.Create(ctx, b, "B", lease)
		require.NoError(t, err)
		require.False(t, created)
		require.Equal(t, got.ID, again.ID)
		require.Equal(t, "A", *again.LeaseOwner)

		byRef, err := h.store.GetByReference(ctx, prefix+"create")
		require.NoError(t, err)
		require.Equal(t, got.ID, byRef.ID)
		_, err = h.store.GetByReference(ctx, prefix+"missing")
		require.ErrorIs(t, err, transfers.ErrNotFound)
	})

	t.Run("claim respects lease, attempts and cooldown", func(t *testing.T) {
		h := mk(t)
		tr, _, err := h.store.Create(ctx, transfers.NewTransfer(input(prefix+"claim")), "A", 3*tick)
		require.NoError(t, err)

		_, ok, err := h.store.Claim(ctx, tr.ID, "B", transfers.ClaimOptions{Lease: lease})
		require.NoError(t, err)
		require.False(t, ok, "lease still held by A")

		h.wait(4 * tick)
		_, ok, err = h.store.Claim(ctx, tr.ID, "B", transfers.ClaimOptions{Lease: lease, MinIdle: time.Hour})
		require.NoError(t, err)
		require.False(t, ok, "cooldown not elapsed")

		got, ok, err := h.store.Claim(ctx, tr.ID, "B", transfers.ClaimOptions{Lease: 3 * tick, CountAttempt: false})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, got.Attempts, "client-driven claims do not consume attempts")
		require.Equal(t, "B", *got.LeaseOwner)

		h.wait(4 * tick)
		got, ok, err = h.store.Claim(ctx, tr.ID, "C", transfers.ClaimOptions{Lease: lease, CountAttempt: true})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, got.Attempts)
	})

	t.Run("save and finish are fenced by owner and state", func(t *testing.T) {
		h := mk(t)
		tr, _, err := h.store.Create(ctx, transfers.NewTransfer(input(prefix+"save")), "A", lease)
		require.NoError(t, err)

		id := int64(42)
		tr.State = transfers.StateUserPosted
		tr.UserTxID = &id
		tr.Result.BalanceAfter = 7
		tr.Result.Postings = []byte(`[{"x":1}]`)
		require.ErrorIs(t, h.store.Save(ctx, tr, transfers.StateCreated, "other"), transfers.ErrLeaseLost)
		require.ErrorIs(t, h.store.Save(ctx, tr, transfers.StateChannelPosted, "A"), transfers.ErrLeaseLost)
		require.NoError(t, h.store.Save(ctx, tr, transfers.StateCreated, "A"))

		got, err := h.store.Get(ctx, tr.ID)
		require.NoError(t, err)
		require.Equal(t, transfers.StateUserPosted, got.State)
		require.Equal(t, int64(42), *got.UserTxID)
		require.Equal(t, int64(7), got.Result.BalanceAfter)
		require.JSONEq(t, `[{"x":1}]`, string(got.Result.Postings))
		require.NotNil(t, got.LeaseOwner, "lease kept until completion or release")

		tr.State = transfers.StateCompleted
		require.NoError(t, h.store.Save(ctx, tr, transfers.StateUserPosted, "A"))
		got, _ = h.store.Get(ctx, tr.ID)
		require.Equal(t, transfers.StateCompleted, got.State)
		require.Nil(t, got.LeaseOwner)
		require.NotNil(t, got.CompletedAt)

		// terminal rows cannot be released, finished or claimed
		require.ErrorIs(t, h.store.Release(ctx, tr.ID, "A", "x"), transfers.ErrLeaseLost)
		_, ok, err := h.store.Claim(ctx, tr.ID, "A", transfers.ClaimOptions{Lease: lease})
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("release and finish", func(t *testing.T) {
		h := mk(t)
		tr, _, _ := h.store.Create(ctx, transfers.NewTransfer(input(prefix+"fin")), "A", lease)
		require.ErrorIs(t, h.store.Release(ctx, tr.ID, "B", "nope"), transfers.ErrLeaseLost)
		require.NoError(t, h.store.Release(ctx, tr.ID, "A", "boom"))
		got, _ := h.store.Get(ctx, tr.ID)
		require.Nil(t, got.LeaseOwner)
		require.Equal(t, "boom", got.LastError)

		got, ok, err := h.store.Claim(ctx, tr.ID, "A", transfers.ClaimOptions{Lease: lease})
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, h.store.Finish(ctx, got.ID, "A", transfers.StateFailed, "rejected"))
		got, _ = h.store.Get(ctx, tr.ID)
		require.Equal(t, transfers.StateFailed, got.State)

		in := input(prefix + "fin")
		in.Amount, in.RequestedAmount = 2000, 2000
		rearmed, ok, err := h.store.Rearm(ctx, tr.ID, transfers.NewTransfer(in), "C", lease)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, transfers.StateCreated, rearmed.State)
		require.Equal(t, int64(2000), rearmed.Amount)
		require.Equal(t, 1, rearmed.Attempts)
		require.Equal(t, tr.ID, rearmed.ID)
		_, ok, _ = h.store.Rearm(ctx, tr.ID, transfers.NewTransfer(in), "D", lease)
		require.False(t, ok, "only failed transfers can be re-armed")
	})

	t.Run("claim batch honours grace, backoff, lease, limit", func(t *testing.T) {
		h := mk(t)
		// isolate from rows of other subtests: finish everything first
		drain(t, h, lease)

		ids := make([]string, 0, 5)
		for i := 0; i < 5; i++ {
			ref := fmt.Sprintf("%sbatch-%d", prefix, i)
			_, _, err := h.store.Create(ctx, transfers.NewTransfer(input(ref)), "A", tick)
			require.NoError(t, err)
			ids = append(ids, ref)
		}
		opts := transfers.RecoveryOptions{Grace: 4 * tick, MaxBackoff: time.Hour, Lease: 4 * tick, Limit: 3}

		got, err := h.store.ClaimBatch(ctx, "R1", opts)
		require.NoError(t, err)
		require.Empty(t, got, "too young")

		h.wait(5 * tick)
		got, err = h.store.ClaimBatch(ctx, "R1", opts)
		require.NoError(t, err)
		require.Len(t, got, 3, "limit")
		for _, g := range got {
			require.Equal(t, 2, g.Attempts)
			require.Equal(t, "R1", *g.LeaseOwner)
		}
		rest, err := h.store.ClaimBatch(ctx, "R2", opts)
		require.NoError(t, err)
		require.Len(t, rest, 2, "leased rows are not handed out twice")

		// attempts=2 means the next recovery waits 2*grace after the last touch
		h.wait(5 * tick)
		again, err := h.store.ClaimBatch(ctx, "R3", opts)
		require.NoError(t, err)
		require.Empty(t, again, "backoff doubled")
		h.wait(5 * tick)
		again, err = h.store.ClaimBatch(ctx, "R3", opts)
		require.NoError(t, err)
		require.Len(t, again, 3)
	})

	t.Run("concurrent claim batch never hands out a row twice", func(t *testing.T) {
		h := mk(t)
		drain(t, h, lease)
		const rows = 30
		for i := 0; i < rows; i++ {
			ref := fmt.Sprintf("%sconc-%d", prefix, i)
			_, _, err := h.store.Create(ctx, transfers.NewTransfer(input(ref)), "A", tick)
			require.NoError(t, err)
		}
		h.wait(5 * tick)

		var mu sync.Mutex
		seen := map[string]string{}
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				owner := fmt.Sprintf("W%d", w)
				for {
					got, err := h.store.ClaimBatch(ctx, owner, transfers.RecoveryOptions{
						Grace: tick, MaxBackoff: time.Hour, Lease: time.Hour, Limit: 4,
					})
					if err != nil || len(got) == 0 {
						return
					}
					mu.Lock()
					for _, g := range got {
						if prev, dup := seen[g.Reference]; dup {
							t.Errorf("%s claimed by %s and %s", g.Reference, prev, owner)
						}
						seen[g.Reference] = owner
					}
					mu.Unlock()
				}
			}(w)
		}
		wg.Wait()
		require.Len(t, seen, rows)
	})
}

// drain moves every non-terminal row to failed so a subtest starts clean.
func drain(t *testing.T, h storeHarness, lease time.Duration) {
	t.Helper()
	h.wait(lease + tick)
	for {
		got, err := h.store.ClaimBatch(context.Background(), "drain", transfers.RecoveryOptions{
			Grace: 0, MaxBackoff: time.Hour, Lease: time.Hour, Limit: 50,
		})
		require.NoError(t, err)
		if len(got) == 0 {
			return
		}
		for _, g := range got {
			require.NoError(t, h.store.Finish(context.Background(), g.ID, "drain", transfers.StateFailed, "drained"))
		}
	}
}
