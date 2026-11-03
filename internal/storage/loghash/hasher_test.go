package loghash_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/storage/loghash"
)

func newHasher(e *env, grace time.Duration, batch int) *loghash.Hasher {
	return loghash.NewHasher(logging.Testing(), e.db, loghash.Config{GapGrace: grace, BatchSize: batch})
}

func hashedUpTo(t *testing.T, e *env, name string) (n int, head uint64) {
	t.Helper()
	require.NoError(t, e.db.NewRaw(`select count(hash), coalesce(max(id) filter (where hash is not null), 0) from bkt.logs where ledger = ?`, name).Scan(e.ctx, &n, &head))
	return
}

// The chain built asynchronously must be byte for byte the chain the SYNC trigger builds.
func TestAsyncChainEqualsSyncChain(t *testing.T) {
	e := newEnv(t)
	syncL := e.createLedger(t, "sync", "SYNC")
	asyncL := e.createLedger(t, "async", "ASYNC")

	e.insertLogs(t, "sync", 1, 40)
	e.insertLogs(t, "async", 1, 40)
	require.Equal(t, 0, countHashed(t, e, "async"), "ASYNC ledger commits logs without hash")

	h := newHasher(e, 10*time.Millisecond, 7) // small batches: exercise the batch boundaries
	res, err := h.HashLedgerUntilCaughtUp(e.ctx, asyncL)
	require.NoError(t, err)
	require.Equal(t, 40, res.Hashed)
	require.EqualValues(t, 40, res.HeadID)
	require.Zero(t, res.Unhashed)
	_ = syncL

	require.Equal(t, e.hashes(t, "sync"), e.hashes(t, "async"))
}

func countHashed(t *testing.T, e *env, name string) (n int) {
	t.Helper()
	require.NoError(t, e.db.NewRaw(`select count(hash) from bkt.logs where ledger = ?`, name).Scan(e.ctx, &n))
	return
}

// A restarted hasher (empty memory) resumes from the head, never rehashes, and a second hasher running at the same time is harmless.
func TestHasherResumesAfterRestartWithoutDuplicates(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "async", "ASYNC")

	e.insertLogs(t, "async", 1, 10)
	h1 := newHasher(e, 10*time.Millisecond, 100)
	_, err := h1.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	before := e.hashes(t, "async")

	// "crash": a brand new hasher, with no state
	e.insertLogs(t, "async", 11, 25)
	h2 := newHasher(e, 10*time.Millisecond, 100)
	res, err := h2.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	require.Equal(t, 15, res.Hashed, "only the new logs are hashed")
	after := e.hashes(t, "async")
	for id := uint64(1); id <= 10; id++ {
		require.Equal(t, before[id], after[id], "log %d must not be rehashed", id)
	}

	// nothing left: nothing happens
	res, err = h2.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	require.Zero(t, res.Hashed)

	// the same chain as a SYNC ledger fed with the same logs
	e.createLedger(t, "sync", "SYNC")
	e.insertLogs(t, "sync", 1, 25)
	require.Equal(t, e.hashes(t, "sync"), after)
}

func TestTwoHashersConcurrently(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "async", "ASYNC")
	e.insertLogs(t, "async", 1, 300)

	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			h := newHasher(e, 10*time.Millisecond, 50)
			for j := 0; j < 10; j++ {
				if _, err := h.HashLedgerUntilCaughtUp(e.ctx, l); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	require.NoError(t, <-done)
	require.NoError(t, <-done)

	n, head := hashedUpTo(t, e, "async")
	require.Equal(t, 300, n)
	require.EqualValues(t, 300, head)

	e.createLedger(t, "sync", "SYNC")
	e.insertLogs(t, "sync", 1, 300)
	require.Equal(t, e.hashes(t, "sync"), e.hashes(t, "async"))
}

// A log id allocated by a transaction that is still in flight must not be skipped when later ids commit first:
// the chain waits for it, then keeps the id order.
func TestInFlightLowerIDIsNotSkipped(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "async", "ASYNC")
	h := newHasher(e, 20*time.Millisecond, 100)

	e.insertLogs(t, "async", 1, 4)

	// transaction A allocated id 5 and is slow to commit
	txA, err := e.db.BeginTx(e.ctx, &sql.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, insertLog(e.ctx, txA, "async", 5, payloadFor(5)))

	// B and C committed after A allocated its id
	e.insertLogs(t, "async", 6, 8)

	for i := 0; i < 4; i++ {
		res, err := h.HashLedgerUntilCaughtUp(e.ctx, l)
		require.NoError(t, err)
		require.EqualValues(t, 4, res.HeadID, "must not chain over the in flight id 5 (iteration %d)", i)
		require.True(t, res.Blocked)
		time.Sleep(30 * time.Millisecond)
	}

	require.NoError(t, txA.Commit())

	res, err := h.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	require.EqualValues(t, 8, res.HeadID)
	require.Zero(t, res.Unhashed)

	e.createLedger(t, "sync", "SYNC")
	e.insertLogs(t, "sync", 1, 8)
	require.Equal(t, e.hashes(t, "sync"), e.hashes(t, "async"), "ids are chained in id order")
}

// A rolled back transaction leaves a hole forever: the chain must move past it, but only once it is known to be final.
func TestRolledBackIDIsSkippedOnceFinal(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "async", "ASYNC")
	h := newHasher(e, 20*time.Millisecond, 100)

	e.insertLogs(t, "async", 1, 4)
	txA, err := e.db.BeginTx(e.ctx, &sql.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, insertLog(e.ctx, txA, "async", 5, payloadFor(5)))
	e.insertLogs(t, "async", 6, 8)

	res, err := h.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	require.EqualValues(t, 4, res.HeadID)

	require.NoError(t, txA.Rollback())

	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err = h.HashLedgerUntilCaughtUp(e.ctx, l)
		require.NoError(t, err)
		if res.HeadID == 8 {
			break
		}
		require.True(t, time.Now().Before(deadline), "chain stuck at %d", res.HeadID)
		time.Sleep(25 * time.Millisecond)
	}

	report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
	require.NoError(t, err)
	require.True(t, report.OK(), "%+v", report)
	require.EqualValues(t, 7, report.Verified)
	require.EqualValues(t, 1, report.IDHoles)
	require.Zero(t, report.Tail.Count)
}

// The hasher must refuse a ledger that is not ASYNC.
func TestHasherRejectsNonAsyncLedger(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "sync", "SYNC")
	_, err := newHasher(e, 0, 0).HashLedger(e.ctx, l)
	require.Error(t, err)
}

func TestVerifyDetectsTampering(t *testing.T) {
	setup := func(t *testing.T) (*env, string) {
		e := newEnv(t)
		l := e.createLedger(t, "async", "ASYNC")
		e.insertLogs(t, "async", 1, 30)
		_, err := newHasher(e, 10*time.Millisecond, 100).HashLedgerUntilCaughtUp(e.ctx, l)
		require.NoError(t, err)
		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{PageSize: 7})
		require.NoError(t, err)
		require.True(t, report.OK(), "%+v", report)
		require.EqualValues(t, 30, report.Verified)
		require.EqualValues(t, 30, *report.HeadID)
		return e, report.HeadHash
	}

	t.Run("modified content", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.db.ExecContext(e.ctx, `update bkt.logs set memento = convert_to('{"transaction":{"id":17,"postings":[{"source":"world","destination":"mallory","asset":"USD/2","amount":1700}]}}', 'UTF8') where ledger = 'async' and id = 17`)
		require.NoError(t, err)

		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{PageSize: 7})
		require.NoError(t, err)
		require.False(t, report.OK())
		require.EqualValues(t, 17, report.FirstMismatch.ID)
		require.Equal(t, loghash.ReasonHashMismatch, report.FirstMismatch.Reason)
		require.EqualValues(t, 29, report.Verified, "only the tampered row fails, the rest of the chain still links")
	})

	t.Run("modified date", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.db.ExecContext(e.ctx, `update bkt.logs set date = date + interval '1 day' where ledger = 'async' and id = 3`)
		require.NoError(t, err)
		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
		require.NoError(t, err)
		require.EqualValues(t, 3, report.FirstMismatch.ID)
	})

	t.Run("deleted row", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.db.ExecContext(e.ctx, `delete from bkt.logs where ledger = 'async' and id = 12`)
		require.NoError(t, err)
		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
		require.NoError(t, err)
		require.False(t, report.OK())
		require.EqualValues(t, 13, report.FirstMismatch.ID, "the link after the removed row breaks")
	})

	t.Run("hash erased", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.db.ExecContext(e.ctx, `update bkt.logs set hash = null where ledger = 'async' and id = 9`)
		require.NoError(t, err)
		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
		require.NoError(t, err)
		require.False(t, report.OK())
		require.EqualValues(t, 9, report.FirstMismatch.ID)
		require.Equal(t, loghash.ReasonUnchainedBelowHead, report.FirstMismatch.Reason)
	})

	t.Run("backdated insert below the head", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.db.ExecContext(e.ctx, `delete from bkt.logs where ledger = 'async' and id = 20`)
		require.NoError(t, err)
		// the attacker deletes then re-inserts a forged log 20 without hash and recomputes nothing
		require.NoError(t, insertLog(e.ctx, e.db, "async", 20, `{"forged":true}`))
		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
		require.NoError(t, err)
		require.False(t, report.OK())
		require.EqualValues(t, 20, report.FirstMismatch.ID)
	})

	t.Run("full recompute is only caught by the anchor", func(t *testing.T) {
		e, anchorHash := setup(t)
		// the attacker rewrites log 10 and recomputes every following hash
		_, err := e.db.ExecContext(e.ctx, `update bkt.logs set memento = convert_to('{"x":1}', 'UTF8'), hash = null where ledger = 'async' and id >= 10`)
		require.NoError(t, err)
		// recompute with the hasher's own function: need a head of 9
		l, err := loghash.GetLedger(e.ctx, e.db, "async")
		require.NoError(t, err)
		_, err = newHasher(e, 10*time.Millisecond, 100).HashLedgerUntilCaughtUp(e.ctx, *l)
		require.NoError(t, err)

		report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
		require.NoError(t, err)
		require.True(t, report.OK(), "a consistent rewrite is invisible to the chain alone")

		id := uint64(30)
		report, err = loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{AnchorID: &id, AnchorHash: anchorHash})
		require.NoError(t, err)
		require.False(t, report.OK())
		require.False(t, report.Anchor.Match)
	})
}

func TestVerifyReportsTail(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "async", "ASYNC")
	e.insertLogs(t, "async", 1, 10)
	_, err := newHasher(e, 10*time.Millisecond, 100).HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	e.insertLogs(t, "async", 11, 15)

	report, err := loghash.Verify(e.ctx, e.db, "bkt", "async", "ASYNC", loghash.VerifyOptions{})
	require.NoError(t, err)
	require.True(t, report.OK())
	require.EqualValues(t, 10, report.Verified)
	require.EqualValues(t, 5, report.Tail.Count)
	require.EqualValues(t, 11, *report.Tail.FirstID)
	require.EqualValues(t, 15, *report.Tail.LastID)
	require.Greater(t, report.Tail.LagSeconds, 0.0)
}

func TestSetHashMode(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "led", "SYNC")
	e.insertLogs(t, "led", 1, 5) // hashed by the trigger

	prev, err := loghash.SetHashMode(e.ctx, e.db, "led", "ASYNC")
	require.NoError(t, err)
	require.Equal(t, "SYNC", prev)
	got, err := loghash.GetLedger(e.ctx, e.db, "led")
	require.NoError(t, err)
	require.Equal(t, "ASYNC", got.Features["HASH_LOGS"])

	// no trigger anymore
	e.insertLogs(t, "led", 6, 12)
	require.Equal(t, 5, countHashed(t, e, "led"))

	// the chain continues from the SYNC head
	_, err = newHasher(e, 10*time.Millisecond, 100).HashLedgerUntilCaughtUp(e.ctx, *got)
	require.NoError(t, err)
	require.Equal(t, 12, countHashed(t, e, "led"))
	report, err := loghash.Verify(e.ctx, e.db, "bkt", "led", "ASYNC", loghash.VerifyOptions{})
	require.NoError(t, err)
	require.True(t, report.OK(), "%+v", report)
	require.EqualValues(t, 12, report.Verified)

	// back to SYNC refuses while logs are unchained
	e.insertLogs(t, "led", 13, 14)
	_, err = loghash.SetHashMode(e.ctx, e.db, "led", "SYNC")
	require.Error(t, err)
	got, err = loghash.GetLedger(e.ctx, e.db, "led")
	require.NoError(t, err)
	require.Equal(t, "ASYNC", got.Features["HASH_LOGS"], "refused switch must not change anything")

	_, err = newHasher(e, 10*time.Millisecond, 100).HashLedgerUntilCaughtUp(e.ctx, *got)
	require.NoError(t, err)
	_, err = loghash.SetHashMode(e.ctx, e.db, "led", "SYNC")
	require.NoError(t, err)
	// trigger back: new logs are hashed on insert and continue the chain
	e.insertLogs(t, "led", 15, 16)
	require.Equal(t, 16, countHashed(t, e, "led"))
	report, err = loghash.Verify(e.ctx, e.db, "bkt", "led", "SYNC", loghash.VerifyOptions{})
	require.NoError(t, err)
	require.True(t, report.OK(), "%+v", report)
	_ = l
	var _ bun.IDB = e.db
}

// A hasher whose cached head is stale (another hasher moved the chain) recovers by itself.
func TestStaleCachedHead(t *testing.T) {
	e := newEnv(t)
	l := e.createLedger(t, "async", "ASYNC")
	h1 := newHasher(e, 10*time.Millisecond, 100)
	h2 := newHasher(e, 10*time.Millisecond, 100)

	e.insertLogs(t, "async", 1, 10)
	_, err := h1.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)

	e.insertLogs(t, "async", 11, 20)
	res, err := h2.HashLedgerUntilCaughtUp(e.ctx, l)
	require.NoError(t, err)
	require.Equal(t, 10, res.Hashed)

	e.insertLogs(t, "async", 21, 25)
	res, err = h1.HashLedgerUntilCaughtUp(e.ctx, l) // its cached head is 10
	require.NoError(t, err)
	require.Equal(t, 5, res.Hashed)

	e.createLedger(t, "sync", "SYNC")
	e.insertLogs(t, "sync", 1, 25)
	require.Equal(t, e.hashes(t, "sync"), e.hashes(t, "async"))
}
