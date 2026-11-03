package transfers_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/hotrows"
	"github.com/formancehq/ledger/internal/transfers"
)

// These tests change process wide shard settings (hotrows and the environment), so they are not
// parallel. They restore the defaults when done.

func setShards(t *testing.T, revenue, channelWorld, control uint32) {
	t.Helper()
	prevRevenue, prevWorld := hotrows.RevenueShards(), hotrows.ChannelWorldShards()
	t.Cleanup(func() {
		hotrows.SetRevenueShards(prevRevenue)
		hotrows.SetChannelWorldShards(prevWorld)
	})
	hotrows.SetRevenueShards(revenue)
	hotrows.SetChannelWorldShards(channelWorld)
	t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", fmt.Sprint(control))
}

// referenceMovingBetween finds a business reference whose revenue/world shard differs between a and b.
func referenceMovingBetween(t *testing.T, a, b uint32) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		ref := fmt.Sprintf("ref-shard-%d", i)
		if hotrows.ShardFor(ref, a) != hotrows.ShardFor(ref, b) {
			return ref
		}
	}
	t.Fatalf("no reference moves between %d and %d shards", a, b)
	return ""
}

// controlCountMovingWallet returns a shard count whose control shard for wallet differs from the one at count from.
func controlCountMovingWallet(t *testing.T, wallet, ccy string, from uint32) uint32 {
	t.Helper()
	t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", fmt.Sprint(from))
	before := hotrows.SystemControlAddress(wallet, ccy)
	for n := uint32(2); n < 64; n++ {
		if n == from {
			continue
		}
		t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", fmt.Sprint(n))
		if hotrows.SystemControlAddress(wallet, ccy) != before {
			return n
		}
	}
	t.Fatal("no control shard count moves the wallet")
	return 0
}

func movementFor(t *testing.T, e *env, ledger, reference string) transfers.Movement {
	t.Helper()
	for _, m := range e.ledgers.Movements() {
		if m.Ledger == ledger && m.Reference == reference {
			return m
		}
	}
	t.Fatalf("no movement for %s on %s", reference, ledger)
	return transfers.Movement{}
}

func TestLegsUseTheShardedHotRowAccounts(t *testing.T) {
	setShards(t, 16, 16, 16)
	ref := referenceMovingBetween(t, 16, 1)
	shard := hotrows.ShardFor(ref, 16)

	e := newEnv(t)
	in := inputFor(transfers.KindDebit, ref)
	out, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, transfers.StatusCompleted, out.Status)

	control := hotrows.SystemControlAddress(in.WalletID, in.Currency)
	require.True(t, strings.HasPrefix(control, "system:control:NGN:"), control)
	user := movementFor(t, e, userLedger, ref)
	require.Equal(t, control, user.Destination, "user leg must use the same sharded control account as the handler")

	channel := movementFor(t, e, channelLedger, ref)
	require.Equal(t, fmt.Sprintf("world:%d", shard), channel.Destination)
	require.True(t, channel.UnboundedOverdraft)

	fee := movementFor(t, e, revenueLedger, ref+"-user-fee")
	require.Equal(t, fmt.Sprintf("world:%d", shard), fee.Source)
	require.Equal(t, fmt.Sprintf("revenue:accumulated:%d", shard), fee.Destination)
	require.True(t, fee.UnboundedOverdraft, "world:{s} is not the world account and must allow an overdraft")
	cost := movementFor(t, e, revenueLedger, ref+"-processing-fee")
	require.Equal(t, fmt.Sprintf("world:%d", shard), cost.Source)
	require.Equal(t, fmt.Sprintf("revenue:channel_processing_cost:%d", shard), cost.Destination)
	require.True(t, cost.UnboundedOverdraft)

	// Books: the sharded pair nets out and the totals are those of the unsharded layout.
	l := e.ledgers
	require.Equal(t, int64(100), l.Balance(revenueLedger, fmt.Sprintf("revenue:accumulated:%d", shard), asset))
	require.Equal(t, int64(30), l.Balance(revenueLedger, fmt.Sprintf("revenue:channel_processing_cost:%d", shard), asset))
	require.Equal(t, int64(-130), l.Balance(revenueLedger, fmt.Sprintf("world:%d", shard), asset))
	require.Equal(t, int64(900), l.Balance(channelLedger, fmt.Sprintf("world:%d", shard), asset))
	require.Equal(t, int64(1000), l.Balance(userLedger, control, asset))
	require.Zero(t, l.Balance(revenueLedger, "world", asset), "the real world account must stay untouched")
}

func TestUnshardedLayoutIsUnchanged(t *testing.T) {
	setShards(t, 1, 1, 1)
	e := newEnv(t)
	_, err := e.svc.Execute(context.Background(), inputFor(transfers.KindDebit, "ref-plain"))
	require.NoError(t, err)
	e.expectBooks(transfers.KindDebit, "ref-plain")
	for _, m := range e.ledgers.Movements() {
		if m.Ledger == revenueLedger {
			require.Equal(t, "world", m.Source)
			require.False(t, m.UnboundedOverdraft)
		}
	}
}

// A client retry, a duplicate and a recovery run with the same reference must land on the same
// accounts and post nothing new.
func TestIdempotentReplayLandsOnTheSameShard(t *testing.T) {
	setShards(t, 16, 16, 16)
	ref := referenceMovingBetween(t, 16, 1)

	e := newEnv(t)
	in := inputFor(transfers.KindDebit, ref)
	_, err := e.svc.Execute(context.Background(), in)
	require.NoError(t, err)
	first := e.ledgers.Movements()

	for i := 0; i < 3; i++ {
		out, err := e.svc.Execute(context.Background(), in)
		require.NoError(t, err)
		require.True(t, out.Replayed)
	}
	require.Equal(t, first, e.ledgers.Movements(), "a replay must not post anything")
	require.Equal(t, 1, e.ledgers.Applied(revenueLedger, ref+"-user-fee"))
	require.Equal(t, 1, e.ledgers.Applied(channelLedger, ref))

	// Drive the executor legs directly: each one replays onto the very same transaction.
	exec := transfers.NewLedgerExecutor(e.ledgers, nil)
	tr := &transfers.Transfer{
		Reference: ref, UserLedger: userLedger, WalletID: in.WalletID, Currency: in.Currency, Kind: in.Kind,
		Amount: in.Amount, ChannelID: in.ChannelID, ChannelAmount: in.ChannelAmount,
		UserFee: in.UserFee, ProcessingFee: in.ProcessingFee, NetRevenue: in.NetRevenue,
		UserMetadata: in.UserMetadata,
	}
	u, err := exec.PostUser(context.Background(), tr)
	require.NoError(t, err)
	require.True(t, u.Replayed)
	c, err := exec.PostChannel(context.Background(), tr)
	require.NoError(t, err)
	require.True(t, c.Replayed)
	r, err := exec.PostRevenue(context.Background(), tr)
	require.NoError(t, err)
	require.True(t, r.Replayed)
	require.Equal(t, first, e.ledgers.Movements())
}

// Changing the shard counts while a transfer is in flight (a restart with other flags) must not turn
// the replay of an already posted leg into ErrReferenceMismatch / failed_needs_review.
func TestChangingShardCountsMidFlightDoesNotParkTheTransfer(t *testing.T) {
	type point struct {
		name    string
		install func(e *env)
	}
	points := []point{
		{"after user leg", func(e *env) { e.exec.crashAfter["user"] = true }},
		{"after channel leg", func(e *env) { e.exec.crashAfter["channel"] = true }},
		{"between the two revenue postings", func(e *env) {
			e.ledgers.AfterCommit = func(m transfers.Movement) error {
				if strings.HasSuffix(m.Reference, "-user-fee") {
					panic(crash{at: "mid revenue"})
				}
				return nil
			}
		}},
		{"after revenue leg", func(e *env) { e.exec.crashAfter["revenue"] = true }},
	}
	for _, kind := range kinds {
		for _, p := range points {
			t.Run(string(kind)+"/"+p.name, func(t *testing.T) {
				const oldN = 16
				setShards(t, oldN, oldN, oldN)
				ref := referenceMovingBetween(t, oldN, 1)
				newControl := controlCountMovingWallet(t, "u1-NGN", "NGN", oldN)
				t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", fmt.Sprint(oldN))

				e := newEnv(t)
				in := inputFor(kind, ref)
				p.install(e)
				require.True(t, runCrashing(t, func() { _, _ = e.svc.Execute(context.Background(), in) }), "crash point not reached")
				e.ledgers.AfterCommit = nil

				// Restart with other shard counts: every hot row account moves.
				hotrows.SetRevenueShards(1)
				hotrows.SetChannelWorldShards(1)
				t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", fmt.Sprint(newControl))

				e.store.Advance(e.cfg.Lease + e.cfg.Grace + time.Second)
				stats, err := e.svc.RecoverBatch(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, stats.Claimed)
				require.Equal(t, 1, stats.Completed, "recovery must complete the transfer")
				require.Zero(t, stats.Escalated, "a moved shard is not a reference mismatch")

				require.Equal(t, transfers.StateCompleted, e.state(ref))
				// Each leg was committed exactly once, on the shard chosen when it was first posted.
				require.Equal(t, 1, e.ledgers.Applied(userLedger, ref))
				require.Equal(t, 1, e.ledgers.Applied(channelLedger, ref))
				require.Equal(t, 1, e.ledgers.Applied(revenueLedger, ref+"-user-fee"))
				require.Equal(t, 1, e.ledgers.Applied(revenueLedger, ref+"-processing-fee"))
			})
		}
	}
}

// Equivalence is for hot row shards only: a different real account is still a mismatch.
func TestReplayOnAnUnrelatedAccountIsStillAMismatch(t *testing.T) {
	setShards(t, 1, 1, 1)
	e := newEnv(t)
	ref := "ref-mismatch"
	in := inputFor(transfers.KindDebit, ref)
	e.ledgers.Preexisting(context.Background(), transfers.Movement{
		Ledger: revenueLedger, Reference: ref + "-user-fee", Source: "world", Destination: "revenue:something_else",
		Asset: asset, Amount: in.UserFee,
	})
	exec := transfers.NewLedgerExecutor(e.ledgers, nil)
	_, err := exec.PostRevenue(context.Background(), &transfers.Transfer{
		Reference: ref, Currency: in.Currency, UserFee: in.UserFee,
	})
	require.ErrorIs(t, err, transfers.ErrReferenceMismatch)
}
