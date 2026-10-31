package transfers_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"

	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	"github.com/formancehq/ledger/internal/transfers"
	"github.com/formancehq/ledger/internal/transfers/transferstest"
)

// These tests exercise the real SQL store (leases, SKIP LOCKED) with the fake
// ledger. They need TRANSFERS_TEST_DSN, see store_contract_test.go.

func TestServiceOnPostgresCrashThenConcurrentRecoveryAndRetries(t *testing.T) {
	dsn := os.Getenv("TRANSFERS_TEST_DSN")
	if dsn == "" {
		t.Skip("TRANSFERS_TEST_DSN not set")
	}
	db := openDB(t, dsn)
	ctx := context.Background()

	cfg := transfers.Config{
		Grace: 150 * time.Millisecond, MaxBackoff: time.Second, Lease: 300 * time.Millisecond,
		MaxAttempts: 10, BatchSize: 10, ClientRetryCooldown: 0,
	}
	ledgers := transferstest.NewFakeLedgers()
	fees := transferstest.NewFakeFees()
	ledgers.Fund(userLedger, "users:u1:wallets:NGN:available", asset, startFunds)
	exec := newFaultExec(transfers.NewLedgerExecutor(ledgers, fees))
	svc := transfers.NewService(transfers.NewBunStore(db), exec, cfg, logging.Testing())

	const n = 6
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("pg-%d-%d", time.Now().UnixNano(), i)
		in := inputFor(transfers.KindDebit, ref)
		in.Amount, in.RequestedAmount = 100, 100
		in.ChannelAmount, in.UserFee, in.ProcessingFee, in.NetRevenue = 90, 10, 3, 7

		// Alternate where the "process" dies.
		legs := []string{"user", "channel", "revenue"}
		exec.crashAfter[legs[i%3]] = true //nolint:gosec // G602: index is bounded by construction (modulo / shard count)
		require.True(t, runCrashing(t, func() { _, _ = svc.Execute(ctx, in) }))

		time.Sleep(cfg.Lease + cfg.Grace + 100*time.Millisecond)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < 6; w++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, err := svc.RecoverBatch(ctx)
				require.NoError(t, err)
			}()
			go func() {
				defer wg.Done()
				<-start
				out, err := svc.Execute(ctx, in)
				require.NoError(t, err)
				require.Contains(t, []transfers.Status{transfers.StatusCompleted, transfers.StatusInProgress, transfers.StatusPending}, out.Status)
			}()
		}
		close(start)
		wg.Wait()

		// Settle anything a lease still held.
		require.Eventually(t, func() bool {
			_, _ = svc.RecoverBatch(ctx)
			out, err := svc.Execute(ctx, in)
			return err == nil && out.Status == transfers.StatusCompleted && out.Transfer.State == transfers.StateCompleted
		}, 5*time.Second, 100*time.Millisecond)

		require.Equal(t, 1, ledgers.Applied(userLedger, ref))
		require.Equal(t, 1, ledgers.Applied(channelLedger, ref))
		require.Equal(t, 1, ledgers.Applied(revenueLedger, ref+"-user-fee"))
		require.Equal(t, 1, ledgers.Applied(revenueLedger, ref+"-processing-fee"))
		require.Equal(t, 1, fees.Rows(ref))
	}
	require.Equal(t, startFunds-n*100, ledgers.Balance(userLedger, "users:u1:wallets:NGN:available", asset))
	require.Equal(t, int64(-90*n), ledgers.Balance(channelLedger, "channel:ch1", asset))
}

func TestBunFeeRecorderWritesOncePerReferenceUnderConcurrency(t *testing.T) {
	dsn := os.Getenv("TRANSFERS_TEST_DSN")
	if dsn == "" {
		t.Skip("TRANSFERS_TEST_DSN not set")
	}
	db := openDB(t, dsn)
	ctx := context.Background()
	rec := transfers.NewBunFeeRecorder(db)
	ref := "fee-" + uuid.NewString()

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wallet := "u1-NGN"
			require.NoError(t, rec.Record(ctx, &channelmodels.ChannelFeeRecord{
				ChannelID: "ch1", Currency: "NGN", WalletID: &wallet, Reference: ref,
				TotalAmount: 100, PrincipalAmount: 90, UserFeeAmount: 10, ProcessingFeeAmount: 3, NetRevenueAmount: 7,
				Metadata: map[string]any{"wallet_id": wallet},
			}))
		}()
	}
	wg.Wait()

	var count int
	require.NoError(t, db.NewSelect().Model((*channelmodels.ChannelFeeRecord)(nil)).
		ColumnExpr("count(*)").Where("reference = ?", ref).Scan(ctx, &count))
	require.Equal(t, 1, count)
}
