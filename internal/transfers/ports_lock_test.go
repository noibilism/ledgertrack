package transfers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ledgerinternal "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/walletlock"
)

type fakeLedgerController struct {
	ledgercontroller.Controller
	creates atomic.Int32
}

func (f *fakeLedgerController) CreateTransaction(_ context.Context, _ ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledgerinternal.Log, *ledgerinternal.CreatedTransaction, bool, error) {
	f.creates.Add(1)
	return nil, &ledgerinternal.CreatedTransaction{}, false, nil
}

type fakeSystem struct {
	systemcontroller.Controller
	ctrl *fakeLedgerController
}

func (f *fakeSystem) GetLedgerController(context.Context, string) (ledgercontroller.Controller, error) {
	return f.ctrl, nil
}

func postAsync(ctx context.Context, l *SystemLedgers, m Movement) chan error {
	out := make(chan error, 1)
	go func() {
		_, err := l.Post(ctx, m)
		out <- err
	}()
	return out
}

func TestPostWalletLegWaitsForTheWalletLock(t *testing.T) {
	t.Parallel()
	ctrl := &fakeLedgerController{}
	ledgers := NewSystemLedgers(&fakeSystem{ctrl: ctrl})

	const ledgerName = "lock-test-ledger"
	held, err := walletlock.Default.Lock(context.Background(), walletlock.Key(ledgerName, "u1-USD"))
	require.NoError(t, err)

	res := postAsync(context.Background(), ledgers, Movement{
		Ledger: ledgerName, Reference: "r1", Asset: "USD/2", Amount: 100,
		Source: "users:u1:wallets:USD:available", Destination: "system:control:USD",
	})
	select {
	case <-res:
		t.Fatal("a wallet leg must wait while the wallet is locked by a plain wallet route")
	case <-time.After(150 * time.Millisecond):
	}
	require.Zero(t, ctrl.creates.Load())

	held()
	select {
	case err := <-res:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the wallet leg never ran after the lock was released")
	}
	require.EqualValues(t, 1, ctrl.creates.Load())
}

func TestPostNonWalletLegIsNotBlocked(t *testing.T) {
	t.Parallel()
	ctrl := &fakeLedgerController{}
	ledgers := NewSystemLedgers(&fakeSystem{ctrl: ctrl})

	const ledgerName = "lock-test-ledger-2"
	held, err := walletlock.Default.Lock(context.Background(), walletlock.Key(ledgerName, "u1-USD"))
	require.NoError(t, err)
	defer held()

	res := postAsync(context.Background(), ledgers, Movement{
		Ledger: ledgerName, Reference: "r2", Asset: "USD/2", Amount: 100,
		Source: "world", Destination: "channel:c1", UnboundedOverdraft: true,
	})
	select {
	case err := <-res:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("a leg that touches no wallet account must not wait for a wallet lock")
	}
}

func TestPostCancelledWhileWaitingIsRejectedAndWritesNothing(t *testing.T) {
	t.Parallel()
	ctrl := &fakeLedgerController{}
	ledgers := NewSystemLedgers(&fakeSystem{ctrl: ctrl})

	const ledgerName = "lock-test-ledger-3"
	held, err := walletlock.Default.Lock(context.Background(), walletlock.Key(ledgerName, "u1-USD"))
	require.NoError(t, err)
	defer held()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = <-postAsync(ctx, ledgers, Movement{
		Ledger: ledgerName, Reference: "r3", Asset: "USD/2", Amount: 100,
		Source: "users:u1:wallets:USD:lien", Destination: "channel:c1",
	})
	var rejected *RejectedError
	require.True(t, errors.As(err, &rejected), "got %v", err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, ctrl.creates.Load())
}
