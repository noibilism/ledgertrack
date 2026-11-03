package walletlock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKeyFromAddress(t *testing.T) {
	t.Parallel()

	k, ok := KeyFromAddress("ledgertrack", "users:lb-w000000:wallets:USD:available")
	require.True(t, ok)
	require.Equal(t, "ledgertrack/lb-w000000-USD", k)

	// available and lien of one wallet share a key
	k2, ok := KeyFromAddress("ledgertrack", "users:lb-w000000:wallets:USD:lien")
	require.True(t, ok)
	require.Equal(t, k, k2)

	// another currency is another wallet
	k3, ok := KeyFromAddress("ledgertrack", "users:lb-w000000:wallets:EUR:available")
	require.True(t, ok)
	require.NotEqual(t, k, k3)

	for _, a := range []string{"world", "system:control:USD", "system:control:USD:7", "channel:abc", "revenue:accumulated", "users:u:wallets:USD", ""} {
		_, ok := KeyFromAddress("ledgertrack", a)
		require.False(t, ok, a)
	}
}

func TestLockAddressesIgnoresNonWalletAccounts(t *testing.T) {
	t.Parallel()
	l := New()
	unlock, err := l.LockAddresses(context.Background(), "ledger", "world", "channel:c1")
	require.NoError(t, err)
	require.Zero(t, l.Size())
	unlock()
}

func TestLockAddressesBlocksTheSameWallet(t *testing.T) {
	t.Parallel()
	l := New()
	held, err := l.LockAddresses(context.Background(), "ledger", "users:u1:wallets:USD:available", "channel:c1")
	require.NoError(t, err)

	got := make(chan struct{})
	go func() {
		u, err := l.LockAddresses(context.Background(), "ledger", "channel:c2", "users:u1:wallets:USD:lien")
		if err == nil {
			u()
		}
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("the lien account of the same wallet must wait for the available account lock")
	case <-time.After(100 * time.Millisecond):
	}
	held()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("the second caller never got the lock")
	}
	require.Zero(t, l.Size(), "entries must be garbage collected")
}

func TestLockAddressesTwoWalletsInAnyOrderNeverDeadlock(t *testing.T) {
	t.Parallel()
	l := New()
	a := "users:a:wallets:USD:available"
	b := "users:b:wallets:USD:available"

	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 200; i++ {
		first, second := a, b
		if i%2 == 1 {
			first, second = b, a
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := l.LockAddresses(context.Background(), "ledger", first, second)
			require.NoError(t, err)
			time.Sleep(time.Microsecond)
			u()
		}()
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock between callers locking the same two wallets in opposite orders")
	}
	require.Zero(t, l.Size())
}

func TestLockAddressesCancelReleasesWhatWasTaken(t *testing.T) {
	t.Parallel()
	l := New()
	// someone holds wallet b
	holdB, err := l.Lock(context.Background(), Key("ledger", "b-USD"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = l.LockAddresses(ctx, "ledger", "users:a:wallets:USD:available", "users:b:wallets:USD:available")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// wallet a must have been released again
	u, err := l.Lock(context.Background(), Key("ledger", "a-USD"))
	require.NoError(t, err)
	u()
	holdB()
	require.Zero(t, l.Size())
}
