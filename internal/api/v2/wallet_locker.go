package v2

import (
	"context"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/ledger"
)

// walletLocker serialises, inside this process, the ledger writes that touch the same wallet.
//
// Why: a wallet debit locks the wallet row first (balance check, GetBalances) and the
// system:control row second (UpdateVolumes), while a wallet credit locks them in the opposite
// order (the unbounded overdraft source needs no balance read, so only UpdateVolumes locks).
// Two concurrent operations on the same wallet therefore deadlock inside Postgres; the loser is
// only detected after deadlock_timeout (1s) and the whole transaction is replayed, and every
// transaction in the queue holds a pooled connection meanwhile. The same inversion exists between
// lien and lien release on one wallet.
//
// Waiting here costs a goroutine and no database connection, and it is cancelled with the request.
// It is a per-process guard: several server instances can still deadlock in Postgres, which keeps
// detecting and retrying them (see logProcessor.forgeLog).
type walletLocker struct {
	mu      sync.Mutex
	entries map[string]*walletLockEntry
}

type walletLockEntry struct {
	ch   chan struct{}
	refs int
}

func newWalletLocker() *walletLocker {
	return &walletLocker{entries: map[string]*walletLockEntry{}}
}

// lock waits for the exclusive right to write on key, or for ctx to be done.
func (l *walletLocker) lock(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	e, ok := l.entries[key]
	if !ok {
		e = &walletLockEntry{ch: make(chan struct{}, 1)}
		l.entries[key] = e
	}
	e.refs++
	l.mu.Unlock()

	select {
	case e.ch <- struct{}{}:
	case <-ctx.Done():
		l.release(key, e, false)
		return nil, ctx.Err()
	}

	var once sync.Once
	return func() { once.Do(func() { l.release(key, e, true) }) }, nil
}

func (l *walletLocker) release(key string, e *walletLockEntry, held bool) {
	if held {
		<-e.ch
	}
	l.mu.Lock()
	e.refs--
	if e.refs == 0 {
		delete(l.entries, key)
	}
	l.mu.Unlock()
}

// size is the number of keys currently tracked (tests).
func (l *walletLocker) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

var defaultWalletLocker = newWalletLocker()

// createWalletTransaction runs ctrl.CreateTransaction while holding the in-process lock of the
// wallet. The lock is taken before any database transaction is opened, so waiting for it never
// holds a pooled connection.
func createWalletTransaction(
	r *http.Request,
	ctrl ledger.Controller,
	walletID string,
	params ledger.Parameters[ledger.CreateTransaction],
) (*ledgerinternal.Log, *ledgerinternal.CreatedTransaction, bool, error) {
	ctx := r.Context()
	unlock, err := defaultWalletLocker.lock(ctx, chi.URLParam(r, "ledger")+"/"+walletID)
	if err != nil {
		return nil, nil, false, err
	}
	defer unlock()

	return ctrl.CreateTransaction(ctx, params)
}
