// Package walletlock serialises, inside one process, the ledger writes that touch the same wallet.
//
// Why: a wallet debit locks the wallet row first (balance check, GetBalances) and the
// system:control row second (UpdateVolumes), while a wallet credit locks them in the opposite
// order (the unbounded overdraft source needs no balance read, so only UpdateVolumes locks).
// Two concurrent operations on the same wallet therefore deadlock inside Postgres; the loser is
// only detected after deadlock_timeout (1s) and the whole transaction is replayed, and every
// transaction in the queue holds a pooled connection meanwhile. The same inversion exists between
// lien and lien release on one wallet.
//
// Every code path that writes to a wallet account on the wallet ledger must take this lock, not
// only the plain wallet routes: the wallet leg of a channel transfer (channel_debit, lien_pay) is
// posted by the transfers orchestrator and used to bypass it.
//
// Waiting here costs a goroutine and no database connection, and it is cancelled with the request.
// It is a per-process guard: several server instances can still deadlock in Postgres, which keeps
// detecting and retrying them (see logProcessor.forgeLog).
package walletlock

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// Locker hands out one exclusive lock per key.
type Locker struct {
	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	ch   chan struct{}
	refs int
}

// New returns an empty Locker.
func New() *Locker {
	return &Locker{entries: map[string]*entry{}}
}

// Default is the process wide locker shared by the HTTP handlers and the transfers orchestrator.
var Default = New()

// Lock waits for the exclusive right to write on key, or for ctx to be done.
func (l *Locker) Lock(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	e, ok := l.entries[key]
	if !ok {
		e = &entry{ch: make(chan struct{}, 1)}
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

func (l *Locker) release(key string, e *entry, held bool) {
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

// Size is the number of keys currently tracked (tests).
func (l *Locker) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Key is the lock key of a wallet: the ledger name and the wallet id (<userID>-<currency>).
func Key(ledger, walletID string) string {
	return ledger + "/" + walletID
}

// KeyFromAddress returns the lock key of the wallet an account address belongs to, for addresses
// of the form users:<userID>:wallets:<currency>:<available|lien>. The second result is false for
// any other address (channels, revenue, world, system accounts).
func KeyFromAddress(ledger, address string) (string, bool) {
	parts := strings.Split(address, ":")
	if len(parts) != 5 || parts[0] != "users" || parts[2] != "wallets" || parts[1] == "" || parts[3] == "" {
		return "", false
	}
	return Key(ledger, parts[1]+"-"+parts[3]), true
}

// LockAddresses locks the wallets of the given account addresses on a ledger, in sorted key order
// so that two callers locking the same set can never wait on each other. Addresses that are not
// wallet accounts are ignored. The returned function releases everything and is safe to call more
// than once. With no wallet address it returns a no-op.
func (l *Locker) LockAddresses(ctx context.Context, ledger string, addresses ...string) (func(), error) {
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(addresses))
	for _, a := range addresses {
		if k, ok := KeyFromAddress(ledger, a); ok {
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)

	unlocks := make([]func(), 0, len(keys))
	release := func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
	for _, k := range keys {
		u, err := l.Lock(ctx, k)
		if err != nil {
			release()
			return nil, err
		}
		unlocks = append(unlocks, u)
	}
	var once sync.Once
	return func() { once.Do(release) }, nil
}
