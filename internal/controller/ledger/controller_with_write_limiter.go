package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
)

// ErrWriteOverloaded is returned when the write path is saturated: the request waited for a write
// slot longer than the configured limit, or the waiting queue was already full. Nothing was
// written, the call is safe to retry. The HTTP layer maps it to 503 with a Retry-After header.
type ErrWriteOverloaded struct {
	RetryAfter time.Duration
	reason     string
}

func (e ErrWriteOverloaded) Error() string {
	return fmt.Sprintf("ledger write path overloaded (%s), retry in %s", e.reason, e.RetryAfter)
}

func (e ErrWriteOverloaded) Is(target error) bool {
	_, ok := target.(ErrWriteOverloaded)
	return ok
}

func NewErrWriteOverloaded(reason string, retryAfter time.Duration) ErrWriteOverloaded {
	return ErrWriteOverloaded{RetryAfter: retryAfter, reason: reason}
}

// WriteLimiter bounds the number of ledger write transactions in flight in the process.
//
// Every write holds one pooled database connection for its whole duration. Without a bound, more
// concurrent writers than pooled connections (plus the connections readers need) means requests
// queue inside database/sql while holding row locks, which is how the write path turned a load
// spike into a stall. With a bound smaller than the pool, excess requests wait here, without a
// connection and without locks, and are rejected quickly (503 + Retry-After) when the wait is too long.
//
// A nil *WriteLimiter is valid and means "no limit".
type WriteLimiter struct {
	slots      chan struct{}
	maxQueue   int64
	maxWait    time.Duration
	retryAfter time.Duration

	waiting  atomic.Int64
	rejected atomic.Int64
}

// WriteLimiterConfiguration configures a WriteLimiter.
type WriteLimiterConfiguration struct {
	// MaxInFlight is the number of writes allowed to run at the same time.
	MaxInFlight int
	// MaxQueue is the number of requests allowed to wait for a slot. Beyond it requests are
	// rejected immediately. Defaults to 4 x MaxInFlight.
	MaxQueue int
	// MaxWait is how long a request may wait for a slot. Defaults to 2s.
	MaxWait time.Duration
	// RetryAfter is the delay advertised to rejected clients. Defaults to 1s.
	RetryAfter time.Duration
}

// NewWriteLimiter returns nil (no limit) when MaxInFlight <= 0.
func NewWriteLimiter(cfg WriteLimiterConfiguration) *WriteLimiter {
	if cfg.MaxInFlight <= 0 {
		return nil
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 4 * cfg.MaxInFlight
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 2 * time.Second
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = time.Second
	}
	return &WriteLimiter{
		slots:      make(chan struct{}, cfg.MaxInFlight),
		maxQueue:   int64(cfg.MaxQueue),
		maxWait:    cfg.MaxWait,
		retryAfter: cfg.RetryAfter,
	}
}

type writeAdmittedKey struct{}

// WriteAdmitted tells whether the request carried by ctx already owns a write slot.
func WriteAdmitted(ctx context.Context) bool {
	v, _ := ctx.Value(writeAdmittedKey{}).(bool)
	return v
}

// Admit reserves a write slot for the whole request and returns a context flagged as admitted, so that
// the controller wrapper does not ask for a second slot (a request such as a channel debit writes
// several transactions: admitting it once, up front, means it is either rejected before it has
// written anything or allowed to finish all its legs). If ctx is already admitted, Admit is a no-op.
func (l *WriteLimiter) Admit(ctx context.Context) (context.Context, func(), error) {
	if l == nil || WriteAdmitted(ctx) {
		return ctx, func() {}, nil
	}
	release, err := l.Acquire(ctx)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, writeAdmittedKey{}, true), release, nil
}

// DefaultMaxInFlightWrites derives a write bound from the database pool size: it keeps a few
// connections free for reads (balances, history, health) so that writers can never take the
// whole pool. Returns 0 (no limit) when the pool size is unknown.
func DefaultMaxInFlightWrites(maxOpenConns int) int {
	if maxOpenConns <= 0 {
		return 0
	}
	reserve := maxOpenConns / 5
	if reserve < 2 {
		reserve = 2
	}
	ret := maxOpenConns - reserve
	if ret < 1 {
		ret = 1
	}
	return ret
}

// Acquire reserves a write slot. The returned function must be called once the write is over.
func (l *WriteLimiter) Acquire(ctx context.Context) (func(), error) {
	if l == nil {
		return func() {}, nil
	}

	// fast path
	select {
	case l.slots <- struct{}{}:
		return l.releaseFn(), nil
	default:
	}

	if l.waiting.Add(1) > l.maxQueue {
		l.waiting.Add(-1)
		l.rejected.Add(1)
		return nil, NewErrWriteOverloaded("queue full", l.retryAfter)
	}
	defer l.waiting.Add(-1)

	timer := time.NewTimer(l.maxWait)
	defer timer.Stop()

	select {
	case l.slots <- struct{}{}:
		return l.releaseFn(), nil
	case <-timer.C:
		l.rejected.Add(1)
		return nil, NewErrWriteOverloaded("wait timeout", l.retryAfter)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *WriteLimiter) releaseFn() func() {
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			<-l.slots
		}
	}
}

// InFlight is the number of writes currently holding a slot.
func (l *WriteLimiter) InFlight() int {
	if l == nil {
		return 0
	}
	return len(l.slots)
}

// Waiting is the number of requests currently queued for a slot.
func (l *WriteLimiter) Waiting() int {
	if l == nil {
		return 0
	}
	return int(l.waiting.Load())
}

// Rejected is the number of requests rejected since start.
func (l *WriteLimiter) Rejected() int64 {
	if l == nil {
		return 0
	}
	return l.rejected.Load()
}

// ControllerWithWriteLimiter applies a WriteLimiter to every operation that opens a write
// transaction. The limiter must be shared by all controllers of the process, since they all
// share the connection pool.
//
// BeginTX deliberately returns the underlying transactional controller without the limiter: a
// caller that already owns a transaction (bulk, ledger state tracking) holds a connection and must
// not queue for a second slot while holding it.
type ControllerWithWriteLimiter struct {
	Controller
	limiter *WriteLimiter
}

func NewControllerWithWriteLimiter(underlying Controller, limiter *WriteLimiter) Controller {
	if limiter == nil {
		return underlying
	}
	return &ControllerWithWriteLimiter{Controller: underlying, limiter: limiter}
}

func (c *ControllerWithWriteLimiter) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, *bun.Tx, error) {
	return c.Controller.BeginTX(ctx, options)
}

func (c *ControllerWithWriteLimiter) LockLedger(ctx context.Context) (Controller, bun.IDB, func() error, error) {
	return c.Controller.LockLedger(ctx)
}

func (c *ControllerWithWriteLimiter) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer release()
	return c.Controller.CreateTransaction(ctx, parameters)
}

func (c *ControllerWithWriteLimiter) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer release()
	return c.Controller.RevertTransaction(ctx, parameters)
}

func (c *ControllerWithWriteLimiter) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, false, err
	}
	defer release()
	return c.Controller.SaveTransactionMetadata(ctx, parameters)
}

func (c *ControllerWithWriteLimiter) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, false, err
	}
	defer release()
	return c.Controller.SaveAccountMetadata(ctx, parameters)
}

func (c *ControllerWithWriteLimiter) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, false, err
	}
	defer release()
	return c.Controller.DeleteTransactionMetadata(ctx, parameters)
}

func (c *ControllerWithWriteLimiter) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, false, err
	}
	defer release()
	return c.Controller.DeleteAccountMetadata(ctx, parameters)
}

func (c *ControllerWithWriteLimiter) InsertSchema(ctx context.Context, parameters Parameters[InsertSchema]) (*ledger.Log, *ledger.InsertedSchema, bool, error) {
	ctx, release, err := c.limiter.Admit(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer release()
	return c.Controller.InsertSchema(ctx, parameters)
}

// IsErrWriteOverloaded reports whether err is, or wraps, an ErrWriteOverloaded.
func IsErrWriteOverloaded(err error) (ErrWriteOverloaded, bool) {
	var e ErrWriteOverloaded
	if errors.As(err, &e) {
		return e, true
	}
	return ErrWriteOverloaded{}, false
}
