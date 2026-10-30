package ledger

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
)

func TestDefaultMaxInFlightWrites(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, DefaultMaxInFlightWrites(0))
	require.Equal(t, 0, DefaultMaxInFlightWrites(-1))
	require.Equal(t, 1, DefaultMaxInFlightWrites(1))
	require.Equal(t, 1, DefaultMaxInFlightWrites(2))
	require.Equal(t, 3, DefaultMaxInFlightWrites(5))
	require.Equal(t, 16, DefaultMaxInFlightWrites(20))
	require.Equal(t, 56, DefaultMaxInFlightWrites(70))
	require.Equal(t, 80, DefaultMaxInFlightWrites(100))
}

func TestNilWriteLimiterAllowsEverything(t *testing.T) {
	t.Parallel()

	var l *WriteLimiter
	release, err := l.Acquire(context.Background())
	require.NoError(t, err)
	release()
	require.Zero(t, l.InFlight())
	require.Nil(t, NewWriteLimiter(WriteLimiterConfiguration{}))
}

func TestWriteLimiterBoundsConcurrency(t *testing.T) {
	t.Parallel()

	const maxInFlight = 4
	l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: maxInFlight, MaxQueue: 1000, MaxWait: 5 * time.Second})

	var (
		current, peak atomic.Int64
		wg            sync.WaitGroup
	)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := l.Acquire(context.Background())
			require.NoError(t, err)
			defer release()

			n := current.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			current.Add(-1)
		}()
	}
	wg.Wait()

	require.LessOrEqual(t, peak.Load(), int64(maxInFlight))
	require.Zero(t, l.InFlight())
	require.Zero(t, l.Waiting())
	require.Zero(t, l.Rejected())
}

func TestWriteLimiterRejectsWhenQueueIsFull(t *testing.T) {
	t.Parallel()

	l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1, MaxQueue: 1, MaxWait: 5 * time.Second, RetryAfter: 3 * time.Second})

	hold, err := l.Acquire(context.Background())
	require.NoError(t, err)

	// one waiter fits in the queue
	waiterDone := make(chan error, 1)
	go func() {
		release, err := l.Acquire(context.Background())
		if err == nil {
			release()
		}
		waiterDone <- err
	}()
	require.Eventually(t, func() bool { return l.Waiting() == 1 }, time.Second, time.Millisecond)

	// the next one is rejected immediately
	start := time.Now()
	_, err = l.Acquire(context.Background())
	require.Less(t, time.Since(start), time.Second)
	require.ErrorIs(t, err, ErrWriteOverloaded{})
	e, ok := IsErrWriteOverloaded(err)
	require.True(t, ok)
	require.Equal(t, 3*time.Second, e.RetryAfter)
	require.EqualValues(t, 1, l.Rejected())

	hold()
	require.NoError(t, <-waiterDone)
}

func TestWriteLimiterRejectsAfterMaxWait(t *testing.T) {
	t.Parallel()

	l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1, MaxQueue: 10, MaxWait: 30 * time.Millisecond})
	hold, err := l.Acquire(context.Background())
	require.NoError(t, err)
	defer hold()

	_, err = l.Acquire(context.Background())
	require.ErrorIs(t, err, ErrWriteOverloaded{})
	require.Zero(t, l.Waiting())
}

func TestWriteLimiterHonoursContextCancellation(t *testing.T) {
	t.Parallel()

	l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1, MaxQueue: 10, MaxWait: 5 * time.Second})
	hold, err := l.Acquire(context.Background())
	require.NoError(t, err)
	defer hold()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err = l.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, l.Waiting())
}

func TestWriteLimiterReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 2})
	r1, err := l.Acquire(context.Background())
	require.NoError(t, err)
	r2, err := l.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, l.InFlight())

	r1()
	r1()
	require.Equal(t, 1, l.InFlight())
	r2()
	require.Zero(t, l.InFlight())
}

// fakeController only implements what the limiter wrapper touches.
type fakeController struct {
	Controller
	createCalls atomic.Int64
	createErr   error
	txCtrl      Controller
}

func (f *fakeController) CreateTransaction(context.Context, Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
	f.createCalls.Add(1)
	return nil, nil, false, f.createErr
}

func (f *fakeController) BeginTX(context.Context, *sql.TxOptions) (Controller, *bun.Tx, error) {
	return f.txCtrl, &bun.Tx{}, nil
}

func TestControllerWithWriteLimiter(t *testing.T) {
	t.Parallel()

	t.Run("nil limiter returns the underlying controller", func(t *testing.T) {
		t.Parallel()
		f := &fakeController{}
		require.Same(t, f, NewControllerWithWriteLimiter(f, nil))
	})

	t.Run("rejects without touching the underlying controller when saturated", func(t *testing.T) {
		t.Parallel()
		f := &fakeController{}
		l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1, MaxQueue: 1, MaxWait: 20 * time.Millisecond})
		c := NewControllerWithWriteLimiter(f, l)

		hold, err := l.Acquire(context.Background())
		require.NoError(t, err)

		_, _, _, err = c.CreateTransaction(context.Background(), Parameters[CreateTransaction]{})
		require.ErrorIs(t, err, ErrWriteOverloaded{})
		require.Zero(t, f.createCalls.Load())

		hold()
		_, _, _, err = c.CreateTransaction(context.Background(), Parameters[CreateTransaction]{})
		require.NoError(t, err)
		require.EqualValues(t, 1, f.createCalls.Load())
		require.Zero(t, l.InFlight(), "slot must be released after the write")
	})

	t.Run("releases the slot when the write fails", func(t *testing.T) {
		t.Parallel()
		f := &fakeController{createErr: errors.New("boom")}
		l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1})
		c := NewControllerWithWriteLimiter(f, l)

		_, _, _, err := c.CreateTransaction(context.Background(), Parameters[CreateTransaction]{})
		require.EqualError(t, err, "boom")
		require.Zero(t, l.InFlight())
	})

	t.Run("a controller bound to a transaction is not limited", func(t *testing.T) {
		t.Parallel()
		txCtrl := &fakeController{}
		f := &fakeController{txCtrl: txCtrl}
		l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1, MaxWait: 10 * time.Millisecond})
		c := NewControllerWithWriteLimiter(f, l)

		// saturate the limiter: a caller that already holds a transaction must still be able to write
		hold, err := l.Acquire(context.Background())
		require.NoError(t, err)
		defer hold()

		inTx, _, err := c.BeginTX(context.Background(), nil)
		require.NoError(t, err)
		_, _, _, err = inTx.CreateTransaction(context.Background(), Parameters[CreateTransaction]{})
		require.NoError(t, err)
		require.EqualValues(t, 1, txCtrl.createCalls.Load())
	})
}

func TestWriteLimiterAdmitIsReentrant(t *testing.T) {
	t.Parallel()

	l := NewWriteLimiter(WriteLimiterConfiguration{MaxInFlight: 1, MaxQueue: 1, MaxWait: 10 * time.Millisecond})

	ctx, release, err := l.Admit(context.Background())
	require.NoError(t, err)
	require.True(t, WriteAdmitted(ctx))
	require.False(t, WriteAdmitted(context.Background()))
	require.Equal(t, 1, l.InFlight())

	// an admitted request does not take a second slot, whatever the number of legs it writes
	ctx2, release2, err := l.Admit(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, l.InFlight())
	release2()
	require.Equal(t, 1, l.InFlight())

	f := &fakeController{}
	c := NewControllerWithWriteLimiter(f, l)
	for i := 0; i < 3; i++ {
		_, _, _, err = c.CreateTransaction(ctx2, Parameters[CreateTransaction]{})
		require.NoError(t, err)
	}
	require.EqualValues(t, 3, f.createCalls.Load())
	require.Equal(t, 1, l.InFlight())

	release()
	require.Zero(t, l.InFlight())

	// a nil limiter admits everything
	var nilLimiter *WriteLimiter
	_, r, err := nilLimiter.Admit(context.Background())
	require.NoError(t, err)
	r()
}
