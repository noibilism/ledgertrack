package v2

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/walletlock"
)

func TestWalletLockerIsExclusivePerKey(t *testing.T) {
	t.Parallel()

	l := walletlock.New()

	var (
		inside, violations atomic.Int64
		wg                 sync.WaitGroup
	)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, err := l.Lock(context.Background(), "ledger/w1")
			require.NoError(t, err)
			defer unlock()

			if inside.Add(1) != 1 {
				violations.Add(1)
			}
			time.Sleep(200 * time.Microsecond)
			inside.Add(-1)
		}()
	}
	wg.Wait()

	require.Zero(t, violations.Load())
	require.Zero(t, l.Size(), "entries must be garbage collected")
}

func TestWalletLockerDoesNotBlockOtherKeys(t *testing.T) {
	t.Parallel()

	l := walletlock.New()
	unlock1, err := l.Lock(context.Background(), "ledger/w1")
	require.NoError(t, err)
	defer unlock1()

	done := make(chan struct{})
	go func() {
		defer close(done)
		unlock2, err := l.Lock(context.Background(), "ledger/w2")
		require.NoError(t, err)
		unlock2()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a different wallet must not wait")
	}
}

func TestWalletLockerCancelWhileWaiting(t *testing.T) {
	t.Parallel()

	l := walletlock.New()
	unlock, err := l.Lock(context.Background(), "k")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = l.Lock(ctx, "k")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	unlock()
	unlock() // idempotent
	require.Zero(t, l.Size())

	// the key is usable again
	unlock, err = l.Lock(context.Background(), "k")
	require.NoError(t, err)
	unlock()
	require.Zero(t, l.Size())
}

// Two writes on the same wallet must never reach the ledger controller at the same time, otherwise
// they deadlock in Postgres (credit and debit lock system:control and the wallet in opposite orders).
func TestWalletWritesOnSameWalletAreSerialized(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()

	var (
		inside, violations, calls atomic.Int64
	)
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
			if inside.Add(1) != 1 {
				violations.Add(1)
			}
			calls.Add(1)
			time.Sleep(2 * time.Millisecond)
			inside.Add(-1)
			return &ledger.Log{}, &ledger.CreatedTransaction{Transaction: ledger.NewTransaction()}, false, nil
		}).
		AnyTimes()

	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op := "credit"
			if i%2 == 1 {
				op = "lien"
			}
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/test/wallets/hot-USD/%s", op),
				api.Buffer(t, WalletTransactionRequest{Amount: testJSONNumber("100"), Reference: fmt.Sprintf("ref-%d", i)}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusCreated, rec.Code)
		}(i)
	}
	wg.Wait()

	require.EqualValues(t, 24, calls.Load())
	require.Zero(t, violations.Load())
	require.Zero(t, defaultWalletLocker.Size())
}

func TestWalletWriteOverloadedAnswers503WithRetryAfter(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		Return(nil, nil, false, ledgercontroller.NewErrWriteOverloaded("queue full", 3*time.Second))

	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	req := httptest.NewRequest(http.MethodPost, "/test/wallets/user123-USD/credit",
		api.Buffer(t, WalletTransactionRequest{Amount: testJSONNumber("100"), Reference: "ref1"}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "3", rec.Header().Get("Retry-After"))

	errResp := api.ErrorResponse{}
	api.Decode(t, rec.Body, &errResp)
	require.Equal(t, common.ErrOverloaded, errResp.ErrorCode)
}
