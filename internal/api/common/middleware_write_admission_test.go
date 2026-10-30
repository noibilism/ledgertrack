package common

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/api"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestWriteAdmissionNilLimiterIsPassThrough(t *testing.T) {
	t.Parallel()

	called := false
	h := WriteAdmission(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	require.True(t, called)
}

func TestWriteAdmissionFlagsTheContextAndReleasesTheSlot(t *testing.T) {
	t.Parallel()

	limiter := ledgercontroller.NewWriteLimiter(ledgercontroller.WriteLimiterConfiguration{MaxInFlight: 1})
	h := WriteAdmission(limiter)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.True(t, ledgercontroller.WriteAdmitted(r.Context()))
		require.Equal(t, 1, limiter.InFlight())
		w.WriteHeader(http.StatusCreated)
	}))

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		require.Equal(t, http.StatusCreated, rec.Code)
		require.Zero(t, limiter.InFlight())
	}
}

func TestWriteAdmissionDoesNotLimitReads(t *testing.T) {
	t.Parallel()

	limiter := ledgercontroller.NewWriteLimiter(ledgercontroller.WriteLimiterConfiguration{MaxInFlight: 1, MaxQueue: 1, MaxWait: time.Millisecond})
	hold, err := limiter.Acquire(t.Context())
	require.NoError(t, err)
	defer hold()

	h := WriteAdmission(limiter)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.False(t, ledgercontroller.WriteAdmitted(r.Context()))
		w.WriteHeader(http.StatusOK)
	}))
	for _, m := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(m, "/", nil))
		require.Equal(t, http.StatusOK, rec.Code, m)
	}
}

func TestWriteAdmissionAnswers503WithRetryAfterWhenSaturated(t *testing.T) {
	t.Parallel()

	limiter := ledgercontroller.NewWriteLimiter(ledgercontroller.WriteLimiterConfiguration{
		MaxInFlight: 1, MaxQueue: 1, MaxWait: time.Second, RetryAfter: 2 * time.Second,
	})

	started := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	h := WriteAdmission(limiter)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-unblock
		w.WriteHeader(http.StatusCreated)
	}))

	// request 1 takes the only slot
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	}()
	<-started

	// request 2 waits in the queue
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	}()
	require.Eventually(t, func() bool { return limiter.Waiting() == 1 }, time.Second, time.Millisecond)

	// request 3 finds the queue full and is rejected immediately, without reaching the handler
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "2", rec.Header().Get("Retry-After"))
	errResp := api.ErrorResponse{}
	api.Decode(t, rec.Body, &errResp)
	require.Equal(t, ErrOverloaded, errResp.ErrorCode)

	// unblock the others; request 2 must also complete (handler channel is closed only once)
	close(unblock)
	wg.Wait()
}
