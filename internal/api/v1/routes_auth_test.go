package v1

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledger "github.com/formancehq/ledger/internal"
)

const testValidToken = "valid-test-token"

type fakeAuthenticator struct{}

func (fakeAuthenticator) Authenticate(_ http.ResponseWriter, r *http.Request) (bool, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return false, errors.New("no authorization header")
	}
	if header != "Bearer "+testValidToken {
		return false, errors.New("invalid token")
	}
	return true, nil
}

// panicReporter makes gomock failures panic so serve() can recover them: a request
// that passes the auth gate may hit an unexpected mock call, which only matters
// here as "not a 401".
type panicReporter struct{}

func (panicReporter) Errorf(string, ...any) {}
func (panicReporter) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
func (panicReporter) Helper() {}

func serve(router http.Handler, req *http.Request) (code int) {
	rec := httptest.NewRecorder()
	defer func() {
		if recover() != nil {
			code = http.StatusInternalServerError
		}
	}()
	router.ServeHTTP(rec, req)
	return rec.Code
}

// The v1 API (mounted at the root, "/*") must be behind auth like v2, including /_info.
func TestV1RoutesRequireAuth(t *testing.T) {
	ctrl := gomock.NewController(panicReporter{})
	ledgerController := NewLedgerController(ctrl)
	systemController := NewSystemController(ctrl)
	systemController.EXPECT().GetLedger(gomock.Any(), gomock.Any()).AnyTimes().Return(&ledger.Ledger{}, nil)
	systemController.EXPECT().GetLedgerController(gomock.Any(), gomock.Any()).AnyTimes().Return(ledgerController, nil)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()

	router := NewRouter(systemController, fakeAuthenticator{}, "develop", false)

	routes := []struct{ method, path string }{
		{http.MethodGet, "/_info"},
		{http.MethodGet, "/ledger1/_info"},
		{http.MethodGet, "/ledger1/stats"},
		{http.MethodGet, "/ledger1/logs"},
		{http.MethodGet, "/ledger1/accounts"},
		{http.MethodPost, "/ledger1/accounts/acc1/metadata"},
		{http.MethodDelete, "/ledger1/accounts/acc1/metadata/k"},
		{http.MethodGet, "/ledger1/transactions"},
		{http.MethodPost, "/ledger1/transactions"},
		{http.MethodPost, "/ledger1/transactions/1/revert"},
		{http.MethodPost, "/ledger1/transactions/1/metadata"},
		{http.MethodDelete, "/ledger1/transactions/1/metadata/k"},
		{http.MethodGet, "/ledger1/balances"},
		{http.MethodGet, "/ledger1/aggregate/balances"},
	}

	newRequest := func(method, path, authorization string) *http.Request {
		body := ""
		if method == http.MethodPost {
			body = "{"
		}
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		return req
	}

	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			require.Equal(t, http.StatusUnauthorized, serve(router, newRequest(rt.method, rt.path, "")))
			require.Equal(t, http.StatusUnauthorized, serve(router, newRequest(rt.method, rt.path, "Bearer wrong")))

			code := serve(router, newRequest(rt.method, rt.path, "Bearer "+testValidToken))
			require.NotEqual(t, http.StatusUnauthorized, code)
			require.NotEqual(t, http.StatusNotFound, code, "route not registered")
			require.NotEqual(t, http.StatusMethodNotAllowed, code, "route not registered")
		})
	}
}
