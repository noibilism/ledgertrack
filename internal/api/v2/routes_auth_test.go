package v2

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/oidc"

	"github.com/formancehq/ledger/internal/cba/services"
)

const testValidToken = "valid-test-token"

// fakeAuthenticator accepts exactly one bearer token.
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

// financeReportingServiceStub only exists so that the /reports/finance routes are
// registered. Its methods are never expected to be reached when auth rejects the
// request; when auth lets the request through the nil embedded interface panics,
// which serve() converts to a 500 (anything but 401 proves the gate was passed).
type financeReportingServiceStub struct {
	services.FinanceReportingService
}

// panicReporter makes gomock failures panic so that serve() can recover them.
type panicReporter struct{}

func (panicReporter) Errorf(string, ...any) {}
func (panicReporter) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
func (panicReporter) Helper() {}

type routeCase struct {
	method string
	path   string
	// group is a human readable name for the route group, mirrored in docs/security/auth.md.
	group string
}

// newFullyWiredRouter registers every optional route group (CBA, channels, reports,
// exporters/pipelines) so none of them can be missing from the auth assertions.
func newFullyWiredRouter(t *testing.T, authenticator auth.Authenticator) chi.Router {
	t.Helper()

	// A reporter that panics instead of calling t.FailNow: once a request passes the
	// auth gate its handler may call an unexpected mock method, which serve() turns
	// into a 500. Only the status code relative to 401 matters here.
	ctrl := gomock.NewController(panicReporter{})
	ledgerController := NewLedgerController(ctrl)
	systemController := NewSystemController(ctrl)
	systemController.EXPECT().GetLedgerController(gomock.Any(), gomock.Any()).AnyTimes().Return(ledgerController, nil)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	// logs/import calls Import from a goroutine, where a panic could not be recovered.
	ledgerController.EXPECT().Import(gomock.Any(), gomock.Any()).Return(errors.New("stubbed")).AnyTimes()

	productService, _ := newProductServiceForHTTPTests()
	clientService, kycService, _, _ := newClientAndKYCServicesForHTTPTests()
	accountService, _, _, _, _ := newAccountServiceForHTTPTests()
	reportingService, _, _, _, _ := newReportingServiceForHTTPTests()

	return NewRouter(
		systemController,
		authenticator,
		"develop",
		WithExporters(true),
		WithProductService(productService),
		WithClientService(clientService),
		WithKYCService(kycService),
		WithAccountService(accountService),
		WithReportingService(reportingService),
		WithFinanceReportingService(financeReportingServiceStub{}),
		WithChannelFeeConfigService(newChannelFeeConfigServiceForHTTPTests()),
		WithChannelRevenueReportingService(&channelRevenueReportingServiceForHTTPTests{}),
	)
}

// serve runs the request and returns the status code. A handler panic (reached only
// once the auth gate has been passed, with stubbed dependencies) is reported as 500.
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

func newRouteRequest(tc routeCase, token string) *http.Request {
	body := ""
	if tc.method != http.MethodGet && tc.method != http.MethodHead && tc.method != http.MethodDelete {
		// Invalid JSON: handlers that are reached fail validation with a 4xx before
		// touching any dependency, so no state is changed by these tests.
		body = "{"
	}
	req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	return req
}

// moneyAndDataRoutes lists every mutating route plus the report routes, per group.
var moneyAndDataRoutes = []routeCase{
	// ledgers and system
	{http.MethodPost, "/ledgertrack", "ledger create"},
	{http.MethodPut, "/ledgertrack/metadata", "ledger metadata"},
	{http.MethodDelete, "/ledgertrack/metadata/k", "ledger metadata"},
	{http.MethodDelete, "/_/buckets/b1", "system buckets"},
	{http.MethodPost, "/_/buckets/b1/restore", "system buckets"},
	{http.MethodPost, "/_/exporters", "system exporters"},
	{http.MethodPut, "/_/exporters/e1", "system exporters"},
	{http.MethodDelete, "/_/exporters/e1", "system exporters"},
	{http.MethodPost, "/ledgertrack/pipelines", "pipelines"},
	{http.MethodPost, "/ledgertrack/pipelines/p1/start", "pipelines"},
	{http.MethodPost, "/ledgertrack/pipelines/p1/reset", "pipelines"},
	{http.MethodPost, "/ledgertrack/_bulk", "bulk"},
	{http.MethodPost, "/ledgertrack/logs/import", "logs import"},
	{http.MethodPost, "/ledgertrack/logs/export", "logs export"},
	{http.MethodPost, "/ledgertrack/schema/v1", "schema"},

	// native ledger transactions
	{http.MethodPost, "/ledgertrack/transactions", "native transactions"},
	{http.MethodPost, "/ledgertrack/transactions/1/revert", "native transactions"},
	{http.MethodPost, "/ledgertrack/transactions/1/metadata", "native transactions"},
	{http.MethodDelete, "/ledgertrack/transactions/1/metadata/k", "native transactions"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/metadata", "native accounts metadata"},
	{http.MethodDelete, "/ledgertrack/accounts/acc1/metadata/k", "native accounts metadata"},

	// wallets
	{http.MethodPost, "/ledgertrack/wallets", "wallets"},
	{http.MethodPost, "/ledgertrack/wallets/w1/credit", "wallets"},
	{http.MethodPost, "/ledgertrack/wallets/w1/debit", "wallets"},
	{http.MethodPost, "/ledgertrack/wallets/w1/lien", "wallets"},
	{http.MethodPost, "/ledgertrack/wallets/w1/lien/release", "wallets"},

	// CBA accounts
	{http.MethodPost, "/ledgertrack/accounts", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/credit", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/debit", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/lien", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/lien/release", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/activate", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/suspend", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/freeze", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/dormant", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/reactivate", "cba accounts"},
	{http.MethodPost, "/ledgertrack/accounts/acc1/close", "cba accounts"},

	// channels
	{http.MethodPost, "/ledgertrack/channels", "channels"},
	{http.MethodPost, "/ledgertrack/channels/c1/credit", "channels"},
	{http.MethodPut, "/ledgertrack/channels/c1/fees/config", "channel fees"},

	// products
	{http.MethodPost, "/ledgertrack/products", "products"},
	{http.MethodPatch, "/ledgertrack/products/p1", "products"},
	{http.MethodPost, "/ledgertrack/products/p1/activate", "products"},
	{http.MethodPost, "/ledgertrack/products/p1/retire", "products"},

	// clients and KYC
	{http.MethodPost, "/ledgertrack/clients", "clients"},
	{http.MethodPatch, "/ledgertrack/clients/c1", "clients"},
	{http.MethodPost, "/ledgertrack/clients/c1/activate", "clients"},
	{http.MethodPost, "/ledgertrack/clients/c1/suspend", "clients"},
	{http.MethodPost, "/ledgertrack/clients/c1/reactivate", "clients"},
	{http.MethodPost, "/ledgertrack/clients/c1/close", "clients"},
	{http.MethodPost, "/ledgertrack/clients/c1/kyc", "client kyc"},
	{http.MethodPost, "/ledgertrack/clients/c1/kyc/k1/verify", "client kyc"},
	{http.MethodPost, "/ledgertrack/clients/c1/kyc/k1/reject", "client kyc"},

	// reports (read only, but expose balances and PII)
	{http.MethodGet, "/ledgertrack/reports/clients/c1/portfolio", "reports"},
	{http.MethodGet, "/ledgertrack/reports/accounts/acc1/statement", "reports"},
	{http.MethodGet, "/ledgertrack/reports/transactions/daily", "reports"},
	{http.MethodGet, "/ledgertrack/reports/interest-fees", "reports"},
	{http.MethodGet, "/ledgertrack/reports/finance/trial-balance", "finance reports"},
	{http.MethodGet, "/ledgertrack/reports/finance/balance-sheet", "finance reports"},
	{http.MethodGet, "/ledgertrack/reports/finance/pnl", "finance reports"},
	{http.MethodGet, "/ledgertrack/reports/finance/cash-flow", "finance reports"},
	{http.MethodPost, "/ledgertrack/reports/channels/revenue", "channel revenue reports"},
	{http.MethodPost, "/ledgertrack/reports/channels/revenue/timeseries", "channel revenue reports"},
	{http.MethodPost, "/ledgertrack/reports/channels/revenue/export", "channel revenue reports"},
	{http.MethodPost, "/ledgertrack/reports/channels/dashboard", "channel revenue reports"},

	// reads that still leak financial data
	{http.MethodGet, "/_info", "info"},
	{http.MethodGet, "/", "ledger list"},
	{http.MethodGet, "/ledgertrack/transactions", "native transactions"},
	{http.MethodGet, "/ledgertrack/accounts", "native accounts"},
	{http.MethodGet, "/ledgertrack/aggregate/balances", "balances"},
	{http.MethodGet, "/ledgertrack/logs", "logs"},
	{http.MethodGet, "/ledgertrack/wallets/balances", "wallets"},
	{http.MethodGet, "/ledgertrack/wallets/w1/statement", "wallets"},
	{http.MethodGet, "/ledgertrack/channels/c1/history", "channels"},
	{http.MethodGet, "/ledgertrack/products", "products"},
	{http.MethodGet, "/ledgertrack/clients", "clients"},
	{http.MethodGet, "/_/exporters", "system exporters"},
}

func TestRoutesRejectMissingOrInvalidToken(t *testing.T) {
	router := newFullyWiredRouter(t, fakeAuthenticator{})

	for _, tc := range moneyAndDataRoutes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			require.Equal(t, http.StatusUnauthorized, serve(router, newRouteRequest(tc, "")),
				"%s %s (%s) must return 401 without a token", tc.method, tc.path, tc.group)
			require.Equal(t, http.StatusUnauthorized, serve(router, newRouteRequest(tc, "Bearer wrong")),
				"%s %s (%s) must return 401 with an invalid token", tc.method, tc.path, tc.group)
			require.Equal(t, http.StatusUnauthorized, serve(router, newRouteRequest(tc, testValidToken)),
				"%s %s (%s) must return 401 when the scheme is not Bearer", tc.method, tc.path, tc.group)
		})
	}
}

func TestRoutesPassThroughWithValidToken(t *testing.T) {
	router := newFullyWiredRouter(t, fakeAuthenticator{})

	for _, tc := range moneyAndDataRoutes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			code := serve(router, newRouteRequest(tc, "Bearer "+testValidToken))
			require.NotEqual(t, http.StatusUnauthorized, code,
				"%s %s (%s) must pass the auth gate with a valid token", tc.method, tc.path, tc.group)
			// 404/405 would mean the route is not registered, which would make the
			// 401 assertions above meaningless for it.
			require.NotEqual(t, http.StatusNotFound, code, "%s %s is not registered", tc.method, tc.path)
			require.NotEqual(t, http.StatusMethodNotAllowed, code, "%s %s is not registered", tc.method, tc.path)
		})
	}
}

// TestAuthRunsBeforeHandlers makes sure a rejected request never reaches a handler
// or the system controller (strict mock: any unexpected call fails the test).
func TestAuthRunsBeforeHandlers(t *testing.T) {
	ctrl := gomock.NewController(t)
	systemController := NewSystemController(ctrl) // no expectations set
	router := NewRouter(systemController, fakeAuthenticator{}, "develop")

	for _, tc := range []routeCase{
		{http.MethodPost, "/ledgertrack/transactions", ""},
		{http.MethodPost, "/ledgertrack/wallets/w1/credit", ""},
		{http.MethodPost, "/ledgertrack/_bulk", ""},
		{http.MethodDelete, "/_/buckets/b1", ""},
	} {
		require.Equal(t, http.StatusUnauthorized, serve(router, newRouteRequest(tc, "")))
	}
}

// staticKeySet verifies signatures against a single in-memory RSA key.
type staticKeySet struct{ key *rsa.PublicKey }

func (k staticKeySet) VerifySignature(_ context.Context, jws *jose.JSONWebSignature) ([]byte, error) {
	return jws.Verify(k.key)
}

func signToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, nil)
	require.NoError(t, err)
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	jws, err := signer.Sign(payload)
	require.NoError(t, err)
	token, err := jws.CompactSerialize()
	require.NoError(t, err)
	return token
}

// TestRealJWTAuthenticatorScopes exercises the go-libs JWT authenticator (not a fake)
// behind the router, with scope checking enabled, to pin down the exact semantics
// documented in docs/security/auth.md.
func TestRealJWTAuthenticatorScopes(t *testing.T) {
	const issuer = "https://issuer.cray.test"
	const svc = "ledgertrack"

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	authenticator := auth.NewJWTAuth(staticKeySet{key: &key.PublicKey}, issuer, svc, true)
	router := newFullyWiredRouter(t, authenticator)

	claims := func(iss string, exp time.Time, scope string) map[string]any {
		return map[string]any{"iss": iss, "sub": "svc-posting", "exp": exp.Unix(), "scope": scope}
	}
	future := time.Now().Add(time.Hour)

	post := routeCase{http.MethodPost, "/ledgertrack/wallets/w1/credit", "wallets"}
	get := routeCase{http.MethodGet, "/ledgertrack/transactions", "native transactions"}

	tests := []struct {
		name     string
		token    string
		req      routeCase
		wantAuth bool
	}{
		{"write scope can POST", signToken(t, key, claims(issuer, future, svc+":write")), post, true},
		{"write scope can GET", signToken(t, key, claims(issuer, future, svc+":write")), get, true},
		{"read scope can GET", signToken(t, key, claims(issuer, future, svc+":read")), get, true},
		{"read scope can not POST", signToken(t, key, claims(issuer, future, svc+":read")), post, false},
		{"no scope can not GET", signToken(t, key, claims(issuer, future, "")), get, false},
		{"scope of another service is rejected", signToken(t, key, claims(issuer, future, "other:write")), post, false},
		{"wrong issuer is rejected", signToken(t, key, claims("https://evil.test", future, svc+":write")), post, false},
		{"expired token is rejected", signToken(t, key, claims(issuer, time.Now().Add(-time.Minute), svc+":write")), post, false},
		{"token without exp is rejected", signToken(t, key, map[string]any{"iss": issuer, "scope": svc + ":write"}), post, false},
		{"token signed by another key is rejected", signToken(t, otherKey, claims(issuer, future, svc+":write")), post, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := serve(router, newRouteRequest(tt.req, "Bearer "+tt.token))
			if tt.wantAuth {
				require.NotEqual(t, http.StatusUnauthorized, code)
			} else {
				require.Equal(t, http.StatusUnauthorized, code)
			}
		})
	}

	t.Run("unsigned alg=none token is rejected", func(t *testing.T) {
		header := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0" // {"alg":"none","typ":"JWT"}
		payload, err := json.Marshal(claims(issuer, future, svc+":write"))
		require.NoError(t, err)
		token := header + "." + base64URL(payload) + "."
		require.Equal(t, http.StatusUnauthorized, serve(router, newRouteRequest(post, "Bearer "+token)))
	})
}

func base64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

var _ oidc.KeySet = staticKeySet{}
