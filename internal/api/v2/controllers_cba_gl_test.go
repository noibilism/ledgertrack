package v2

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	libtime "github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type glRepositoryForHTTPTests struct {
	accounts   map[string]*models.GLAccount
	lastFilter repositories.GLAccountFilter
}

func newGLRepositoryForHTTPTests() *glRepositoryForHTTPTests {
	return &glRepositoryForHTTPTests{accounts: map[string]*models.GLAccount{}}
}

func (s *glRepositoryForHTTPTests) Create(_ context.Context, account *models.GLAccount) error {
	if _, ok := s.accounts[account.Code]; ok {
		return postgres.ErrConstraintsFailed{}
	}
	if account.ParentCode != nil {
		if _, ok := s.accounts[*account.ParentCode]; !ok {
			return errors.New("foreign key violation")
		}
	}
	if account.ID == uuid.Nil {
		account.ID = uuid.New()
	}
	copied := *account
	s.accounts[account.Code] = &copied
	return nil
}

func (s *glRepositoryForHTTPTests) Update(_ context.Context, account *models.GLAccount) error {
	current, ok := s.accounts[account.Code]
	if !ok {
		return postgres.ErrNotFound
	}
	current.Name, current.Status, current.Metadata = account.Name, account.Status, account.Metadata
	return nil
}

func (s *glRepositoryForHTTPTests) GetByCode(_ context.Context, code string) (*models.GLAccount, error) {
	account, ok := s.accounts[code]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *account
	return &copied, nil
}

func (s *glRepositoryForHTTPTests) GetByAddress(_ context.Context, ledgerName, address string) (*models.GLAccount, error) {
	for _, account := range s.accounts {
		if account.Ledger == ledgerName && account.Address == address {
			copied := *account
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *glRepositoryForHTTPTests) List(_ context.Context, filter repositories.GLAccountFilter) ([]models.GLAccount, error) {
	s.lastFilter = filter
	ret := make([]models.GLAccount, 0, len(s.accounts))
	for _, account := range s.accounts {
		if filter.Type != nil && account.Type != *filter.Type {
			continue
		}
		if filter.Currency != nil && account.Currency != *filter.Currency {
			continue
		}
		if filter.Status != nil && account.Status != *filter.Status {
			continue
		}
		if filter.ParentCode != nil && (account.ParentCode == nil || *account.ParentCode != *filter.ParentCode) {
			continue
		}
		ret = append(ret, *account)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Code < ret[j].Code })
	return ret, nil
}

func (s *glRepositoryForHTTPTests) RunExclusive(ctx context.Context, fn func(context.Context, repositories.GLAccountRepository) error) error {
	tx := &glRepositoryForHTTPTests{accounts: map[string]*models.GLAccount{}}
	for code, account := range s.accounts {
		copied := *account
		tx.accounts[code] = &copied
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	s.accounts = tx.accounts
	return nil
}

type glAccountsResponse struct {
	Accounts []models.GLAccount `json:"accounts"`
}

type glRejectionResponse struct {
	ErrorCode    string                  `json:"errorCode"`
	ErrorMessage string                  `json:"errorMessage"`
	Report       services.GLImportReport `json:"report"`
}

func newGLRouterForTests(t *testing.T, extra ...RouterOption) (http.Handler, *glRepositoryForHTTPTests, *LedgerController) {
	t.Helper()
	repo := newGLRepositoryForHTTPTests()
	systemController, mockLedger := newTestingSystemController(t, false)
	mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()

	opts := []RouterOption{
		WithGLAccountService(services.NewGLAccountService(repo)),
		WithGLReportingService(services.NewGLReportingService(repo)),
	}
	opts = append(opts, extra...)
	return NewRouter(systemController, auth.NewNoAuth(), "develop", opts...), repo, mockLedger
}

func doGL(t *testing.T, router http.Handler, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Buffer
	switch v := body.(type) {
	case nil:
		reader = bytes.NewBuffer(nil)
	case string:
		reader = bytes.NewBufferString(v)
	default:
		reader = api.Buffer(t, v)
	}
	req := httptest.NewRequest(method, target, reader)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func doGLCSV(t *testing.T, router http.Handler, target, csv string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewBufferString(csv))
	req.Header.Set("Content-Type", "text/csv; charset=utf-8")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func glBody(code, name, typ, currency, parent, address string) services.CreateGLAccountInput {
	return services.CreateGLAccountInput{
		Code: code, Name: name, Type: typ, Currency: currency, ParentCode: parent, Ledger: "ledgertrack", Address: address,
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, rec.Code, rec.Body.String())
	errResponse := api.ErrorResponse{}
	api.Decode(t, bytes.NewReader(rec.Body.Bytes()), &errResponse)
	require.EqualValues(t, code, errResponse.ErrorCode)
}

func TestGLAccountsCreateReadList(t *testing.T) {
	router, _, _ := newGLRouterForTests(t)

	rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1000", "Assets", "asset", "ngn", "", "gl:assets"))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	created, ok := api.DecodeSingleResponse[models.GLAccount](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "1000", created.Code)
	require.Equal(t, "NGN", created.Currency)
	require.Equal(t, models.GLSideDebit, created.NormalSide, "defaulted from the type")
	require.Equal(t, models.GLAccountStatusActive, created.Status)

	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1010", "Cash", "asset", "NGN", "1000", "gl:assets:cash")).Code)
	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("2000", "Deposits", "liability", "USD", "", "gl:liabilities:deposits")).Code)

	rec = doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/1010", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	read, ok := api.DecodeSingleResponse[models.GLAccount](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "gl:assets:cash", read.Address)
	require.NotNil(t, read.ParentCode)
	require.Equal(t, "1000", *read.ParentCode)

	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/9999", nil), http.StatusNotFound, "NOT_FOUND")

	list := func(query string) []string {
		rec := doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts"+query, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		response, ok := api.DecodeSingleResponse[glAccountsResponse](t, rec.Body)
		require.True(t, ok)
		codes := make([]string, 0, len(response.Accounts))
		for _, account := range response.Accounts {
			codes = append(codes, account.Code)
		}
		return codes
	}
	require.Equal(t, []string{"1000", "1010", "2000"}, list(""))
	require.Equal(t, []string{"1000", "1010"}, list("?type=ASSET"))
	require.Equal(t, []string{"2000"}, list("?currency=usd"))
	require.Equal(t, []string{"1010"}, list("?parent=1000"))
	require.Equal(t, []string{"1000", "1010", "2000"}, list("?status=active"))
	require.Empty(t, list("?status=closed"))
	require.Equal(t, []string{"1010"}, list("?type=asset&currency=NGN&parent=1000&status=active"))

	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts?type=revenue", nil), http.StatusBadRequest, common.ErrValidation)
	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts?status=archived", nil), http.StatusBadRequest, common.ErrValidation)
}

func TestGLAccountsCreateErrors(t *testing.T) {
	router, _, _ := newGLRouterForTests(t)
	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1000", "Assets", "asset", "NGN", "", "gl:assets")).Code)

	t.Run("validation", func(t *testing.T) {
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1001", "Bad", "asset", "XXX", "", "gl:bad")), http.StatusBadRequest, common.ErrValidation)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1001", "Bad", "asset", "NGN", "", "users:1:wallets")), http.StatusBadRequest, common.ErrValidation)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1001", "Bad", "asset", "NGN", "404", "gl:bad")), http.StatusBadRequest, common.ErrValidation)
	})
	t.Run("duplicate code or address", func(t *testing.T) {
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1000", "Again", "asset", "NGN", "", "gl:other")), http.StatusConflict, common.ErrConflict)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("1001", "Again", "asset", "NGN", "", "gl:assets")), http.StatusConflict, common.ErrConflict)
	})
	t.Run("malformed or unknown fields", func(t *testing.T) {
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", "{not json"), http.StatusBadRequest, common.ErrValidation)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", `{"code":"1","name":"x","type":"asset","currency":"NGN","ledger":"ledgertrack","address":"gl:x","parentCode":"1000"}`), http.StatusBadRequest, common.ErrValidation)
	})
	t.Run("only the ledgertrack ledger serves the registry", func(t *testing.T) {
		assertErrorCode(t, doGL(t, router, http.MethodGet, "/other/gl/accounts", nil), http.StatusNotFound, "NOT_FOUND")
	})
}

func TestGLAccountsPatch(t *testing.T) {
	router, _, _ := newGLRouterForTests(t)
	for _, in := range []services.CreateGLAccountInput{
		glBody("1000", "Assets", "asset", "NGN", "", "gl:assets"),
		glBody("1010", "Cash", "asset", "NGN", "1000", "gl:assets:cash"),
	} {
		require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", in).Code)
	}

	rec := doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/1010", `{"name":"Cash at bank","metadata":{"owner":"treasury"}}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	patched, ok := api.DecodeSingleResponse[models.GLAccount](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "Cash at bank", patched.Name)
	require.Equal(t, "treasury", patched.Metadata["owner"])

	// immutable fields are rejected rather than ignored
	for _, field := range []string{`"type":"liability"`, `"address":"gl:x"`, `"code":"2"`, `"currency":"USD"`, `"parent_code":"1000"`, `"normal_side":"credit"`, `"ledger":"x"`} {
		assertErrorCode(t, doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/1010", `{`+field+`}`), http.StatusBadRequest, common.ErrValidation)
	}
	assertErrorCode(t, doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/1010", `{"status":"archived"}`), http.StatusBadRequest, common.ErrValidation)
	assertErrorCode(t, doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/9999", `{"name":"x"}`), http.StatusNotFound, "NOT_FOUND")

	// closing a parent that still has an active child is refused, closing the child first works
	assertErrorCode(t, doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/1000", `{"status":"closed"}`), http.StatusBadRequest, common.ErrValidation)
	require.Equal(t, http.StatusOK, doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/1010", `{"status":"closed"}`).Code)
	require.Equal(t, http.StatusOK, doGL(t, router, http.MethodPatch, "/ledgertrack/gl/accounts/1000", `{"status":"closed"}`).Code)
}

func TestGLAccountsImportJSON(t *testing.T) {
	router, repo, _ := newGLRouterForTests(t)

	rows := []services.CreateGLAccountInput{
		glBody("1010", "Cash", "asset", "NGN", "1000", "gl:assets:cash"),
		glBody("1000", "Assets", "asset", "NGN", "", "gl:assets"),
		glBody("2000", "Deposits", "liability", "NGN", "", "gl:liabilities:deposits"),
	}

	t.Run("dry run validates and writes nothing", func(t *testing.T) {
		rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import?dry_run=true", rows)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		report, ok := api.DecodeSingleResponse[services.GLImportReport](t, rec.Body)
		require.True(t, ok)
		require.True(t, report.DryRun)
		require.False(t, report.Committed)
		require.True(t, report.CanCommit)
		require.Equal(t, 3, report.ToCreate)
		require.Len(t, report.Rows, 3)
		require.Empty(t, repo.accounts)
	})

	t.Run("a dry run reports problems with status 200", func(t *testing.T) {
		bad := append([]services.CreateGLAccountInput{}, rows...)
		bad[2].Address = "bad:address"
		rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import?dry_run=true", bad)
		require.Equal(t, http.StatusOK, rec.Code)
		report, ok := api.DecodeSingleResponse[services.GLImportReport](t, rec.Body)
		require.True(t, ok)
		require.False(t, report.CanCommit)
		require.Equal(t, "invalid", report.Rows[2].Status)
		require.Contains(t, report.Rows[2].Errors[0], "address must start with gl:")
		require.Empty(t, repo.accounts)
	})

	t.Run("commit", func(t *testing.T) {
		rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", rows)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		report, ok := api.DecodeSingleResponse[services.GLImportReport](t, rec.Body)
		require.True(t, ok)
		require.True(t, report.Committed)
		require.False(t, report.DryRun)
		require.Len(t, repo.accounts, 3)
	})

	t.Run("re-importing identical rows is a no-op", func(t *testing.T) {
		rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", rows)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		report, ok := api.DecodeSingleResponse[services.GLImportReport](t, rec.Body)
		require.True(t, ok)
		require.True(t, report.Committed)
		require.Equal(t, 3, report.Unchanged)
		require.Zero(t, report.ToCreate)
		require.Len(t, repo.accounts, 3)
	})

	t.Run("a conflicting row rejects the whole import with 409 and the report", func(t *testing.T) {
		mixed := append([]services.CreateGLAccountInput{}, rows...)
		mixed[0].Name = "Renamed cash"
		mixed = append(mixed, glBody("5000", "Interest", "expense", "NGN", "", "gl:expenses:interest"))
		rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", mixed)
		require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
		var rejection glRejectionResponse
		api.Decode(t, bytes.NewReader(rec.Body.Bytes()), &rejection)
		require.Equal(t, common.ErrConflict, rejection.ErrorCode)
		require.Equal(t, 1, rejection.Report.Conflicts)
		require.Equal(t, "conflict", rejection.Report.Rows[0].Status)
		require.Contains(t, rejection.Report.Rows[0].Conflicts[0], "name:")
		require.Equal(t, "create", rejection.Report.Rows[3].Status)
		require.Len(t, repo.accounts, 3, "the valid new row was not applied either")
		require.NotContains(t, repo.accounts, "5000")
	})

	t.Run("an invalid row rejects the whole import with 400 and the report", func(t *testing.T) {
		bad := []services.CreateGLAccountInput{
			glBody("6000", "Fine", "asset", "NGN", "", "gl:fine"),
			glBody("6001", "Cycle a", "asset", "NGN", "6002", "gl:a"),
			glBody("6002", "Cycle b", "asset", "NGN", "6001", "gl:b"),
		}
		rec := doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", bad)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		var rejection glRejectionResponse
		api.Decode(t, bytes.NewReader(rec.Body.Bytes()), &rejection)
		require.Equal(t, common.ErrValidation, rejection.ErrorCode)
		require.Equal(t, 2, rejection.Report.Invalid)
		require.Contains(t, rejection.Report.Rows[1].Errors[0], "cycle")
		require.Len(t, repo.accounts, 3)
	})

	t.Run("request errors", func(t *testing.T) {
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import?dry_run=maybe", rows), http.StatusBadRequest, common.ErrValidation)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", `{"accounts":[]}`), http.StatusBadRequest, common.ErrValidation)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", `[]`), http.StatusBadRequest, common.ErrValidation)
		assertErrorCode(t, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts/import", `[{"code":"1","parentCode":"x"}]`), http.StatusBadRequest, common.ErrValidation)
	})
}

func TestGLAccountsImportCSV(t *testing.T) {
	router, repo, _ := newGLRouterForTests(t)
	csv := "code,name,type,normal_side,currency,parent_code,ledger,address\n" +
		"1010,Cash,asset,,NGN,1000,ledgertrack,gl:assets:cash\n" +
		"1000,Assets,asset,debit,NGN,,ledgertrack,gl:assets\n"

	rec := doGLCSV(t, router, "/ledgertrack/gl/accounts/import?dry_run=true", csv)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	report, ok := api.DecodeSingleResponse[services.GLImportReport](t, rec.Body)
	require.True(t, ok)
	require.True(t, report.CanCommit)
	require.Empty(t, repo.accounts)

	rec = doGLCSV(t, router, "/ledgertrack/gl/accounts/import", csv)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, repo.accounts, 2)

	rec = doGLCSV(t, router, "/ledgertrack/gl/accounts/import", csv)
	require.Equal(t, http.StatusOK, rec.Code)
	report, ok = api.DecodeSingleResponse[services.GLImportReport](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, 2, report.Unchanged)

	assertErrorCode(t, doGLCSV(t, router, "/ledgertrack/gl/accounts/import", "code,name\n1,x\n"), http.StatusBadRequest, common.ErrValidation)
}

func expectGLBalances(mockLedger *LedgerController, balances map[string]*big.Int, calls *[]storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]) {
	mockLedger.EXPECT().
		GetAggregatedBalances(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error) {
			if calls != nil {
				*calls = append(*calls, q)
			}
			address := ""
			_ = (q.Builder.Walk(func(operator, key string, value any) error {
				if operator == "$match" && key == "address" {
					address, _ = value.(string)
				}
				return nil
			}))
			ret := ledger.BalancesByAssets{}
			if amount, ok := balances[address]; ok {
				ret["NGN/2"] = amount
			}
			return ret, nil
		}).
		AnyTimes()
}

func TestGLAccountBalance(t *testing.T) {
	router, _, mockLedger := newGLRouterForTests(t)
	var calls []storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]
	expectGLBalances(mockLedger, map[string]*big.Int{"gl:liabilities:deposits": big.NewInt(12_500)}, &calls)

	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("2000", "Deposits", "liability", "NGN", "", "gl:liabilities:deposits")).Code)
	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", glBody("2001", "Empty", "liability", "NGN", "", "gl:liabilities:empty")).Code)

	rec := doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/2000/balance", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	balance, ok := api.DecodeSingleResponse[services.GLAccountBalance](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "NGN/2", balance.Asset)
	require.EqualValues(t, 12_500, balance.Balance)
	require.EqualValues(t, 12_500, balance.Credit)
	require.EqualValues(t, 12_500, balance.NaturalBalance)
	require.False(t, balance.Abnormal)
	require.Nil(t, balance.PIT)
	require.Nil(t, calls[0].PIT)

	pit := libtime.New(libtime.Now().Add(-1).UTC().Truncate(1e9))
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/2000/balance?pit="+url.QueryEscape(pit.Format(libtime.RFC3339Nano)), nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	balance, ok = api.DecodeSingleResponse[services.GLAccountBalance](t, rec.Body)
	require.True(t, ok)
	require.NotNil(t, balance.PIT)
	require.NotNil(t, calls[1].PIT)
	require.True(t, calls[1].PIT.Equal(pit))

	// no postings at the address: zero, not an error
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/2001/balance", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	balance, ok = api.DecodeSingleResponse[services.GLAccountBalance](t, rec.Body)
	require.True(t, ok)
	require.Zero(t, balance.Balance)

	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/9999/balance", nil), http.StatusNotFound, "NOT_FOUND")
	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/gl/accounts/2000/balance?pit=yesterday", nil), http.StatusBadRequest, common.ErrValidation)
}

func TestGLTrialBalanceReport(t *testing.T) {
	// the existing finance reporting service is mounted too: both trial balances must coexist
	router, _, mockLedger := newGLRouterForTests(t, WithFinanceReportingService(services.NewFinanceReportingService()))
	var calls []storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]
	expectGLBalances(mockLedger, map[string]*big.Int{
		"gl:assets:cash":         big.NewInt(-30_000),
		"gl:liabilities:deposit": big.NewInt(20_000),
		"gl:income:fees":         big.NewInt(10_000),
	}, &calls)

	for _, in := range []services.CreateGLAccountInput{
		glBody("1000", "Cash", "asset", "NGN", "", "gl:assets:cash"),
		glBody("2000", "Deposits", "liability", "NGN", "", "gl:liabilities:deposit"),
		glBody("4000", "Fees", "income", "NGN", "", "gl:income:fees"),
		glBody("9000", "Cash USD", "asset", "USD", "", "gl:assets:cash_usd"),
	} {
		require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", in).Code)
	}

	rec := doGL(t, router, http.MethodGet, "/ledgertrack/reports/finance/gl-trial-balance?currency=NGN", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	report, ok := api.DecodeSingleResponse[services.GLTrialBalanceReport](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "NGN", report.Currency)
	require.Len(t, report.Accounts, 3)
	require.True(t, report.Balanced)
	require.EqualValues(t, 30_000, report.TotalDebits)
	require.EqualValues(t, 30_000, report.TotalCredits)
	require.Len(t, report.Subtotals, 5)
	require.Len(t, calls, 6, "USD accounts are not read; the wallet and control reads add three")

	// asOf is forwarded as the point in time
	asOf := libtime.New(libtime.Now().UTC().Add(-3600 * 1e9).Truncate(1e9))
	rec = doGL(t, router, http.MethodGet, "/ledgertrack/reports/finance/gl-trial-balance?currency=ngn&asOf="+url.QueryEscape(asOf.Format(libtime.RFC3339Nano)), nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	report, ok = api.DecodeSingleResponse[services.GLTrialBalanceReport](t, rec.Body)
	require.True(t, ok)
	require.NotNil(t, report.AsOf)
	require.NotNil(t, calls[6].PIT)
	require.True(t, calls[6].PIT.Equal(asOf))

	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/reports/finance/gl-trial-balance", nil), http.StatusBadRequest, common.ErrValidation)
	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/reports/finance/gl-trial-balance?currency=XXX", nil), http.StatusBadRequest, common.ErrValidation)
	assertErrorCode(t, doGL(t, router, http.MethodGet, "/ledgertrack/reports/finance/gl-trial-balance?currency=NGN&asOf=nope", nil), http.StatusBadRequest, common.ErrValidation)
	assertErrorCode(t, doGL(t, router, http.MethodGet, "/other/reports/finance/gl-trial-balance?currency=NGN", nil), http.StatusNotFound, "NOT_FOUND")
}

func TestGLTrialBalanceTreatsMissingLedgerAsZeroAndFlagsImbalance(t *testing.T) {
	repo := newGLRepositoryForHTTPTests()
	ctrl := gomock.NewController(t)
	mockLedger := NewLedgerController(ctrl)
	mockLedger.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	backend := NewSystemController(ctrl)
	backend.EXPECT().GetLedgerController(gomock.Any(), "ledgertrack").Return(mockLedger, nil).AnyTimes()
	backend.EXPECT().GetLedgerController(gomock.Any(), "revenue-NGN").Return(nil, postgres.ErrNotFound).AnyTimes()
	expectGLBalances(mockLedger, map[string]*big.Int{"gl:assets:cash": big.NewInt(500)}, nil)

	router := NewRouter(backend, auth.NewNoAuth(), "develop",
		WithGLAccountService(services.NewGLAccountService(repo)),
		WithGLReportingService(services.NewGLReportingService(repo)),
	)

	cash := glBody("1000", "Cash", "asset", "NGN", "", "gl:assets:cash")
	fees := glBody("4000", "Fees", "income", "NGN", "", "gl:income:fees")
	fees.Ledger = "revenue-NGN"
	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", cash).Code)
	require.Equal(t, http.StatusCreated, doGL(t, router, http.MethodPost, "/ledgertrack/gl/accounts", fees).Code)

	rec := doGL(t, router, http.MethodGet, "/ledgertrack/reports/finance/gl-trial-balance?currency=NGN", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	report, ok := api.DecodeSingleResponse[services.GLTrialBalanceReport](t, rec.Body)
	require.True(t, ok)
	require.Len(t, report.Accounts, 2)
	require.False(t, report.Balanced)
	require.EqualValues(t, -500, report.Difference)
	require.True(t, strings.HasPrefix(report.Accounts[0].Code, "1000"))
}
