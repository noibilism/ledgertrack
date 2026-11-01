package bankman

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

type fakeLookup struct {
	account *models.Account
	client  *models.Client
}

func (f fakeLookup) Account(_ context.Context, id string) (*models.Account, error) {
	if f.account != nil && (id == "bankman-acct-1" || id == f.account.ID.String()) {
		return f.account, nil
	}
	return nil, services.ErrLegacyNotFound
}

func (f fakeLookup) ClientByID(_ context.Context, id string) (*models.Client, error) {
	if f.client != nil && id == "bm-client-1" {
		return f.client, nil
	}
	return nil, services.ErrLegacyNotFound
}

func (f fakeLookup) Client(_ context.Context, id string) (*models.Client, error) {
	if f.client != nil && id == "ext-1" {
		return f.client, nil
	}
	return nil, services.ErrLegacyNotFound
}

type call struct {
	Method, Path, RawQuery string
	Body                   map[string]any
	Auth                   string
}

// backend stands in for the v2 router. It is a real chi router so that the test also proves the compat layer
// drops the outer route context (otherwise routing would use the wrong path and nothing would match).
type backend struct {
	mu    sync.Mutex
	calls []call
	// per route behaviour
	post   func(path string, body map[string]any) (int, string)
	listed string // JSON for GET /transactions

	accounts string // JSON for GET /accounts?client_id=
	lienBal  string // balance of the lien address, "" = the address was never touched (404)
	avail    int64
}

func (b *backend) handler() http.Handler {
	r := chi.NewRouter()
	record := func(req *http.Request) call {
		c := call{Method: req.Method, Path: req.URL.Path, RawQuery: req.URL.RawQuery, Auth: req.Header.Get("Authorization")}
		if req.Body != nil {
			raw, _ := io.ReadAll(req.Body)
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &c.Body)
			}
		}
		b.mu.Lock()
		b.calls = append(b.calls, c)
		b.mu.Unlock()
		return c
	}
	r.Post("/ledgertrack/accounts/{id}/{op}", func(w http.ResponseWriter, req *http.Request) {
		c := record(req)
		code, body := b.post(req.URL.Path, c.Body)
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	})
	r.Post("/ledgertrack/accounts/{id}/lien/release", func(w http.ResponseWriter, req *http.Request) {
		c := record(req)
		code, body := b.post(req.URL.Path, c.Body)
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	})
	r.Post("/ledgertrack/transactions/{id}/revert", func(w http.ResponseWriter, req *http.Request) {
		record(req)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"data":{"id":99}}`))
	})
	r.Get("/ledgertrack/accounts", func(w http.ResponseWriter, req *http.Request) {
		record(req)
		_, _ = w.Write([]byte(b.accounts))
	})
	r.Get("/ledgertrack/accounts/{id}", func(w http.ResponseWriter, req *http.Request) {
		record(req)
		if strings.HasPrefix(chi.URLParam(req, "id"), "users:") {
			if b.lienBal == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"data":{"volumes":{"NGN/2":{"balance":` + b.lienBal + `}}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"wallet_info":{"available_balance":` + strconv.FormatInt(b.avail, 10) + `,"lien_address":"users:w1:wallets:NGN:lien"}}}`))
	})
	r.Get("/ledgertrack/transactions", func(w http.ResponseWriter, req *http.Request) {
		record(req)
		_, _ = w.Write([]byte(b.listed))
	})
	return r
}

func (b *backend) last() call { return b.calls[len(b.calls)-1] }

func setup(t *testing.T, b *backend) (http.Handler, fakeLookup) {
	t.Helper()
	currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{"NGN": {Precision: 2, Enabled: true}})
	t.Cleanup(func() { currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{}) })

	clientID := uuid.New()
	lookup := fakeLookup{
		account: &models.Account{ID: uuid.New(), ClientID: clientID, Currency: "NGN"},
		client:  &models.Client{ID: clientID},
	}
	// Mount like the real router: behind a chi mux with a strip prefix and a route-context reset.
	compat := New(lookup, b.handler(), "ledgertrack")
	mux := chi.NewRouter()
	mux.Handle("/bankman/api/v1/*", http.StripPrefix("/bankman/api/v1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chi.RouteContext(r.Context()).Reset()
		compat.ServeHTTP(w, r)
	})))
	return mux, lookup
}

func do(h http.Handler, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func okPost(_ string, _ map[string]any) (int, string) {
	return http.StatusCreated, `{"data":{"txid":5,"balance_before":10000,"balance_after":160000}}`
}

func TestDepositTranslatesAmountAndReference(t *testing.T) {
	b := &backend{post: okPost}
	h, lookup := setup(t, b)

	rec, out := do(h, http.MethodPost, "/bankman/api/v1/transactions/deposits/bankman-acct-1",
		`{"external_id":"ext-1","amount":1500,"transaction_reference":"REF-1","session_id":"s1","channel":"web"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	c := b.last()
	if want := "/ledgertrack/accounts/" + lookup.account.ID.String() + "/credit"; c.Path != want {
		t.Fatalf("path %q, want %q", c.Path, want)
	}
	// A bare integer would be read as minor units by LedgerTrack, so the amount must carry its decimals.
	if c.Body["amount"] != "1500.00" {
		t.Fatalf("amount %v, want 1500.00", c.Body["amount"])
	}
	if c.Body["reference"] != "REF-1" {
		t.Fatalf("reference %v", c.Body["reference"])
	}
	if c.Auth != "Bearer test" {
		t.Fatalf("authorization not forwarded: %q", c.Auth)
	}
	md := c.Body["metadata"].(map[string]any)
	if md[MetaTransactionID] != TransactionID("REF-1") || md["session_id"] != "s1" || md["channel"] != "web" {
		t.Fatalf("metadata %v", md)
	}
	data := out["data"].(map[string]any)
	if out["status"] != "success" || data["transaction_id"] != TransactionID("REF-1") || data["status"] != "completed" {
		t.Fatalf("response %v", out)
	}
}

func TestAmountRoundsHalfEven(t *testing.T) {
	currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{"NGN": {Precision: 2, Enabled: true}})
	t.Cleanup(func() { currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{}) })

	for in, want := range map[string]string{
		"10.125": "10.12", "10.135": "10.14", "10.5": "10.50", "7": "7.00", "0.005": "",
	} {
		got, err := majorAmount(json.Number(in), "NGN")
		if want == "" {
			if err == nil {
				t.Errorf("%s: expected an error (rounds to zero), got %s", in, got)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%s: got %q err %v, want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "abc", "-5", "0"} {
		if _, err := majorAmount(json.Number(bad), "NGN"); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestDuplicateReferenceIsIdempotent(t *testing.T) {
	b := &backend{
		post: func(string, map[string]any) (int, string) {
			return http.StatusConflict, `{"errorMessage":"duplicate reference"}`
		},
		listed: `{"cursor":{"data":[{"id":5,"reference":"REF-1"}]}}`,
	}
	h, _ := setup(t, b)
	rec, out := do(h, http.MethodPost, "/bankman/api/v1/transactions/deposits/bankman-acct-1",
		`{"external_id":"ext-1","amount":10,"transaction_reference":"REF-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	data := out["data"].(map[string]any)
	if data["idempotent"] != true || data["transaction_id"] != TransactionID("REF-1") {
		t.Fatalf("response %v", out)
	}
}

func TestWithdrawalInsufficientFundsIs422(t *testing.T) {
	b := &backend{post: func(string, map[string]any) (int, string) {
		return http.StatusBadRequest, `{"errorCode":"VALIDATION","errorMessage":"insufficient funds"}`
	}}
	h, _ := setup(t, b)
	rec, out := do(h, http.MethodPost, "/bankman/api/v1/transactions/withdrawals/bankman-acct-1",
		`{"external_id":"ext-1","amount":10,"transaction_reference":"W-1"}`)
	if rec.Code != http.StatusUnprocessableEntity || out["status"] != "error" || out["message"] != "insufficient funds" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestWithdrawalReturnsBalances(t *testing.T) {
	b := &backend{post: okPost}
	h, _ := setup(t, b)
	rec, out := do(h, http.MethodPost, "/bankman/api/v1/transactions/withdrawals/bankman-acct-1",
		`{"external_id":"ext-1","amount":10,"transaction_reference":"W-2"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	bal := out["data"].(map[string]any)["account_balance"].(map[string]any)
	if bal["previous_available_balance"] != "100.00" || bal["current_available_balance"] != "1600.00" {
		t.Fatalf("balances %v", bal)
	}
}

func TestOwnershipAndLookupErrors(t *testing.T) {
	b := &backend{post: okPost}
	h, _ := setup(t, b)

	for name, tc := range map[string]struct{ path, body string }{
		"unknown account": {"/bankman/api/v1/transactions/deposits/nope", `{"external_id":"ext-1","amount":10,"transaction_reference":"X"}`},
		"unknown client":  {"/bankman/api/v1/transactions/deposits/bankman-acct-1", `{"external_id":"other","amount":10,"transaction_reference":"X"}`},
	} {
		rec, _ := do(h, http.MethodPost, tc.path, tc.body)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", name, rec.Code)
		}
	}
	if len(b.calls) != 0 {
		t.Fatalf("no ledger call expected, got %d", len(b.calls))
	}

	rec, _ := do(h, http.MethodPost, "/bankman/api/v1/transactions/deposits/bankman-acct-1", `{"external_id":"ext-1","amount":10}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("missing reference: status %d, want 422", rec.Code)
	}
}

func TestClientMismatchIsNotFound(t *testing.T) {
	b := &backend{post: okPost}
	h, lookup := setup(t, b)
	lookup.account.ClientID = uuid.New() // the account now belongs to someone else
	rec, _ := do(h, http.MethodPost, "/bankman/api/v1/transactions/deposits/bankman-acct-1", `{"external_id":"ext-1","amount":10,"transaction_reference":"X"}`)
	if rec.Code != http.StatusNotFound || len(b.calls) != 0 {
		t.Fatalf("status %d, calls %d", rec.Code, len(b.calls))
	}
}

func TestPlaceLienCarriesExpiry(t *testing.T) {
	b := &backend{post: okPost}
	h, lookup := setup(t, b)
	rec, _ := do(h, http.MethodPost, "/bankman/api/v1/transactions/liens/bankman-acct-1",
		`{"external_id":"ext-1","amount":"250.5","transaction_reference":"L-1","expiry_date":"2026-12-01"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	c := b.last()
	if c.Path != "/ledgertrack/accounts/"+lookup.account.ID.String()+"/lien" || c.Body["amount"] != "250.50" {
		t.Fatalf("call %+v", c)
	}
	if c.Body["metadata"].(map[string]any)["expiry_date"] != "2026-12-01" {
		t.Fatalf("metadata %v", c.Body["metadata"])
	}
}

func lienListing(accountID string) string {
	return `{"cursor":{"data":[{"id":7,"reference":"L-1","metadata":{"account_id":"` + accountID + `"},` +
		`"postings":[{"source":"a","destination":"b","asset":"NGN/2","amount":5000}]}]}}`
}

func TestReleaseUsesTheLienAmountAndASingleReference(t *testing.T) {
	b := &backend{post: okPost}
	h, lookup := setup(t, b)
	b.listed = lienListing(lookup.account.ID.String())

	rec, out := do(h, http.MethodPut, "/bankman/api/v1/transactions/liens/some-lien-id",
		`{"external_id":"ext-1","transaction_reference":"CALLER-REF","description":"done"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	c := b.last()
	if !strings.HasSuffix(c.Path, "/lien/release") || c.Body["mode"] != "release_only" {
		t.Fatalf("call %+v", c)
	}
	// 5000 minor units of a 2dp currency
	if c.Body["amount"] != "50.00" {
		t.Fatalf("amount %v", c.Body["amount"])
	}
	// derived from the lien, so a second release with any caller reference collides with the first
	if c.Body["reference"] != "bankman-lien-release:7" {
		t.Fatalf("reference %v", c.Body["reference"])
	}
	md := c.Body["metadata"].(map[string]any)
	if md[MetaLienRef] != "7" || md["caller_reference"] != "CALLER-REF" {
		t.Fatalf("metadata %v", md)
	}
	if out["data"].(map[string]any)["status"] != "completed" {
		t.Fatalf("response %v", out)
	}
	// the lien was looked up by the id the caller holds
	first := b.calls[0]
	if first.Method != http.MethodGet || !strings.Contains(first.RawQuery, "metadata%5Bbankman_transaction_id%5D=some-lien-id") {
		t.Fatalf("lookup %+v", first)
	}
}

func TestReleaseAndWithdrawPays(t *testing.T) {
	b := &backend{post: okPost}
	h, lookup := setup(t, b)
	b.listed = lienListing(lookup.account.ID.String())
	rec, _ := do(h, http.MethodPost, "/bankman/api/v1/transactions/liens/x/release-and-withdraw", `{"external_id":"ext-1"}`)
	if rec.Code != http.StatusAccepted || b.last().Body["mode"] != "release_and_debit" {
		t.Fatalf("status %d call %+v", rec.Code, b.last())
	}
}

func TestReleaseOfUnknownLienIsNotFound(t *testing.T) {
	b := &backend{post: okPost, listed: `{"cursor":{"data":[]}}`}
	h, _ := setup(t, b)
	rec, _ := do(h, http.MethodPut, "/bankman/api/v1/transactions/liens/ghost", `{"external_id":"ext-1"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestSecondReleaseIsIdempotent(t *testing.T) {
	b := &backend{post: func(string, map[string]any) (int, string) {
		return http.StatusConflict, `{"errorMessage":"duplicate reference"}`
	}}
	h, lookup := setup(t, b)
	b.listed = lienListing(lookup.account.ID.String())
	rec, out := do(h, http.MethodPut, "/bankman/api/v1/transactions/liens/x", `{"external_id":"ext-1"}`)
	if rec.Code != http.StatusAccepted || out["data"].(map[string]any)["idempotent"] != true {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestReversalBothRoutes(t *testing.T) {
	for name, tc := range map[string]struct{ path, body string }{
		"body": {"/bankman/api/v1/transactions/reversal", `{"original_transaction_id":"orig-uuid","reason":"duplicate"}`},
		"path": {"/bankman/api/v1/transactions/orig-uuid/reversals", `{"reason":"duplicate"}`},
	} {
		t.Run(name, func(t *testing.T) {
			b := &backend{post: okPost, listed: `{"cursor":{"data":[{"id":12,"reference":"REF-1"}]}}`}
			h, _ := setup(t, b)
			rec, out := do(h, http.MethodPost, tc.path, tc.body)
			if rec.Code != http.StatusCreated {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			c := b.last()
			if c.Path != "/ledgertrack/transactions/12/revert" {
				t.Fatalf("path %q", c.Path)
			}
			md := c.Body["metadata"].(map[string]any)
			if md[MetaReversalOf] != "orig-uuid" || md["reason"] != "duplicate" {
				t.Fatalf("metadata %v", md)
			}
			if out["data"].(map[string]any)["original_transaction_id"] != "orig-uuid" {
				t.Fatalf("response %v", out)
			}
		})
	}
}

func TestReversalOfUnknownTransaction(t *testing.T) {
	b := &backend{post: okPost, listed: `{"cursor":{"data":[]}}`}
	h, _ := setup(t, b)
	rec, _ := do(h, http.MethodPost, "/bankman/api/v1/transactions/reversal", `{"original_transaction_id":"ghost"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestTransactionIDIsStable(t *testing.T) {
	if TransactionID("A") != TransactionID("A") || TransactionID("A") == TransactionID("B") {
		t.Fatal("transaction ids must be deterministic and distinct per reference")
	}
	if _, err := uuid.Parse(TransactionID("A")); err != nil {
		t.Fatal(err)
	}
}

func TestAccountReadHasBankmanShapeAndBalances(t *testing.T) {
	b := &backend{avail: 150000, lienBal: "25000"}
	h, lookup := setup(t, b)
	lookup.account.AccountNumber = "LT-1"
	lookup.account.Status = "active"
	lookup.account.Metadata = map[string]any{"legacy_source_id": "bankman-acct-1", "legacy_account_number": "1234567890"}

	rec, out := do(h, http.MethodGet, "/bankman/api/v1/accounts/bankman-acct-1", "")
	if rec.Code != http.StatusOK || out["status"] != "success" {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	d := out["data"].(map[string]any)
	// 1500.00 available, 250.00 held, so the ledger balance is 1750.00; all rendered like decimal:4 casts
	if d["available_balance"] != "1500.0000" || d["locked_amount"] != "250.0000" || d["actual_balance"] != "1750.0000" {
		t.Fatalf("balances %v", d)
	}
	if d["id"] != "bankman-acct-1" || d["account_number"] != "1234567890" || d["currency"] != "NGN" || d["status"] != "active" {
		t.Fatalf("fields %v", d)
	}
	for _, hidden := range []string{"client_id", "product_id", "wallet_role", "created_at"} {
		if _, ok := d[hidden]; ok {
			t.Errorf("%s must stay hidden", hidden)
		}
	}
}

func TestAccountWithoutLienActivityHoldsNothing(t *testing.T) {
	b := &backend{avail: 5000}
	h, _ := setup(t, b)
	_, out := do(h, http.MethodGet, "/bankman/api/v1/accounts/bankman-acct-1", "")
	d := out["data"].(map[string]any)
	if d["locked_amount"] != "0.0000" || d["actual_balance"] != "50.0000" {
		t.Fatalf("%v", d)
	}
}

func TestAccountListByExternalAndClientID(t *testing.T) {
	for name, query := range map[string]string{"external_id": "external_id=ext-1", "client_id": "client_id=bm-client-1"} {
		t.Run(name, func(t *testing.T) {
			b := &backend{avail: 100, accounts: `{"data":{"accounts":[{"id":"` + uuid.NewString() + `","currency":"NGN","status":"active"}]}}`}
			h, lookup := setup(t, b)
			rec, out := do(h, http.MethodGet, "/bankman/api/v1/accounts?"+query, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if n := len(out["data"].([]any)); n != 1 {
				t.Fatalf("accounts %d", n)
			}
			if !strings.Contains(b.calls[0].RawQuery, "client_id="+lookup.client.ID.String()) {
				t.Fatalf("list was not scoped to the client: %q", b.calls[0].RawQuery)
			}
		})
	}
}

func TestAccountListEdgeCases(t *testing.T) {
	b := &backend{}
	h, _ := setup(t, b)

	rec, _ := do(h, http.MethodGet, "/bankman/api/v1/accounts?external_id=unknown", "")
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown external_id: %d, want 404 (as Bankman)", rec.Code)
	}
	rec, out := do(h, http.MethodGet, "/bankman/api/v1/accounts?client_id=unknown", "")
	if rec.Code != http.StatusOK || len(out["data"].([]any)) != 0 {
		t.Errorf("unknown client_id: %d %s, want 200 and an empty list", rec.Code, rec.Body.String())
	}
	rec, _ = do(h, http.MethodGet, "/bankman/api/v1/accounts", "")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("unfiltered list: %d, want 422", rec.Code)
	}
	rec, _ = do(h, http.MethodGet, "/bankman/api/v1/accounts/nope", "")
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown account: %d, want 404", rec.Code)
	}
}
