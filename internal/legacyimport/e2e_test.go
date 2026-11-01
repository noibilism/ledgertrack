package legacyimport

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, url, token string) *Client {
	t.Helper()
	c, err := NewClient(ClientConfig{
		BaseURL: url, Ledger: "ledgertrack", Token: token, MaxRetries: 3,
		BackoffBase: time.Millisecond, BackoffMax: 2 * time.Millisecond,
		HTTP: &http.Client{Timeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// load builds the prepared dataset against the fake's currency registry.
func load(t *testing.T, c *Client, dir string) (*Prepared, *Snapshot) {
	t.Helper()
	ctx := context.Background()
	snap, err := LoadSnapshot(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	ds, issues := LoadDataset(dir)
	if HasErrors(issues) {
		t.Fatal(issues)
	}
	prep, issues := Prepare(ds, PrepareOptions{Precision: snap.PrecisionMap(), Authoritative: true})
	if HasErrors(issues) {
		t.Fatal(issues)
	}
	return prep, snap
}

func doApply(t *testing.T, c *Client, dir, statePath string, opt Options) *Result {
	t.Helper()
	prep, snap := load(t, c, dir)
	st, err := LoadState(statePath, c.Target())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	res, err := NewEngine(c, prep, snap, st, opt).Apply(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func doPlan(t *testing.T, c *Client, dir, statePath string) *Plan {
	t.Helper()
	prep, snap := load(t, c, dir)
	var st *State
	if statePath != "" {
		var err error
		if st, err = LoadState(statePath, c.Target()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = st.Close() }()
	}
	pl, err := BuildPlan(context.Background(), c, prep, snap, st, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return pl
}

func doVerify(t *testing.T, c *Client, dir string, deep bool) *VerifyReport {
	t.Helper()
	prep, snap := load(t, c, dir)
	rep, err := Verify(context.Background(), c, prep, snap, Options{Concurrency: 3}, deep)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func planCount(p *Plan, a PlanAction) int {
	n := 0
	for _, it := range p.Items {
		if it.Action == a {
			n++
		}
	}
	return n
}

// Expected sizes for docs/import/samples: 2 products, 3 clients, 4 accounts,
// 3 non-zero opening balances and 3 liens.
const (
	wantProducts = 2
	wantClients  = 3
	wantAccounts = 4
	wantTxs      = 6
)

func assertSamplesImported(t *testing.T, f *fakeLT) {
	t.Helper()
	p, c, a, x := f.sizes()
	if p != wantProducts || c != wantClients || a != wantAccounts || x != wantTxs {
		t.Fatalf("server holds %d products, %d clients, %d accounts, %d transactions; want %d, %d, %d, %d",
			p, c, a, x, wantProducts, wantClients, wantAccounts, wantTxs)
	}
}

func TestEndToEndPlanApplyVerify(t *testing.T) {
	f := newFake()
	srv := f.start(t)
	c := testClient(t, srv.URL, "")
	state := filepath.Join(t.TempDir(), "state.json")

	// Plan on an empty server creates everything and touches nothing.
	pl := doPlan(t, c, samplesDir, state)
	if planCount(pl, ActionConflict) != 0 || planCount(pl, ActionCreate) == 0 {
		t.Fatalf("unexpected plan:\n%s", pl.Summary())
	}
	if p, cl, a, x := f.sizes(); p+cl+a+x != 0 {
		t.Fatal("plan wrote to the server")
	}
	if n := f.count("POST"); n != 0 {
		t.Fatalf("plan sent %d POST requests", n)
	}

	res := doApply(t, c, samplesDir, state, Options{Concurrency: 4})
	if res.Failed() != 0 {
		t.Fatalf("apply failed:\n%s", res.Summary())
	}
	assertSamplesImported(t, f)

	// Opening balances come from the dedicated source, never from system:control.
	f.mu.Lock()
	for addr := range f.volumes {
		if strings.HasPrefix(addr, "system:control") {
			t.Errorf("import touched %s", addr)
		}
	}
	f.mu.Unlock()
	// Source debits equal the sums of the input, in each currency's minor units.
	if got := f.balance("gl:migration:opening:NGN", "NGN/2"); got.String() != "-132500050" {
		t.Errorf("NGN migration source balance = %s", got)
	}
	if got := f.balance("gl:migration:opening:USDT", "USDT/6"); got.String() != "-5000123456" {
		t.Errorf("USDT migration source balance = %s", got)
	}
	// Wallet addresses use the server generated client number and product code.
	w1 := walletOf(f, "a-1")
	if !strings.HasPrefix(w1, "client-CL-2026-") || !strings.HasSuffix(w1, "-MERCH-NGN") {
		t.Errorf("unexpected wallet id %q", w1)
	}
	if got := f.balance("users:"+w1+":wallets:NGN:available", "NGN/2"); got.String() != "100000050" {
		t.Errorf("a-1 available = %s", got)
	}
	if got := f.balance("users:"+w1+":wallets:NGN:lien", "NGN/2"); got.String() != "25000000" {
		t.Errorf("a-1 lien = %s", got)
	}
	// Lien transactions carry the original legacy reference.
	if _, ok := f.txByRef["BM-HOLD-000451"]; !ok {
		t.Error("lien transaction with the original reference is missing")
	}
	if _, ok := f.txByRef["import:opening:b-1"]; !ok {
		t.Error("opening transaction import:opening:b-1 is missing")
	}
	// Lifecycle went through the real flow.
	f.mu.Lock()
	for _, k := range f.kyc {
		if k["verifier"] != KYCVerifier {
			t.Errorf("kyc verifier = %v", k["verifier"])
		}
	}
	statuses := map[string]string{}
	for _, cl := range f.clients {
		statuses[str(cl["corporate_data"].(map[string]any), "legal_name")] = str(cl, "status")
	}
	f.mu.Unlock()
	if statuses["Acme Stores Ltd"] != "active" || statuses["Globex Trading Ltd"] != "suspended" {
		t.Errorf("client statuses = %v", statuses)
	}

	// A second plan is a no-op everywhere.
	pl = doPlan(t, c, samplesDir, state)
	if planCount(pl, ActionCreate)+planCount(pl, ActionUpdate)+planCount(pl, ActionConflict) != 0 {
		t.Fatalf("plan after apply is not a no-op:\n%v", pl.Items)
	}
	// ... also without the state file.
	pl = doPlan(t, c, samplesDir, "")
	if planCount(pl, ActionCreate)+planCount(pl, ActionUpdate)+planCount(pl, ActionConflict) != 0 {
		t.Fatalf("plan without state is not a no-op:\n%v", pl.Items)
	}

	// Re-running apply is a no-op, with and without the state file.
	res = doApply(t, c, samplesDir, state, Options{Concurrency: 4})
	if res.Failed() != 0 {
		t.Fatal(res.Summary())
	}
	assertSamplesImported(t, f)
	res = doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "fresh-state.json"), Options{Concurrency: 4})
	if res.Failed() != 0 {
		t.Fatal(res.Summary())
	}
	assertSamplesImported(t, f)
	for kind, m := range res.Counts {
		if m[string(outCreated)] != 0 {
			t.Errorf("second apply created %d %s", m[string(outCreated)], kind)
		}
	}

	// Verification passes with zero tolerance, and so does the deep check.
	for _, deep := range []bool{false, true} {
		rep := doVerify(t, c, samplesDir, deep)
		if !rep.Pass() {
			t.Fatalf("verify (deep=%v) failed:\n%s", deep, rep.Summary())
		}
		var buf bytes.Buffer
		if err := rep.WriteCSV(&buf); err != nil {
			t.Fatal(err)
		}
		rows, err := csv.NewReader(&buf).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		// header + 4 accounts + 2 currency totals
		if len(rows) != 7 {
			t.Fatalf("csv has %d rows", len(rows))
		}
		last := rows[len(rows)-1]
		if last[0] != "currency_total" || last[1] != "USDT" || last[5] != "" || last[9] != "5000.123456" || last[10] != "5000.123456" || last[11] != "5000.123456" || last[12] != "PASS" {
			t.Errorf("unexpected currency row %v", last)
		}
	}
}

func TestVerifyIsZeroTolerance(t *testing.T) {
	f := newFake()
	srv := f.start(t)
	c := testClient(t, srv.URL, "")
	state := filepath.Join(t.TempDir(), "state.json")
	if res := doApply(t, c, samplesDir, state, Options{}); res.Failed() != 0 {
		t.Fatal(res.Summary())
	}

	// One minor unit too many on a lien account.
	addr := "users:" + walletOf(f, "a-1") + ":wallets:NGN:lien"
	f.mu.Lock()
	v := f.vol(addr, "NGN/2")
	v[0].Add(v[0], big.NewInt(1))
	f.mu.Unlock()

	rep := doVerify(t, c, samplesDir, false)
	if rep.Pass() {
		t.Fatal("verify passed despite a 0.01 difference")
	}
	var failedAccount, failedTotal bool
	for _, r := range rep.Rows {
		if r.RowType == "account" && r.SourceID == "a-1" && r.Result == "FAIL" && strings.Contains(r.Note, "lien differs by 0.01") {
			failedAccount = true
		}
		if r.RowType == "currency_total" && r.SourceID == "NGN" && r.Result == "FAIL" {
			failedTotal = true
		}
	}
	if !failedAccount || !failedTotal {
		t.Fatalf("expected account and currency failures:\n%s", rep.Summary())
	}
}

func TestResumeAfterMidRunCrash(t *testing.T) {
	// Sweep the crash point across the whole run: whatever the server had
	// processed when it died, the next run must converge without duplicates.
	for _, crashAfter := range []int{1, 2, 4, 6, 9, 13, 17, 21, 26, 30} {
		t.Run(fmt.Sprintf("crash_after_%d_mutations", crashAfter), func(t *testing.T) {
			f := newFake()
			f.crashAfter = crashAfter
			srv := f.start(t)
			c := testClient(t, srv.URL, "")
			state := filepath.Join(t.TempDir(), "state.json")

			res := doApply(t, c, samplesDir, state, Options{Concurrency: 3})
			if res.Failed() == 0 {
				t.Fatal("expected failures while the server is down")
			}
			st, err := ReadStateReadOnly(state)
			if err != nil {
				t.Fatal(err)
			}
			if len(st.Problems()) == 0 {
				t.Fatal("state file does not record the failures")
			}

			f.restart()
			res = doApply(t, c, samplesDir, state, Options{Concurrency: 3})
			if res.Failed() != 0 {
				t.Fatalf("resume failed:\n%s", res.Summary())
			}
			assertSamplesImported(t, f)
			if rep := doVerify(t, c, samplesDir, true); !rep.Pass() {
				t.Fatalf("verify failed after resume:\n%s", rep.Summary())
			}
			st, _ = ReadStateReadOnly(state)
			if len(st.Problems()) != 0 {
				t.Fatalf("state still has problems: %v", st.Problems())
			}
		})
	}
}

func TestCrashWithLostStateFileStillConverges(t *testing.T) {
	f := newFake()
	f.crashAfter = 14
	srv := f.start(t)
	c := testClient(t, srv.URL, "")
	doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "s1.json"), Options{})
	f.restart()
	// New state file: the server is the source of truth. A client created just
	// before the crash has no marker yet, so the operator opts in to adopt it.
	res := doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "s2.json"), Options{AdoptUnmarkedClients: true})
	if res.Failed() != 0 {
		t.Fatal(res.Summary())
	}
	assertSamplesImported(t, f)
	if rep := doVerify(t, c, samplesDir, true); !rep.Pass() {
		t.Fatal(rep.Summary())
	}
}

func TestLostResponseDoesNotDuplicate(t *testing.T) {
	// The server commits the request and the connection dies before the answer
	// arrives. For every kind of create the importer must find what it created.
	for _, suffix := range []string{"/products", "/clients", "/kyc", "/accounts", "/transactions"} {
		t.Run(strings.TrimPrefix(suffix, "/"), func(t *testing.T) {
			f := newFake()
			var dropped atomic.Bool
			f.dropResponse = func(method, path string, n int) bool {
				if strings.HasSuffix(path, suffix) && !dropped.Load() {
					dropped.Store(true)
					return true
				}
				return false
			}
			srv := f.start(t)
			c := testClient(t, srv.URL, "")
			res := doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "state.json"), Options{Concurrency: 1})
			if res.Failed() != 0 {
				t.Fatalf("apply failed:\n%s", res.Summary())
			}
			if !dropped.Load() {
				t.Fatal("fault was never injected")
			}
			assertSamplesImported(t, f)
			f.mu.Lock()
			kycCount := len(f.kyc)
			f.mu.Unlock()
			if kycCount != wantClients {
				t.Fatalf("%d KYC records for %d clients", kycCount, wantClients)
			}
			if rep := doVerify(t, c, samplesDir, true); !rep.Pass() {
				t.Fatal(rep.Summary())
			}
		})
	}
}

func TestConflictingDataIsReportedAndNeverOverwritten(t *testing.T) {
	f := newFake()
	srv := f.start(t)
	c := testClient(t, srv.URL, "")

	// 1. A product with the same code but another currency.
	f.mu.Lock()
	if _, e := f.createProduct(map[string]any{"code": "MERCH-NGN", "name": "Pre-existing", "category": "x", "currency": "USDT"}); e != nil {
		t.Fatal(e)
	}
	// 2. A pre-existing client that looks like m-1003 but has no import marker.
	if _, e := f.createClient(map[string]any{"type": "corporate", "contact": map[string]any{"phone": "+2348000000003"},
		"corporate_data": map[string]any{"legal_name": "Initech Services Ltd", "registration_number": "RC0000003"}}); e != nil {
		t.Fatal(e)
	}
	// 3. A transaction already using an opening reference with another amount.
	f.txCount++
	f.txByRef["import:opening:b-2"] = map[string]any{"id": f.txCount, "reference": "import:opening:b-2", "metadata": map[string]string{},
		"postings": []any{map[string]any{"source": "world", "destination": "somewhere", "asset": "USDT/6", "amount": "1"}}}
	f.mu.Unlock()

	pl := doPlan(t, c, samplesDir, "")
	conflicts := map[string]bool{}
	for _, it := range pl.Items {
		if it.Action == ActionConflict {
			conflicts[it.Kind+":"+it.SourceID] = true
		}
	}
	for _, want := range []string{"product:cur-ngn", "client:m-1003"} {
		if !conflicts[want] {
			t.Errorf("plan lacks conflict %s:\n%s", want, pl.Summary())
		}
	}

	res := doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "state.json"), Options{})
	if res.Failed() == 0 {
		t.Fatal("apply reported no problems")
	}
	if res.Counts[KindProduct][string(outConflict)] != 1 || res.Counts[KindClient][string(outConflict)] != 1 {
		t.Fatalf("unexpected result:\n%s", res.Summary())
	}
	// Nothing pre-existing was modified.
	f.mu.Lock()
	for _, p := range f.products {
		if p["code"] == "MERCH-NGN" && (p["currency"] != "USDT" || p["status"] != "draft") {
			t.Errorf("conflicting product was modified: %v", p)
		}
	}
	if len(f.clients) != 3 { // pre-existing + m-1001 + m-1002, m-1003 not created
		t.Errorf("%d clients on the server", len(f.clients))
	}
	if f.txByRef["import:opening:b-2"]["postings"].([]any)[0].(map[string]any)["amount"] != "1" {
		t.Error("conflicting transaction was modified")
	}
	f.mu.Unlock()
	// The unrelated, healthy part of the data set still imported: USDT product and a-2 chain.
	if _, ok := f.txByRef["BM-HOLD-000990"]; ok {
		t.Error("lien was posted although its opening balance conflicts")
	}
	if rep := doVerify(t, c, samplesDir, false); rep.Pass() {
		t.Fatal("verify passed on a conflicted import")
	}
}

func TestIdempotentPlanningOfPartialImport(t *testing.T) {
	// Stop right after the account phase: plan must then show only the
	// postings and final statuses as work, and nothing as create for objects
	// that exist.
	f := newFake()
	f.crashAfter = 0
	srv := f.start(t)
	c := testClient(t, srv.URL, "")
	state := filepath.Join(t.TempDir(), "state.json")
	f.dropResponse = nil

	ctx, cancel := context.WithCancel(context.Background())
	prep, snap := load(t, c, samplesDir)
	st, err := LoadState(state, c.Target())
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(c, prep, snap, st, Options{Concurrency: 2, Logf: func(format string, args ...any) {
		if strings.HasPrefix(format, "phase ") && len(args) > 0 && args[0] == "opening balances" {
			cancel()
		}
	}})
	if _, err := eng.Apply(ctx); err == nil {
		t.Fatal("expected cancellation")
	}
	_ = st.Close()

	pl := doPlan(t, c, samplesDir, state)
	counts := pl.Counts()
	for _, kind := range []string{KindProduct, KindClient, KindAccount} {
		if counts[kind][ActionCreate] != 0 {
			t.Errorf("plan wants to create %s again: %v", kind, counts[kind])
		}
	}
	if counts[KindBalance][ActionCreate] != 3 || counts[KindLien][ActionCreate] != 3 {
		t.Errorf("plan should still post 3 balances and 3 liens: %v %v", counts[KindBalance], counts[KindLien])
	}
	if counts[KindAccount][ActionUpdate] != 1 { // a-3 still has to be suspended
		t.Errorf("account updates = %v", counts[KindAccount])
	}
}

func TestRetriesAndNoBlindRetryOfNonIdempotentCalls(t *testing.T) {
	f := newFake()
	f.fail503["/transactions"] = 2
	f.fail503["/clients"] = 1
	srv := f.start(t)
	c := testClient(t, srv.URL, "")
	res := doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "state.json"), Options{Concurrency: 1})
	if res.Failed() != 0 {
		t.Fatal(res.Summary())
	}
	assertSamplesImported(t, f)
	// Exactly one extra POST /clients, sent only after listing the clients to
	// prove that the failed attempt created nothing.
	creates := 0
	f.mu.Lock()
	for _, l := range f.log {
		if l == "POST /v2/ledgertrack/clients" {
			creates++
		}
	}
	f.mu.Unlock()
	if creates != 4 {
		t.Errorf("POST /clients sent %d times, want 3 + 1 retry after verification", creates)
	}
	// The transaction POST was retried transparently after two 503s.
	txPosts := 0
	f.mu.Lock()
	for _, l := range f.log {
		if l == "POST /v2/ledgertrack/transactions" {
			txPosts++
		}
	}
	f.mu.Unlock()
	if txPosts != wantTxs+2 {
		t.Errorf("POST /transactions sent %d times, want %d", txPosts, wantTxs+2)
	}
}

func TestClientDoesNotRetryUnsafeRequests(t *testing.T) {
	var calls atomic.Int32
	srv := newStatusServer(&calls, 503)
	defer srv.Close()
	c := testClient(t, srv.URL, "")
	_, err := c.CreateProduct(context.Background(), map[string]any{"code": "X"})
	var amb *AmbiguousError
	if err == nil || !errors.As(err, &amb) {
		t.Fatalf("want AmbiguousError, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("unsafe POST was sent %d times", calls.Load())
	}
	calls.Store(0)
	if _, err := c.ListProducts(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if calls.Load() != 4 { // 1 + MaxRetries(3)
		t.Fatalf("GET was sent %d times, want 4", calls.Load())
	}
	calls.Store(0)
	if _, _, err := c.CreateTransaction(context.Background(), TxRequest{Reference: "r", Script: TxScript{Plain: "x"}}); err == nil {
		t.Fatal("expected an error")
	}
	if calls.Load() != 4 {
		t.Fatalf("idempotent POST was sent %d times, want 4", calls.Load())
	}
	if _, _, err := c.CreateTransaction(context.Background(), TxRequest{}); err == nil {
		t.Fatal("a transaction without reference must be refused")
	}
}

func TestBearerToken(t *testing.T) {
	f := newFake()
	f.token = "s3cret"
	srv := f.start(t)
	if _, err := testClient(t, srv.URL, "").Currencies(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want 401, got %v", err)
	}
	if _, err := testClient(t, srv.URL, "s3cret").Currencies(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRateLimit(t *testing.T) {
	f := newFake()
	srv := f.start(t)
	c, err := NewClient(ClientConfig{BaseURL: srv.URL, Rate: 50}) // 20ms between requests
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 6; i++ {
		if _, err := c.Currencies(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d < 90*time.Millisecond {
		t.Fatalf("6 requests at 50/s took only %v", d)
	}
}

func walletOf(f *fakeLT, legacyAccountID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.accounts {
		if md, _ := a["metadata"].(map[string]any); md[metaLegacySourceID] == legacyAccountID {
			return str(a, "wallet_id")
		}
	}
	return ""
}

func newStatusServer(calls *atomic.Int32, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
	}))
}

func TestAdoptUnmarkedClientOnlyWhenAsked(t *testing.T) {
	f := newFake()
	srv := f.start(t)
	c := testClient(t, srv.URL, "")
	f.mu.Lock()
	if _, e := f.createClient(map[string]any{"type": "corporate", "contact": map[string]any{"phone": "+2348000000003"},
		"corporate_data": map[string]any{"legal_name": "Initech Services Ltd", "registration_number": "RC0000003"}}); e != nil {
		t.Fatal(e)
	}
	f.mu.Unlock()

	prep, snap := load(t, c, samplesDir)
	pl, err := BuildPlan(context.Background(), c, prep, snap, nil, Options{AdoptUnmarkedClients: true})
	if err != nil || pl.Conflicts() != 0 {
		t.Fatalf("plan with adoption: %v\n%s", err, pl.Summary())
	}
	res := doApply(t, c, samplesDir, filepath.Join(t.TempDir(), "state.json"), Options{AdoptUnmarkedClients: true})
	if res.Failed() != 0 {
		t.Fatal(res.Summary())
	}
	if _, cl, _, _ := f.sizes(); cl != 3 {
		t.Fatalf("%d clients, want 3 (the pre-existing one adopted)", cl)
	}
	if rep := doVerify(t, c, samplesDir, true); !rep.Pass() {
		t.Fatal(rep.Summary())
	}
}
