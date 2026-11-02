package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type seen struct {
	Method, Path, Query, Auth, ResourceID, Body string
}

type fake struct {
	*httptest.Server
	mu    sync.Mutex
	calls []seen
}

func newFake(t *testing.T, reply func(r *http.Request, body string) (int, string)) *fake {
	t.Helper()
	f := &fake{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.calls = append(f.calls, seen{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Get("X-Bankman-Resource-Id"), string(raw)})
		f.mu.Unlock()
		code, body := reply(r, string(raw))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fake) got() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.calls...)
}

func gateway(t *testing.T, bankman, lt *fake, tweak func(*config)) (*shadow, *httptest.Server) {
	t.Helper()
	cfg := config{Bankman: bankman.URL, LedgerTrack: lt.URL, LTPrefix: "/bankman/api/v1", Workers: 4, QueueSize: 100,
		Timeout: 2 * time.Second, MaxBody: 1 << 20, ReadSample: 1, Retries: 2, IgnoreFields: []string{"created_at", "account_number"}}
	if tweak != nil {
		tweak(&cfg)
	}
	s := newShadow(cfg, nil)
	h, err := s.handler()
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(func() { srv.Close(); s.close() })
	return s, srv
}

func call(t *testing.T, srv *httptest.Server, method, path, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer caller")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

const depositReply = `{"status":"success","message":"Deposit successful","data":{"transaction_id":"tx-1","account_id":"acct-1","status":"completed","balance_after":"1500.0000"}}`

func TestCallerGetsBankmansAnswerAndLedgerTrackGetsACopy(t *testing.T) {
	bank := newFake(t, func(*http.Request, string) (int, string) { return 201, depositReply })
	lt := newFake(t, func(*http.Request, string) (int, string) {
		return 201, `{"status":"success","message":"Deposit successful","data":{"transaction_id":"tx-1","account_id":"acct-1","status":"completed","balance_after":1500}}`
	})
	s, srv := gateway(t, bank, lt, nil)

	code, body := call(t, srv, http.MethodPost, "/api/v1/transactions/deposits/acct-1", `{"amount":500}`)
	if code != 201 || body != depositReply {
		t.Fatalf("caller saw %d %s", code, body)
	}
	waitFor(t, "replay", func() bool { return s.st.Mirrored.Load() == 1 })
	got := lt.got()[0]
	if got.Path != "/bankman/api/v1/transactions/deposits/acct-1" || got.Auth != "Bearer caller" || got.Body != `{"amount":500}` || got.ResourceID != "tx-1" {
		t.Errorf("replay = %+v", got)
	}
	if s.st.Matched.Load() != 1 || s.st.Mismatched.Load() != 0 {
		t.Errorf("1500.0000 and 1500 should match: matched=%d mismatched=%d diffs=%v", s.st.Matched.Load(), s.st.Mismatched.Load(), s.recent())
	}
}

func TestVersionPrefixesBothMap(t *testing.T) {
	bank := newFake(t, func(*http.Request, string) (int, string) { return 200, `{"status":"success"}` })
	lt := newFake(t, func(*http.Request, string) (int, string) { return 200, `{"status":"success"}` })
	s, srv := gateway(t, bank, lt, nil)
	call(t, srv, http.MethodGet, "/v1/accounts?external_id=e1", "")
	call(t, srv, http.MethodGet, "/api/v1/products", "")
	waitFor(t, "replays", func() bool { return s.st.Mirrored.Load() == 2 })
	paths := map[string]bool{}
	for _, c := range lt.got() {
		paths[c.Path+"?"+c.Query] = true
	}
	if !paths["/bankman/api/v1/accounts?external_id=e1"] || !paths["/bankman/api/v1/products?"] {
		t.Errorf("paths = %v", paths)
	}
}

func TestDifferencesAreReported(t *testing.T) {
	bank := newFake(t, func(*http.Request, string) (int, string) {
		return 200, `{"status":"success","data":{"available_balance":"100.0000","account_number":"111","name":"A"}}`
	})
	lt := newFake(t, func(*http.Request, string) (int, string) {
		return 200, `{"status":"success","data":{"available_balance":"90.0000","account_number":"999","name":"A"}}`
	})
	s, srv := gateway(t, bank, lt, nil)
	call(t, srv, http.MethodGet, "/api/v1/accounts/acct-1", "")
	waitFor(t, "diff", func() bool { return s.st.Mismatched.Load() == 1 })
	d := s.recent()[0].Diffs
	if len(d) != 1 || d[0].Path != "$.data.available_balance" || d[0].Bankman != `"100"` || d[0].LedgerTr != `"90"` {
		t.Errorf("diffs = %+v (account_number is ignored)", d)
	}

	lt2 := newFake(t, func(*http.Request, string) (int, string) {
		return 404, `{"status":"error","message":"Account not found"}`
	})
	s2, srv2 := gateway(t, bank, lt2, nil)
	call(t, srv2, http.MethodGet, "/api/v1/accounts/acct-1", "")
	waitFor(t, "status diff", func() bool { return s2.st.Mismatched.Load() == 1 })
	if s2.recent()[0].Diffs[0].Path != "status" {
		t.Errorf("diffs = %+v", s2.recent()[0].Diffs)
	}
}

func TestWritesForOneAccountReachLedgerTrackInOrder(t *testing.T) {
	bank := newFake(t, func(*http.Request, string) (int, string) { return 201, depositReply })
	first := true
	var mu sync.Mutex
	lt := newFake(t, func(_ *http.Request, body string) (int, string) {
		mu.Lock()
		slow := first
		first = false
		mu.Unlock()
		if slow {
			time.Sleep(150 * time.Millisecond) // the first call is slow; the second must still wait for it
		}
		return 201, depositReply
	})
	s, srv := gateway(t, bank, lt, nil)
	call(t, srv, http.MethodPost, "/api/v1/transactions/deposits/acct-1", `{"n":1}`)
	call(t, srv, http.MethodPost, "/api/v1/transactions/deposits/acct-1", `{"n":2}`)
	waitFor(t, "both", func() bool { return s.st.Mirrored.Load() == 2 })
	got := lt.got()
	if got[0].Body != `{"n":1}` || got[1].Body != `{"n":2}` {
		t.Errorf("order = %v", got)
	}
}

func TestALienReleaseFollowsItsPlacement(t *testing.T) {
	bank := newFake(t, func(r *http.Request, _ string) (int, string) {
		return 200, `{"status":"success","data":{"transaction_id":"lien-9","account_id":"acct-1"}}`
	})
	lt := newFake(t, func(*http.Request, string) (int, string) {
		return 200, `{"status":"success","data":{"transaction_id":"lien-9","account_id":"acct-1"}}`
	})
	s, srv := gateway(t, bank, lt, nil)
	call(t, srv, http.MethodPost, "/api/v1/transactions/liens/acct-1", `{}`)
	waitFor(t, "placement", func() bool { return s.st.Mirrored.Load() == 1 })
	if k := s.partitionKey(http.MethodPut, "/transactions/liens/lien-9"); k != "acct-1" {
		t.Errorf("a release is ordered with its account, got key %q", k)
	}
	if k := s.partitionKey(http.MethodPut, "/transactions/liens/unknown"); k != "lien:unknown" {
		t.Errorf("unknown lien key %q", k)
	}
}

func TestBankmanErrorsAndOutagesNeverTouchTheCaller(t *testing.T) {
	bank := newFake(t, func(*http.Request, string) (int, string) { return 500, `{"status":"error"}` })
	lt := newFake(t, func(*http.Request, string) (int, string) { return 200, `{}` })
	s, srv := gateway(t, bank, lt, nil)
	if code, _ := call(t, srv, http.MethodPost, "/api/v1/transactions/deposits/acct-1", `{}`); code != 500 {
		t.Fatalf("caller must see Bankman's 500, got %d", code)
	}
	if s.st.Skipped.Load() != 1 || len(lt.got()) != 0 {
		t.Errorf("a Bankman 5xx is not replayed: skipped=%d replays=%d", s.st.Skipped.Load(), len(lt.got()))
	}

	ok := newFake(t, func(*http.Request, string) (int, string) { return 200, `{"status":"success"}` })
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	cfgLT := &fake{Server: &httptest.Server{URL: deadURL}}
	s2, srv2 := gateway(t, ok, cfgLT, nil)
	start := time.Now()
	if code, _ := call(t, srv2, http.MethodGet, "/api/v1/products", ""); code != 200 {
		t.Fatalf("caller status %d", code)
	}
	if time.Since(start) > time.Second {
		t.Errorf("a dead LedgerTrack slowed the caller: %v", time.Since(start))
	}
	waitFor(t, "error counted", func() bool { return s2.st.Errors.Load() == 1 })
}

func TestPauseTurnsTheGatewayIntoAPlainProxy(t *testing.T) {
	bank := newFake(t, func(*http.Request, string) (int, string) { return 200, `{"status":"success"}` })
	lt := newFake(t, func(*http.Request, string) (int, string) { return 200, `{"status":"success"}` })
	s, srv := gateway(t, bank, lt, func(c *config) { c.AdminToken = "adm" })

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/shadow/pause", nil)
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 404 {
		t.Errorf("admin without a token should be 404, got %d", resp.StatusCode)
	}
	req.Header.Set("Authorization", "Bearer adm")
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 204 {
		t.Errorf("pause = %d", resp.StatusCode)
	}
	call(t, srv, http.MethodGet, "/api/v1/products", "")
	time.Sleep(50 * time.Millisecond)
	if len(lt.got()) != 0 || s.st.Skipped.Load() != 1 {
		t.Errorf("paused gateway must not replay: replays=%d skipped=%d", len(lt.got()), s.st.Skipped.Load())
	}
}

func TestCompareNormalisesNumbers(t *testing.T) {
	ig := map[string]bool{}
	if d := compareReplies([]byte(`{"a":"1500.0000","b":[1,2],"c":null}`), []byte(`{"a":1500,"b":[1,2],"c":null}`), ig); len(d) != 0 {
		t.Errorf("diffs = %v", d)
	}
	if d := compareReplies([]byte(`{"b":[1,2]}`), []byte(`{"b":[1]}`), ig); len(d) == 0 {
		t.Errorf("array length difference missed")
	}
	if d := compareReplies([]byte(`not json`), []byte(`not json`), ig); len(d) != 0 {
		t.Errorf("identical text should match: %v", d)
	}
}
