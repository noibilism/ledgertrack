package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func adminCall(t *testing.T, srv *httptest.Server, method, path, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func ok200(*http.Request, string) (int, string) { return 200, `{"status":"success","data":{}}` }

func cutoverGateway(t *testing.T, hold time.Duration) (*shadow, *httptest.Server, *fake, *fake) {
	bankman, lt := newFake(t, ok200), newFake(t, ok200)
	s, srv := gateway(t, bankman, lt, func(c *config) { c.AdminToken = "tok"; c.HoldMax = hold })
	return s, srv, bankman, lt
}

func status(t *testing.T, srv *httptest.Server) cutoverStatus {
	_, body := adminCall(t, srv, http.MethodGet, "/shadow/cutover/status", "")
	var st cutoverStatus
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err, body)
	}
	return st
}

func TestHeldWritesWaitAndThenGoToBankman(t *testing.T) {
	_, srv, bankman, _ := cutoverGateway(t, 5*time.Second)
	adminCall(t, srv, http.MethodPost, "/shadow/cutover/hold", "")
	done := make(chan int, 1)
	go func() {
		resp, err := http.Post(srv.URL+"/bankman/api/v1/transactions/deposits/a1", "application/json", strings.NewReader(`{"amount":"1"}`))
		if err != nil {
			done <- 0
			return
		}
		_ = resp.Body.Close()
		done <- resp.StatusCode
	}()
	time.Sleep(150 * time.Millisecond)
	if n := len(bankman.got()); n != 0 {
		t.Fatalf("write reached Bankman during the hold: %d", n)
	}
	// Reads keep flowing during a hold.
	if c, _ := call(t, srv, http.MethodGet, "/bankman/api/v1/accounts/a1", ""); c != 200 {
		t.Fatalf("read during hold: %d", c)
	}
	adminCall(t, srv, http.MethodPost, "/shadow/cutover/release", "")
	select {
	case c := <-done:
		if c != 200 {
			t.Fatalf("released write: %d", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write never released")
	}
}

func TestHeldWriteGets503AfterTheHoldLimit(t *testing.T) {
	_, srv, bankman, _ := cutoverGateway(t, 200*time.Millisecond)
	adminCall(t, srv, http.MethodPost, "/shadow/cutover/hold", "")
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/bankman/api/v1/transactions/deposits/a1", strings.NewReader(`{}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d retry-after=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if len(bankman.got()) != 0 {
		t.Fatal("a refused write reached Bankman")
	}
}

func TestSwitchNeedsAQuietReconciledHold(t *testing.T) {
	s, srv, bankman, lt := cutoverGateway(t, 5*time.Second)
	sw := func() int {
		c, _ := adminCall(t, srv, http.MethodPost, "/shadow/cutover/switch?to=ledgertrack", "")
		return c
	}
	if sw() != http.StatusConflict {
		t.Fatal("switched without a hold")
	}
	adminCall(t, srv, http.MethodPost, "/shadow/cutover/hold", "")
	waitFor(t, "drain", func() bool { return status(t, srv).Drained })
	if sw() != http.StatusConflict {
		t.Fatal("switched without a reconciliation")
	}
	stale := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	adminCall(t, srv, http.MethodPost, "/shadow/reconcile", `{"started_at":"`+stale+`","checked":10,"mismatches":0}`)
	if sw() != http.StatusConflict {
		t.Fatal("switched on a reconciliation that predates the quiet point")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	adminCall(t, srv, http.MethodPost, "/shadow/reconcile", `{"started_at":"`+now+`","checked":10,"mismatches":2}`)
	if sw() != http.StatusConflict {
		t.Fatal("switched with differing accounts")
	}
	adminCall(t, srv, http.MethodPost, "/shadow/reconcile", `{"started_at":"`+now+`","checked":10,"mismatches":0}`)
	if sw() != http.StatusOK {
		t.Fatalf("switch refused: %+v", status(t, srv))
	}
	if st := status(t, srv); st.Primary != "ledgertrack" || st.Gate.Checked != 0 {
		t.Fatalf("state after switch: %+v", st)
	}

	adminCall(t, srv, http.MethodPost, "/shadow/cutover/release", "")
	// LedgerTrack now answers, on its compatibility prefix, and Bankman gets the copy on the caller's own path.
	call(t, srv, http.MethodPost, "/bankman/api/v1/transactions/deposits/a1", `{"amount":"5"}`)
	waitFor(t, "reverse copy", func() bool { return len(bankman.got()) == 1 })
	if g := lt.got(); len(g) != 1 || g[0].Path != "/bankman/api/v1/transactions/deposits/a1" {
		t.Fatalf("LedgerTrack saw %+v", g)
	}
	if g := bankman.got(); g[0].Path != "/bankman/api/v1/transactions/deposits/a1" || g[0].ResourceID != "" || g[0].Body != `{"amount":"5"}` {
		t.Fatalf("Bankman copy %+v", g[0])
	}
	_ = s
}

func TestAWriteInFlightKeepsTheHoldFromDraining(t *testing.T) {
	release := make(chan struct{})
	bankman := newFake(t, func(*http.Request, string) (int, string) { <-release; return 200, `{"status":"success","data":{}}` })
	lt := newFake(t, ok200)
	_, srv := gateway(t, bankman, lt, func(c *config) { c.AdminToken = "tok" })
	go func() {
		if resp, err := http.Post(srv.URL+"/bankman/api/v1/transactions/deposits/a1", "application/json", strings.NewReader(`{}`)); err == nil {
			_ = resp.Body.Close()
		}
	}()
	waitFor(t, "write in flight", func() bool { return status(t, srv).InflightWrite == 1 })
	adminCall(t, srv, http.MethodPost, "/shadow/cutover/hold", "")
	time.Sleep(150 * time.Millisecond)
	if status(t, srv).Drained {
		t.Fatal("drained with a write in flight")
	}
	close(release)
	waitFor(t, "drain", func() bool { return status(t, srv).Drained })
}
