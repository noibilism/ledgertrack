package legacyimport

import (
	"encoding/json"
	"github.com/formancehq/ledger/internal/currency"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStateRoundTripAndResumeSemantics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := LoadState(path, "http://x|ledgertrack")
	if err != nil {
		t.Fatal(err)
	}
	st.Set(KindClient, "m-1", StatusInflight, "creating")
	// A later failure must not erase the fact that a create was started.
	st.Fail(KindClient, "m-1", StatusFailed, os.ErrDeadlineExceeded)
	st.Update(KindBalance, "b-1", func(it *ItemState) {
		it.Status = StatusDone
		it.Extra = map[string]string{"tx_id": "7"}
	})
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := LoadState(path, "http://x|ledgertrack")
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if it := again.Get(KindClient, "m-1"); it == nil || it.Status != StatusFailed || !it.CreateStarted || it.Error == "" {
		t.Fatalf("client state lost: %+v", it)
	}
	if it := again.Get(KindBalance, "b-1"); it == nil || it.Extra["tx_id"] != "7" {
		t.Fatalf("balance state lost: %+v", it)
	}
	if got := again.Counts()[KindClient][StatusFailed]; got != 1 {
		t.Fatalf("counts = %v", again.Counts())
	}
	if len(again.Problems()) != 1 {
		t.Fatalf("problems = %v", again.Problems())
	}
	// Get returns a copy.
	cp := again.Get(KindBalance, "b-1")
	cp.Extra["tx_id"] = "changed"
	if again.Get(KindBalance, "b-1").Extra["tx_id"] != "7" {
		t.Fatal("Get leaked internal state")
	}
}

func TestStateIsBoundToItsTargetAndLocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := LoadState(path, "http://staging|ledgertrack")
	if err != nil {
		t.Fatal(err)
	}
	st.Set(KindProduct, "p", StatusDone, "")
	// A second process (second lock on the same file) is refused.
	if _, err := LoadState(path, "http://staging|ledgertrack"); err == nil || !strings.Contains(err.Error(), "another") {
		t.Fatalf("second lock accepted: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// The same file cannot be reused against production.
	if _, err := LoadState(path, "http://prod|ledgertrack"); err == nil || !strings.Contains(err.Error(), "belongs to target") {
		t.Fatalf("state reused against another target: %v", err)
	}
}

func TestStateFlushIsAtomicAndSkipsWhenClean(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := LoadState(path, "t")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Flush(); err != nil { // clean: nothing written
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("clean state was written")
	}
	st.Set(KindLien, "l", StatusDone, "")
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
	}
	raw, _ := os.ReadFile(path)
	if !json.Valid(raw) {
		t.Fatal("state file is not valid JSON")
	}
}

func TestCorruptStateIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadState(path, "t"); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("got %v", err)
	}
}

func TestStatusReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, _ := LoadState(path, "t")
	st.Set(KindAccount, "a-1", StatusDone, "ok")
	st.Fail(KindBalance, "b-1", StatusConflict, os.ErrExist)
	_ = st.Close()
	ro, err := ReadStateReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	out := StatusReport(ro, 10)
	for _, want := range []string{"account", "done=1", "conflict=1", "b-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output lacks %q:\n%s", want, out)
		}
	}
}

func TestResponseDecodingAcceptsStringsAndNumbers(t *testing.T) {
	var tx Tx
	for _, raw := range []string{
		`{"id":12,"reference":"r","postings":[{"source":"a","destination":"b","asset":"NGN/2","amount":150}]}`,
		`{"id":"12","reference":"r","postings":[{"source":"a","destination":"b","asset":"NGN/2","amount":"150"}]}`,
	} {
		if err := json.Unmarshal([]byte(raw), &tx); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if string(tx.ID) != "12" || tx.Postings[0].Amount.String() != "150" {
			t.Fatalf("decoded %+v", tx)
		}
	}
	var v struct {
		Input bigNum `json:"input"`
	}
	if err := json.Unmarshal([]byte(`{"input":123456789012345678901234567890}`), &v); err != nil || v.Input.String() != "123456789012345678901234567890" {
		t.Fatalf("big numbers must stay exact: %v %v", v.Input, err)
	}
}

func TestOptionsValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  Options
		ok   bool
	}{
		{"defaults", Options{}, true},
		{"six decimal asset", Options{AssetTemplate: "{CCY}/{PREC}"}, true},
		{"system control forbidden", Options{SourceTemplate: "system:control:{CCY}"}, false},
		{"world forbidden", Options{SourceTemplate: "world"}, false},
		{"needs a currency placeholder", Options{SourceTemplate: "gl:migration:opening"}, false},
		{"bad address", Options{SourceTemplate: "gl:migration:{CCY} x"}, false},
		{"bad asset", Options{AssetTemplate: "usd/2"}, false},
	} {
		err := tc.opt.Validate()
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v", tc.name, err)
		}
	}
}

// The importer's default asset names must equal what the server produces via
// currency.AssetFor, otherwise imported funds are invisible to balance,
// statement and finance report endpoints.
func TestDefaultAssetTemplateMatchesServerAssetFor(t *testing.T) {
	currency.SetDefinitions(map[string]currency.Definition{
		"NGN":  {Precision: 2, Enabled: true},
		"USDT": {Precision: 6, Enabled: true},
		"JPY":  {Precision: 0, Enabled: true},
	})
	t.Cleanup(func() { currency.SetDefinitions(map[string]currency.Definition{}) })

	var opt Options
	opt.defaults()
	for ccy, prec := range map[string]int{"NGN": 2, "USDT": 6, "JPY": 0} {
		want, err := currency.AssetFor(ccy)
		if err != nil {
			t.Fatal(err)
		}
		if got := opt.asset(ccy, prec); got != want {
			t.Errorf("%s: importer asset %q != server asset %q", ccy, got, want)
		}
	}
	if err := opt.Validate(); err != nil {
		t.Fatal(err)
	}
	// A zero-precision asset must also pass the importer's own asset validation.
	if a := opt.asset("JPY", 0); !assetRe.MatchString(a) {
		t.Errorf("%q rejected by assetRe", a)
	}
}
