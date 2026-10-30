package legacyimport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const samplesDir = "../../docs/import/samples"

func offlineOpts() PrepareOptions {
	return PrepareOptions{Precision: map[string]int{"NGN": 2, "USDT": 6}, Authoritative: true}
}

func loadSamples(t *testing.T) *Dataset {
	t.Helper()
	ds, issues := LoadDataset(samplesDir)
	if HasErrors(issues) {
		t.Fatalf("sample files do not load: %v", issues)
	}
	return ds
}

func TestSamplesAreValid(t *testing.T) {
	ds := loadSamples(t)
	prep, issues := Prepare(ds, offlineOpts())
	if HasErrors(issues) {
		t.Fatalf("samples do not validate: %v", issues)
	}
	ngn, usdt := prep.Totals["NGN"], prep.Totals["USDT"]
	if got := FormatAmount(ngn.Total, ngn.Precision); got != "1325000.50" {
		t.Errorf("NGN total = %s", got)
	}
	if got := FormatAmount(usdt.Total, usdt.Precision); got != "5000.123456" {
		t.Errorf("USDT total = %s", got)
	}
	if got := FormatAmount(ngn.Lien, ngn.Precision); got != "250000.00" {
		t.Errorf("NGN lien = %s", got)
	}
}

// copyDir copies the samples so a test can break one file.
func copySamples(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(samplesDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(samplesDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func replaceInFile(t *testing.T, dir, name, old, new string) {
	t.Helper()
	p := filepath.Join(dir, name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("%s does not contain %q", name, old)
	}
	if err := os.WriteFile(p, []byte(strings.Replace(string(b), old, new, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func hasIssue(issues []Issue, sub string) bool {
	for _, i := range issues {
		if i.Severity == "error" && strings.Contains(strings.ToLower(i.Message), strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func TestValidationFindings(t *testing.T) {
	cases := []struct {
		name string
		file string
		old  string
		new  string
		want string
	}{
		{"excess fractional digits", FileBalances, `"amount":"75000.00","available":"75000.00"`, `"amount":"75000.001","available":"75000.00"`, "exceed currency precision"},
		{"sum mismatch", FileBalances, `"available":"75000.00","lien":"0.00"`, `"available":"74999.99","lien":"0.00"`, "amount 75000.00 != available"},
		{"negative amount", FileBalances, `"amount":"0","available":"0","lien":"0"`, `"amount":"-1","available":"0","lien":"0"`, "does not match"},
		{"unknown account", FileBalances, `"account_source_id":"a-3"`, `"account_source_id":"a-99"`, "not found in accounts.jsonl"},
		{"currency mismatch with product", FileBalances, `"account_source_id":"a-2","currency":"USDT"`, `"account_source_id":"a-2","currency":"NGN"`, "differs from the currency"},
		{"unknown client", FileAccounts, `"client_source_id":"m-1003"`, `"client_source_id":"m-404"`, "not found in clients.jsonl"},
		{"unknown product", FileAccounts, `"product_code":"MERCH-USDT"`, `"product_code":"NOPE"`, "not found in products.jsonl"},
		{"duplicate account pair", FileAccounts, `"client_source_id":"m-1002","product_code":"MERCH-NGN"`, `"client_source_id":"m-1001","product_code":"MERCH-NGN"`, "one account per client and product"},
		{"duplicate lien reference", FileLiens, `"reference":"BM-HOLD-000452"`, `"reference":"BM-HOLD-000451"`, "duplicate reference"},
		{"lien sum mismatch", FileLiens, `"amount":"100000.00"`, `"amount":"90000.00"`, "differs from the sum of liens.jsonl"},
		{"reserved reference namespace", FileLiens, `"reference":"BM-HOLD-000990"`, `"reference":"import:opening:x"`, "reserved import: namespace"},
		{"min opening balance", FileProducts, `"precision":2}`, `"precision":2,"rules":{"min_opening_balance":"10"}}`, "min_opening_balance must be 0"},
		{"client level 2 needs opt-in", FileClients, `"kyc_level":1,"kyc_source":"bankman merchant_kyc tier=1 approved 2024-07-20"`, `"kyc_level":2,"kyc_source":"x"`, "--allow-synthesized-kyc-evidence"},
		{"closed account with funds", FileAccounts, `"legacy_account_number":"BM-NGN-0002","status":"suspended"`, `"legacy_account_number":"BM-NGN-0002","status":"closed"`, "closed but has a non-zero balance"},
		{"precision differs from registry", FileProducts, `"precision":6`, `"precision":8`, "differs from currency registry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := copySamples(t)
			replaceInFile(t, dir, tc.file, tc.old, tc.new)
			ds, issues := LoadDataset(dir)
			if !HasErrors(issues) {
				_, more := Prepare(ds, offlineOpts())
				issues = append(issues, more...)
			}
			if !hasIssue(issues, tc.want) {
				t.Fatalf("expected an error containing %q, got %v", tc.want, issues)
			}
		})
	}
}

func TestSchemaRejectsUnknownFieldsAndBadFormats(t *testing.T) {
	dir := copySamples(t)
	replaceInFile(t, dir, FileBalances, `"source_id":"b-1"`, `"source_id":"b-1","surprise":true`)
	replaceInFile(t, dir, FileLiens, `"created_at":"2026-10-30T14:02:11Z"`, `"created_at":"yesterday"`)
	replaceInFile(t, dir, FileProducts, `"code":"MERCH-NGN"`, `"code":"MERCH NGN"`)
	_, issues := LoadDataset(dir)
	for _, want := range []string{"surprise", "created_at", "code"} {
		if !hasIssue(issues, want) {
			t.Errorf("no schema error mentioning %q in %v", want, issues)
		}
	}
}

func TestOfflineValidationAssumesTwoDecimalsWithWarning(t *testing.T) {
	dir := copySamples(t)
	replaceInFile(t, dir, FileProducts, `,"precision":6`, ``)
	ds, issues := LoadDataset(dir)
	if HasErrors(issues) {
		t.Fatal(issues)
	}
	_, issues = Prepare(ds, PrepareOptions{Precision: map[string]int{"NGN": 2}})
	var warned bool
	for _, i := range issues {
		if i.Severity == "warning" && strings.Contains(i.Message, "assuming 2") {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("expected an 'assuming 2' warning, got %v", issues)
	}
	// 6 decimals then fails against the assumed 2dp.
	if !hasIssue(issues, "exceed currency precision") {
		t.Fatalf("expected excess digit error, got %v", issues)
	}
}

func TestIndistinguishableClientsRejected(t *testing.T) {
	dir := copySamples(t)
	replaceInFile(t, dir, FileClients, `"legal_name":"Initech Services Ltd","registration_number":"RC0000003","phone":"+2348000000003"`,
		`"legal_name":"Globex Trading Ltd","registration_number":"RC0000002","phone":"+2348000000002"`)
	ds, issues := LoadDataset(dir)
	if HasErrors(issues) {
		t.Fatal(issues)
	}
	_, issues = Prepare(ds, offlineOpts())
	if !hasIssue(issues, "indistinguishable") {
		t.Fatalf("got %v", issues)
	}
}
