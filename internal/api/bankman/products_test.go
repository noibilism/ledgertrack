package bankman

import (
	"net/http"
	"strings"
	"testing"

	"github.com/formancehq/ledger/internal/cba/repositories"
)

func TestProductsKeepBankmanIDsAndHideTheMarker(t *testing.T) {
	b := &backend{products: `{"data":{"products":[` +
		`{"id":"11111111-1111-1111-1111-111111111111","code":"WAL-NGN","name":"Wallet NGN","category":"wallet","currency":"NGN","status":"active",` +
		`"description":"Merchant wallet [legacy_source_id=bm-prod-7]","rules":{"min_opening_balance":"0.00"},"created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:05Z"},` +
		`{"id":"22222222-2222-2222-2222-222222222222","code":"NEW","name":"New","category":"wallet","currency":"GHS","status":"retired","created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:05Z"}]}}`}
	h, _ := setup(t, b)

	rec, out := do(h, http.MethodGet, "/bankman/api/v1/products", "")
	if rec.Code != http.StatusOK || out["status"] != "success" || out["message"] != "Products retrieved successfully" {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	rows := out["data"].([]any)
	if len(rows) != 2 {
		t.Fatalf("products %d", len(rows))
	}
	imported := rows[0].(map[string]any)
	if imported["id"] != "bm-prod-7" || imported["description"] != "Merchant wallet" ||
		imported["product_name"] != "Wallet NGN" || imported["product_type"] != "wallet" ||
		imported["currency"] != "NGN" || imported["is_active"] != true {
		t.Errorf("imported product = %v", imported)
	}
	native := rows[1].(map[string]any)
	if native["id"] != "22222222-2222-2222-2222-222222222222" || native["is_active"] != false || native["description"] != nil {
		t.Errorf("native product = %v", native)
	}
}

func TestHealthIsPublicAndShaped(t *testing.T) {
	rec, out := do(http.HandlerFunc(Health), http.MethodGet, "/bankman/health", "")
	if rec.Code != http.StatusOK || out["status"] != "ok" || out["timestamp"] == nil {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAggregatedMerchantBalances(t *testing.T) {
	testStats = []repositories.WalletStat{{Currency: "NGN", Wallets: 5, Clients: 3}}
	t.Cleanup(func() { testStats = nil })
	b := &backend{aggs: map[string]string{
		"users::wallets:NGN:available": `{"data":{"NGN/2":1234500}}`,
		"users::wallets:NGN:lien":      `{"data":{"NGN/2":50000}}`,
	}}
	h, _ := setup(t, b)
	rec, out := do(h, http.MethodGet, "/bankman/api/v1/reports/aggregated-merchant-balances", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	row := out["data"].(map[string]any)["balances"].([]any)[0].(map[string]any)
	if row["currency"] != "NGN" || row["total_available_balance"] != "12345.0000" ||
		row["total_locked_balance"] != "500.0000" || row["total_ledger_balance"] != "12845.0000" ||
		row["merchant_count"] != float64(3) {
		t.Errorf("row = %v", row)
	}
}

func TestAllHistorySelectsWalletTransactionsAndSkipsOthers(t *testing.T) {
	b := &backend{}
	h, lookup := setupWallet(t, b)
	wallet := strings.Replace(depositTx(), `"metadata":{`, `"metadata":{"account_id":"`+lookup.account.ID.String()+`",`, 1)
	internal := `{"id":12,"reference":"FEE-1","metadata":{},"postings":[]}`
	b.listed = `{"cursor":{"hasMore":true,"next":"t","data":[` + wallet + `,` + internal + `]}}`

	rec, out := do(h, http.MethodGet, "/bankman/api/v1/transactions/accounts/all-history?per_page=2&currency=ngn&start_date=2026-11-01", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	data := out["data"].(map[string]any)
	if n := len(data["transactions"].([]any)); n != 1 {
		t.Fatalf("rows %d, want the wallet transaction only", n)
	}
	if data["pagination"].(map[string]any)["has_more_pages"] != true {
		t.Errorf("pagination %v", data["pagination"])
	}
	q := b.last().RawQuery
	if !strings.Contains(q, "account=users%3A%3Awallets%3ANGN%3Aavailable") || !strings.Contains(q, "startTime=2026-11-01T00%3A00%3A00Z") {
		t.Errorf("query %q", q)
	}

	rec, _ = do(h, http.MethodGet, "/bankman/api/v1/transactions/accounts/all-history?search=x", "")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("unsupported filter: %d", rec.Code)
	}
}
