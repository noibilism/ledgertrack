package bankman

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/shopspring/decimal"

	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

// aggregate sums the balances of every wallet address matching the pattern, in minor units.
func (h *Handler) aggregate(r *http.Request, pattern, currency string) (int64, error) {
	asset, err := currencyregistry.AssetFor(currency)
	if err != nil {
		return 0, err
	}
	rp, err := h.call(r, http.MethodGet, "/"+url.PathEscape(h.ledger)+"/aggregate/balances?address="+url.QueryEscape(pattern), nil)
	if err != nil {
		return 0, err
	}
	if !rp.ok() {
		return 0, errStatus(rp)
	}
	var out struct {
		Data map[string]json.Number `json:"data"`
	}
	if err := json.Unmarshal(rp.Body, &out); err != nil {
		return 0, err
	}
	v, ok := out.Data[asset]
	if !ok || v.String() == "" {
		return 0, nil
	}
	d, err := decimal.NewFromString(v.String())
	if err != nil {
		return 0, err
	}
	return d.IntPart(), nil
}

// aggregatedMerchantBalances answers GET /reports/aggregated-merchant-balances: wallet totals per currency
// over active accounts. Bankman also lists a per-product breakdown; LedgerTrack cannot sum a ledger address
// pattern by product, so `products` is empty (see docs).
func (h *Handler) aggregatedMerchantBalances(w http.ResponseWriter, r *http.Request) {
	stats, err := h.lookup.ActiveWalletStats(r.Context())
	if err != nil {
		fail(w, http.StatusInternalServerError, "Failed to retrieve aggregated balances")
		return
	}
	rows := make([]map[string]any, 0, len(stats))
	for _, s := range stats {
		available, err := h.aggregate(r, "users::wallets:"+s.Currency+":available", s.Currency)
		if err != nil {
			fail(w, http.StatusInternalServerError, "Failed to retrieve aggregated balances")
			return
		}
		lien, err := h.aggregate(r, "users::wallets:"+s.Currency+":lien", s.Currency)
		if err != nil {
			fail(w, http.StatusInternalServerError, "Failed to retrieve aggregated balances")
			return
		}
		rows = append(rows, map[string]any{
			"currency":                s.Currency,
			"total_ledger_balance":    fourDecimals(available+lien, s.Currency),
			"total_locked_balance":    fourDecimals(lien, s.Currency),
			"total_available_balance": fourDecimals(available, s.Currency),
			"products":                []any{},
			"merchant_count":          s.Clients,
		})
	}
	respond(w, http.StatusOK, "Success", map[string]any{"balances": rows})
}
