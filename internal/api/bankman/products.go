package bankman

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/formancehq/ledger/internal/cba/models"
)

// legacyProductMarker is the tag the importer appends to a product description to keep the Bankman id.
var legacyProductMarker = regexp.MustCompile(`\s*\[legacy_source_id=([^\]]+)\]`)

// productID is the id callers know the product by: the Bankman id for imported products.
func productID(p *models.Product) string {
	if m := legacyProductMarker.FindStringSubmatch(p.Description); m != nil {
		return m[1]
	}
	return p.ID.String()
}

type productView struct {
	ID                           string   `json:"id"`
	ProductName                  string   `json:"product_name"`
	ProductType                  string   `json:"product_type"`
	Currency                     string   `json:"currency"`
	CurrencyName                 string   `json:"currency_name"`
	MinimumAmount                *string  `json:"minimum_amount"`
	MaximumAmount                *string  `json:"maximum_amount"`
	InterestRate                 *string  `json:"interest_rate"`
	InterestRateType             *string  `json:"interest_rate_type"`
	InterestCalculationFrequency *string  `json:"interest_calculation_frequency"`
	InterestPostingFrequency     *string  `json:"interest_posting_frequency"`
	RepaymentFrequency           *string  `json:"repayment_frequency"`
	AmortizationType             *string  `json:"amortization_type"`
	GracePeriodDays              *int     `json:"grace_period_days"`
	LatePaymentPenaltyRate       *string  `json:"late_payment_penalty_rate"`
	Description                  *string  `json:"description"`
	IsActive                     bool     `json:"is_active"`
	Charges                      []string `json:"charges"`
	CreatedAt                    string   `json:"created_at"`
	UpdatedAt                    string   `json:"updated_at"`
}

func newProductView(p *models.Product) productView {
	v := productView{
		ID: productID(p), ProductName: p.Name, ProductType: p.Category,
		Currency: p.Currency, CurrencyName: p.Currency, IsActive: p.Status == models.ProductStatusActive,
		Charges:   []string{},
		CreatedAt: p.CreatedAt.UTC().Format(time.RFC3339), UpdatedAt: p.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if d := strings.TrimSpace(legacyProductMarker.ReplaceAllString(p.Description, "")); d != "" {
		v.Description = &d
	}
	if p.Rules.MinOpeningBalance != "" {
		s := p.Rules.MinOpeningBalance
		v.MinimumAmount = &s
	}
	if p.Rules.MaxBalance != nil {
		v.MaximumAmount = p.Rules.MaxBalance
	}
	if ic := p.InterestConfig; ic != nil && ic.Rate != "" {
		rate, typ := ic.Rate, ic.Type
		v.InterestRate, v.InterestRateType = &rate, &typ
		if ic.AccrualFrequency != "" {
			f := ic.AccrualFrequency
			v.InterestCalculationFrequency = &f
		}
		if ic.PostingFrequency != "" {
			f := ic.PostingFrequency
			v.InterestPostingFrequency = &f
		}
	}
	return v
}

// listProducts answers GET /products with every product, as Bankman does.
func (h *Handler) listProducts(w http.ResponseWriter, r *http.Request) {
	rp, err := h.call(r, http.MethodGet, "/products", nil)
	if err != nil || !rp.ok() {
		fail(w, http.StatusInternalServerError, "Failed to retrieve products")
		return
	}
	var out struct {
		Data struct {
			Products []models.Product `json:"products"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rp.Body, &out); err != nil {
		fail(w, http.StatusInternalServerError, "Failed to retrieve products")
		return
	}
	views := make([]productView, 0, len(out.Data.Products))
	for i := range out.Data.Products {
		views = append(views, newProductView(&out.Data.Products[i]))
	}
	respond(w, http.StatusOK, "Products retrieved successfully", views)
}

// Health answers Bankman's unauthenticated GET /health.
func Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok", "service": "ledgertrack", "timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	})
}
