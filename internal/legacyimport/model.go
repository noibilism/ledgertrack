package legacyimport

import (
	"encoding/json"
	"math/big"
)

// Canonical input records. One JSON object per line, one file per entity type.
// Every record carries a stable source_id from the legacy system.

// ProductRec is one line of products.jsonl (legacy currency).
type ProductRec struct {
	SourceID       string          `json:"source_id"`
	Code           string          `json:"code"`
	Name           string          `json:"name"`
	Description    string          `json:"description,omitempty"`
	Category       string          `json:"category"`
	Currency       string          `json:"currency"`
	Precision      *int            `json:"precision,omitempty"`
	Status         string          `json:"status,omitempty"`
	Rules          json.RawMessage `json:"rules,omitempty"`
	InterestConfig json.RawMessage `json:"interest_config,omitempty"`
	FeeSchedule    json.RawMessage `json:"fee_schedule,omitempty"`

	Line int `json:"-"`
}

// Address is the optional client address.
type Address struct {
	Line1   string `json:"line1,omitempty"`
	Line2   string `json:"line2,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Country string `json:"country,omitempty"`
	Postal  string `json:"postal,omitempty"`
}

// ClientRec is one line of clients.jsonl (legacy merchant, imported as a corporate client).
type ClientRec struct {
	SourceID             string   `json:"source_id"`
	LegalName            string   `json:"legal_name"`
	TradingName          string   `json:"trading_name,omitempty"`
	RegistrationNumber   string   `json:"registration_number,omitempty"`
	TaxID                string   `json:"tax_id,omitempty"`
	IncorporationCountry string   `json:"incorporation_country,omitempty"`
	Industry             string   `json:"industry,omitempty"`
	Email                string   `json:"email,omitempty"`
	Phone                string   `json:"phone"`
	Address              *Address `json:"address,omitempty"`
	Status               string   `json:"status,omitempty"`
	KYCLevel             int      `json:"kyc_level"`
	KYCSource            string   `json:"kyc_source"`
	KYCIdentifier        string   `json:"kyc_identifier,omitempty"`
	LegacyCreatedAt      string   `json:"legacy_created_at,omitempty"`

	Line int `json:"-"`
}

// AccountRec is one line of accounts.jsonl (merchant balance per currency).
type AccountRec struct {
	SourceID            string `json:"source_id"`
	ClientSourceID      string `json:"client_source_id"`
	ProductCode         string `json:"product_code"`
	LegacyAccountNumber string `json:"legacy_account_number"`
	Status              string `json:"status,omitempty"`
	OpenedAt            string `json:"opened_at,omitempty"`

	Line int `json:"-"`
}

// BalanceRec is one line of balances.jsonl. Amounts are decimal strings in major units.
type BalanceRec struct {
	SourceID        string `json:"source_id"`
	AccountSourceID string `json:"account_source_id"`
	Currency        string `json:"currency"`
	Amount          string `json:"amount"`
	Available       string `json:"available"`
	Lien            string `json:"lien"`
	AsOf            string `json:"as_of,omitempty"`

	Line int `json:"-"`

	// Resolved minor units, filled by Prepare.
	total, available, lien *big.Int
}

// LienRec is one line of liens.jsonl.
type LienRec struct {
	SourceID        string `json:"source_id"`
	AccountSourceID string `json:"account_source_id"`
	Reference       string `json:"reference"`
	Amount          string `json:"amount"`
	CreatedAt       string `json:"created_at"`
	Description     string `json:"description,omitempty"`

	Line int `json:"-"`

	amount *big.Int
}

// Dataset is the full parsed input.
type Dataset struct {
	Products []*ProductRec
	Clients  []*ClientRec
	Accounts []*AccountRec
	Balances []*BalanceRec
	Liens    []*LienRec
}

// Allowed status values.
var (
	ClientStatuses  = []string{"pending", "active", "suspended", "closed"}
	AccountStatuses = []string{"pending", "active", "frozen", "dormant", "suspended", "closed"}
	ProductStatuses = []string{"active", "retired"}
)

// StatusOrDefault returns the effective client status.
func (c *ClientRec) StatusOrDefault() string {
	if c.Status == "" {
		return "active"
	}
	return c.Status
}

// StatusOrDefault returns the effective account status.
func (a *AccountRec) StatusOrDefault() string {
	if a.Status == "" {
		return "active"
	}
	return a.Status
}

// StatusOrDefault returns the effective product status.
func (p *ProductRec) StatusOrDefault() string {
	if p.Status == "" {
		return "active"
	}
	return p.Status
}
