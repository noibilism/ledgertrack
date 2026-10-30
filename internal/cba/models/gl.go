package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

const (
	GLAccountTypeAsset     = "asset"
	GLAccountTypeLiability = "liability"
	GLAccountTypeEquity    = "equity"
	GLAccountTypeIncome    = "income"
	GLAccountTypeExpense   = "expense"

	GLSideDebit  = "debit"
	GLSideCredit = "credit"

	GLAccountStatusActive = "active"
	GLAccountStatusClosed = "closed"

	// GLAddressPrefix is the mandatory prefix of every ledger address held by the GL registry.
	GLAddressPrefix = "gl:"
)

// GLAccountTypes lists the account types in the order they appear in reports.
var GLAccountTypes = []string{
	GLAccountTypeAsset,
	GLAccountTypeLiability,
	GLAccountTypeEquity,
	GLAccountTypeIncome,
	GLAccountTypeExpense,
}

// GLAccount is one line of the chart of accounts. Balances are never stored here:
// they are always read from the ledger at Address.
type GLAccount struct {
	bun.BaseModel `bun:"_system.gl_accounts,alias:gl_accounts"`

	ID         uuid.UUID      `json:"id" bun:"id,type:uuid,pk"`
	Code       string         `json:"code" bun:"code,type:varchar(64),notnull"`
	Name       string         `json:"name" bun:"name,type:varchar(255),notnull"`
	Type       string         `json:"type" bun:"type,type:varchar(16),notnull"`
	NormalSide string         `json:"normal_side" bun:"normal_side,type:varchar(8),notnull"`
	Currency   string         `json:"currency" bun:"currency,type:varchar(16),notnull"`
	ParentCode *string        `json:"parent_code" bun:"parent_code,type:varchar(64)"`
	Ledger     string         `json:"ledger" bun:"ledger,type:varchar(255),notnull"`
	Address    string         `json:"address" bun:"address,type:varchar(512),notnull"`
	Status     string         `json:"status" bun:"status,type:varchar(16),notnull"`
	Metadata   map[string]any `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt  time.Time      `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt  time.Time      `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}

// DefaultGLNormalSide returns the conventional normal side for an account type.
func DefaultGLNormalSide(accountType string) string {
	switch accountType {
	case GLAccountTypeAsset, GLAccountTypeExpense:
		return GLSideDebit
	default:
		return GLSideCredit
	}
}
