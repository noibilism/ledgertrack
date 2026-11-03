package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

const (
	FeeOverrideScopeClient  = "client"
	FeeOverrideScopeAccount = "account"

	FeeConfigStatusActive  = "active"
	FeeConfigStatusRevoked = "revoked"
)

// FeeOverride replaces the product's transaction fees for one event on the accounts it covers.
// A client override covers the client's accounts in Currency, optionally only those of ProductID.
// An account override covers one account. Rules are all the fee rules for Event, so a combined
// fee (flat plus percentage) is one override.
type FeeOverride struct {
	bun.BaseModel `bun:"_system.fee_overrides,alias:fee_overrides"`

	ID            uuid.UUID        `json:"id" bun:"id,type:uuid,pk"`
	Scope         string           `json:"scope" bun:"scope,type:varchar(16),notnull"`
	ClientID      *uuid.UUID       `json:"client_id,omitempty" bun:"client_id,type:uuid,nullzero"`
	AccountID     *uuid.UUID       `json:"account_id,omitempty" bun:"account_id,type:uuid,nullzero"`
	ProductID     *uuid.UUID       `json:"product_id,omitempty" bun:"product_id,type:uuid,nullzero"`
	Currency      string           `json:"currency" bun:"currency,type:varchar(16),notnull"`
	Event         string           `json:"event" bun:"event,type:varchar(16),notnull"`
	Rules         []TransactionFee `json:"rules" bun:"rules,type:jsonb,notnull"`
	EffectiveFrom time.Time        `json:"effective_from" bun:"effective_from,type:timestamp without time zone,notnull"`
	EffectiveTo   *time.Time       `json:"effective_to,omitempty" bun:"effective_to,type:timestamp without time zone,nullzero"`
	Status        string           `json:"status" bun:"status,type:varchar(16),notnull"`
	Metadata      map[string]any   `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt     time.Time        `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt     time.Time        `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}

// TaxRule is a tax charged on top of a transaction in one country. Base says whether the rate
// applies to the fee or to the transaction amount. PayableAccounts maps a currency to the GL
// account that holds the tax collected in that currency until it is remitted.
type TaxRule struct {
	bun.BaseModel `bun:"_system.tax_rules,alias:tax_rules"`

	ID              uuid.UUID         `json:"id" bun:"id,type:uuid,pk"`
	Country         string            `json:"country" bun:"country,type:varchar(2),notnull"`
	Code            string            `json:"code" bun:"code,type:varchar(32),notnull"`
	Name            string            `json:"name" bun:"name,type:varchar(255),notnull"`
	Event           string            `json:"event" bun:"event,type:varchar(16),notnull"`
	Base            string            `json:"base" bun:"base,type:varchar(16),notnull"`
	Rate            string            `json:"rate" bun:"rate,type:varchar(32),notnull"`
	MinAmount       *string           `json:"min,omitempty" bun:"min_amount,type:varchar(32),nullzero"`
	MaxAmount       *string           `json:"max,omitempty" bun:"max_amount,type:varchar(32),nullzero"`
	PayableAccounts map[string]string `json:"payable_accounts" bun:"payable_accounts,type:jsonb,notnull"`
	EffectiveFrom   time.Time         `json:"effective_from" bun:"effective_from,type:timestamp without time zone,notnull"`
	EffectiveTo     *time.Time        `json:"effective_to,omitempty" bun:"effective_to,type:timestamp without time zone,nullzero"`
	Status          string            `json:"status" bun:"status,type:varchar(16),notnull"`
	Metadata        map[string]any    `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt       time.Time         `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt       time.Time         `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}
