package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

const (
	ReserveScopeClient  = "client"
	ReserveScopeAccount = "account"

	ReserveStatusActive  = "active"
	ReserveStatusRevoked = "revoked"

	// HoldModeCalendar counts HoldHours as elapsed clock hours. HoldModeBusiness counts only the hours
	// inside the rule's business calendar.
	HoldModeCalendar = "calendar"
	HoldModeBusiness = "business"
)

// ReserveRule holds back a share of every credit into a wallet. The held share stays in the
// ledger balance (the wallet's lien) for HoldHours, the agreed SLA, and is then released to the
// available balance. A client rule covers the client's accounts in Currency, optionally only those
// of ProductID. An account rule covers one account.
type ReserveRule struct {
	bun.BaseModel `bun:"_system.reserve_rules,alias:reserve_rules"`

	ID        uuid.UUID  `json:"id" bun:"id,type:uuid,pk"`
	Scope     string     `json:"scope" bun:"scope,type:varchar(16),notnull"`
	ClientID  *uuid.UUID `json:"client_id,omitempty" bun:"client_id,type:uuid,nullzero"`
	AccountID *uuid.UUID `json:"account_id,omitempty" bun:"account_id,type:uuid,nullzero"`
	ProductID *uuid.UUID `json:"product_id,omitempty" bun:"product_id,type:uuid,nullzero"`
	Currency  string     `json:"currency" bun:"currency,type:varchar(16),notnull"`
	// Percentage of each credit that is held, as a decimal string: "10" is 10%.
	Percentage string `json:"percentage" bun:"percentage,type:varchar(32),notnull"`
	// HoldHours is how long the held share stays in the ledger balance before it becomes available.
	HoldHours int `json:"hold_hours" bun:"hold_hours,type:integer,notnull"`
	// HoldMode is "calendar" or "business". Business needs CalendarID.
	HoldMode   string     `json:"hold_mode" bun:"hold_mode,type:varchar(16),notnull"`
	CalendarID *uuid.UUID `json:"calendar_id,omitempty" bun:"calendar_id,type:uuid,nullzero"`
	// CreditTypes limits the rule to these credit types. Empty means every credit.
	CreditTypes   []string       `json:"credit_types" bun:"credit_types,type:jsonb,notnull,default:'[]'::jsonb"`
	EffectiveFrom time.Time      `json:"effective_from" bun:"effective_from,type:timestamp without time zone,notnull"`
	EffectiveTo   *time.Time     `json:"effective_to,omitempty" bun:"effective_to,type:timestamp without time zone,nullzero"`
	Status        string         `json:"status" bun:"status,type:varchar(16),notnull"`
	Metadata      map[string]any `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt     time.Time      `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt     time.Time      `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}
