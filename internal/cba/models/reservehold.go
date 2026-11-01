package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

const (
	// HoldStatusPending: the hold is recorded and the credit is being posted. It is not in the ledger yet.
	HoldStatusPending = "pending"
	// HoldStatusHeld: the credit is posted and the held share sits in the wallet lien until ReleaseAt.
	HoldStatusHeld = "held"
	// HoldStatusReleased: the held share was moved from the lien to the available balance.
	HoldStatusReleased = "released"
	// HoldStatusVoided: the credit never reached the ledger, so nothing is held.
	HoldStatusVoided = "voided"
	// HoldStatusNeedsReview: releasing failed too many times (for example the lien was spent by something else).
	HoldStatusNeedsReview = "needs_review"

	HoldReleaseSLA    = "sla"
	HoldReleaseManual = "manual"
)

// ReserveHold is the register entry of one credit that was partly held back by a reserve rule.
// The held share is in the wallet's lien (ledger balance) until ReleaseAt, then moves to available.
type ReserveHold struct {
	bun.BaseModel `bun:"_system.reserve_holds,alias:reserve_holds"`

	ID        uuid.UUID  `json:"id" bun:"id,type:uuid,pk"`
	RuleID    *uuid.UUID `json:"rule_id,omitempty" bun:"rule_id,type:uuid,nullzero"`
	AccountID uuid.UUID  `json:"account_id" bun:"account_id,type:uuid,notnull"`
	ClientID  uuid.UUID  `json:"client_id" bun:"client_id,type:uuid,notnull"`
	Ledger    string     `json:"ledger" bun:"ledger,type:varchar(255),notnull"`
	WalletID  string     `json:"wallet_id" bun:"wallet_id,type:varchar(255),notnull"`
	Currency  string     `json:"currency" bun:"currency,type:varchar(16),notnull"`

	// CreditReference is the reference of the credit that was split; it is unique per wallet.
	CreditReference   string `json:"credit_reference" bun:"credit_reference,type:varchar(255),notnull"`
	CreditTxID        *int64 `json:"credit_tx_id,omitempty" bun:"credit_tx_id,type:bigint,nullzero"`
	CreditType        string `json:"credit_type,omitempty" bun:"credit_type,type:varchar(32),notnull,default:''"`
	CreditAmountMinor int64  `json:"credit_amount_minor" bun:"credit_amount_minor,type:bigint,notnull"`
	// AmountMinor is the share held in the lien.
	AmountMinor int64     `json:"amount_minor" bun:"amount_minor,type:bigint,notnull"`
	ReleaseAt   time.Time `json:"release_at" bun:"release_at,type:timestamp without time zone,notnull"`

	Status           string     `json:"status" bun:"status,type:varchar(16),notnull"`
	ReleasedAt       *time.Time `json:"released_at,omitempty" bun:"released_at,type:timestamp without time zone,nullzero"`
	ReleaseReason    string     `json:"release_reason,omitempty" bun:"release_reason,type:varchar(16),notnull,default:''"`
	ReleaseReference string     `json:"release_reference,omitempty" bun:"release_reference,type:varchar(255),notnull,default:''"`

	LeaseOwner string     `json:"-" bun:"lease_owner,type:varchar(128),notnull,default:''"`
	LeaseUntil *time.Time `json:"-" bun:"lease_until,type:timestamp without time zone,nullzero"`
	Attempts   int        `json:"attempts" bun:"attempts,type:integer,notnull,default:0"`
	LastError  string     `json:"last_error,omitempty" bun:"last_error,type:text,notnull,default:''"`

	CreatedAt time.Time `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt time.Time `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}
