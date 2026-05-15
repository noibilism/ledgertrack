package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type FeeLayer struct {
	From       *string `json:"from,omitempty"`
	To         *string `json:"to,omitempty"`
	Flat       *string `json:"flat,omitempty"`
	Percentage *string `json:"percentage,omitempty"`
}

type FeeStructure struct {
	Type       string     `json:"type"`
	Flat       *string    `json:"flat,omitempty"`
	Percentage *string    `json:"percentage,omitempty"`
	Min        *string    `json:"min,omitempty"`
	Max        *string    `json:"max,omitempty"`
	Layers     []FeeLayer `json:"layers,omitempty"`
}

type ChannelFeeConfig struct {
	bun.BaseModel `bun:"_system.channel_fee_configs,alias:channel_fee_configs"`

	ID            uuid.UUID    `json:"id" bun:"id,type:uuid,pk"`
	ChannelID     string       `json:"channel_id" bun:"channel_id,type:varchar(255),notnull"`
	Currency      string       `json:"currency" bun:"currency,type:varchar(16),notnull"`
	Enabled       bool         `json:"enabled" bun:"enabled,type:boolean,notnull"`
	UserFee       FeeStructure `json:"user_fee" bun:"user_fee,type:jsonb,notnull,default:'{}'::jsonb"`
	ProcessingFee FeeStructure `json:"processing_fee" bun:"processing_fee,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt     time.Time    `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt     time.Time    `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}

type ChannelFeeConfigAudit struct {
	bun.BaseModel `bun:"_system.channel_fee_config_audits,alias:channel_fee_config_audits"`

	ID        uuid.UUID      `json:"id" bun:"id,type:uuid,pk"`
	ChannelID string         `json:"channel_id" bun:"channel_id,type:varchar(255),notnull"`
	Actor     *string        `json:"actor,omitempty" bun:"actor,type:varchar(255),nullzero"`
	Action    string         `json:"action" bun:"action,type:varchar(32),notnull"`
	Before    map[string]any `json:"before,omitempty" bun:"before,type:jsonb,nullzero"`
	After     map[string]any `json:"after,omitempty" bun:"after,type:jsonb,nullzero"`
	CreatedAt time.Time      `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
}

type ChannelFeeRecord struct {
	bun.BaseModel `bun:"_system.channel_fee_records,alias:channel_fee_records"`

	ID                uuid.UUID      `json:"id" bun:"id,type:uuid,pk"`
	ChannelID          string         `json:"channel_id" bun:"channel_id,type:varchar(255),notnull"`
	Currency           string         `json:"currency" bun:"currency,type:varchar(16),notnull"`
	WalletID           *string        `json:"wallet_id,omitempty" bun:"wallet_id,type:varchar(255),nullzero"`
	Reference          string         `json:"reference" bun:"reference,type:varchar(255),notnull"`
	LedgerTxID         *int64         `json:"ledger_tx_id,omitempty" bun:"ledger_tx_id,type:bigint,nullzero"`
	ChannelTxID        *int64         `json:"channel_tx_id,omitempty" bun:"channel_tx_id,type:bigint,nullzero"`
	RevenueTxID        *int64         `json:"revenue_tx_id,omitempty" bun:"revenue_tx_id,type:bigint,nullzero"`
	OccurredAt         time.Time      `json:"occurred_at" bun:"occurred_at,type:timestamp without time zone,nullzero"`
	TotalAmount        int64          `json:"total_amount" bun:"total_amount,type:bigint,notnull"`
	PrincipalAmount    int64          `json:"principal_amount" bun:"principal_amount,type:bigint,notnull"`
	UserFeeAmount      int64          `json:"user_fee_amount" bun:"user_fee_amount,type:bigint,notnull"`
	ProcessingFeeAmount int64         `json:"processing_fee_amount" bun:"processing_fee_amount,type:bigint,notnull"`
	NetRevenueAmount   int64          `json:"net_revenue_amount" bun:"net_revenue_amount,type:bigint,notnull"`
	Metadata           map[string]any `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
}

