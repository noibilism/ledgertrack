package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/uptrace/bun"
)

const (
	ProductStatusDraft   = "draft"
	ProductStatusActive  = "active"
	ProductStatusRetired = "retired"

	ClientTypeIndividual = "individual"
	ClientTypeCorporate  = "corporate"

	ClientStatusPending   = "pending"
	ClientStatusActive    = "active"
	ClientStatusSuspended = "suspended"
	ClientStatusClosed    = "closed"

	KYCStatusPending  = "pending"
	KYCStatusVerified = "verified"
	KYCStatusRejected = "rejected"
	KYCStatusExpired  = "expired"

	AccountStatusPending   = "pending"
	AccountStatusActive    = "active"
	AccountStatusDormant   = "dormant"
	AccountStatusSuspended = "suspended"
	AccountStatusClosed    = "closed"

	FeePostingStatusPendingRecovery  = "pending_recovery"
	FeePostingStatusPosted           = "posted"
	FeePostingStatusWriteoffRequired = "writeoff_required"
)

type TransactionLimits struct {
	DailyDebitLimit   *string `json:"daily_debit_limit,omitempty"`
	DailyCreditLimit  *string `json:"daily_credit_limit,omitempty"`
	SingleDebitLimit  *string `json:"single_debit_limit,omitempty"`
	SingleCreditLimit *string `json:"single_credit_limit,omitempty"`
}

type ProductRules struct {
	MinOpeningBalance    string             `json:"min_opening_balance,omitempty"`
	MinBalance           string             `json:"min_balance,omitempty"`
	MaxBalance           *string            `json:"max_balance,omitempty"`
	AllowNegativeBalance bool               `json:"allow_negative_balance,omitempty"`
	AllowDebits          bool               `json:"allow_debits,omitempty"`
	AllowCredits         bool               `json:"allow_credits,omitempty"`
	RequiresKYCLevel     int                `json:"requires_kyc_level,omitempty"`
	EligibleClientTypes  []string           `json:"eligible_client_types,omitempty"`
	DormancyDays         *int               `json:"dormancy_days,omitempty"`
	TransactionLimits    *TransactionLimits `json:"transaction_limits,omitempty"`
}

type InterestTier struct {
	MinBalance string  `json:"min_balance"`
	MaxBalance *string `json:"max_balance,omitempty"`
	Rate       string  `json:"rate"`
}

type InterestConfig struct {
	Type             string         `json:"type"`
	Rate             string         `json:"rate,omitempty"`
	AccrualFrequency string         `json:"accrual_frequency,omitempty"`
	PostingFrequency string         `json:"posting_frequency,omitempty"`
	Tiers            []InterestTier `json:"tiers,omitempty"`
}

type MaintenanceFee struct {
	Amount    string `json:"amount"`
	Frequency string `json:"frequency"`
	Currency  string `json:"currency"`
}

type TransactionFee struct {
	Event    string  `json:"event"`
	Type     string  `json:"type"`
	Value    string  `json:"value"`
	Min      *string `json:"min,omitempty"`
	Max      *string `json:"max,omitempty"`
	Currency string  `json:"currency,omitempty"`
}

type FeeSchedule struct {
	MaintenanceFee  *MaintenanceFee  `json:"maintenance_fee,omitempty"`
	TransactionFees []TransactionFee `json:"transaction_fees,omitempty"`
	PenaltyFees     map[string]any   `json:"penalty_fees,omitempty"`
}

type Address struct {
	Line1   string `json:"line1,omitempty"`
	Line2   string `json:"line2,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Country string `json:"country,omitempty"`
	Postal  string `json:"postal,omitempty"`
}

type ClientContact struct {
	Email   string   `json:"email,omitempty"`
	Phone   string   `json:"phone,omitempty"`
	Address *Address `json:"address,omitempty"`
}

type IndividualData struct {
	FirstName        string  `json:"first_name,omitempty"`
	MiddleName       string  `json:"middle_name,omitempty"`
	LastName         string  `json:"last_name,omitempty"`
	DateOfBirth      *string `json:"date_of_birth,omitempty"`
	Gender           *string `json:"gender,omitempty"`
	NationalIDType   string  `json:"national_id_type,omitempty"`
	NationalIDNumber string  `json:"national_id_number,omitempty"`
	Nationality      string  `json:"nationality,omitempty"`
	Occupation       string  `json:"occupation,omitempty"`
	Employer         *string `json:"employer,omitempty"`
}

type BeneficialOwner struct {
	Name         string `json:"name"`
	OwnershipPct string `json:"ownership_pct"`
	NationalID   string `json:"national_id,omitempty"`
}

type AuthorizedSignatory struct {
	ClientID     string `json:"client_id"`
	Role         string `json:"role"`
	SigningLimit string `json:"signing_limit,omitempty"`
}

type CorporateData struct {
	LegalName             string                `json:"legal_name,omitempty"`
	TradingName           *string               `json:"trading_name,omitempty"`
	RegistrationNumber    string                `json:"registration_number,omitempty"`
	TaxID                 string                `json:"tax_id,omitempty"`
	IncorporationDate     *string               `json:"incorporation_date,omitempty"`
	IncorporationCountry  string                `json:"incorporation_country,omitempty"`
	Industry              string                `json:"industry,omitempty"`
	BeneficialOwners      []BeneficialOwner     `json:"beneficial_owners,omitempty"`
	AuthorizedSignatories []AuthorizedSignatory `json:"authorized_signatories,omitempty"`
}

type KYCDocument struct {
	Type      string         `json:"type,omitempty"`
	Reference string         `json:"reference,omitempty"`
	Provider  string         `json:"provider,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type Product struct {
	bun.BaseModel `bun:"_system.products,alias:products"`

	ID             uuid.UUID       `json:"id" bun:"id,type:uuid,pk"`
	Code           string          `json:"code" bun:"code,type:varchar(64),notnull"`
	Name           string          `json:"name" bun:"name,type:varchar(255),notnull"`
	Description    string          `json:"description,omitempty" bun:"description,type:text,nullzero"`
	Category       string          `json:"category" bun:"category,type:varchar(128),notnull"`
	Currency       string          `json:"currency" bun:"currency,type:varchar(16),notnull"`
	Status         string          `json:"status" bun:"status,type:varchar(32),notnull"`
	Rules          ProductRules    `json:"rules" bun:"rules,type:jsonb,notnull,default:'{}'::jsonb"`
	InterestConfig *InterestConfig `json:"interest_config,omitempty" bun:"interest_config,type:jsonb,nullzero"`
	FeeSchedule    *FeeSchedule    `json:"fee_schedule,omitempty" bun:"fee_schedule,type:jsonb,nullzero"`
	CreatedAt      time.Time       `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt      time.Time       `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}

type Client struct {
	bun.BaseModel `bun:"_system.clients,alias:clients"`

	ID             uuid.UUID       `json:"id" bun:"id,type:uuid,pk"`
	ClientNumber   string          `json:"client_number" bun:"client_number,type:varchar(64),notnull"`
	Type           string          `json:"type" bun:"type,type:varchar(32),notnull"`
	Status         string          `json:"status" bun:"status,type:varchar(32),notnull"`
	KYCLevel       int             `json:"kyc_level" bun:"kyc_level,type:int,notnull"`
	KYCStatus      string          `json:"kyc_status" bun:"kyc_status,type:varchar(32),notnull"`
	KYCData        map[string]any  `json:"kyc_data,omitempty" bun:"kyc_data,type:jsonb,notnull,default:'{}'::jsonb"`
	Contact        ClientContact   `json:"contact" bun:"contact,type:jsonb,notnull,default:'{}'::jsonb"`
	IndividualData *IndividualData `json:"individual_data,omitempty" bun:"individual_data,type:jsonb,nullzero"`
	CorporateData  *CorporateData  `json:"corporate_data,omitempty" bun:"corporate_data,type:jsonb,nullzero"`
	CreatedAt      time.Time       `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt      time.Time       `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}

type Account struct {
	bun.BaseModel `bun:"_system.accounts,alias:accounts"`

	ID              uuid.UUID       `json:"id" bun:"id,type:uuid,pk"`
	AccountNumber   string          `json:"account_number" bun:"account_number,type:varchar(32),notnull"`
	ClientID        uuid.UUID       `json:"client_id" bun:"client_id,type:uuid,notnull"`
	ProductID       uuid.UUID       `json:"product_id" bun:"product_id,type:uuid,notnull"`
	Currency        string          `json:"currency" bun:"currency,type:varchar(16),notnull"`
	Status          string          `json:"status" bun:"status,type:varchar(32),notnull"`
	WalletID        string          `json:"wallet_id" bun:"wallet_id,type:varchar(255),notnull"`
	FreezeDebits    bool            `json:"freeze_debits" bun:"freeze_debits,type:boolean,notnull"`
	OpenedAt        time.Time       `json:"opened_at" bun:"opened_at,type:timestamp without time zone,nullzero"`
	ActivatedAt     *time.Time      `json:"activated_at,omitempty" bun:"activated_at,type:timestamp without time zone,nullzero"`
	ClosedAt        *time.Time      `json:"closed_at,omitempty" bun:"closed_at,type:timestamp without time zone,nullzero"`
	LastActivityAt  *time.Time      `json:"last_activity_at,omitempty" bun:"last_activity_at,type:timestamp without time zone,nullzero"`
	InterestAccrued decimal.Decimal `json:"interest_accrued" bun:"interest_accrued,type:numeric"`
	Metadata        map[string]any  `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
}

type KYCRecord struct {
	bun.BaseModel `bun:"_system.kyc_records,alias:kyc_records"`

	ID          uuid.UUID      `json:"id" bun:"id,type:uuid,pk"`
	ClientID    uuid.UUID      `json:"client_id" bun:"client_id,type:uuid,notnull"`
	Level       int            `json:"level" bun:"level,type:int,notnull"`
	Status      string         `json:"status" bun:"status,type:varchar(32),notnull"`
	SubmittedAt time.Time      `json:"submitted_at" bun:"submitted_at,type:timestamp without time zone,nullzero"`
	VerifiedAt  *time.Time     `json:"verified_at,omitempty" bun:"verified_at,type:timestamp without time zone,nullzero"`
	ExpiresAt   *time.Time     `json:"expires_at,omitempty" bun:"expires_at,type:timestamp without time zone,nullzero"`
	Verifier    string         `json:"verifier,omitempty" bun:"verifier,type:varchar(255),nullzero"`
	Reason      string         `json:"reason,omitempty" bun:"reason,type:text,nullzero"`
	Documents   []KYCDocument  `json:"documents,omitempty" bun:"documents,type:jsonb,notnull,default:'[]'::jsonb"`
	Payload     map[string]any `json:"payload,omitempty" bun:"payload,type:jsonb,notnull,default:'{}'::jsonb"`
}

type InterestAccrual struct {
	bun.BaseModel `bun:"_system.interest_accruals,alias:interest_accruals"`

	ID              uuid.UUID       `json:"id" bun:"id,type:uuid,pk"`
	AccountID       uuid.UUID       `json:"account_id" bun:"account_id,type:uuid,notnull"`
	AccrualDate     time.Time       `json:"accrual_date" bun:"accrual_date,type:date,notnull"`
	BalanceBasis    decimal.Decimal `json:"balance_basis" bun:"balance_basis,type:numeric"`
	Rate            decimal.Decimal `json:"rate" bun:"rate,type:numeric"`
	Amount          decimal.Decimal `json:"amount" bun:"amount,type:numeric"`
	Posted          bool            `json:"posted" bun:"posted,type:boolean,notnull"`
	PostedReference string          `json:"posted_reference,omitempty" bun:"posted_reference,type:varchar(255),nullzero"`
	Metadata        map[string]any  `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt       time.Time       `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
}

type FeePosting struct {
	bun.BaseModel `bun:"_system.fee_postings,alias:fee_postings"`

	ID              uuid.UUID       `json:"id" bun:"id,type:uuid,pk"`
	AccountID       uuid.UUID       `json:"account_id" bun:"account_id,type:uuid,notnull"`
	EventType       string          `json:"event_type" bun:"event_type,type:varchar(64),notnull"`
	Reference       string          `json:"reference" bun:"reference,type:varchar(255),notnull"`
	LinkedReference string          `json:"linked_reference" bun:"linked_reference,type:varchar(255),notnull"`
	Amount          decimal.Decimal `json:"amount" bun:"amount,type:numeric"`
	Currency        string          `json:"currency" bun:"currency,type:varchar(16),notnull"`
	Status          string          `json:"status" bun:"status,type:varchar(64),notnull"`
	Metadata        map[string]any  `json:"metadata,omitempty" bun:"metadata,type:jsonb,notnull,default:'{}'::jsonb"`
	CreatedAt       time.Time       `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
}

type AccountDailyUsage struct {
	bun.BaseModel `bun:"_system.account_daily_usages,alias:account_daily_usages"`

	ID            uuid.UUID       `json:"id" bun:"id,type:uuid,pk"`
	AccountID     uuid.UUID       `json:"account_id" bun:"account_id,type:uuid,notnull"`
	UsageDate     time.Time       `json:"usage_date" bun:"usage_date,type:date,notnull"`
	DebitAmount   decimal.Decimal `json:"debit_amount" bun:"debit_amount,type:numeric"`
	CreditAmount  decimal.Decimal `json:"credit_amount" bun:"credit_amount,type:numeric"`
	DebitCount    int64           `json:"debit_count" bun:"debit_count,type:bigint,notnull"`
	CreditCount   int64           `json:"credit_count" bun:"credit_count,type:bigint,notnull"`
	LastReference string          `json:"last_reference,omitempty" bun:"last_reference,type:varchar(255),nullzero"`
	CreatedAt     time.Time       `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt     time.Time       `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}
