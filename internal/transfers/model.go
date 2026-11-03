// Package transfers makes wallet operations that touch several Formance
// ledgers (user ledger, channels-{CCY}, revenue-{CCY}) recoverable.
//
// Ledgers cannot share a database transaction, so instead of pretending the
// flow is atomic we persist the intent in _system.transfers before the first
// ledger write and then drive the legs forward. Each leg is idempotent because
// it reuses the business reference (a duplicate reference is "already done"),
// which makes it safe to re-drive a transfer from any state, from any process.
// See docs/design/atomic-transfers.md.
package transfers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Kind string

const (
	// KindDebit is POST /wallets/{id}/debit with a channel.
	KindDebit Kind = "debit"
	// KindReleasePay is POST /wallets/{id}/lien/release with mode PAY and a channel.
	KindReleasePay Kind = "release_pay"
)

type State string

const (
	StateCreated       State = "created"
	StateUserPosted    State = "user_posted"
	StateChannelPosted State = "channel_posted"
	StateRevenuePosted State = "revenue_posted"
	StateCompleted     State = "completed"
	// StateFailed is terminal and means nothing was posted (the request was
	// rejected on the first leg). It is not recovered, so a rejected request can
	// never turn into a debit later.
	StateFailed State = "failed"
	// StateNeedsReview is terminal for automation: the principal (or part of
	// the transfer) is posted and a human must decide.
	StateNeedsReview State = "failed_needs_review"
)

// Terminal reports whether automation must leave the transfer alone.
func (s State) Terminal() bool {
	return s == StateCompleted || s == StateFailed || s == StateNeedsReview
}

// Result is what the HTTP layer needs to answer the original request. It is
// stored so that a retry returns the same body without re-running any leg.
type Result struct {
	Timestamp     json.RawMessage   `json:"timestamp,omitempty"`
	Postings      json.RawMessage   `json:"postings,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	BalanceBefore int64             `json:"balance_before"`
	BalanceAfter  int64             `json:"balance_after"`
	Warning       string            `json:"warning,omitempty"`
}

type Transfer struct {
	bun.BaseModel `bun:"_system.transfers,alias:transfers"`

	ID             uuid.UUID `bun:"id,type:uuid,pk"`
	Reference      string    `bun:"reference,type:varchar(255),notnull"`
	IdempotencyKey string    `bun:"idempotency_key,type:varchar(255),notnull"`
	RequestHash    string    `bun:"request_hash,type:varchar(64),notnull"`
	UserLedger     string    `bun:"user_ledger,type:varchar(63),notnull"`
	WalletID       string    `bun:"wallet_id,type:varchar(255),notnull"`
	Currency       string    `bun:"currency,type:varchar(16),notnull"`
	Kind           Kind      `bun:"kind,type:varchar(32),notnull"`
	State          State     `bun:"state,type:varchar(32),notnull"`

	Amount        int64  `bun:"amount,type:bigint,notnull"`
	ChannelID     string `bun:"channel_id,type:varchar(255),notnull"`
	ChannelAmount int64  `bun:"channel_amount,type:bigint,notnull"`
	UserFee       int64  `bun:"user_fee,type:bigint,notnull"`
	ProcessingFee int64  `bun:"processing_fee,type:bigint,notnull"`
	NetRevenue    int64  `bun:"net_revenue,type:bigint,notnull"`

	UserMetadata map[string]string `bun:"user_metadata,type:jsonb,notnull"`

	UserTxID    *int64 `bun:"user_tx_id,type:bigint,nullzero"`
	ChannelTxID *int64 `bun:"channel_tx_id,type:bigint,nullzero"`
	RevenueTxID *int64 `bun:"revenue_tx_id,type:bigint,nullzero"`
	Result      Result `bun:"result,type:jsonb,notnull"`

	Attempts   int        `bun:"attempts,type:integer,notnull"`
	LastError  string     `bun:"last_error,type:text,notnull"`
	LeaseOwner *string    `bun:"lease_owner,type:varchar(255),nullzero"`
	LeaseUntil *time.Time `bun:"lease_until,type:timestamp without time zone,nullzero"`

	CreatedAt   time.Time  `bun:"created_at,type:timestamp without time zone,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,type:timestamp without time zone,notnull"`
	CompletedAt *time.Time `bun:"completed_at,type:timestamp without time zone,nullzero"`
}

// Copy returns a deep enough copy for the in-memory store and for tests.
func (t *Transfer) Copy() *Transfer {
	c := *t
	c.UserMetadata = make(map[string]string, len(t.UserMetadata))
	for k, v := range t.UserMetadata {
		c.UserMetadata[k] = v
	}
	c.Result.Metadata = make(map[string]string, len(t.Result.Metadata))
	for k, v := range t.Result.Metadata {
		c.Result.Metadata[k] = v
	}
	c.UserTxID = copyInt64(t.UserTxID)
	c.ChannelTxID = copyInt64(t.ChannelTxID)
	c.RevenueTxID = copyInt64(t.RevenueTxID)
	if t.LeaseOwner != nil {
		v := *t.LeaseOwner
		c.LeaseOwner = &v
	}
	if t.LeaseUntil != nil {
		v := *t.LeaseUntil
		c.LeaseUntil = &v
	}
	if t.CompletedAt != nil {
		v := *t.CompletedAt
		c.CompletedAt = &v
	}
	return &c
}

func copyInt64(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// NeedsChannelLeg is true for every transfer routed through this package.
func (t *Transfer) NeedsChannelLeg() bool { return t.ChannelID != "" }

// NeedsRevenueLeg is true when fees must be booked on the revenue ledger. The
// fee record (the last step) is written under the same condition.
func (t *Transfer) NeedsRevenueLeg() bool {
	return t.ChannelID != "" && (t.UserFee > 0 || t.ProcessingFee > 0)
}

// Input is the already validated request, with fees resolved.
type Input struct {
	Reference      string
	IdempotencyKey string
	UserLedger     string
	WalletID       string
	Currency       string
	Kind           Kind
	// RequestedAmount is the amount as sent by the client (0 when the client
	// asked for it to be computed). It is only used to recognise a retry.
	RequestedAmount int64
	Amount          int64
	ChannelID       string
	ChannelAmount   int64
	UserFee         int64
	ProcessingFee   int64
	NetRevenue      int64
	// UserMetadata is attached to the user-ledger transaction.
	UserMetadata map[string]string
}

// fingerprint identifies "the same request" for retry detection. Computed fees
// are excluded on purpose: a retry must find its transfer even if the fee
// configuration changed in between.
func (in Input) fingerprint() string {
	h := sha256.Sum256([]byte(strings.Join([]string{
		string(in.Kind), in.UserLedger, in.WalletID, in.Currency,
		fmt.Sprint(in.RequestedAmount), in.ChannelID, fmt.Sprint(in.ChannelAmount),
	}, "|")))
	return hex.EncodeToString(h[:])
}

// NewTransfer builds the row for a request (state created, not yet stored).
func NewTransfer(in Input) *Transfer {
	md := map[string]string{}
	for k, v := range in.UserMetadata {
		md[k] = v
	}
	return &Transfer{
		ID:             uuid.New(),
		Reference:      in.Reference,
		IdempotencyKey: in.IdempotencyKey,
		RequestHash:    in.fingerprint(),
		UserLedger:     in.UserLedger,
		WalletID:       in.WalletID,
		Currency:       in.Currency,
		Kind:           in.Kind,
		State:          StateCreated,
		Amount:         in.Amount,
		ChannelID:      in.ChannelID,
		ChannelAmount:  in.ChannelAmount,
		UserFee:        in.UserFee,
		ProcessingFee:  in.ProcessingFee,
		NetRevenue:     in.NetRevenue,
		UserMetadata:   md,
	}
}

func channelLedgerName(currency string) string { return fmt.Sprintf("channels-%s", currency) }
func revenueLedgerName(currency string) string { return fmt.Sprintf("revenue-%s", currency) }

// userIDFromWallet mirrors the wallet handlers: walletID is {userID}-{currency}.
func userIDFromWallet(walletID string) string {
	if i := strings.LastIndex(walletID, "-"); i >= 0 {
		return walletID[:i]
	}
	return walletID
}

// Response renders the body of the original 201 response. Keys and shapes are
// identical to the pre-existing wallet handlers (channel_tx_id now carries the
// real transaction id; it used to print a pointer address).
func (t *Transfer) Response(extraWarning string) map[string]any {
	md := map[string]string{}
	if t.NeedsChannelLeg() {
		md["channel_ledger"] = channelLedgerName(t.Currency)
		if t.ChannelTxID != nil {
			md["channel_tx_id"] = fmt.Sprintf("%d", *t.ChannelTxID)
		}
		md["channel_user_fee_amount"] = fmt.Sprintf("%d", t.UserFee)
		md["channel_processing_fee_amount"] = fmt.Sprintf("%d", t.ProcessingFee)
		md["channel_net_revenue_amount"] = fmt.Sprintf("%d", t.NetRevenue)
		if t.NeedsRevenueLeg() {
			md["revenue_ledger"] = revenueLedgerName(t.Currency)
			if t.RevenueTxID != nil {
				md["revenue_tx_id"] = fmt.Sprintf("%d", *t.RevenueTxID)
			}
		}
	}
	for k, v := range t.Result.Metadata {
		md[k] = v
	}

	var txid any
	if t.UserTxID != nil {
		txid = uint64(*t.UserTxID)
	}
	resp := map[string]any{
		"txid":           txid,
		"timestamp":      t.Result.Timestamp,
		"postings":       t.Result.Postings,
		"metadata":       md,
		"balance_before": t.Result.BalanceBefore,
		"balance_after":  t.Result.BalanceAfter,
	}
	warning := t.Result.Warning
	if extraWarning != "" {
		warning = extraWarning
	}
	if warning != "" {
		resp["warning"] = warning
	}
	return resp
}
