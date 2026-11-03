package transfers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	ledgerinternal "github.com/formancehq/ledger/internal"
	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	"github.com/formancehq/ledger/internal/hotrows"
)

// UserLeg is the outcome of the first leg.
type UserLeg struct {
	TxID     int64
	Result   Result
	Replayed bool
}

type ChannelLeg struct {
	TxID     int64
	Warning  string
	Replayed bool
}

type RevenueLeg struct {
	// TxID is the last revenue transaction posted (processing fee when both
	// fees exist), matching the pre-existing response.
	TxID     *int64
	Replayed bool
}

// LegExecutor performs the individual legs. Every method MUST be idempotent on
// the transfer reference: calling it again after it succeeded (or crashed
// half way) must not move money twice. It is an interface so failure-injection
// tests can fail or "crash" after any leg.
type LegExecutor interface {
	PostUser(ctx context.Context, t *Transfer) (UserLeg, error)
	PostChannel(ctx context.Context, t *Transfer) (ChannelLeg, error)
	PostRevenue(ctx context.Context, t *Transfer) (RevenueLeg, error)
	// RecordFee writes the channel fee record exactly once per reference.
	RecordFee(ctx context.Context, t *Transfer) error
}

// FeeRecorder stores channel fee records. Implementations must be idempotent
// on Reference.
type FeeRecorder interface {
	Record(ctx context.Context, rec *channelmodels.ChannelFeeRecord) error
}

// LedgerExecutor is the production LegExecutor.
type LedgerExecutor struct {
	ledgers Ledgers
	fees    FeeRecorder
}

var _ LegExecutor = (*LedgerExecutor)(nil)

// NewLedgerExecutor builds the production executor. fees may be nil, in which
// case fee records are skipped (as when no channel fee service is configured).
func NewLedgerExecutor(ledgers Ledgers, fees FeeRecorder) *LedgerExecutor {
	return &LedgerExecutor{ledgers: ledgers, fees: fees}
}

func samePosting(m Movement, tx ledgerinternal.Transaction) bool {
	if len(tx.Postings) != 1 {
		return false
	}
	p := tx.Postings[0]
	// A replayed leg may sit on another shard than the one the current shard counts pick (the
	// counts can change between the first attempt and the recovery run), so hot-row accounts are
	// compared as logical accounts. Everything else must match exactly.
	return hotrows.EquivalentAccount(p.Source, m.Source) && hotrows.EquivalentAccount(p.Destination, m.Destination) &&
		p.Asset == m.Asset &&
		p.Amount != nil && p.Amount.Cmp(big.NewInt(m.Amount)) == 0
}

// post posts a movement and verifies that a pre-existing transaction with the
// same reference is really the one we would have created.
func (e *LedgerExecutor) post(ctx context.Context, m Movement) (*Posted, error) {
	posted, err := e.ledgers.Post(ctx, m)
	if err != nil {
		return nil, err
	}
	if posted.Replayed && !samePosting(m, posted.Tx) {
		return nil, fmt.Errorf("%w: reference %q on ledger %s", ErrReferenceMismatch, m.Reference, m.Ledger)
	}
	return posted, nil
}

func txID(tx ledgerinternal.Transaction) (int64, error) {
	if tx.ID == nil {
		return 0, fmt.Errorf("ledger returned a transaction without id")
	}
	return int64(*tx.ID), nil
}

func (e *LedgerExecutor) PostUser(ctx context.Context, t *Transfer) (UserLeg, error) {
	userID := userIDFromWallet(t.WalletID)
	available := fmt.Sprintf("users:%s:wallets:%s:available", userID, t.Currency)
	asset := AssetName(t.Currency)

	m := Movement{
		Ledger:         t.UserLedger,
		Reference:      t.Reference,
		IdempotencyKey: t.IdempotencyKey,
		Asset:          asset,
		Amount:         t.Amount,
		Metadata:       t.UserMetadata,
	}
	switch t.Kind {
	case KindDebit:
		m.Source = available
		m.Destination = hotrows.SystemControlAddress(t.WalletID, t.Currency)
	case KindReleasePay:
		m.Source = fmt.Sprintf("users:%s:wallets:%s:lien", userID, t.Currency)
		m.Destination = "world"
	default:
		return UserLeg{}, fmt.Errorf("unknown transfer kind %q", t.Kind)
	}

	posted, err := e.post(ctx, m)
	if err != nil {
		return UserLeg{}, err
	}
	tx := posted.Tx
	id, err := txID(tx)
	if err != nil {
		return UserLeg{}, err
	}

	var before, after int64
	pre := tx.PostCommitVolumes.SubtractPostings(tx.Postings)
	if v, ok := pre[available][asset]; ok && v.Input != nil && v.Output != nil {
		before = new(big.Int).Sub(v.Input, v.Output).Int64()
	}
	if v, ok := tx.PostCommitVolumes[available][asset]; ok && v.Input != nil && v.Output != nil {
		after = new(big.Int).Sub(v.Input, v.Output).Int64()
	}

	ts, err := json.Marshal(tx.Timestamp)
	if err != nil {
		return UserLeg{}, err
	}
	postings, err := json.Marshal(tx.Postings)
	if err != nil {
		return UserLeg{}, err
	}
	md := make(map[string]string, len(tx.Metadata))
	for k, v := range tx.Metadata {
		md[k] = v
	}
	return UserLeg{
		TxID:     id,
		Replayed: posted.Replayed,
		Result: Result{
			Timestamp:     ts,
			Postings:      postings,
			Metadata:      md,
			BalanceBefore: before,
			BalanceAfter:  after,
		},
	}, nil
}

func (e *LedgerExecutor) PostChannel(ctx context.Context, t *Transfer) (ChannelLeg, error) {
	channelAccount := fmt.Sprintf("channel:%s", t.ChannelID)
	asset := AssetName(t.Currency)
	posted, err := e.post(ctx, Movement{
		Ledger:             channelLedgerName(t.Currency),
		Reference:          t.Reference,
		Source:             channelAccount,
		Destination:        hotrows.ChannelSink(t.Reference),
		Asset:              asset,
		Amount:             t.ChannelAmount,
		UnboundedOverdraft: true,
	})
	if err != nil {
		return ChannelLeg{}, err
	}
	id, err := txID(posted.Tx)
	if err != nil {
		return ChannelLeg{}, err
	}
	leg := ChannelLeg{TxID: id, Replayed: posted.Replayed}
	if vol, ok := posted.Tx.PostCommitVolumes[channelAccount][asset]; ok && vol.Input != nil && vol.Output != nil {
		bal := vol.Balance()
		leg.Warning = fmt.Sprintf("Channel balance: %s %s", bal.String(), t.Currency)
		if bal.Sign() < 0 {
			leg.Warning = fmt.Sprintf("Channel balance is negative: %s %s", bal.String(), t.Currency)
		}
	}
	return leg, nil
}

func (e *LedgerExecutor) PostRevenue(ctx context.Context, t *Transfer) (RevenueLeg, error) {
	// The shard is a function of the business reference (t.Reference, never the per leg reference)
	// and the shard count, which is read once so both entries agree even if it changes meanwhile.
	shards := hotrows.RevenueShards()
	source := hotrows.RevenueSourceN(t.Reference, shards)
	entries := []struct {
		suffix      string
		destination string
		amount      int64
	}{
		{"user-fee", hotrows.RevenueDestinationN(hotrows.AccumulatedBase, t.Reference, shards), t.UserFee},
		{"processing-fee", hotrows.RevenueDestinationN(hotrows.ProcessingCostBase, t.Reference, shards), t.ProcessingFee},
	}
	asset := AssetName(t.Currency)

	var leg RevenueLeg
	allReplayed := true
	for _, entry := range entries {
		if entry.amount <= 0 {
			continue
		}
		posted, err := e.post(ctx, Movement{
			Ledger:      revenueLedgerName(t.Currency),
			Reference:   fmt.Sprintf("%s-%s", t.Reference, entry.suffix),
			Source:      source,
			Destination: entry.destination,
			Asset:       asset,
			Amount:      entry.amount,
			// world:{s} is an ordinary account, only the real world may go negative by itself.
			UnboundedOverdraft: hotrows.IsUnboundedSource(source),
		})
		if err != nil {
			return RevenueLeg{}, err
		}
		allReplayed = allReplayed && posted.Replayed
		id, err := txID(posted.Tx)
		if err != nil {
			return RevenueLeg{}, err
		}
		leg.TxID = &id
	}
	leg.Replayed = allReplayed
	return leg, nil
}

func (e *LedgerExecutor) RecordFee(ctx context.Context, t *Transfer) error {
	if e.fees == nil {
		return nil
	}
	md := map[string]any{"wallet_id": t.WalletID}
	if t.Kind == KindReleasePay {
		md["mode"] = "PAY"
	}
	wallet := t.WalletID
	return e.fees.Record(ctx, &channelmodels.ChannelFeeRecord{
		ChannelID:           t.ChannelID,
		Currency:            t.Currency,
		WalletID:            &wallet,
		Reference:           t.Reference,
		LedgerTxID:          t.UserTxID,
		ChannelTxID:         t.ChannelTxID,
		RevenueTxID:         t.RevenueTxID,
		OccurredAt:          t.CreatedAt.UTC(),
		TotalAmount:         t.Amount,
		PrincipalAmount:     t.ChannelAmount,
		UserFeeAmount:       t.UserFee,
		ProcessingFeeAmount: t.ProcessingFee,
		NetRevenueAmount:    t.NetRevenue,
		Metadata:            md,
	})
}
