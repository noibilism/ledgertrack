package transfers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"
)

// utcNow is the database clock in the naive-UTC convention used by _system.
const utcNow = "(now() at time zone 'utc')"

const nonTerminalStates = "('created', 'user_posted', 'channel_posted', 'revenue_posted')"

type BunStore struct {
	db bun.IDB
}

var _ Store = (*BunStore)(nil)

func NewBunStore(db bun.IDB) *BunStore {
	return &BunStore{db: db}
}

func secs(d time.Duration) float64 { return d.Seconds() }

func (s *BunStore) Create(ctx context.Context, t *Transfer, owner string, lease time.Duration) (*Transfer, bool, error) {
	md, err := json.Marshal(t.UserMetadata)
	if err != nil {
		return nil, false, err
	}
	out := &Transfer{}
	err = s.db.NewRaw(`
		insert into _system.transfers (
			id, reference, idempotency_key, request_hash, user_ledger, wallet_id, currency, kind, state,
			amount, channel_id, channel_amount, user_fee, processing_fee, net_revenue, user_metadata,
			attempts, lease_owner, lease_until
		) values (
			?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?::jsonb,
			1, ?, `+utcNow+` + make_interval(secs => ?::float8)
		)
		on conflict (reference) do nothing
		returning *`,
		t.ID, t.Reference, t.IdempotencyKey, t.RequestHash, t.UserLedger, t.WalletID, t.Currency, string(t.Kind), string(StateCreated),
		t.Amount, t.ChannelID, t.ChannelAmount, t.UserFee, t.ProcessingFee, t.NetRevenue, string(md),
		owner, secs(lease),
	).Scan(ctx, out)
	switch {
	case err == nil:
		return out, true, nil
	case errors.Is(err, sql.ErrNoRows):
		existing, err := s.GetByReference(ctx, t.Reference)
		if err != nil {
			return nil, false, err
		}
		return existing, false, nil
	default:
		return nil, false, postgres.ResolveError(err)
	}
}

func (s *BunStore) Get(ctx context.Context, id uuid.UUID) (*Transfer, error) {
	return s.one(ctx, "id = ?", id)
}

func (s *BunStore) GetByReference(ctx context.Context, reference string) (*Transfer, error) {
	return s.one(ctx, "reference = ?", reference)
}

func (s *BunStore) one(ctx context.Context, where string, arg any) (*Transfer, error) {
	out := &Transfer{}
	err := s.db.NewSelect().Model(out).Where(where, arg).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return out, postgres.ResolveError(err)
}

func (s *BunStore) Rearm(ctx context.Context, id uuid.UUID, t *Transfer, owner string, lease time.Duration) (*Transfer, bool, error) {
	md, err := json.Marshal(t.UserMetadata)
	if err != nil {
		return nil, false, err
	}
	out := &Transfer{}
	err = s.db.NewRaw(`
		update _system.transfers set
			state = 'created', idempotency_key = ?, request_hash = ?, user_ledger = ?, wallet_id = ?, currency = ?, kind = ?,
			amount = ?, channel_id = ?, channel_amount = ?, user_fee = ?, processing_fee = ?, net_revenue = ?, user_metadata = ?::jsonb,
			user_tx_id = null, channel_tx_id = null, revenue_tx_id = null, result = '{}'::jsonb,
			attempts = 1, last_error = '', completed_at = null,
			lease_owner = ?, lease_until = `+utcNow+` + make_interval(secs => ?::float8),
			created_at = `+utcNow+`, updated_at = `+utcNow+`
		where id = ? and state = 'failed'
		returning *`,
		t.IdempotencyKey, t.RequestHash, t.UserLedger, t.WalletID, t.Currency, string(t.Kind),
		t.Amount, t.ChannelID, t.ChannelAmount, t.UserFee, t.ProcessingFee, t.NetRevenue, string(md),
		owner, secs(lease), id,
	).Scan(ctx, out)
	switch {
	case err == nil:
		return out, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	default:
		return nil, false, postgres.ResolveError(err)
	}
}

func (s *BunStore) Claim(ctx context.Context, id uuid.UUID, owner string, opts ClaimOptions) (*Transfer, bool, error) {
	inc := 0
	if opts.CountAttempt {
		inc = 1
	}
	out := &Transfer{}
	err := s.db.NewRaw(`
		update _system.transfers set
			lease_owner = ?, lease_until = `+utcNow+` + make_interval(secs => ?::float8),
			attempts = attempts + ?, updated_at = `+utcNow+`
		where id = ?
		  and state in `+nonTerminalStates+`
		  and (lease_until is null or lease_until < `+utcNow+`)
		  and updated_at <= `+utcNow+` - make_interval(secs => ?::float8)
		returning *`,
		owner, secs(opts.Lease), inc, id, secs(opts.MinIdle),
	).Scan(ctx, out)
	switch {
	case err == nil:
		return out, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	default:
		return nil, false, postgres.ResolveError(err)
	}
}

func (s *BunStore) ClaimBatch(ctx context.Context, owner string, opts RecoveryOptions) ([]*Transfer, error) {
	out := make([]*Transfer, 0, opts.Limit)
	// The inner select locks candidate rows and skips rows locked by another
	// recovery worker; the outer update takes the lease in the same statement,
	// so a row can be returned to at most one claimer.
	err := s.db.NewRaw(`
		with picked as (
			select id from _system.transfers
			where state in `+nonTerminalStates+`
			  and (lease_until is null or lease_until < `+utcNow+`)
			  and updated_at < `+utcNow+` - make_interval(secs => least(?::float8 * power(2, greatest(attempts - 1, 0)), ?::float8))
			order by updated_at asc
			limit ?
			for update skip locked
		)
		update _system.transfers t set
			lease_owner = ?, lease_until = `+utcNow+` + make_interval(secs => ?::float8),
			attempts = t.attempts + 1, updated_at = `+utcNow+`
		from picked
		where t.id = picked.id
		returning t.*`,
		secs(opts.Grace), secs(opts.MaxBackoff), opts.Limit, owner, secs(opts.Lease),
	).Scan(ctx, &out)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, postgres.ResolveError(err)
	}
	return out, nil
}

func (s *BunStore) Save(ctx context.Context, t *Transfer, from State, owner string) error {
	result, err := json.Marshal(t.Result)
	if err != nil {
		return err
	}
	res, err := s.db.NewRaw(`
		update _system.transfers set
			state = ?, user_tx_id = ?, channel_tx_id = ?, revenue_tx_id = ?, result = ?::jsonb, last_error = ?,
			updated_at = `+utcNow+`,
			completed_at = case when ? = 'completed' then `+utcNow+` else completed_at end,
			lease_owner = case when ? = 'completed' then null else lease_owner end,
			lease_until = case when ? = 'completed' then null else lease_until end
		where id = ? and state = ? and lease_owner = ?`,
		string(t.State), t.UserTxID, t.ChannelTxID, t.RevenueTxID, string(result), t.LastError,
		string(t.State), string(t.State), string(t.State),
		t.ID, string(from), owner,
	).Exec(ctx)
	return expectOneRow(res, err)
}

func (s *BunStore) Release(ctx context.Context, id uuid.UUID, owner string, lastError string) error {
	res, err := s.db.NewRaw(`
		update _system.transfers set
			lease_owner = null, lease_until = null, last_error = ?, updated_at = `+utcNow+`
		where id = ? and lease_owner = ? and state in `+nonTerminalStates,
		lastError, id, owner,
	).Exec(ctx)
	return expectOneRow(res, err)
}

func (s *BunStore) Finish(ctx context.Context, id uuid.UUID, owner string, state State, lastError string) error {
	if state != StateFailed && state != StateNeedsReview {
		return fmt.Errorf("finish: %q is not a terminal failure state", state)
	}
	res, err := s.db.NewRaw(`
		update _system.transfers set
			state = ?, last_error = ?, lease_owner = null, lease_until = null, updated_at = `+utcNow+`
		where id = ? and lease_owner = ? and state in `+nonTerminalStates,
		string(state), lastError, id, owner,
	).Exec(ctx)
	return expectOneRow(res, err)
}

func expectOneRow(res sql.Result, err error) error {
	if err != nil {
		return postgres.ResolveError(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrLeaseLost
	}
	return nil
}
