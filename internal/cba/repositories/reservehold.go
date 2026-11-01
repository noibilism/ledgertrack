package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

type ReserveHoldFilter struct {
	AccountID *uuid.UUID
	ClientID  *uuid.UUID
	WalletID  *string
	Status    *string
	// DueBefore keeps held entries whose release time is at or before it.
	DueBefore *time.Time
	Limit     int
}

// ReserveHoldRepository is the register of held shares of credits.
type ReserveHoldRepository interface {
	// AnyActiveRule says whether at least one reserve rule is in force at `at`. It is a cheap check the
	// credit path uses to skip all reserve work when no rule exists.
	AnyActiveRule(ctx context.Context, at time.Time) (bool, error)

	// CreatePending records the hold before the credit is posted. When a row for the same
	// (ledger, wallet, credit reference) exists it is returned with created=false.
	CreatePending(ctx context.Context, h *models.ReserveHold) (existing *models.ReserveHold, created bool, err error)
	// Rearm turns a voided hold back into pending with the fields of h, for a retry of the same reference.
	Rearm(ctx context.Context, h *models.ReserveHold) (bool, error)
	Get(ctx context.Context, id uuid.UUID) (*models.ReserveHold, error)
	// GetByCredit returns the hold of a credit, or a not-found error.
	GetByCredit(ctx context.Context, ledger, walletID, reference string) (*models.ReserveHold, error)
	List(ctx context.Context, f ReserveHoldFilter) ([]models.ReserveHold, error)

	// MarkHeld moves a pending hold to held once the credit is in the ledger. It is a no-op on a hold
	// that is already held.
	MarkHeld(ctx context.Context, id uuid.UUID, creditTxID int64) error
	// Void ends a pending hold whose credit never reached the ledger.
	Void(ctx context.Context, id uuid.UUID, owner string) error

	// ClaimDue leases up to limit held entries whose release time has come. Rows locked by another
	// claimer are skipped.
	ClaimDue(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error)
	// ClaimStalePending leases pending entries created before olderThan.
	ClaimStalePending(ctx context.Context, owner string, olderThan, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error)
	// ClaimByID leases one held entry for a manual release. ok=false means it is not held or is leased.
	ClaimByID(ctx context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) (*models.ReserveHold, bool, error)

	MarkReleased(ctx context.Context, id uuid.UUID, owner, reason, reference string, at time.Time) error
	// Fail gives the lease back and records why. When needsReview is set the hold leaves the queue.
	Fail(ctx context.Context, id uuid.UUID, owner, lastError string, needsReview bool) error
}

type BunReserveHoldRepository struct {
	db bun.IDB
}

func NewReserveHoldRepository(db bun.IDB) *BunReserveHoldRepository {
	return &BunReserveHoldRepository{db: db}
}

func (r *BunReserveHoldRepository) AnyActiveRule(ctx context.Context, at time.Time) (bool, error) {
	exists, err := r.db.NewSelect().Model((*models.ReserveRule)(nil)).
		Where("status = ?", models.ReserveStatusActive).
		Where("(effective_to is null or effective_to > ?)", at).
		Exists(ctx)
	return exists, postgres.ResolveError(err)
}

func (r *BunReserveHoldRepository) CreatePending(ctx context.Context, h *models.ReserveHold) (*models.ReserveHold, bool, error) {
	setUUID(&h.ID)
	h.Status = models.HoldStatusPending
	res, err := r.db.NewInsert().Model(h).
		On("CONFLICT (ledger, wallet_id, credit_reference) DO NOTHING").
		Returning("*").Exec(ctx)
	if err != nil {
		return nil, false, postgres.ResolveError(err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return nil, true, nil
	}
	existing := &models.ReserveHold{}
	err = r.db.NewSelect().Model(existing).
		Where("ledger = ?", h.Ledger).Where("wallet_id = ?", h.WalletID).Where("credit_reference = ?", h.CreditReference).
		Scan(ctx)
	return existing, false, postgres.ResolveError(err)
}

func (r *BunReserveHoldRepository) Rearm(ctx context.Context, h *models.ReserveHold) (bool, error) {
	res, err := r.db.NewUpdate().Model((*models.ReserveHold)(nil)).
		Set("status = ?", models.HoldStatusPending).
		Set("rule_id = ?", h.RuleID).
		Set("credit_type = ?", h.CreditType).
		Set("credit_amount_minor = ?", h.CreditAmountMinor).
		Set("amount_minor = ?", h.AmountMinor).
		Set("release_at = ?", h.ReleaseAt).
		Set("credit_tx_id = null").
		Set("lease_owner = ''").Set("lease_until = null").
		Set("attempts = 0").Set("last_error = ''").
		Set("created_at = ?", time.Now().UTC()).Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", h.ID).Where("status = ?", models.HoldStatusVoided).
		Exec(ctx)
	if err != nil {
		return false, postgres.ResolveError(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (r *BunReserveHoldRepository) Get(ctx context.Context, id uuid.UUID) (*models.ReserveHold, error) {
	h := &models.ReserveHold{}
	err := r.db.NewSelect().Model(h).Where("id = ?", id).Scan(ctx)
	return h, postgres.ResolveError(err)
}

func (r *BunReserveHoldRepository) GetByCredit(ctx context.Context, ledger, walletID, reference string) (*models.ReserveHold, error) {
	h := &models.ReserveHold{}
	err := r.db.NewSelect().Model(h).
		Where("ledger = ?", ledger).Where("wallet_id = ?", walletID).Where("credit_reference = ?", reference).
		Scan(ctx)
	return h, postgres.ResolveError(err)
}

func (r *BunReserveHoldRepository) List(ctx context.Context, f ReserveHoldFilter) ([]models.ReserveHold, error) {
	out := make([]models.ReserveHold, 0)
	q := r.db.NewSelect().Model(&out).OrderExpr("release_at asc, created_at asc")
	if f.AccountID != nil {
		q = q.Where("account_id = ?", *f.AccountID)
	}
	if f.ClientID != nil {
		q = q.Where("client_id = ?", *f.ClientID)
	}
	if f.WalletID != nil {
		q = q.Where("wallet_id = ?", *f.WalletID)
	}
	if f.Status != nil {
		q = q.Where("status = ?", *f.Status)
	}
	if f.DueBefore != nil {
		q = q.Where("release_at <= ?", *f.DueBefore)
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return out, postgres.ResolveError(q.Limit(limit).Scan(ctx))
}

func (r *BunReserveHoldRepository) MarkHeld(ctx context.Context, id uuid.UUID, creditTxID int64) error {
	res, err := r.db.NewUpdate().Model((*models.ReserveHold)(nil)).
		Set("status = ?", models.HoldStatusHeld).
		Set("credit_tx_id = ?", creditTxID).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).Where("status = ?", models.HoldStatusPending).
		Exec(ctx)
	if err != nil {
		return postgres.ResolveError(err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return nil
	}
	// not pending: fine if it is already held (a retry), an error otherwise
	h, err := r.Get(ctx, id)
	if err != nil {
		return err
	}
	if h.Status == models.HoldStatusHeld || h.Status == models.HoldStatusReleased {
		return nil
	}
	return postgres.ErrNotFound
}

func (r *BunReserveHoldRepository) Void(ctx context.Context, id uuid.UUID, owner string) error {
	_, err := r.db.NewUpdate().Model((*models.ReserveHold)(nil)).
		Set("status = ?", models.HoldStatusVoided).
		Set("lease_owner = ''").Set("lease_until = null").
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).Where("status = ?", models.HoldStatusPending).Where("lease_owner = ?", owner).
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunReserveHoldRepository) claim(ctx context.Context, inner string, owner string, now time.Time, lease time.Duration, args ...any) ([]models.ReserveHold, error) {
	out := make([]models.ReserveHold, 0)
	all := append([]any{owner, now.Add(lease), now}, args...)
	err := r.db.NewRaw(sprintfClaim(inner), all...).Scan(ctx, &out)
	return out, postgres.ResolveError(err)
}

func sprintfClaim(inner string) string {
	// kept as a function so the query text stays in one place
	return "update _system.reserve_holds set lease_owner = ?, lease_until = ?, attempts = attempts + 1, updated_at = ? where id in (" + inner + ") returning *"
}

func (r *BunReserveHoldRepository) ClaimDue(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error) {
	return r.claim(ctx,
		`select id from _system.reserve_holds
		 where status = 'held' and release_at <= ? and (lease_until is null or lease_until < ?)
		 order by release_at limit ? for update skip locked`,
		owner, now, lease, now, now, limit)
}

func (r *BunReserveHoldRepository) ClaimStalePending(ctx context.Context, owner string, olderThan, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error) {
	return r.claim(ctx,
		`select id from _system.reserve_holds
		 where status = 'pending' and created_at < ? and (lease_until is null or lease_until < ?)
		 order by created_at limit ? for update skip locked`,
		owner, now, lease, olderThan, now, limit)
}

func (r *BunReserveHoldRepository) ClaimByID(ctx context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) (*models.ReserveHold, bool, error) {
	rows, err := r.claim(ctx,
		`select id from _system.reserve_holds
		 where id = ? and status = 'held' and (lease_until is null or lease_until < ?) for update skip locked`,
		owner, now, lease, id, now)
	if err != nil || len(rows) == 0 {
		return nil, false, err
	}
	return &rows[0], true, nil
}

func (r *BunReserveHoldRepository) MarkReleased(ctx context.Context, id uuid.UUID, owner, reason, reference string, at time.Time) error {
	_, err := r.db.NewUpdate().Model((*models.ReserveHold)(nil)).
		Set("status = ?", models.HoldStatusReleased).
		Set("released_at = ?", at).
		Set("release_reason = ?", reason).
		Set("release_reference = ?", reference).
		Set("lease_owner = ''").Set("lease_until = null").Set("last_error = ''").
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).Where("status = ?", models.HoldStatusHeld).Where("lease_owner = ?", owner).
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunReserveHoldRepository) Fail(ctx context.Context, id uuid.UUID, owner, lastError string, needsReview bool) error {
	q := r.db.NewUpdate().Model((*models.ReserveHold)(nil)).
		Set("lease_owner = ''").Set("lease_until = null").
		Set("last_error = ?", lastError).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).Where("lease_owner = ?", owner)
	if needsReview {
		q = q.Set("status = ?", models.HoldStatusNeedsReview)
	}
	_, err := q.Exec(ctx)
	return postgres.ResolveError(err)
}
