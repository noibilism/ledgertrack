package transfers

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrLeaseLost is returned when a driver tries to change a transfer it no
	// longer owns (its lease expired and someone else took over, or the state
	// already moved). The caller must stop driving and re-read the row.
	ErrLeaseLost = errors.New("transfer lease lost")
	// ErrNotFound is returned when a transfer does not exist.
	ErrNotFound = errors.New("transfer not found")
)

// ClaimOptions tunes Store.Claim.
type ClaimOptions struct {
	Lease time.Duration
	// CountAttempt increments the attempt counter. Client-driven re-drives do
	// not count, so a retry storm cannot exhaust the recovery budget.
	CountAttempt bool
	// MinIdle refuses the claim when the row was touched more recently than
	// this (cooldown for client-driven re-drives).
	MinIdle time.Duration
}

// RecoveryOptions tunes Store.ClaimBatch.
type RecoveryOptions struct {
	// Grace is how long a non-terminal transfer must be untouched before
	// recovery picks it up. It doubles with every attempt up to MaxBackoff.
	Grace      time.Duration
	MaxBackoff time.Duration
	Lease      time.Duration
	Limit      int
}

// Store persists transfers. All time decisions (lease expiry, grace) are taken
// by the store using one clock (the database clock for the SQL implementation)
// so that skew between application pods cannot matter.
type Store interface {
	// Create inserts the transfer already leased to owner with attempts=1. If a
	// transfer with the same reference exists, nothing is written and the
	// existing row is returned with created=false.
	Create(ctx context.Context, t *Transfer, owner string, lease time.Duration) (existing *Transfer, created bool, err error)
	Get(ctx context.Context, id uuid.UUID) (*Transfer, error)
	GetByReference(ctx context.Context, reference string) (*Transfer, error)

	// Rearm turns a `failed` transfer back into `created` with the parameters
	// of t, leased to owner. ok=false means the row is no longer `failed`.
	Rearm(ctx context.Context, id uuid.UUID, t *Transfer, owner string, lease time.Duration) (rearmed *Transfer, ok bool, err error)

	// Claim takes the lease of a non-terminal transfer if nobody holds it.
	Claim(ctx context.Context, id uuid.UUID, owner string, opts ClaimOptions) (*Transfer, bool, error)
	// ClaimBatch leases up to Limit stale non-terminal transfers. Rows locked
	// by a concurrent claimer are skipped (FOR UPDATE SKIP LOCKED).
	ClaimBatch(ctx context.Context, owner string, opts RecoveryOptions) ([]*Transfer, error)

	// Save persists t.State, leg ids, result and last_error, provided the row is
	// still in state `from` and leased to owner. Moving to StateCompleted also
	// clears the lease and sets completed_at.
	Save(ctx context.Context, t *Transfer, from State, owner string) error
	// Release gives the lease back, recording why the drive stopped.
	Release(ctx context.Context, id uuid.UUID, owner string, lastError string) error
	// Finish moves the transfer to a terminal failure state (failed or
	// failed_needs_review) and clears the lease.
	Finish(ctx context.Context, id uuid.UUID, owner string, state State, lastError string) error
}
