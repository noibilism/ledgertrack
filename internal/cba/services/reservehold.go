package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

// ReserveLedger is what the hold service needs from the ledger.
type ReserveLedger interface {
	// Release moves the held share from the wallet lien to its available balance. It is idempotent on
	// the reference: posting it twice moves the money once.
	Release(ctx context.Context, hold models.ReserveHold, reference string) error
	// FindCredit returns the id of the ledger transaction with the given reference, if there is one.
	FindCredit(ctx context.Context, ledger, reference string) (txID int64, found bool, err error)
}

type PlanCreditRequest struct {
	Ledger      string
	WalletID    string
	Currency    string
	Reference   string
	AmountMinor int64
	CreditType  string
	At          time.Time
}

// CreditPlan tells the credit path how to split a credit. The hold is already recorded as pending.
type CreditPlan struct {
	Hold           models.ReserveHold
	ReserveMinor   int64
	AvailableMinor int64
}

type ReleaseOptions struct {
	Owner string
	Batch int
	Lease time.Duration
	// PendingGrace is how old a pending hold must be before recovery looks for its credit in the ledger.
	PendingGrace time.Duration
	// MaxAttempts is the number of failed releases before a hold goes to needs_review.
	MaxAttempts int
}

type ReleaseStats struct {
	Claimed   int
	Released  int
	Failed    int
	Escalated int
	Recovered int
	Voided    int
}

// ReserveHoldService splits credits into the wallet lien under reserve rules and releases the held
// share to the available balance once its SLA has passed.
type ReserveHoldService interface {
	// PlanCredit decides whether a reserve applies to a credit and records the hold. It returns nil
	// when nothing is held, in which case the credit is posted as usual.
	PlanCredit(context.Context, PlanCreditRequest) (*CreditPlan, error)
	// ConfirmCredit marks the hold as held once the credit is in the ledger.
	ConfirmCredit(ctx context.Context, holdID uuid.UUID, creditTxID int64) error
	GetHold(context.Context, uuid.UUID) (*models.ReserveHold, error)
	ListHolds(context.Context, repositories.ReserveHoldFilter) ([]models.ReserveHold, error)
	// ReleaseHold releases one hold now, ahead of its SLA (Backoffice).
	ReleaseHold(context.Context, uuid.UUID, string) (*models.ReserveHold, error)
	// RunRelease releases every due hold and settles pending holds whose credit call was interrupted.
	RunRelease(context.Context, ReleaseOptions) (ReleaseStats, error)
}

// rulesCacheTTL is how long "no reserve rule exists" is trusted. A new rule starts applying to credits
// within this time.
const rulesCacheTTL = 5 * time.Second

type DefaultReserveHoldService struct {
	holds    repositories.ReserveHoldRepository
	accounts repositories.AccountRepository
	rules    ReserveRuleService
	ledger   ReserveLedger
	now      func() time.Time

	mu            sync.Mutex
	rulesChecked  time.Time
	rulesPresence bool
}

func NewReserveHoldService(
	holds repositories.ReserveHoldRepository,
	accounts repositories.AccountRepository,
	rules ReserveRuleService,
	ledger ReserveLedger,
) *DefaultReserveHoldService {
	return &DefaultReserveHoldService{
		holds: holds, accounts: accounts, rules: rules, ledger: ledger,
		now: func() time.Time { return time.Now().UTC() },
	}
}

func (s *DefaultReserveHoldService) anyRule(ctx context.Context) (bool, error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.rulesChecked.IsZero() && now.Sub(s.rulesChecked) < rulesCacheTTL {
		return s.rulesPresence, nil
	}
	present, err := s.holds.AnyActiveRule(ctx, now)
	if err != nil {
		return false, err
	}
	s.rulesChecked, s.rulesPresence = now, present
	return present, nil
}

func (s *DefaultReserveHoldService) PlanCredit(ctx context.Context, req PlanCreditRequest) (*CreditPlan, error) {
	if req.AmountMinor <= 0 || req.Reference == "" {
		return nil, nil
	}
	present, err := s.anyRule(ctx)
	if err != nil || !present {
		return nil, err
	}
	account, err := s.accounts.GetByWalletID(ctx, req.WalletID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) || postgres.IsNotFoundError(err) {
			return nil, nil // a wallet outside the CBA accounts has no reserve
		}
		return nil, err
	}
	if account.Currency != req.Currency {
		return nil, nil
	}
	at := req.At.UTC()
	if req.At.IsZero() {
		at = s.now()
	}

	// A retry of the same reference keeps the split it was first given, even if the rule changed since.
	existing, err := s.holds.GetByCredit(ctx, req.Ledger, req.WalletID, req.Reference)
	switch {
	case err == nil && existing.Status != models.HoldStatusVoided:
		return planOf(existing), nil
	case err != nil && !(errors.Is(err, postgres.ErrNotFound) || postgres.IsNotFoundError(err)):
		return nil, err
	}

	preview, err := s.rules.Preview(ctx, ReservePreviewRequest{
		AccountID: account.ID, AmountMinor: req.AmountMinor, CreditType: req.CreditType, At: at,
	})
	if err != nil {
		return nil, err
	}
	if preview.ReserveMinor <= 0 || preview.ReleaseAt == nil {
		return nil, nil
	}

	hold := &models.ReserveHold{
		AccountID: account.ID, ClientID: account.ClientID, Ledger: req.Ledger, WalletID: req.WalletID,
		Currency: account.Currency, CreditReference: req.Reference, CreditType: preview.CreditType,
		CreditAmountMinor: req.AmountMinor, AmountMinor: preview.ReserveMinor,
		RuleID: preview.RuleID, ReleaseAt: *preview.ReleaseAt,
	}

	if existing != nil { // voided: the earlier attempt never reached the ledger
		hold.ID = existing.ID
		ok, err := s.holds.Rearm(ctx, hold)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: hold %s changed while retrying", ErrReserveConflict, existing.ID)
		}
		hold.Status = models.HoldStatusPending
		return planOf(hold), nil
	}

	raced, created, err := s.holds.CreatePending(ctx, hold)
	if err != nil {
		return nil, err
	}
	if !created { // a concurrent attempt with the same reference got there first
		return planOf(raced), nil
	}
	return planOf(hold), nil
}

func planOf(h *models.ReserveHold) *CreditPlan {
	return &CreditPlan{Hold: *h, ReserveMinor: h.AmountMinor, AvailableMinor: h.CreditAmountMinor - h.AmountMinor}
}

func (s *DefaultReserveHoldService) ConfirmCredit(ctx context.Context, holdID uuid.UUID, creditTxID int64) error {
	return s.holds.MarkHeld(ctx, holdID, creditTxID)
}

func (s *DefaultReserveHoldService) GetHold(ctx context.Context, id uuid.UUID) (*models.ReserveHold, error) {
	h, err := s.holds.Get(ctx, id)
	if err != nil {
		return nil, reserveNotFound(err, "reserve hold")
	}
	return h, nil
}

func (s *DefaultReserveHoldService) ListHolds(ctx context.Context, f repositories.ReserveHoldFilter) ([]models.ReserveHold, error) {
	return s.holds.List(ctx, f)
}

func releaseReference(hold models.ReserveHold) string {
	return "reserve-release:" + hold.ID.String()
}

// release posts the release to the ledger and records it. The caller holds the lease.
func (s *DefaultReserveHoldService) release(ctx context.Context, hold models.ReserveHold, owner, reason string) error {
	reference := releaseReference(hold)
	if err := s.ledger.Release(ctx, hold, reference); err != nil {
		return err
	}
	return s.holds.MarkReleased(ctx, hold.ID, owner, reason, reference, s.now())
}

func (s *DefaultReserveHoldService) ReleaseHold(ctx context.Context, id uuid.UUID, owner string) (*models.ReserveHold, error) {
	if owner == "" {
		owner = "manual:" + uuid.NewString()
	}
	hold, ok, err := s.holds.ClaimByID(ctx, id, owner, s.now(), 2*time.Minute)
	if err != nil {
		return nil, err
	}
	if !ok {
		current, err := s.GetHold(ctx, id)
		if err != nil {
			return nil, err
		}
		if current.Status == models.HoldStatusReleased {
			return current, nil // already released: same answer
		}
		return nil, fmt.Errorf("%w: hold %s is %s and cannot be released now", ErrReserveConflict, id, current.Status)
	}
	if err := s.release(ctx, *hold, owner, models.HoldReleaseManual); err != nil {
		_ = s.holds.Fail(ctx, hold.ID, owner, err.Error(), false)
		return nil, err
	}
	return s.GetHold(ctx, id)
}

func (s *DefaultReserveHoldService) RunRelease(ctx context.Context, opts ReleaseOptions) (ReleaseStats, error) {
	var stats ReleaseStats
	now := s.now()

	// 1. holds whose SLA has passed
	due, err := s.holds.ClaimDue(ctx, opts.Owner, now, opts.Lease, opts.Batch)
	if err != nil {
		return stats, err
	}
	stats.Claimed += len(due)
	for _, hold := range due {
		if err := s.release(ctx, hold, opts.Owner, models.HoldReleaseSLA); err != nil {
			needsReview := hold.Attempts >= opts.MaxAttempts
			_ = s.holds.Fail(ctx, hold.ID, opts.Owner, err.Error(), needsReview)
			if needsReview {
				stats.Escalated++
			} else {
				stats.Failed++
			}
			continue
		}
		stats.Released++
	}

	// 2. credits that were interrupted between recording the hold and confirming it
	stale, err := s.holds.ClaimStalePending(ctx, opts.Owner, now.Add(-opts.PendingGrace), now, opts.Lease, opts.Batch)
	if err != nil {
		return stats, err
	}
	stats.Claimed += len(stale)
	for _, hold := range stale {
		txID, found, err := s.ledger.FindCredit(ctx, hold.Ledger, hold.CreditReference)
		if err != nil {
			_ = s.holds.Fail(ctx, hold.ID, opts.Owner, err.Error(), false)
			stats.Failed++
			continue
		}
		if found {
			if err := s.holds.MarkHeld(ctx, hold.ID, txID); err != nil {
				_ = s.holds.Fail(ctx, hold.ID, opts.Owner, err.Error(), false)
				stats.Failed++
				continue
			}
			_ = s.holds.Fail(ctx, hold.ID, opts.Owner, "", false) // give the lease back
			stats.Recovered++
			continue
		}
		if err := s.holds.Void(ctx, hold.ID, opts.Owner); err != nil {
			stats.Failed++
			continue
		}
		stats.Voided++
	}
	return stats, nil
}
