package services

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

// reserveHoldRepositoryStub is an in-memory repositories.ReserveHoldRepository with the same state rules
// as the SQL one: leases, status guards and a unique key per credit.
type reserveHoldRepositoryStub struct {
	holds        map[uuid.UUID]*models.ReserveHold
	rulesExist   bool
	ruleChecks   int
	failMarkHeld error
}

func newReserveHoldRepositoryStub() *reserveHoldRepositoryStub {
	return &reserveHoldRepositoryStub{holds: map[uuid.UUID]*models.ReserveHold{}, rulesExist: true}
}

func (s *reserveHoldRepositoryStub) AnyActiveRule(context.Context, time.Time) (bool, error) {
	s.ruleChecks++
	return s.rulesExist, nil
}
func (s *reserveHoldRepositoryStub) find(ledger, wallet, ref string) *models.ReserveHold {
	for _, h := range s.holds {
		if h.Ledger == ledger && h.WalletID == wallet && h.CreditReference == ref {
			return h
		}
	}
	return nil
}
func (s *reserveHoldRepositoryStub) CreatePending(_ context.Context, h *models.ReserveHold) (*models.ReserveHold, bool, error) {
	if e := s.find(h.Ledger, h.WalletID, h.CreditReference); e != nil {
		c := *e
		return &c, false, nil
	}
	if h.ID == uuid.Nil {
		h.ID = uuid.New()
	}
	h.Status = models.HoldStatusPending
	h.CreatedAt = time.Now().UTC()
	c := *h
	s.holds[h.ID] = &c
	return nil, true, nil
}
func (s *reserveHoldRepositoryStub) Rearm(_ context.Context, h *models.ReserveHold) (bool, error) {
	cur, ok := s.holds[h.ID]
	if !ok || cur.Status != models.HoldStatusVoided {
		return false, nil
	}
	cur.Status, cur.RuleID, cur.CreditType = models.HoldStatusPending, h.RuleID, h.CreditType
	cur.CreditAmountMinor, cur.AmountMinor, cur.ReleaseAt = h.CreditAmountMinor, h.AmountMinor, h.ReleaseAt
	cur.CreditTxID, cur.LeaseOwner, cur.LeaseUntil, cur.Attempts = nil, "", nil, 0
	return true, nil
}
func (s *reserveHoldRepositoryStub) Get(_ context.Context, id uuid.UUID) (*models.ReserveHold, error) {
	if h, ok := s.holds[id]; ok {
		c := *h
		return &c, nil
	}
	return nil, postgres.ErrNotFound
}
func (s *reserveHoldRepositoryStub) GetByCredit(_ context.Context, ledger, wallet, ref string) (*models.ReserveHold, error) {
	if h := s.find(ledger, wallet, ref); h != nil {
		c := *h
		return &c, nil
	}
	return nil, postgres.ErrNotFound
}
func (s *reserveHoldRepositoryStub) List(_ context.Context, f repositories.ReserveHoldFilter) ([]models.ReserveHold, error) {
	out := []models.ReserveHold{}
	for _, h := range s.holds {
		if f.Status != nil && h.Status != *f.Status {
			continue
		}
		if f.AccountID != nil && h.AccountID != *f.AccountID {
			continue
		}
		if f.DueBefore != nil && h.ReleaseAt.After(*f.DueBefore) {
			continue
		}
		out = append(out, *h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReleaseAt.Before(out[j].ReleaseAt) })
	return out, nil
}
func (s *reserveHoldRepositoryStub) MarkHeld(_ context.Context, id uuid.UUID, txID int64) error {
	if s.failMarkHeld != nil {
		return s.failMarkHeld
	}
	h, ok := s.holds[id]
	if !ok {
		return postgres.ErrNotFound
	}
	switch h.Status {
	case models.HoldStatusPending:
		h.Status, h.CreditTxID = models.HoldStatusHeld, &txID
	case models.HoldStatusHeld, models.HoldStatusReleased:
	default:
		return postgres.ErrNotFound
	}
	return nil
}
func (s *reserveHoldRepositoryStub) Void(_ context.Context, id uuid.UUID, owner string) error {
	if h := s.holds[id]; h != nil && h.Status == models.HoldStatusPending && h.LeaseOwner == owner {
		h.Status, h.LeaseOwner, h.LeaseUntil = models.HoldStatusVoided, "", nil
	}
	return nil
}
func leaseFree(h *models.ReserveHold, now time.Time) bool {
	return h.LeaseUntil == nil || h.LeaseUntil.Before(now)
}
func (s *reserveHoldRepositoryStub) claim(match func(*models.ReserveHold) bool, owner string, now time.Time, lease time.Duration, limit int) []models.ReserveHold {
	ids := []*models.ReserveHold{}
	for _, h := range s.holds {
		if match(h) && leaseFree(h, now) {
			ids = append(ids, h)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].ReleaseAt.Before(ids[j].ReleaseAt) })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := []models.ReserveHold{}
	for _, h := range ids {
		until := now.Add(lease)
		h.LeaseOwner, h.LeaseUntil, h.Attempts = owner, &until, h.Attempts+1
		out = append(out, *h)
	}
	return out
}
func (s *reserveHoldRepositoryStub) ClaimDue(_ context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error) {
	return s.claim(func(h *models.ReserveHold) bool {
		return h.Status == models.HoldStatusHeld && !h.ReleaseAt.After(now)
	}, owner, now, lease, limit), nil
}
func (s *reserveHoldRepositoryStub) ClaimStalePending(_ context.Context, owner string, olderThan, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error) {
	return s.claim(func(h *models.ReserveHold) bool {
		return h.Status == models.HoldStatusPending && h.CreatedAt.Before(olderThan)
	}, owner, now, lease, limit), nil
}
func (s *reserveHoldRepositoryStub) ClaimByID(_ context.Context, id uuid.UUID, owner string, now time.Time, lease time.Duration) (*models.ReserveHold, bool, error) {
	rows := s.claim(func(h *models.ReserveHold) bool { return h.ID == id && h.Status == models.HoldStatusHeld }, owner, now, lease, 1)
	if len(rows) == 0 {
		return nil, false, nil
	}
	return &rows[0], true, nil
}
func (s *reserveHoldRepositoryStub) MarkReleased(_ context.Context, id uuid.UUID, owner, reason, ref string, at time.Time) error {
	if h := s.holds[id]; h != nil && h.Status == models.HoldStatusHeld && h.LeaseOwner == owner {
		h.Status, h.ReleaseReason, h.ReleaseReference, h.ReleasedAt = models.HoldStatusReleased, reason, ref, &at
		h.LeaseOwner, h.LeaseUntil = "", nil
	}
	return nil
}
func (s *reserveHoldRepositoryStub) Fail(_ context.Context, id uuid.UUID, owner, lastError string, review bool) error {
	if h := s.holds[id]; h != nil && h.LeaseOwner == owner {
		h.LeaseOwner, h.LeaseUntil, h.LastError = "", nil, lastError
		if review {
			h.Status = models.HoldStatusNeedsReview
		}
	}
	return nil
}

// reserveLedgerStub records releases and answers FindCredit from a map.
type reserveLedgerStub struct {
	released   map[string]int // reference -> times the money moved
	credits    map[string]int64
	releaseErr error
	calls      int
}

func newReserveLedgerStub() *reserveLedgerStub {
	return &reserveLedgerStub{released: map[string]int{}, credits: map[string]int64{}}
}
func (l *reserveLedgerStub) Release(_ context.Context, _ models.ReserveHold, reference string) error {
	l.calls++
	if l.releaseErr != nil {
		return l.releaseErr
	}
	// like the real ledger, a repeated reference moves the money once
	if l.released[reference] == 0 {
		l.released[reference] = 1
	}
	return nil
}
func (l *reserveLedgerStub) FindCredit(_ context.Context, _, reference string) (int64, bool, error) {
	id, ok := l.credits[reference]
	return id, ok, nil
}

type holdFixture struct {
	*reserveFixture
	repo   *reserveHoldRepositoryStub
	ledger *reserveLedgerStub
	holds  *DefaultReserveHoldService
}

func newHoldFixture(t *testing.T) *holdFixture {
	t.Helper()
	f := &holdFixture{reserveFixture: newReserveFixture(t), repo: newReserveHoldRepositoryStub(), ledger: newReserveLedgerStub()}
	f.holds = NewReserveHoldService(f.repo, f.accounts, f.svc, f.ledger)
	f.holds.now = func() time.Time { return f.now }
	return f
}

func (f *holdFixture) plan(t *testing.T, ref string, minor int64, creditType string) *CreditPlan {
	t.Helper()
	p, err := f.holds.PlanCredit(f.ctx, PlanCreditRequest{
		Ledger: "ledgertrack", WalletID: f.account.WalletID, Currency: "NGN", Reference: ref, AmountMinor: minor, CreditType: creditType,
	})
	require.NoError(t, err)
	return p
}

func TestPlanCreditSplitsAndRecordsPendingHold(t *testing.T) {
	f := newHoldFixture(t)
	rule := f.clientRule(t, "10", 72)

	p := f.plan(t, "cr-1", 10_000_000, "")
	require.NotNil(t, p)
	require.Equal(t, int64(1_000_000), p.ReserveMinor)
	require.Equal(t, int64(9_000_000), p.AvailableMinor)
	require.Equal(t, models.HoldStatusPending, p.Hold.Status)
	require.Equal(t, rule.ID, *p.Hold.RuleID)
	require.Equal(t, f.now.Add(72*time.Hour), p.Hold.ReleaseAt)
	require.Equal(t, f.account.ID, p.Hold.AccountID)
	require.Len(t, f.repo.holds, 1)

	require.NoError(t, f.holds.ConfirmCredit(f.ctx, p.Hold.ID, 77))
	got, err := f.holds.GetHold(f.ctx, p.Hold.ID)
	require.NoError(t, err)
	require.Equal(t, models.HoldStatusHeld, got.Status)
	require.EqualValues(t, 77, *got.CreditTxID)
}

func TestPlanCreditReturnsNilWhenNothingIsHeld(t *testing.T) {
	f := newHoldFixture(t)

	// no rule at all: nothing is looked up past the cheap existence check
	f.repo.rulesExist = false
	require.Nil(t, f.plan(t, "cr-1", 1_000_000, ""))

	// rules exist but none covers this credit
	f.repo.rulesExist = true
	f.holds.rulesChecked = time.Time{}
	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "20", HoldHours: 24, CreditTypes: []string{"card"}})
	require.NoError(t, err)
	require.Nil(t, f.plan(t, "cr-2", 1_000_000, "payout_return"))
	require.Empty(t, f.repo.holds, "no hold row for a credit that is not held")

	// a wallet that is not a CBA account, and a currency that does not match the account
	p, err := f.holds.PlanCredit(f.ctx, PlanCreditRequest{Ledger: "ledgertrack", WalletID: "unknown-NGN", Currency: "NGN", Reference: "x", AmountMinor: 100})
	require.NoError(t, err)
	require.Nil(t, p)
	p, err = f.holds.PlanCredit(f.ctx, PlanCreditRequest{Ledger: "ledgertrack", WalletID: f.account.WalletID, Currency: "GHS", Reference: "y", AmountMinor: 100, CreditType: "card"})
	require.NoError(t, err)
	require.Nil(t, p)
}

func TestRuleExistenceIsCachedBriefly(t *testing.T) {
	f := newHoldFixture(t)
	f.repo.rulesExist = false
	for i := 0; i < 5; i++ {
		require.Nil(t, f.plan(t, "cr", 1000, ""))
	}
	require.Equal(t, 1, f.repo.ruleChecks, "credits do not hit the database for the rule check each time")

	f.now = f.now.Add(rulesCacheTTL + time.Second)
	f.repo.rulesExist = true
	f.clientRule(t, "10", 24)
	require.NotNil(t, f.plan(t, "cr-new", 1_000_000, ""), "a new rule applies after the cache expires")
	require.Equal(t, 2, f.repo.ruleChecks)
}

func TestFullHoldPostsNothingAvailable(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "100", 24)
	p := f.plan(t, "cr-1", 5_000, "")
	require.Equal(t, int64(5_000), p.ReserveMinor)
	require.Zero(t, p.AvailableMinor)
}

func TestRetryKeepsTheOriginalSplit(t *testing.T) {
	f := newHoldFixture(t)
	rule := f.clientRule(t, "10", 72)
	first := f.plan(t, "cr-1", 10_000_000, "")

	// the rule is revoked and replaced before the client retries the same reference
	f.now = f.now.Add(time.Hour)
	_, err := f.svc.RevokeRule(f.ctx, rule.ID)
	require.NoError(t, err)
	f.clientRule(t, "50", 24)

	again := f.plan(t, "cr-1", 10_000_000, "")
	require.Equal(t, first.Hold.ID, again.Hold.ID)
	require.Equal(t, first.ReserveMinor, again.ReserveMinor)
	require.Equal(t, first.Hold.ReleaseAt, again.Hold.ReleaseAt)
	require.Len(t, f.repo.holds, 1)

	// a different reference gets the new rule
	other := f.plan(t, "cr-2", 10_000_000, "")
	require.Equal(t, int64(5_000_000), other.ReserveMinor)
}

func TestVoidedHoldIsRearmedOnRetry(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 72)
	p := f.plan(t, "cr-1", 10_000_000, "")
	f.repo.holds[p.Hold.ID].Status = models.HoldStatusVoided // the first attempt never reached the ledger

	again := f.plan(t, "cr-1", 20_000_000, "")
	require.Equal(t, p.Hold.ID, again.Hold.ID, "same row is reused")
	require.Equal(t, models.HoldStatusPending, again.Hold.Status)
	require.Equal(t, int64(2_000_000), again.ReserveMinor)
	require.Len(t, f.repo.holds, 1)
}

func (f *holdFixture) heldHold(t *testing.T, ref string, minor int64, tx int64) models.ReserveHold {
	t.Helper()
	p := f.plan(t, ref, minor, "")
	require.NotNil(t, p)
	require.NoError(t, f.holds.ConfirmCredit(f.ctx, p.Hold.ID, tx))
	return p.Hold
}

func releaseOpts() ReleaseOptions {
	return ReleaseOptions{Owner: "w1", Batch: 100, Lease: 2 * time.Minute, PendingGrace: 10 * time.Minute, MaxAttempts: 3}
}

func TestRunReleaseReleasesOnlyDueHoldsOnce(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	due := f.heldHold(t, "cr-due", 10_000_000, 1)
	later := f.heldHold(t, "cr-later", 10_000_000, 2)
	f.repo.holds[later.ID].ReleaseAt = f.now.Add(48 * time.Hour)

	f.now = f.now.Add(25 * time.Hour)
	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Released)

	got, _ := f.holds.GetHold(f.ctx, due.ID)
	require.Equal(t, models.HoldStatusReleased, got.Status)
	require.Equal(t, models.HoldReleaseSLA, got.ReleaseReason)
	require.Equal(t, "reserve-release:"+due.ID.String(), got.ReleaseReference)
	require.Equal(t, 1, f.ledger.released[got.ReleaseReference])
	other, _ := f.holds.GetHold(f.ctx, later.ID)
	require.Equal(t, models.HoldStatusHeld, other.Status)

	// a second pass finds nothing to do
	stats, err = f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Zero(t, stats.Claimed)
	require.Equal(t, 1, f.ledger.calls)
}

func TestRunReleaseSkipsLeasedHoldsAndRetriesAfterLeaseExpiry(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)
	f.now = f.now.Add(25 * time.Hour)

	// another worker holds the lease
	until := f.now.Add(time.Minute)
	f.repo.holds[h.ID].LeaseOwner, f.repo.holds[h.ID].LeaseUntil = "w2", &until
	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Zero(t, stats.Claimed)

	// it died: after the lease expires another worker takes over
	f.now = f.now.Add(2 * time.Minute)
	stats, err = f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Released)
}

func TestFailedReleaseIsRetriedThenEscalated(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)
	f.now = f.now.Add(25 * time.Hour)
	f.ledger.releaseErr = errors.New("insufficient funds in lien")

	for attempt := 1; attempt <= 2; attempt++ {
		stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
		require.NoError(t, err)
		require.Equal(t, 1, stats.Failed, "attempt %d", attempt)
		got, _ := f.holds.GetHold(f.ctx, h.ID)
		require.Equal(t, models.HoldStatusHeld, got.Status)
		require.Contains(t, got.LastError, "insufficient funds")
	}
	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Escalated, "third failure reaches max attempts")
	got, _ := f.holds.GetHold(f.ctx, h.ID)
	require.Equal(t, models.HoldStatusNeedsReview, got.Status)

	stats, err = f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Zero(t, stats.Claimed, "a hold in review leaves the queue")
}

func TestInterruptedCreditIsSettledFromTheLedger(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	reached := f.plan(t, "cr-reached", 10_000_000, "") // the ledger got it, the confirm was lost
	lost := f.plan(t, "cr-lost", 10_000_000, "")       // the ledger never got it
	fresh := f.plan(t, "cr-fresh", 10_000_000, "")     // still inside the grace period
	f.ledger.credits["cr-reached"] = 501
	for _, id := range []uuid.UUID{reached.Hold.ID, lost.Hold.ID} {
		f.repo.holds[id].CreatedAt = f.now.Add(-time.Hour)
	}
	f.repo.holds[fresh.Hold.ID].CreatedAt = f.now.Add(-time.Minute)

	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Recovered)
	require.Equal(t, 1, stats.Voided)

	got, _ := f.holds.GetHold(f.ctx, reached.Hold.ID)
	require.Equal(t, models.HoldStatusHeld, got.Status)
	require.EqualValues(t, 501, *got.CreditTxID)
	require.Empty(t, got.LeaseOwner)
	got, _ = f.holds.GetHold(f.ctx, lost.Hold.ID)
	require.Equal(t, models.HoldStatusVoided, got.Status)
	got, _ = f.holds.GetHold(f.ctx, fresh.Hold.ID)
	require.Equal(t, models.HoldStatusPending, got.Status, "a credit still in flight is not touched")
}

func TestManualReleaseAheadOfSLA(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 72)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)

	got, err := f.holds.ReleaseHold(f.ctx, h.ID, "backoffice")
	require.NoError(t, err)
	require.Equal(t, models.HoldStatusReleased, got.Status)
	require.Equal(t, models.HoldReleaseManual, got.ReleaseReason)
	require.Equal(t, 1, f.ledger.calls)

	again, err := f.holds.ReleaseHold(f.ctx, h.ID, "backoffice")
	require.NoError(t, err, "releasing twice gives the same answer")
	require.Equal(t, models.HoldStatusReleased, again.Status)
	require.Equal(t, 1, f.ledger.calls, "the money is not moved twice")

	// the SLA runner finds nothing left to release
	f.now = f.now.Add(100 * time.Hour)
	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Zero(t, stats.Claimed)
}

func TestManualReleaseRefusals(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 72)
	pending := f.plan(t, "cr-pending", 10_000_000, "")

	_, err := f.holds.ReleaseHold(f.ctx, pending.Hold.ID, "backoffice")
	require.ErrorIs(t, err, ErrReserveConflict, "a hold whose credit is not confirmed cannot be released")
	_, err = f.holds.ReleaseHold(f.ctx, uuid.New(), "backoffice")
	require.ErrorIs(t, err, ErrReserveNotFound)

	held := f.heldHold(t, "cr-held", 10_000_000, 1)
	f.ledger.releaseErr = errors.New("ledger unavailable")
	_, err = f.holds.ReleaseHold(f.ctx, held.ID, "backoffice")
	require.Error(t, err)
	got, _ := f.holds.GetHold(f.ctx, held.ID)
	require.Equal(t, models.HoldStatusHeld, got.Status, "a failed release leaves the hold held")
	require.Empty(t, got.LeaseOwner, "and gives the lease back so it can be retried")
	f.ledger.releaseErr = nil
	_, err = f.holds.ReleaseHold(f.ctx, held.ID, "backoffice")
	require.NoError(t, err)
}

func TestListHoldsFilters(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	f.heldHold(t, "a", 1_000_000, 1)
	f.heldHold(t, "b", 1_000_000, 2)
	held := models.HoldStatusHeld
	all, err := f.holds.ListHolds(f.ctx, repositories.ReserveHoldFilter{Status: &held})
	require.NoError(t, err)
	require.Len(t, all, 2)
	soon := f.now.Add(time.Hour)
	none, err := f.holds.ListHolds(f.ctx, repositories.ReserveHoldFilter{DueBefore: &soon})
	require.NoError(t, err)
	require.Empty(t, none)
}
