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
	draws        []*models.ReserveDraw
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
func (s *reserveHoldRepositoryStub) pendingDraw(holdID uuid.UUID) bool {
	for _, d := range s.draws {
		if d.HoldID == holdID && d.Status == models.DrawStatusPending {
			return true
		}
	}
	return false
}

func (s *reserveHoldRepositoryStub) Draw(_ context.Context, ledger, wallet, ref string, amount int64, now time.Time) ([]models.ReserveDraw, error) {
	out := []models.ReserveDraw{}
	for _, d := range s.draws {
		if d.Ledger == ledger && d.WalletID == wallet && d.Reference == ref && d.Status != models.DrawStatusReverted {
			out = append(out, *d)
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	hs := []*models.ReserveHold{}
	for _, h := range s.holds {
		if h.Ledger == ledger && h.WalletID == wallet && h.Status == models.HoldStatusHeld && h.AmountMinor > 0 && leaseFree(h, now) {
			hs = append(hs, h)
		}
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].ReleaseAt.Before(hs[j].ReleaseAt) })
	remaining := amount
	for _, h := range hs {
		if remaining <= 0 {
			break
		}
		take := h.AmountMinor
		if take > remaining {
			take = remaining
		}
		h.AmountMinor -= take
		if h.AmountMinor == 0 {
			h.Status, h.ReleaseReason = models.HoldStatusReleased, models.HoldReleaseDrawn
		}
		d := &models.ReserveDraw{ID: uuid.New(), HoldID: h.ID, Ledger: ledger, WalletID: wallet, Reference: ref, AmountMinor: take, Status: models.DrawStatusPending, CreatedAt: now}
		s.draws = append(s.draws, d)
		out = append(out, *d)
		remaining -= take
	}
	return out, nil
}
func (s *reserveHoldRepositoryStub) ConfirmDraws(_ context.Context, ledger, wallet, ref string) error {
	for _, d := range s.draws {
		if d.Ledger == ledger && d.WalletID == wallet && d.Reference == ref && d.Status == models.DrawStatusPending {
			d.Status = models.DrawStatusApplied
		}
	}
	return nil
}
func (s *reserveHoldRepositoryStub) RevertDraws(_ context.Context, ledger, wallet, ref string) error {
	for _, d := range s.draws {
		if d.Ledger == ledger && d.WalletID == wallet && d.Reference == ref && d.Status == models.DrawStatusPending {
			d.Status = models.DrawStatusReverted
			h := s.holds[d.HoldID]
			h.AmountMinor += d.AmountMinor
			if h.Status == models.HoldStatusReleased && h.ReleaseReason == models.HoldReleaseDrawn {
				h.Status, h.ReleaseReason = models.HoldStatusHeld, ""
			}
		}
	}
	return nil
}
func (s *reserveHoldRepositoryStub) StaleDraws(_ context.Context, olderThan time.Time, limit int) ([]models.DrawRef, error) {
	seen := map[models.DrawRef]bool{}
	out := []models.DrawRef{}
	for _, d := range s.draws {
		ref := models.DrawRef{Ledger: d.Ledger, WalletID: d.WalletID, Reference: d.Reference}
		if d.Status == models.DrawStatusPending && d.CreatedAt.Before(olderThan) && !seen[ref] && len(out) < limit {
			seen[ref] = true
			out = append(out, ref)
		}
	}
	return out, nil
}
func (s *reserveHoldRepositoryStub) ClaimDue(_ context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]models.ReserveHold, error) {
	return s.claim(func(h *models.ReserveHold) bool {
		return h.Status == models.HoldStatusHeld && !h.ReleaseAt.After(now) && !s.pendingDraw(h.ID)
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

func (f *holdFixture) draw(t *testing.T, ref string, minor int64) *DrawPlan {
	t.Helper()
	p, err := f.holds.DrawReserve(f.ctx, DrawRequest{Ledger: "ledgertrack", WalletID: f.account.WalletID, Reference: ref, AmountMinor: minor})
	require.NoError(t, err)
	return p
}

func TestDrawTakesOldestHoldFirstAndKeepsTheRest(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	older := f.heldHold(t, "cr-old", 10_000_000, 1) // holds 1,000,000
	f.now = f.now.Add(time.Hour)
	newer := f.heldHold(t, "cr-new", 10_000_000, 2) // holds 1,000,000, releases an hour later

	p := f.draw(t, "chargeback-1", 1_500_000)
	require.NotNil(t, p)
	require.Equal(t, int64(1_500_000), p.DrawnMinor)
	require.Zero(t, p.RemainderMinor)
	require.Len(t, p.Draws, 2)

	got, _ := f.holds.GetHold(f.ctx, older.ID)
	require.Equal(t, models.HoldStatusReleased, got.Status)
	require.Equal(t, models.HoldReleaseDrawn, got.ReleaseReason)
	require.Zero(t, got.AmountMinor)
	got, _ = f.holds.GetHold(f.ctx, newer.ID)
	require.Equal(t, models.HoldStatusHeld, got.Status)
	require.EqualValues(t, 500_000, got.AmountMinor)
}

func TestDrawCoversLessThanTheDebitWhenLittleIsHeld(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	f.heldHold(t, "cr-1", 10_000_000, 1)
	p := f.draw(t, "chargeback-1", 5_000_000)
	require.Equal(t, int64(1_000_000), p.DrawnMinor)
	require.Equal(t, int64(4_000_000), p.RemainderMinor, "the rest comes from the available balance")
	require.Nil(t, f.draw(t, "chargeback-2", 100), "nothing is left to draw")
}

func TestDrawIsIdempotentOnTheReference(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)
	first := f.draw(t, "chargeback-1", 400_000)
	again := f.draw(t, "chargeback-1", 400_000)
	require.Equal(t, first.DrawnMinor, again.DrawnMinor)
	got, _ := f.holds.GetHold(f.ctx, h.ID)
	require.EqualValues(t, 600_000, got.AmountMinor, "a retry does not draw twice")

	_, err := f.holds.DrawReserve(f.ctx, DrawRequest{Ledger: "ledgertrack", WalletID: f.account.WalletID, Reference: "chargeback-1", AmountMinor: 100_000})
	require.ErrorIs(t, err, ErrReserveConflict, "a retry with a smaller amount than was drawn is refused")
	_, err = f.holds.DrawReserve(f.ctx, DrawRequest{Ledger: "ledgertrack", WalletID: f.account.WalletID, Reference: "", AmountMinor: 1})
	require.ErrorIs(t, err, ErrReserveValidation)
}

func TestRevertGivesTheHoldBack(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)
	f.draw(t, "chargeback-1", 1_000_000) // uses the hold up
	got, _ := f.holds.GetHold(f.ctx, h.ID)
	require.Equal(t, models.HoldStatusReleased, got.Status)

	require.NoError(t, f.holds.RevertDraw(f.ctx, "ledgertrack", f.account.WalletID, "chargeback-1"))
	got, _ = f.holds.GetHold(f.ctx, h.ID)
	require.Equal(t, models.HoldStatusHeld, got.Status)
	require.EqualValues(t, 1_000_000, got.AmountMinor)
	require.Empty(t, got.ReleaseReason)

	// the same reference can draw again after a revert
	require.Equal(t, int64(1_000_000), f.draw(t, "chargeback-1", 1_000_000).DrawnMinor)
}

func TestPendingDrawBlocksReleaseUntilSettled(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 24)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)
	f.now = f.now.Add(25 * time.Hour) // the SLA has passed
	f.draw(t, "chargeback-1", 400_000)

	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Zero(t, stats.Released, "the SLA release waits while a debit is mid-air")

	require.NoError(t, f.holds.ConfirmDraw(f.ctx, "ledgertrack", f.account.WalletID, "chargeback-1"))
	stats, err = f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Released)
	require.Equal(t, 1, len(f.ledger.released))
	got, _ := f.holds.GetHold(f.ctx, h.ID)
	require.EqualValues(t, 600_000, got.AmountMinor, "only what is left is released")
}

func TestInterruptedDebitIsSettledFromTheLedger(t *testing.T) {
	f := newHoldFixture(t)
	f.clientRule(t, "10", 48)
	h := f.heldHold(t, "cr-1", 10_000_000, 1)
	f.draw(t, "cb-posted", 300_000)
	f.draw(t, "cb-lost", 200_000)
	f.draw(t, "cb-fresh", 100_000)
	f.ledger.credits["cb-posted"] = 900 // the debit reached the ledger

	for _, d := range f.repo.draws {
		if d.Reference != "cb-fresh" {
			d.CreatedAt = f.now.Add(-time.Hour)
		}
	}
	stats, err := f.holds.RunRelease(f.ctx, releaseOpts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.DrawsApplied)
	require.Equal(t, 1, stats.DrawsReverted)

	got, _ := f.holds.GetHold(f.ctx, h.ID)
	require.EqualValues(t, 1_000_000-300_000-100_000, got.AmountMinor, "the lost debit gave its 200,000 back; the fresh one is still in flight")
}
