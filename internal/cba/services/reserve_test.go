package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

type reserveRepositoryStub struct {
	rules map[uuid.UUID]*models.ReserveRule
}

func newReserveRepositoryStub() *reserveRepositoryStub {
	return &reserveRepositoryStub{rules: map[uuid.UUID]*models.ReserveRule{}}
}

func (s *reserveRepositoryStub) CreateRule(_ context.Context, r *models.ReserveRule) error {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	c := *r
	s.rules[r.ID] = &c
	return nil
}
func (s *reserveRepositoryStub) GetRule(_ context.Context, id uuid.UUID) (*models.ReserveRule, error) {
	if r, ok := s.rules[id]; ok {
		c := *r
		return &c, nil
	}
	return nil, postgres.ErrNotFound
}
func (s *reserveRepositoryStub) ListRules(_ context.Context, f repositories.ReserveRuleFilter) ([]models.ReserveRule, error) {
	out := []models.ReserveRule{}
	for _, r := range s.rules {
		if f.ClientID != nil && (r.ClientID == nil || *r.ClientID != *f.ClientID) {
			continue
		}
		if f.AccountID != nil && (r.AccountID == nil || *r.AccountID != *f.AccountID) {
			continue
		}
		if f.Status != nil && r.Status != *f.Status {
			continue
		}
		out = append(out, *r)
	}
	return out, nil
}
func (s *reserveRepositoryStub) UpdateRule(_ context.Context, r *models.ReserveRule) error {
	c := *r
	s.rules[r.ID] = &c
	return nil
}
func (s *reserveRepositoryStub) ActiveRulesFor(_ context.Context, accountID, clientID uuid.UUID, at time.Time) ([]models.ReserveRule, error) {
	out := []models.ReserveRule{}
	for _, r := range s.rules {
		if !inForce(r.Status, r.EffectiveFrom, r.EffectiveTo, at) {
			continue
		}
		if (r.AccountID != nil && *r.AccountID == accountID) || (r.ClientID != nil && *r.ClientID == clientID) {
			out = append(out, *r)
		}
	}
	return out, nil
}

type reserveFixture struct {
	svc     *DefaultReserveRuleService
	ctx     context.Context
	now     time.Time
	client  *models.Client
	product *models.Product
	account *models.Account
}

func newReserveFixture(t *testing.T) *reserveFixture {
	t.Helper()
	f := &reserveFixture{ctx: context.Background(), now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	clients, accounts, products := newClientRepositoryStub(), newAccountRepositoryStub(), newProductRepositoryStub()
	f.svc = NewReserveRuleService(newReserveRepositoryStub(), accounts, products, clients)
	f.svc.now = func() time.Time { return f.now }

	f.client = &models.Client{ID: uuid.New(), ClientNumber: "CL-1", Type: "corporate", Status: "active"}
	require.NoError(t, clients.Create(f.ctx, f.client))
	f.product = &models.Product{ID: uuid.New(), Code: "NGN", Name: "NGN wallet", Category: "wallet", Currency: "NGN", Status: models.ProductStatusActive}
	require.NoError(t, products.Create(f.ctx, f.product))
	f.account = &models.Account{ID: uuid.New(), AccountNumber: "1000000001", ClientID: f.client.ID, ProductID: f.product.ID, Currency: "NGN", Status: models.AccountStatusActive, WalletID: "client-CL-1-NGN"}
	require.NoError(t, accounts.Create(f.ctx, f.account))
	return f
}

func (f *reserveFixture) clientRule(t *testing.T, pct string, hours int) *models.ReserveRule {
	t.Helper()
	rule, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: pct, HoldHours: hours})
	require.NoError(t, err)
	return rule
}

func (f *reserveFixture) preview(t *testing.T, minor int64) *ReservePreview {
	t.Helper()
	p, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: minor})
	require.NoError(t, err)
	return p
}

func TestReserveAmountMinorRoundsHalfEven(t *testing.T) {
	for _, tc := range []struct {
		amount int64
		pct    string
		want   int64
	}{
		{10_000_000, "10", 1_000_000},
		{1005, "10", 100},  // 100.5 -> 100 (even)
		{1015, "10", 102},  // 101.5 -> 102 (even)
		{999, "100", 999},  // whole amount
		{1, "0.5", 0},      // 0.005 rounds down
		{3, "50", 2},       // 1.5 -> 2
		{10, "33.3333", 3}, // 3.33333
	} {
		require.Equal(t, tc.want, ReserveAmountMinor(tc.amount, decimal.RequireFromString(tc.pct)), "%d at %s", tc.amount, tc.pct)
	}
}

func TestPreviewWithoutRuleHoldsNothing(t *testing.T) {
	f := newReserveFixture(t)
	p := f.preview(t, 5_000_000)
	require.Equal(t, ReserveSourceNone, p.RuleSource)
	require.Zero(t, p.ReserveMinor)
	require.Equal(t, int64(5_000_000), p.AvailableMinor)
	require.Nil(t, p.ReleaseAt)
}

func TestPreviewSplitsAndSetsReleaseTime(t *testing.T) {
	f := newReserveFixture(t)
	rule := f.clientRule(t, "10", 72)
	p := f.preview(t, 10_000_000)
	require.Equal(t, ReserveSourceClient, p.RuleSource)
	require.Equal(t, rule.ID, *p.RuleID)
	require.Equal(t, int64(1_000_000), p.ReserveMinor)
	require.Equal(t, int64(9_000_000), p.AvailableMinor)
	require.Equal(t, p.AmountMinor, p.ReserveMinor+p.AvailableMinor)
	require.Equal(t, f.now.Add(72*time.Hour), *p.ReleaseAt)
}

func TestAccountRuleBeatsClientRule(t *testing.T) {
	f := newReserveFixture(t)
	f.clientRule(t, "10", 72)
	own, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "account", AccountID: &f.account.ID, Percentage: "25", HoldHours: 24})
	require.NoError(t, err)
	require.Equal(t, "NGN", own.Currency, "currency comes from the account")
	p := f.preview(t, 1_000_000)
	require.Equal(t, ReserveSourceAccount, p.RuleSource)
	require.Equal(t, int64(250_000), p.ReserveMinor)
	require.Equal(t, 24, p.HoldHours)
}

func TestProductRuleBeatsClientWideRule(t *testing.T) {
	f := newReserveFixture(t)
	f.clientRule(t, "10", 72)
	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, ProductID: &f.product.ID, Currency: "NGN", Percentage: "20", HoldHours: 48})
	require.NoError(t, err)
	p := f.preview(t, 1_000_000)
	require.Equal(t, int64(200_000), p.ReserveMinor)
	require.Equal(t, 48, p.HoldHours)
}

func TestOverlappingRulesAreRejected(t *testing.T) {
	f := newReserveFixture(t)
	f.clientRule(t, "10", 72)
	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "5", HoldHours: 24})
	require.ErrorIs(t, err, ErrReserveConflict)

	// a rule for a different product of the same client does not collide
	_, err = f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, ProductID: &f.product.ID, Currency: "NGN", Percentage: "5", HoldHours: 24})
	require.NoError(t, err)
}

func TestRuleCanBeReplacedAfterRevoke(t *testing.T) {
	f := newReserveFixture(t)
	old := f.clientRule(t, "10", 72)
	f.now = f.now.Add(time.Hour)
	revoked, err := f.svc.RevokeRule(f.ctx, old.ID)
	require.NoError(t, err)
	require.Equal(t, models.ReserveStatusRevoked, revoked.Status)
	require.NotNil(t, revoked.EffectiveTo)

	f.clientRule(t, "15", 24)
	p := f.preview(t, 1_000_000)
	require.Equal(t, int64(150_000), p.ReserveMinor)

	// revoking twice is a no-op
	again, err := f.svc.RevokeRule(f.ctx, old.ID)
	require.NoError(t, err)
	require.Equal(t, revoked.EffectiveTo, again.EffectiveTo)
}

func TestFutureRuleDoesNotApplyYet(t *testing.T) {
	f := newReserveFixture(t)
	start := f.now.Add(48 * time.Hour)
	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: 24, EffectiveFrom: &start})
	require.NoError(t, err)
	require.Equal(t, ReserveSourceNone, f.preview(t, 1_000_000).RuleSource)

	later, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000, At: start.Add(time.Minute)})
	require.NoError(t, err)
	require.Equal(t, ReserveSourceClient, later.RuleSource)
}

func TestCreateRuleValidation(t *testing.T) {
	f := newReserveFixture(t)
	other := uuid.New()
	past := f.now.Add(-time.Hour)
	for name, in := range map[string]CreateReserveRuleInput{
		"bad scope":         {Scope: "global", Percentage: "10", HoldHours: 24},
		"zero percentage":   {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "0", HoldHours: 24},
		"over 100":          {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "100.01", HoldHours: 24},
		"not a number":      {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "ten", HoldHours: 24},
		"five decimals":     {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "1.12345", HoldHours: 24},
		"zero hours":        {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: 0},
		"over a year":       {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: MaxReserveHoldHours + 1},
		"client no id":      {Scope: "client", Currency: "NGN", Percentage: "10", HoldHours: 24},
		"client with acct":  {Scope: "client", ClientID: &f.client.ID, AccountID: &f.account.ID, Currency: "NGN", Percentage: "10", HoldHours: 24},
		"unknown currency":  {Scope: "client", ClientID: &f.client.ID, Currency: "ZZZ", Percentage: "10", HoldHours: 24},
		"account no id":     {Scope: "account", Percentage: "10", HoldHours: 24},
		"account + client":  {Scope: "account", AccountID: &f.account.ID, ClientID: &f.client.ID, Percentage: "10", HoldHours: 24},
		"account currency":  {Scope: "account", AccountID: &f.account.ID, Currency: "GHS", Percentage: "10", HoldHours: 24},
		"ends before start": {Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: 24, EffectiveTo: &past},
	} {
		_, err := f.svc.CreateRule(f.ctx, in)
		require.ErrorIs(t, err, ErrReserveValidation, name)
	}

	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &other, Currency: "NGN", Percentage: "10", HoldHours: 24})
	require.ErrorIs(t, err, ErrReserveNotFound)
	_, err = f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "account", AccountID: &other, Percentage: "10", HoldHours: 24})
	require.ErrorIs(t, err, ErrReserveNotFound)
	_, err = f.svc.GetRule(f.ctx, uuid.New())
	require.ErrorIs(t, err, ErrReserveNotFound)
}

func TestPreviewRejectsNonPositiveAmount(t *testing.T) {
	f := newReserveFixture(t)
	_, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 0})
	require.ErrorIs(t, err, ErrReserveValidation)
	_, err = f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: uuid.New(), AmountMinor: 100})
	require.ErrorIs(t, err, ErrReserveNotFound)
}
