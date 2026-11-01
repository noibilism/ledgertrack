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

type calendarRepositoryStub struct {
	calendars map[uuid.UUID]*models.BusinessCalendar
}

func newCalendarRepositoryStub() *calendarRepositoryStub {
	return &calendarRepositoryStub{calendars: map[uuid.UUID]*models.BusinessCalendar{}}
}

func (s *calendarRepositoryStub) CreateCalendar(_ context.Context, c *models.BusinessCalendar) error {
	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	cp := *c
	s.calendars[c.ID] = &cp
	return nil
}
func (s *calendarRepositoryStub) GetCalendar(_ context.Context, id uuid.UUID) (*models.BusinessCalendar, error) {
	if c, ok := s.calendars[id]; ok {
		cp := *c
		return &cp, nil
	}
	return nil, postgres.ErrNotFound
}
func (s *calendarRepositoryStub) ListCalendars(context.Context) ([]models.BusinessCalendar, error) {
	out := []models.BusinessCalendar{}
	for _, c := range s.calendars {
		out = append(out, *c)
	}
	return out, nil
}
func (s *calendarRepositoryStub) UpdateCalendar(_ context.Context, c *models.BusinessCalendar) error {
	cp := *c
	s.calendars[c.ID] = &cp
	return nil
}

type reserveFixture struct {
	calendars *DefaultBusinessCalendarService
	svc       *DefaultReserveRuleService
	ctx       context.Context
	now       time.Time
	client    *models.Client
	product   *models.Product
	account   *models.Account
}

func newReserveFixture(t *testing.T) *reserveFixture {
	t.Helper()
	f := &reserveFixture{ctx: context.Background(), now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	clients, accounts, products := newClientRepositoryStub(), newAccountRepositoryStub(), newProductRepositoryStub()
	calendarRepo := newCalendarRepositoryStub()
	f.calendars = NewBusinessCalendarService(calendarRepo)
	f.svc = NewReserveRuleService(newReserveRepositoryStub(), accounts, products, clients, calendarRepo)
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

func lagosCalendar(t *testing.T, f *reserveFixture, holidays ...string) *models.BusinessCalendar {
	t.Helper()
	cal, err := f.calendars.CreateCalendar(f.ctx, CalendarInput{
		Code: "NG-BANKING", Name: "Nigeria banking hours", Timezone: "Africa/Lagos",
		WorkingDays: []int{1, 2, 3, 4, 5}, OpenTime: "08:00", CloseTime: "17:00", Holidays: holidays,
	})
	require.NoError(t, err)
	return cal
}

func utc(day, hour, minute int) time.Time {
	return time.Date(2026, 11, day, hour, minute, 0, 0, time.UTC)
}

func TestAddBusinessHours(t *testing.T) {
	f := newReserveFixture(t)
	cal := lagosCalendar(t, f, "2026-11-10") // Tuesday holiday
	for name, tc := range map[string]struct {
		start time.Time
		hours int
		want  time.Time
	}{
		// Fri 15:00 Lagos (14:00 UTC): 2h Friday, 9h Monday, Tuesday holiday, 9h Wednesday, 4h Thursday from 08:00
		"skips weekend and holiday": {utc(6, 14, 0), 24, utc(12, 11, 0)},
		// Saturday: counting starts Monday 08:00 Lagos, 9 hours fills the day
		"starts on next opening": {utc(7, 10, 0), 9, utc(9, 16, 0)},
		// Fri 16:30 Lagos: 30 minutes Friday, 30 minutes Monday
		"splits across close": {utc(6, 15, 30), 1, utc(9, 7, 30)},
		// Monday 06:00 Lagos, before opening
		"before opening": {utc(9, 5, 0), 1, utc(9, 8, 0)},
		// inside the day, no wrap
		"same day": {utc(9, 8, 0), 3, utc(9, 11, 0)},
	} {
		got, err := AddBusinessHours(tc.start, tc.hours, cal)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, got, name)
	}
}

func TestBusinessModeRulePreview(t *testing.T) {
	f := newReserveFixture(t)
	cal := lagosCalendar(t, f)
	rule, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{
		Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: 18,
		HoldMode: "business", CalendarID: &cal.ID,
	})
	require.NoError(t, err)
	require.Equal(t, models.HoldModeBusiness, rule.HoldMode)

	// Sunday 1 Nov 12:00 UTC: counting starts Monday 2 Nov 08:00 Lagos; 18h = Monday 9h + Tuesday 9h -> Tuesday 17:00 Lagos
	p, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000, At: utc(1, 12, 0)})
	require.NoError(t, err)
	require.Equal(t, models.HoldModeBusiness, p.HoldMode)
	require.Equal(t, utc(3, 16, 0), *p.ReleaseAt)
}

func TestCalendarModeKeepsClockHours(t *testing.T) {
	f := newReserveFixture(t)
	f.clientRule(t, "10", 72)
	p := f.preview(t, 1_000_000)
	require.Equal(t, models.HoldModeCalendar, p.HoldMode)
	require.Equal(t, f.now.Add(72*time.Hour), *p.ReleaseAt)
}

func TestBusinessModeNeedsExistingCalendar(t *testing.T) {
	f := newReserveFixture(t)
	missing := uuid.New()
	base := CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: 24}

	in := base
	in.HoldMode = "business"
	_, err := f.svc.CreateRule(f.ctx, in)
	require.ErrorIs(t, err, ErrReserveValidation)

	in.CalendarID = &missing
	_, err = f.svc.CreateRule(f.ctx, in)
	require.ErrorIs(t, err, ErrReserveNotFound)

	in = base
	in.CalendarID = &missing // calendar id without business mode
	_, err = f.svc.CreateRule(f.ctx, in)
	require.ErrorIs(t, err, ErrReserveValidation)

	in = base
	in.HoldMode = "weekly"
	_, err = f.svc.CreateRule(f.ctx, in)
	require.ErrorIs(t, err, ErrReserveValidation)
}

func TestCreditTypeRulesAndFallback(t *testing.T) {
	f := newReserveFixture(t)
	f.clientRule(t, "10", 72) // every credit
	card, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{
		Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "20", HoldHours: 24,
		CreditTypes: []string{" Card ", "pos", "card"},
	})
	require.NoError(t, err, "a typed rule sits beside a general one")
	require.Equal(t, []string{"card", "pos"}, card.CreditTypes, "lowercased, de-duplicated, sorted")

	typed, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000, CreditType: "POS"})
	require.NoError(t, err)
	require.Equal(t, int64(200_000), typed.ReserveMinor)
	require.Equal(t, 24, typed.HoldHours)
	require.Equal(t, "pos", typed.CreditType)

	other, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000, CreditType: "bank_transfer"})
	require.NoError(t, err)
	require.Equal(t, int64(100_000), other.ReserveMinor, "falls back to the general rule")

	none, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000})
	require.NoError(t, err)
	require.Equal(t, int64(100_000), none.ReserveMinor, "no credit type matches only general rules")
}

func TestCreditTypeOnlyRuleLeavesOtherCreditsFree(t *testing.T) {
	f := newReserveFixture(t)
	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{
		Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "20", HoldHours: 24, CreditTypes: []string{"card"},
	})
	require.NoError(t, err)
	p, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000, CreditType: "payout_return"})
	require.NoError(t, err)
	require.Equal(t, ReserveSourceNone, p.RuleSource)
	require.Zero(t, p.ReserveMinor)
}

func TestCreditTypeOverlapAndValidation(t *testing.T) {
	f := newReserveFixture(t)
	mk := func(types ...string) error {
		_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{
			Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "10", HoldHours: 24, CreditTypes: types,
		})
		return err
	}
	require.NoError(t, mk("card", "pos"))
	require.ErrorIs(t, mk("pos", "wallet"), ErrReserveConflict, "shares pos")
	require.NoError(t, mk("wallet"))
	require.NoError(t, mk(), "a general rule beside typed ones")
	require.ErrorIs(t, mk(), ErrReserveConflict, "two general rules")

	for _, bad := range []string{"", "Card Payment", "9lives", "a-b", "thisisaverylongcredittypenamethatexceedsthelimit"} {
		require.ErrorIs(t, mk(bad), ErrReserveValidation, bad)
	}
}

func TestAccountRuleIgnoresClientTypedRule(t *testing.T) {
	f := newReserveFixture(t)
	_, err := f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "client", ClientID: &f.client.ID, Currency: "NGN", Percentage: "20", HoldHours: 24, CreditTypes: []string{"card"}})
	require.NoError(t, err)
	_, err = f.svc.CreateRule(f.ctx, CreateReserveRuleInput{Scope: "account", AccountID: &f.account.ID, Percentage: "5", HoldHours: 12})
	require.NoError(t, err)
	p, err := f.svc.Preview(f.ctx, ReservePreviewRequest{AccountID: f.account.ID, AmountMinor: 1_000_000, CreditType: "card"})
	require.NoError(t, err)
	require.Equal(t, ReserveSourceAccount, p.RuleSource, "the account's own rule is the most specific")
	require.Equal(t, int64(50_000), p.ReserveMinor)
}

func TestCalendarValidationAndUpdate(t *testing.T) {
	f := newReserveFixture(t)
	good := CalendarInput{Code: "GH", Name: "Ghana", Timezone: "Africa/Accra", WorkingDays: []int{5, 1, 1, 2}, OpenTime: "08:30", CloseTime: "16:30", Holidays: []string{"2026-12-25", "2026-03-06", "2026-12-25"}}
	cal, err := f.calendars.CreateCalendar(f.ctx, good)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 5}, cal.WorkingDays)
	require.Equal(t, []string{"2026-03-06", "2026-12-25"}, cal.Holidays)

	_, err = f.calendars.CreateCalendar(f.ctx, good)
	require.ErrorIs(t, err, ErrReserveConflict, "duplicate code")

	for name, mutate := range map[string]func(*CalendarInput){
		"bad code":      func(c *CalendarInput) { c.Code = "bad code" },
		"no name":       func(c *CalendarInput) { c.Name = " " },
		"bad zone":      func(c *CalendarInput) { c.Timezone = "Lagos" },
		"no days":       func(c *CalendarInput) { c.WorkingDays = nil },
		"day 0":         func(c *CalendarInput) { c.WorkingDays = []int{0} },
		"day 8":         func(c *CalendarInput) { c.WorkingDays = []int{8} },
		"bad open":      func(c *CalendarInput) { c.OpenTime = "8am" },
		"bad close":     func(c *CalendarInput) { c.CloseTime = "25:00" },
		"under an hour": func(c *CalendarInput) { c.OpenTime, c.CloseTime = "09:00", "09:30" },
		"closes first":  func(c *CalendarInput) { c.OpenTime, c.CloseTime = "17:00", "08:00" },
		"bad holiday":   func(c *CalendarInput) { c.Holidays = []string{"25/12/2026"} },
	} {
		in := good
		in.Code = "X-" + name[:1]
		mutate(&in)
		_, err := f.calendars.CreateCalendar(f.ctx, in)
		require.ErrorIs(t, err, ErrReserveValidation, name)
	}

	upd := good
	upd.Code = ""
	upd.Name = "Ghana banking"
	upd.Holidays = []string{"2026-12-26"}
	updated, err := f.calendars.UpdateCalendar(f.ctx, cal.ID, upd)
	require.NoError(t, err)
	require.Equal(t, "GH", updated.Code)
	require.Equal(t, "Ghana banking", updated.Name)
	require.Equal(t, []string{"2026-12-26"}, updated.Holidays)

	upd.Code = "OTHER"
	_, err = f.calendars.UpdateCalendar(f.ctx, cal.ID, upd)
	require.ErrorIs(t, err, ErrReserveValidation, "code is fixed")
	_, err = f.calendars.UpdateCalendar(f.ctx, uuid.New(), good)
	require.ErrorIs(t, err, ErrReserveNotFound)
}
