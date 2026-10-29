package services

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

// glRepositoryStub mimics the database constraints that matter (unique code, unique ledger+address,
// parent foreign key) and gives RunExclusive real commit/rollback semantics by working on a copy.
type glRepositoryStub struct {
	accounts map[string]*models.GLAccount
	// failCreateAfter makes Create fail once that many accounts were created in this repository (0 disables).
	failCreateAfter int
	created         int
	exclusiveRuns   int
}

func newGLRepositoryStub() *glRepositoryStub {
	return &glRepositoryStub{accounts: map[string]*models.GLAccount{}}
}

func (s *glRepositoryStub) Create(_ context.Context, account *models.GLAccount) error {
	if s.failCreateAfter > 0 && s.created >= s.failCreateAfter {
		return errors.New("simulated database failure")
	}
	if _, ok := s.accounts[account.Code]; ok {
		return postgres.ErrConstraintsFailed{}
	}
	for _, other := range s.accounts {
		if other.Ledger == account.Ledger && other.Address == account.Address {
			return postgres.ErrConstraintsFailed{}
		}
	}
	if account.ParentCode != nil {
		if _, ok := s.accounts[*account.ParentCode]; !ok {
			return errors.New("foreign key violation: parent does not exist yet")
		}
	}
	if account.ID == uuid.Nil {
		account.ID = uuid.New()
	}
	copied := *account
	s.accounts[account.Code] = &copied
	s.created++
	return nil
}

func (s *glRepositoryStub) Update(_ context.Context, account *models.GLAccount) error {
	current, ok := s.accounts[account.Code]
	if !ok {
		return postgres.ErrNotFound
	}
	current.Name = account.Name
	current.Status = account.Status
	current.Metadata = account.Metadata
	return nil
}

func (s *glRepositoryStub) GetByCode(_ context.Context, code string) (*models.GLAccount, error) {
	account, ok := s.accounts[code]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *account
	return &copied, nil
}

func (s *glRepositoryStub) GetByAddress(_ context.Context, ledger, address string) (*models.GLAccount, error) {
	for _, account := range s.accounts {
		if account.Ledger == ledger && account.Address == address {
			copied := *account
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *glRepositoryStub) List(_ context.Context, filter repositories.GLAccountFilter) ([]models.GLAccount, error) {
	ret := make([]models.GLAccount, 0, len(s.accounts))
	for _, account := range s.accounts {
		if filter.Type != nil && account.Type != *filter.Type {
			continue
		}
		if filter.Currency != nil && account.Currency != *filter.Currency {
			continue
		}
		if filter.Status != nil && account.Status != *filter.Status {
			continue
		}
		if filter.ParentCode != nil && (account.ParentCode == nil || *account.ParentCode != *filter.ParentCode) {
			continue
		}
		ret = append(ret, *account)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Code < ret[j].Code })
	return ret, nil
}

func (s *glRepositoryStub) RunExclusive(ctx context.Context, fn func(context.Context, repositories.GLAccountRepository) error) error {
	s.exclusiveRuns++
	tx := &glRepositoryStub{accounts: make(map[string]*models.GLAccount, len(s.accounts)), failCreateAfter: s.failCreateAfter}
	for code, account := range s.accounts {
		copied := *account
		tx.accounts[code] = &copied
	}
	if err := fn(ctx, tx); err != nil {
		return err // rollback: the copy is dropped
	}
	s.accounts = tx.accounts
	return nil
}

func newGLServiceForTests() (GLAccountService, *glRepositoryStub) {
	repo := newGLRepositoryStub()
	return NewGLAccountService(repo), repo
}

func glInput(code, name, typ, currency, parent, address string) CreateGLAccountInput {
	return CreateGLAccountInput{
		Code:       code,
		Name:       name,
		Type:       typ,
		Currency:   currency,
		ParentCode: parent,
		Ledger:     "ledgertrack",
		Address:    address,
	}
}

func TestGLCreateDefaultsNormalSideFromType(t *testing.T) {
	svc, _ := newGLServiceForTests()
	ctx := context.Background()

	cases := map[string]string{
		"asset":     "debit",
		"expense":   "debit",
		"liability": "credit",
		"equity":    "credit",
		"income":    "credit",
	}
	i := 0
	for typ, side := range cases {
		i++
		account, err := svc.Create(ctx, glInput("C"+string(rune('A'+i)), "acct "+typ, strings.ToUpper(typ), "ngn", "", "gl:"+typ))
		require.NoError(t, err, typ)
		require.Equal(t, side, account.NormalSide, typ)
		require.Equal(t, typ, account.Type)
		require.Equal(t, "NGN", account.Currency)
		require.Equal(t, models.GLAccountStatusActive, account.Status)
	}
}

func TestGLCreateAllowsContraAccount(t *testing.T) {
	svc, _ := newGLServiceForTests()
	in := glInput("1900", "Accumulated depreciation", "asset", "NGN", "", "gl:assets:acc_dep")
	in.NormalSide = "credit"
	account, err := svc.Create(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, "credit", account.NormalSide)
}

func TestGLCreateValidation(t *testing.T) {
	currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{
		"NGN": {Precision: 2, Enabled: true},
		"USD": {Precision: 2, Enabled: true},
		"OFF": {Precision: 2, Enabled: false},
	})
	t.Cleanup(restoreGLTestCurrencies)

	svc, _ := newGLServiceForTests()
	ctx := context.Background()

	_, err := svc.Create(ctx, glInput("1000", "Assets NGN", "asset", "NGN", "", "gl:assets"))
	require.NoError(t, err)
	_, err = svc.Create(ctx, glInput("1001", "Cash USD", "asset", "USD", "", "gl:assets:cash_usd"))
	require.NoError(t, err)
	_, err = svc.Create(ctx, glInput("2000", "Liabilities NGN", "liability", "NGN", "", "gl:liabilities"))
	require.NoError(t, err)

	closed := models.GLAccountStatusClosed
	_, err = svc.Patch(ctx, "2000", PatchGLAccountInput{Status: &closed})
	require.NoError(t, err)

	tests := []struct {
		name    string
		input   CreateGLAccountInput
		message string
	}{
		{"missing code", glInput("", "x", "asset", "NGN", "", "gl:x"), "code is required"},
		{"bad code", glInput("a b", "x", "asset", "NGN", "", "gl:x"), "code is required"},
		{"missing name", glInput("3000", " ", "asset", "NGN", "", "gl:x"), "name is required"},
		{"bad type", glInput("3000", "x", "revenue", "NGN", "", "gl:x"), "type must be one of"},
		{"bad side", func() CreateGLAccountInput {
			in := glInput("3000", "x", "asset", "NGN", "", "gl:x")
			in.NormalSide = "left"
			return in
		}(), "normal_side must be one of"},
		{"unknown currency", glInput("3000", "x", "asset", "XXX", "", "gl:x"), "currency XXX is not enabled"},
		{"disabled currency", glInput("3000", "x", "asset", "OFF", "", "gl:x"), "currency OFF is not enabled"},
		{"missing currency", glInput("3000", "x", "asset", "", "", "gl:x"), "currency is required"},
		{"address without gl prefix", glInput("3000", "x", "asset", "NGN", "", "users:1:wallets:NGN:available"), "address must start with gl:"},
		{"address only prefix", glInput("3000", "x", "asset", "NGN", "", "gl:"), "not a valid ledger address"},
		{"address empty segment", glInput("3000", "x", "asset", "NGN", "", "gl::cash"), "not a valid ledger address"},
		{"address wildcard", glInput("3000", "x", "asset", "NGN", "", "gl:cash:*"), "not a valid ledger address"},
		{"address with space", glInput("3000", "x", "asset", "NGN", "", "gl:cash flow"), "not a valid ledger address"},
		{"bad ledger", func() CreateGLAccountInput {
			in := glInput("3000", "x", "asset", "NGN", "", "gl:x")
			in.Ledger = "my ledger"
			return in
		}(), "ledger is required"},
		{"parent is self", glInput("3000", "x", "asset", "NGN", "3000", "gl:x"), "cannot be the account itself"},
		{"parent missing", glInput("3000", "x", "asset", "NGN", "9999", "gl:x"), "parent_code 9999 does not exist"},
		{"parent other currency", glInput("3000", "x", "asset", "NGN", "1001", "gl:x"), "parent 1001 has currency USD"},
		{"parent other type", glInput("3000", "x", "asset", "NGN", "2000", "gl:x"), "parent 2000 has type liability"},
		{"parent closed", glInput("3000", "x", "liability", "NGN", "2000", "gl:x"), "parent 2000 is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Create(ctx, tc.input)
			require.ErrorIs(t, err, ErrGLAccountValidation)
			require.Contains(t, err.Error(), tc.message)
		})
	}
}

func TestGLCreateUniqueness(t *testing.T) {
	svc, _ := newGLServiceForTests()
	ctx := context.Background()

	_, err := svc.Create(ctx, glInput("1000", "Cash", "asset", "NGN", "", "gl:cash"))
	require.NoError(t, err)

	// same code, even identical content: POST is not idempotent, only import is
	_, err = svc.Create(ctx, glInput("1000", "Cash", "asset", "NGN", "", "gl:cash"))
	require.ErrorIs(t, err, ErrGLAccountAlreadyExists)

	// same code in another currency is still the same code
	_, err = svc.Create(ctx, glInput("1000", "Cash", "asset", "USD", "", "gl:cash_usd"))
	require.ErrorIs(t, err, ErrGLAccountAlreadyExists)

	// same ledger + address under another code
	_, err = svc.Create(ctx, glInput("1001", "Other", "asset", "NGN", "", "gl:cash"))
	require.ErrorIs(t, err, ErrGLAccountAlreadyExists)

	// same address in another ledger is fine
	other := glInput("1002", "Cash elsewhere", "asset", "NGN", "", "gl:cash")
	other.Ledger = "channels-NGN"
	_, err = svc.Create(ctx, other)
	require.NoError(t, err)
}

func TestGLImportCycleDetection(t *testing.T) {
	ctx := context.Background()

	t.Run("two node cycle", func(t *testing.T) {
		svc, repo := newGLServiceForTests()
		report, err := svc.Import(ctx, []CreateGLAccountInput{
			glInput("A", "a", "asset", "NGN", "B", "gl:a"),
			glInput("B", "b", "asset", "NGN", "A", "gl:b"),
		}, true)
		require.NoError(t, err)
		require.False(t, report.CanCommit)
		require.Equal(t, 2, report.Invalid)
		require.Contains(t, report.Rows[0].Errors[0], "cycle: A -> B -> A")
		require.Contains(t, report.Rows[1].Errors[0], "cycle: B -> A -> B")
		require.Empty(t, repo.accounts)
	})

	t.Run("three node cycle with a healthy child and a healthy root", func(t *testing.T) {
		svc, _ := newGLServiceForTests()
		report, err := svc.Import(ctx, []CreateGLAccountInput{
			glInput("A", "a", "asset", "NGN", "C", "gl:a"),
			glInput("B", "b", "asset", "NGN", "A", "gl:b"),
			glInput("C", "c", "asset", "NGN", "B", "gl:c"),
			glInput("D", "d", "asset", "NGN", "C", "gl:d"), // hangs off the cycle
			glInput("E", "e", "asset", "NGN", "", "gl:e"),
		}, true)
		require.NoError(t, err)
		require.Equal(t, []string{"invalid", "invalid", "invalid", "invalid", "create"}, rowStatuses(report))
		require.Contains(t, report.Rows[0].Errors[0], "cycle")
		require.Contains(t, report.Rows[3].Errors[0], "does not exist (or is rejected")
	})

	t.Run("no false positive on a long valid chain", func(t *testing.T) {
		svc, _ := newGLServiceForTests()
		report, err := svc.Import(ctx, []CreateGLAccountInput{
			glInput("D", "d", "asset", "NGN", "C", "gl:d"),
			glInput("C", "c", "asset", "NGN", "B", "gl:c"),
			glInput("B", "b", "asset", "NGN", "A", "gl:b"),
			glInput("A", "a", "asset", "NGN", "", "gl:a"),
		}, true)
		require.NoError(t, err)
		require.True(t, report.CanCommit)
		require.Equal(t, 4, report.ToCreate)
	})
}

func TestGLImportDryRunThenCommit(t *testing.T) {
	svc, repo := newGLServiceForTests()
	ctx := context.Background()

	// children listed before their parents: the repository stub enforces the parent foreign key
	rows := []CreateGLAccountInput{
		glInput("1010", "Operating cash", "asset", "NGN", "1000", "gl:assets:cash:operating"),
		glInput("1000", "Assets", "asset", "NGN", "", "gl:assets"),
		glInput("2000", "Customer deposits", "liability", "NGN", "", "gl:liabilities:deposits"),
	}

	dry, err := svc.Import(ctx, rows, true)
	require.NoError(t, err)
	require.True(t, dry.DryRun)
	require.False(t, dry.Committed)
	require.True(t, dry.CanCommit)
	require.Equal(t, 3, dry.Total)
	require.Equal(t, 3, dry.ToCreate)
	require.Equal(t, []string{"create", "create", "create"}, rowStatuses(dry))
	require.Empty(t, repo.accounts, "dry run must not write")
	require.Zero(t, repo.exclusiveRuns, "dry run must not take the write lock")

	committed, err := svc.Import(ctx, rows, false)
	require.NoError(t, err)
	require.False(t, committed.DryRun)
	require.True(t, committed.Committed)
	require.Len(t, repo.accounts, 3)
	require.Equal(t, "debit", repo.accounts["1010"].NormalSide)
	require.Equal(t, "credit", repo.accounts["2000"].NormalSide)
	require.Equal(t, "1000", *repo.accounts["1010"].ParentCode)
}

func TestGLImportIsIdempotent(t *testing.T) {
	svc, repo := newGLServiceForTests()
	ctx := context.Background()

	rows := []CreateGLAccountInput{
		glInput("1000", "Assets", "asset", "NGN", "", "gl:assets"),
		glInput("1010", "Cash", "asset", "ngn", "1000", "gl:assets:cash"),
	}
	_, err := svc.Import(ctx, rows, false)
	require.NoError(t, err)
	before := snapshotGL(repo)

	// again, with cosmetic differences that normalize to the same thing (case, spaces, explicit default side)
	again := []CreateGLAccountInput{
		glInput(" 1000 ", " Assets ", "ASSET", "NGN", "", "gl:assets"),
		glInput("1010", "Cash", "asset", "NGN", "1000", "gl:assets:cash"),
	}
	again[0].NormalSide = "debit"

	for _, dryRun := range []bool{true, false} {
		report, err := svc.Import(ctx, again, dryRun)
		require.NoError(t, err)
		require.True(t, report.CanCommit)
		require.Equal(t, 2, report.Unchanged)
		require.Zero(t, report.ToCreate)
		require.Equal(t, []string{"unchanged", "unchanged"}, rowStatuses(report))
		require.Equal(t, !dryRun, report.Committed)
	}
	require.Equal(t, before, snapshotGL(repo), "re-import must not change anything")

	// a subset and a superset are fine too: only the new row is created
	superset := append(again, glInput("1020", "Petty cash", "asset", "NGN", "1000", "gl:assets:petty"))
	report, err := svc.Import(ctx, superset, false)
	require.NoError(t, err)
	require.Equal(t, 1, report.ToCreate)
	require.Equal(t, 2, report.Unchanged)
	require.Len(t, repo.accounts, 3)
}

func TestGLImportReportsConflictsAndAppliesNothing(t *testing.T) {
	svc, repo := newGLServiceForTests()
	ctx := context.Background()

	_, err := svc.Import(ctx, []CreateGLAccountInput{
		glInput("1000", "Assets", "asset", "NGN", "", "gl:assets"),
		glInput("1010", "Cash", "asset", "NGN", "1000", "gl:assets:cash"),
	}, false)
	require.NoError(t, err)
	before := snapshotGL(repo)

	rows := []CreateGLAccountInput{
		glInput("5000", "Brand new", "expense", "NGN", "", "gl:expenses"),    // would be created
		glInput("1010", "Cash and bank", "liability", "NGN", "", "gl:other"), // differs on several fields
		glInput("5001", "Address thief", "asset", "NGN", "", "gl:assets"),    // address belongs to 1000
	}

	for _, dryRun := range []bool{true, false} {
		report, err := svc.Import(ctx, rows, dryRun)
		if dryRun {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, ErrGLImportRejected)
			require.NotNil(t, report, "the report accompanies the rejection")
		}
		require.False(t, report.CanCommit)
		require.False(t, report.Committed)
		require.Equal(t, 2, report.Conflicts)
		require.Equal(t, 1, report.ToCreate, "the new row is valid but still not applied")
		require.Equal(t, []string{"create", "conflict", "conflict"}, rowStatuses(report))

		conflict := strings.Join(report.Rows[1].Conflicts, "\n")
		require.Contains(t, conflict, `name: existing "Cash", import "Cash and bank"`)
		require.Contains(t, conflict, `type: existing "asset", import "liability"`)
		require.Contains(t, conflict, `normal_side: existing "debit", import "credit"`)
		require.Contains(t, conflict, `address: existing "gl:assets:cash", import "gl:other"`)
		require.Contains(t, conflict, `parent_code: existing "1000", import ""`)
		require.Contains(t, report.Rows[2].Conflicts[0], "already used by account 1000")
	}
	require.Equal(t, before, snapshotGL(repo), "nothing is applied when any row conflicts")
}

func TestGLImportInvalidRowsBlockTheWholeBatch(t *testing.T) {
	svc, repo := newGLServiceForTests()
	ctx := context.Background()

	rows := []CreateGLAccountInput{
		glInput("1000", "Assets", "asset", "NGN", "", "gl:assets"),
		glInput("1001", "Bad", "asset", "NGN", "", "assets:bad"),
		glInput("1000", "Dup code", "asset", "NGN", "", "gl:dup_code"),
		glInput("1002", "Dup address", "asset", "NGN", "", "gl:assets"),
	}
	report, err := svc.Import(ctx, rows, false)
	require.ErrorIs(t, err, ErrGLImportRejected)
	require.Equal(t, []string{"create", "invalid", "invalid", "invalid"}, rowStatuses(report))
	require.Contains(t, report.Rows[2].Errors[0], "duplicate code in this import, first seen at row 1")
	require.Contains(t, report.Rows[3].Errors[0], "already used by row 1 of this import")
	require.Equal(t, 3, report.Invalid)
	require.Empty(t, repo.accounts)
}

func TestGLImportRollsBackWhenTheDatabaseFailsMidWay(t *testing.T) {
	svc, repo := newGLServiceForTests()
	repo.failCreateAfter = 2

	_, err := svc.Import(context.Background(), []CreateGLAccountInput{
		glInput("1", "a", "asset", "NGN", "", "gl:a"),
		glInput("2", "b", "asset", "NGN", "", "gl:b"),
		glInput("3", "c", "asset", "NGN", "", "gl:c"),
	}, false)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrGLImportRejected)
	require.Empty(t, repo.accounts, "the two accounts created before the failure must be rolled back")
}

func TestGLImportMetadataIsOnlyComparedWhenProvided(t *testing.T) {
	svc, _ := newGLServiceForTests()
	ctx := context.Background()

	first := glInput("1000", "Assets", "asset", "NGN", "", "gl:assets")
	first.Metadata = map[string]any{"owner": "finance"}
	_, err := svc.Import(ctx, []CreateGLAccountInput{first}, false)
	require.NoError(t, err)

	noMetadata := glInput("1000", "Assets", "asset", "NGN", "", "gl:assets")
	report, err := svc.Import(ctx, []CreateGLAccountInput{noMetadata}, true)
	require.NoError(t, err)
	require.Equal(t, []string{"unchanged"}, rowStatuses(report))

	different := first
	different.Metadata = map[string]any{"owner": "treasury"}
	report, err = svc.Import(ctx, []CreateGLAccountInput{different}, true)
	require.NoError(t, err)
	require.Equal(t, []string{"conflict"}, rowStatuses(report))
}

func TestGLImportLimits(t *testing.T) {
	svc, _ := newGLServiceForTests()
	_, err := svc.Import(context.Background(), nil, true)
	require.ErrorIs(t, err, ErrGLAccountValidation)

	rows := make([]CreateGLAccountInput, GLImportMaxRows+1)
	_, err = svc.Import(context.Background(), rows, true)
	require.ErrorIs(t, err, ErrGLAccountValidation)
	require.Contains(t, err.Error(), "maximum")
}

func TestGLPatch(t *testing.T) {
	svc, _ := newGLServiceForTests()
	ctx := context.Background()

	_, err := svc.Create(ctx, glInput("1000", "Assets", "asset", "NGN", "", "gl:assets"))
	require.NoError(t, err)
	_, err = svc.Create(ctx, glInput("1010", "Cash", "asset", "NGN", "1000", "gl:assets:cash"))
	require.NoError(t, err)

	name := "  Cash and equivalents "
	updated, err := svc.Patch(ctx, "1010", PatchGLAccountInput{Name: &name, Metadata: map[string]any{"k": "v"}})
	require.NoError(t, err)
	require.Equal(t, "Cash and equivalents", updated.Name)
	require.Equal(t, map[string]any{"k": "v"}, updated.Metadata)
	require.Equal(t, "gl:assets:cash", updated.Address, "immutable fields are untouched")

	// metadata omitted: untouched
	updated, err = svc.Patch(ctx, "1010", PatchGLAccountInput{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"k": "v"}, updated.Metadata)

	empty := " "
	_, err = svc.Patch(ctx, "1010", PatchGLAccountInput{Name: &empty})
	require.ErrorIs(t, err, ErrGLAccountValidation)

	bogus := "archived"
	_, err = svc.Patch(ctx, "1010", PatchGLAccountInput{Status: &bogus})
	require.ErrorIs(t, err, ErrGLAccountValidation)

	_, err = svc.Patch(ctx, "nope", PatchGLAccountInput{Name: &name})
	require.ErrorIs(t, err, ErrGLAccountNotFound)

	closed, active := "closed", "active"
	_, err = svc.Patch(ctx, "1000", PatchGLAccountInput{Status: &closed})
	require.ErrorIs(t, err, ErrGLAccountValidation, "a parent with an active child cannot be closed")
	require.Contains(t, err.Error(), "1010")

	closedChild, err := svc.Patch(ctx, "1010", PatchGLAccountInput{Status: &closed})
	require.NoError(t, err)
	require.Equal(t, "closed", closedChild.Status)
	_, err = svc.Patch(ctx, "1000", PatchGLAccountInput{Status: &closed})
	require.NoError(t, err)

	_, err = svc.Patch(ctx, "1010", PatchGLAccountInput{Status: &active})
	require.ErrorIs(t, err, ErrGLAccountValidation, "a child cannot be reopened under a closed parent")
	reopened, err := svc.Patch(ctx, "1000", PatchGLAccountInput{Status: &active})
	require.NoError(t, err)
	require.Equal(t, "active", reopened.Status)
}

func TestParseGLAccountsCSV(t *testing.T) {
	csvData := "\xef\xbb\xbfcode,name,type,normal_side,currency,parent_code,ledger,address\n" +
		"1000,Assets,asset,,NGN,,ledgertrack,gl:assets\n" +
		"1010,\"Cash, operating\",asset,debit,NGN,1000,ledgertrack,gl:assets:cash\n"
	rows, err := ParseGLAccountsCSV(strings.NewReader(csvData))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "Cash, operating", rows[1].Name)
	require.Equal(t, "1000", rows[1].ParentCode)
	require.Equal(t, "debit", rows[1].NormalSide)
	require.Equal(t, "gl:assets", rows[0].Address)

	t.Run("optional columns may be omitted and order is free", func(t *testing.T) {
		rows, err := ParseGLAccountsCSV(strings.NewReader("address,ledger,currency,type,name,code\ngl:a,ledgertrack,NGN,asset,A,1\n"))
		require.NoError(t, err)
		require.Equal(t, CreateGLAccountInput{Code: "1", Name: "A", Type: "asset", Currency: "NGN", Ledger: "ledgertrack", Address: "gl:a"}, rows[0])
	})

	for name, data := range map[string]string{
		"empty":           "",
		"missing column":  "code,name,type,currency,ledger\n1,a,asset,NGN,ledgertrack\n",
		"unknown column":  "code,name,type,currency,ledger,address,status\n1,a,asset,NGN,ledgertrack,gl:a,active\n",
		"duplicate":       "code,code,name,type,currency,ledger,address\n1,1,a,asset,NGN,ledgertrack,gl:a\n",
		"ragged row":      "code,name,type,currency,ledger,address\n1,a,asset\n",
		"unterminated \"": "code,name,type,currency,ledger,address\n1,\"a,asset,NGN,ledgertrack,gl:a\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseGLAccountsCSV(strings.NewReader(data))
			require.ErrorIs(t, err, ErrGLAccountValidation)
		})
	}
}

func rowStatuses(report *GLImportReport) []string {
	ret := make([]string, len(report.Rows))
	for i, row := range report.Rows {
		ret[i] = row.Status
	}
	return ret
}

func snapshotGL(repo *glRepositoryStub) map[string]models.GLAccount {
	ret := make(map[string]models.GLAccount, len(repo.accounts))
	for code, account := range repo.accounts {
		ret[code] = *account
	}
	return ret
}

func restoreGLTestCurrencies() {
	currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{
		"USD": {Precision: 2, Enabled: true},
		"EUR": {Precision: 2, Enabled: true},
		"BTC": {Precision: 8, Enabled: true},
		"NGN": {Precision: 2, Enabled: true},
		"GHS": {Precision: 2, Enabled: true},
		"KES": {Precision: 2, Enabled: true},
		"ZMW": {Precision: 2, Enabled: true},
	})
}
