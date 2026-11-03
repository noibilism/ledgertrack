package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/internal/cba/models"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

type fakeGLBalances struct {
	// balances is keyed by ledger + "|" + address
	balances map[string]int64
	calls    []string
	lastPIT  *libtime.Time
	err      error
}

func (f *fakeGLBalances) read(_ systemcontroller.Controller) glBalanceReader {
	return func(_ context.Context, ledger, address, asset string, pit *libtime.Time) (int64, error) {
		if f.err != nil {
			return 0, f.err
		}
		f.calls = append(f.calls, ledger+"|"+address+"|"+asset)
		f.lastPIT = pit
		return f.balances[ledger+"|"+address], nil
	}
}

func newGLReportingForTests(t *testing.T, balances map[string]int64) (*DefaultGLReportingService, *glRepositoryStub, *fakeGLBalances) {
	t.Helper()
	repo := newGLRepositoryStub()
	fake := &fakeGLBalances{balances: balances}
	svc := NewGLReportingService(repo).(*DefaultGLReportingService)
	svc.readerFor = fake.read
	return svc, repo, fake
}

func seedGL(t *testing.T, repo *glRepositoryStub, rows ...CreateGLAccountInput) {
	t.Helper()
	_, err := NewGLAccountService(repo).Import(context.Background(), rows, false)
	require.NoError(t, err)
}

func TestGLAsset(t *testing.T) {
	asset, err := glAsset(" ngn ")
	require.NoError(t, err)
	require.Equal(t, "NGN/2", asset)

	asset, err = glAsset("BTC")
	require.NoError(t, err)
	require.Equal(t, "BTC/8", asset, "precision comes from the registry, not a constant")

	_, err = glAsset("XXX")
	require.Error(t, err)
}

func TestGLTrialBalanceBalanced(t *testing.T) {
	svc, repo, fake := newGLReportingForTests(t, map[string]int64{
		"ledgertrack|gl:assets:cash":         -150_000, // debit
		"ledgertrack|gl:liabilities:deposit": 100_000,  // credit
		"revenue-NGN|gl:income:fees":         50_000,   // credit
	})
	seedGL(t, repo,
		glInput("1000", "Cash", "asset", "NGN", "", "gl:assets:cash"),
		glInput("2000", "Deposits", "liability", "NGN", "", "gl:liabilities:deposit"),
		func() CreateGLAccountInput {
			in := glInput("4000", "Fee income", "income", "NGN", "", "gl:income:fees")
			in.Ledger = "revenue-NGN"
			return in
		}(),
		glInput("5000", "Interest expense", "expense", "NGN", "", "gl:expenses:interest"),
		glInput("1500", "Cash USD", "asset", "USD", "", "gl:assets:cash_usd"), // other currency, must be ignored
	)

	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "ngn"})
	require.NoError(t, err)

	require.Equal(t, "NGN", report.Currency)
	require.Equal(t, "NGN/2", report.Asset)
	require.Len(t, report.Accounts, 4)
	require.True(t, report.Balanced)
	require.Zero(t, report.Difference)
	require.EqualValues(t, 150_000, report.TotalDebits)
	require.EqualValues(t, 150_000, report.TotalCredits)
	require.Len(t, fake.calls, 7, "one read per active NGN account, plus the wallet and control reads")
	require.NotContains(t, fake.calls, "ledgertrack|gl:assets:cash_usd|USD/2")

	byCode := map[string]GLAccountBalance{}
	for _, line := range report.Accounts {
		byCode[line.Code] = line
	}
	require.EqualValues(t, 150_000, byCode["1000"].Debit)
	require.EqualValues(t, 150_000, byCode["1000"].NaturalBalance)
	require.False(t, byCode["1000"].Abnormal)
	require.EqualValues(t, 100_000, byCode["2000"].Credit)
	require.EqualValues(t, 100_000, byCode["2000"].NaturalBalance)
	require.EqualValues(t, 50_000, byCode["4000"].Credit)
	require.Zero(t, byCode["5000"].Debit+byCode["5000"].Credit)

	require.Len(t, report.Subtotals, 5)
	types := make([]string, 0, 5)
	for _, st := range report.Subtotals {
		types = append(types, st.Type)
	}
	require.Equal(t, []string{"asset", "liability", "equity", "income", "expense"}, types)
	require.Equal(t, GLTrialBalanceTypeSubtotal{Type: "asset", Accounts: 1, Debit: 150_000, Net: 150_000}, report.Subtotals[0])
	require.Equal(t, GLTrialBalanceTypeSubtotal{Type: "liability", Accounts: 1, Credit: 100_000, Net: -100_000}, report.Subtotals[1])
	require.Equal(t, GLTrialBalanceTypeSubtotal{Type: "equity"}, report.Subtotals[2])
	require.Equal(t, GLTrialBalanceTypeSubtotal{Type: "income", Accounts: 1, Credit: 50_000, Net: -50_000}, report.Subtotals[3])
	require.Equal(t, GLTrialBalanceTypeSubtotal{Type: "expense", Accounts: 1}, report.Subtotals[4])
}

func TestGLTrialBalanceDetectsImbalance(t *testing.T) {
	svc, repo, _ := newGLReportingForTests(t, map[string]int64{
		"ledgertrack|gl:assets:cash":         -150_000,
		"ledgertrack|gl:liabilities:deposit": 100_000,
	})
	seedGL(t, repo,
		glInput("1000", "Cash", "asset", "NGN", "", "gl:assets:cash"),
		glInput("2000", "Deposits", "liability", "NGN", "", "gl:liabilities:deposit"),
	)

	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)
	require.False(t, report.Balanced)
	require.EqualValues(t, 50_000, report.Difference)
}

func TestGLTrialBalanceFlagsAbnormalBalancesAndSkipsClosedAccounts(t *testing.T) {
	svc, repo, fake := newGLReportingForTests(t, map[string]int64{
		"ledgertrack|gl:assets:cash":  2_000, // an overdrawn asset
		"ledgertrack|gl:assets:old":   9_999, // closed, must not be read
		"ledgertrack|gl:equity:other": -2_000,
	})
	seedGL(t, repo,
		glInput("1000", "Cash", "asset", "NGN", "", "gl:assets:cash"),
		glInput("1999", "Old", "asset", "NGN", "", "gl:assets:old"),
		glInput("3000", "Equity", "equity", "NGN", "", "gl:equity:other"),
	)
	closed := models.GLAccountStatusClosed
	_, err := NewGLAccountService(repo).Patch(context.Background(), "1999", PatchGLAccountInput{Status: &closed})
	require.NoError(t, err)

	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)
	require.Len(t, report.Accounts, 2)
	require.NotContains(t, fake.calls, "ledgertrack|gl:assets:old|NGN/2")
	require.True(t, report.Balanced)

	for _, line := range report.Accounts {
		switch line.Code {
		case "1000":
			require.True(t, line.Abnormal, "debit-normal account sitting on the credit side")
			require.EqualValues(t, -2_000, line.NaturalBalance)
			require.EqualValues(t, 2_000, line.Credit)
		case "3000":
			require.True(t, line.Abnormal, "credit-normal account sitting on the debit side")
			require.EqualValues(t, 2_000, line.Debit)
		}
	}
}

func TestGLTrialBalanceContraAccountUsesItsOwnNormalSide(t *testing.T) {
	svc, repo, _ := newGLReportingForTests(t, map[string]int64{"ledgertrack|gl:assets:acc_dep": 300})
	contra := glInput("1900", "Accumulated depreciation", "asset", "NGN", "", "gl:assets:acc_dep")
	contra.NormalSide = "credit"
	seedGL(t, repo, contra)

	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)
	require.False(t, report.Accounts[0].Abnormal)
	require.EqualValues(t, 300, report.Accounts[0].NaturalBalance)
}

func TestGLTrialBalanceAsOfAndValidation(t *testing.T) {
	svc, repo, fake := newGLReportingForTests(t, nil)
	seedGL(t, repo, glInput("1000", "Cash", "asset", "NGN", "", "gl:assets:cash"))

	asOf := libtime.Now()
	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN", AsOf: &asOf})
	require.NoError(t, err)
	require.NotNil(t, report.AsOf)
	require.Equal(t, &asOf, fake.lastPIT)

	_, err = svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{})
	require.ErrorIs(t, err, ErrGLReportingValidation)
	_, err = svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "XXX"})
	require.ErrorIs(t, err, ErrGLReportingValidation)

	fake.err = errors.New("ledger exploded")
	_, err = svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.ErrorContains(t, err, "gl account 1000")
}

func TestGLTrialBalanceWithNoAccountsIsEmptyAndBalanced(t *testing.T) {
	svc, _, _ := newGLReportingForTests(t, nil)
	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "KES"})
	require.NoError(t, err)
	require.Empty(t, report.Accounts)
	require.NotNil(t, report.Accounts, "serialises as [] not null")
	require.True(t, report.Balanced)
}

func TestGLAccountBalance(t *testing.T) {
	svc, repo, fake := newGLReportingForTests(t, map[string]int64{"ledgertrack|gl:liabilities:deposit": 4_200})
	seedGL(t, repo, glInput("2000", "Deposits", "liability", "NGN", "", "gl:liabilities:deposit"))

	pit := libtime.Now()
	balance, err := svc.AccountBalance(context.Background(), nil, "2000", &pit)
	require.NoError(t, err)
	require.EqualValues(t, 4_200, balance.Balance)
	require.EqualValues(t, 4_200, balance.Credit)
	require.EqualValues(t, 4_200, balance.NaturalBalance)
	require.Equal(t, "NGN/2", balance.Asset)
	require.NotNil(t, balance.PIT)
	require.Equal(t, &pit, fake.lastPIT)

	_, err = svc.AccountBalance(context.Background(), nil, "9999", nil)
	require.ErrorIs(t, err, ErrGLAccountNotFound)
}

const (
	walletsAvailable = "ledgertrack|users::wallets:NGN:available"
	walletsLien      = "ledgertrack|users::wallets:NGN:lien"
	controlAll       = "ledgertrack|system:control:NGN:"
)

func TestGLTrialBalanceRollsWalletsIntoCustomerWallets(t *testing.T) {
	// Wallets hold 90_000 available and 10_000 lien, funded 100_000 from system:control (the control balance covers its shards).
	// A 500 fee was paid out of a wallet into fee income.
	svc, repo, _ := newGLReportingForTests(t, map[string]int64{
		walletsAvailable:                       89_500,
		walletsLien:                            10_000,
		controlAll:                             -100_000,
		"ledgertrack|gl:income:fees":           500,
		"ledgertrack|gl:liabilities:customers": 0,
	})
	seedGL(t, repo,
		glInput("201100.NGN", "Customer Wallets", "liability", "NGN", "", "gl:liabilities:customers"),
		glInput("401101.NGN", "Fee income", "income", "NGN", "", "gl:income:fees"),
	)

	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)

	require.Equal(t, GLWalletRollup{
		Ledger: "ledgertrack", AccountCode: "201100.NGN",
		Available: 89_500, Lien: 10_000, Total: 99_500, Control: -100_000,
	}, report.Wallets)
	require.True(t, report.Balanced, "difference %d", report.Difference)
	require.EqualValues(t, 100_000, report.TotalDebits)
	require.EqualValues(t, 100_000, report.TotalCredits)

	byCode := map[string]GLAccountBalance{}
	for _, line := range report.Accounts {
		byCode[line.Code] = line
	}
	wallets := byCode["201100.NGN"]
	require.EqualValues(t, 99_500, wallets.Credit)
	require.EqualValues(t, 99_500, wallets.WalletBalance)
	require.Zero(t, wallets.PostedBalance)
	require.False(t, wallets.Synthetic)
	require.False(t, wallets.Abnormal)

	fees := byCode["401101.NGN"]
	require.EqualValues(t, 500, fees.Credit)
	require.False(t, fees.Abnormal, "fee income paid out of a wallet is a credit")

	control := byCode["system:control"]
	require.True(t, control.Synthetic)
	require.EqualValues(t, 100_000, control.Debit)
	require.EqualValues(t, 100_000, control.NaturalBalance)
	require.False(t, control.Abnormal)
}

func TestGLTrialBalanceKeepsPostedBalanceOfCustomerWalletsApart(t *testing.T) {
	svc, repo, _ := newGLReportingForTests(t, map[string]int64{
		walletsAvailable:                       7_000,
		controlAll:                             -7_250,
		"ledgertrack|gl:liabilities:customers": 250,
	})
	seedGL(t, repo, glInput("201100.NGN", "Customer Wallets", "liability", "NGN", "", "gl:liabilities:customers"))

	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)
	require.True(t, report.Balanced)
	line := report.Accounts[0]
	require.EqualValues(t, 7_250, line.Credit)
	require.EqualValues(t, 250, line.PostedBalance)
	require.EqualValues(t, 7_000, line.WalletBalance)
}

func TestGLTrialBalanceAddsSyntheticWalletsLineWhenAccountIsNotRegistered(t *testing.T) {
	svc, _, _ := newGLReportingForTests(t, map[string]int64{
		walletsAvailable: 4_000,
		controlAll:       -4_000,
	})
	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)
	require.True(t, report.Balanced)
	require.Len(t, report.Accounts, 2)
	require.Equal(t, "201100", report.Accounts[0].Code)
	require.True(t, report.Accounts[0].Synthetic)
	require.EqualValues(t, 4_000, report.Accounts[0].Credit)
	require.Equal(t, "201100", report.Wallets.AccountCode)
}

func TestGLTrialBalanceShowsMovementOutsideTheMappedAddresses(t *testing.T) {
	// 5_000 left a wallet for an address the registry does not know: the trial balance cannot see the
	// other side, so it reports the difference instead of hiding it.
	svc, _, _ := newGLReportingForTests(t, map[string]int64{
		walletsAvailable: 5_000,
		controlAll:       -10_000,
	})
	report, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN"})
	require.NoError(t, err)
	require.False(t, report.Balanced)
	require.EqualValues(t, 5_000, report.Difference)
}

func TestGLTrialBalanceWalletReadsFollowTheAsOfDate(t *testing.T) {
	svc, _, fake := newGLReportingForTests(t, nil)
	asOf := libtime.Now()
	_, err := svc.TrialBalance(context.Background(), nil, GLTrialBalanceFilter{Currency: "NGN", AsOf: &asOf})
	require.NoError(t, err)
	require.Contains(t, fake.calls, "ledgertrack|users::wallets:NGN:available|NGN/2")
	require.Contains(t, fake.calls, "ledgertrack|users::wallets:NGN:lien|NGN/2")
	require.Contains(t, fake.calls, "ledgertrack|system:control:NGN:|NGN/2")
	require.Equal(t, &asOf, fake.lastPIT)
}
