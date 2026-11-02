package services

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

type stubLookup struct {
	LegacyLookupService
	client  *models.Client
	account *models.Account
}

func (s stubLookup) Client(context.Context, string) (*models.Client, error) {
	if s.client == nil {
		return nil, ErrLegacyNotFound
	}
	return s.client, nil
}

func (s stubLookup) Account(context.Context, string) (*models.Account, error) {
	if s.account == nil {
		return nil, ErrLegacyNotFound
	}
	return s.account, nil
}

type stubRoles struct {
	repositories.WalletRoleRepository
	eligible  []models.Account
	classify  repositories.ClassifyInput
	create    repositories.CreateR2SubaccountInput
	returnErr error
}

func (s *stubRoles) EligibleWallets(context.Context, uuid.UUID, string) ([]models.Account, error) {
	return s.eligible, nil
}

func (s *stubRoles) Classify(_ context.Context, in repositories.ClassifyInput) (*models.Account, bool, error) {
	s.classify = in
	return &models.Account{ID: in.AccountID}, false, s.returnErr
}

func (s *stubRoles) CreateR2Subaccount(_ context.Context, in repositories.CreateR2SubaccountInput) (*models.Account, bool, error) {
	s.create = in
	return &models.Account{ID: in.AccountID}, false, s.returnErr
}

type stubAccounts struct{ repositories.AccountRepository }

func (stubAccounts) GetByNumber(context.Context, string) (*models.Account, error) {
	return nil, postgres.ErrNotFound
}

type stubProducts struct {
	repositories.ProductRepository
	product *models.Product
}

func (s stubProducts) Get(context.Context, uuid.UUID) (*models.Product, error) {
	if s.product == nil {
		return nil, postgres.ErrNotFound
	}
	return s.product, nil
}

func wallet(role *string, relationship *string) models.Account {
	return models.Account{ID: uuid.New(), WalletRole: role, R2RelationshipPublicID: relationship}
}

func ptr(s string) *string { return &s }

func newRoleService(roles *stubRoles, lookup stubLookup, products stubProducts) WalletRoleService {
	return NewWalletRoleService(lookup, roles, stubAccounts{}, products)
}

func TestResolveMainWallet(t *testing.T) {
	t.Parallel()
	client := &models.Client{ID: uuid.New(), ClientNumber: "C1"}
	main, r2 := ptr(models.WalletRoleMerchantMain), ptr(models.WalletRoleR2Subaccount)

	cases := map[string]struct {
		wallets []models.Account
		ok      bool
		pick    int
	}{
		"single merchant_main":                {[]models.Account{wallet(main, nil)}, true, 0},
		"main beats unclassified":             {[]models.Account{wallet(nil, nil), wallet(main, nil)}, true, 1},
		"single unclassified is the fallback": {[]models.Account{wallet(nil, nil)}, true, 0},
		"two unclassified is ambiguous":       {[]models.Account{wallet(nil, nil), wallet(nil, nil)}, false, 0},
		"two mains is ambiguous":              {[]models.Account{wallet(main, nil), wallet(main, nil)}, false, 0},
		"r2 only is never a main wallet":      {[]models.Account{wallet(r2, ptr("rel"))}, false, 0},
		"no wallets":                          {nil, false, 0},
		"unclassified plus r2 is the main":    {[]models.Account{wallet(r2, ptr("rel")), wallet(nil, nil)}, true, 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newRoleService(&stubRoles{eligible: tc.wallets}, stubLookup{client: client}, stubProducts{})
			got, err := svc.ResolveMainWallet(context.Background(), "ext", "NGN")
			if !tc.ok {
				require.ErrorIs(t, err, ErrWalletResolution)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wallets[tc.pick].ID, got.ID)
		})
	}

	t.Run("unknown client", func(t *testing.T) {
		t.Parallel()
		svc := newRoleService(&stubRoles{}, stubLookup{}, stubProducts{})
		_, err := svc.ResolveMainWallet(context.Background(), "ext", "NGN")
		require.ErrorIs(t, err, ErrWalletValidation)
	})
	t.Run("bad currency", func(t *testing.T) {
		t.Parallel()
		svc := newRoleService(&stubRoles{}, stubLookup{client: client}, stubProducts{})
		_, err := svc.ResolveMainWallet(context.Background(), "ext", "ngn")
		require.ErrorIs(t, err, ErrWalletValidation)
	})
}

func TestResolveR2Subaccount(t *testing.T) {
	t.Parallel()
	client := &models.Client{ID: uuid.New()}
	r2 := ptr(models.WalletRoleR2Subaccount)
	wanted := wallet(r2, ptr("rel-1"))
	roles := &stubRoles{eligible: []models.Account{wallet(r2, ptr("rel-2")), wanted, wallet(nil, nil)}}
	svc := newRoleService(roles, stubLookup{client: client}, stubProducts{})

	got, err := svc.ResolveR2Subaccount(context.Background(), "ext", "NGN", "rel-1")
	require.NoError(t, err)
	require.Equal(t, wanted.ID, got.ID)

	_, err = svc.ResolveR2Subaccount(context.Background(), "ext", "NGN", "rel-9")
	require.ErrorIs(t, err, ErrWalletResolution)

	_, err = svc.ResolveR2Subaccount(context.Background(), "ext", "NGN", "bad relationship!")
	require.ErrorIs(t, err, ErrWalletValidation)
}

func TestClassifyValidationAndMapping(t *testing.T) {
	t.Parallel()
	client := &models.Client{ID: uuid.New()}
	account := &models.Account{ID: uuid.New(), ClientID: client.ID}
	newSvc := func(roles *stubRoles) WalletRoleService {
		return newRoleService(roles, stubLookup{client: client, account: account}, stubProducts{})
	}
	good := ClassifyWalletInput{ExternalID: "e", WalletRole: models.WalletRoleR2Subaccount, Relationship: ptr("rel"), Evidence: "ticket-1"}

	for name, mutate := range map[string]func(*ClassifyWalletInput){
		"unknown role":            func(in *ClassifyWalletInput) { in.WalletRole = "other" },
		"r2 without relationship": func(in *ClassifyWalletInput) { in.Relationship = nil },
		"blank evidence":          func(in *ClassifyWalletInput) { in.Evidence = "  " },
		"bad relationship":        func(in *ClassifyWalletInput) { in.Relationship = ptr("a b") },
	} {
		in := good
		mutate(&in)
		_, _, err := newSvc(&stubRoles{}).Classify(context.Background(), account.ID.String(), in)
		require.ErrorIs(t, err, ErrWalletValidation, name)
	}

	roles := &stubRoles{}
	_, _, err := newSvc(roles).Classify(context.Background(), account.ID.String(), good)
	require.NoError(t, err)
	require.Equal(t, WalletRoleActor, roles.classify.Actor)
	require.Equal(t, client.ID, roles.classify.ClientID)

	_, _, err = newSvc(&stubRoles{returnErr: repositories.ErrWalletRoleConflict}).Classify(context.Background(), account.ID.String(), good)
	require.ErrorIs(t, err, ErrWalletRoleConflict)
	_, _, err = newSvc(&stubRoles{returnErr: repositories.ErrWalletNotFound}).Classify(context.Background(), account.ID.String(), good)
	require.ErrorIs(t, err, ErrWalletNotFound)
	_, _, err = newSvc(&stubRoles{returnErr: errors.New("boom")}).Classify(context.Background(), account.ID.String(), good)
	require.EqualError(t, err, "boom")
}

func TestCreateR2Subaccount(t *testing.T) {
	t.Parallel()
	client := &models.Client{ID: uuid.New(), ClientNumber: "C1"}
	product := &models.Product{ID: uuid.New(), Code: "WAL-NGN", Status: models.ProductStatusActive}
	in := CreateR2SubaccountInput{
		AccountID: uuid.NewString(), ExternalID: "e", ProductID: product.ID.String(), AccountName: "R2 wallet",
		AccountType: models.AccountTypeMain, Relationship: "rel-1", Evidence: "ticket-1",
	}

	roles := &stubRoles{}
	svc := newRoleService(roles, stubLookup{client: client}, stubProducts{product: product})
	_, _, err := svc.CreateR2Subaccount(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, in.AccountID, roles.create.AccountID.String(), "the caller allocated id is kept")
	require.Equal(t, "client-C1-WAL-NGN-r2-rel-1", roles.create.WalletID)
	require.Len(t, roles.create.AccountNumber, 10)

	retired := *product
	retired.Status = models.ProductStatusRetired
	_, _, err = newRoleService(&stubRoles{}, stubLookup{client: client}, stubProducts{product: &retired}).CreateR2Subaccount(context.Background(), in)
	require.ErrorIs(t, err, ErrWalletRoleConflict)

	_, _, err = newRoleService(&stubRoles{}, stubLookup{client: client}, stubProducts{}).CreateR2Subaccount(context.Background(), in)
	require.ErrorIs(t, err, ErrWalletValidation)

	bad := in
	bad.AccountID = "nope"
	_, _, err = svc.CreateR2Subaccount(context.Background(), bad)
	require.ErrorIs(t, err, ErrWalletValidation)
	bad = in
	bad.AccountType = "savings"
	_, _, err = svc.CreateR2Subaccount(context.Background(), bad)
	require.ErrorIs(t, err, ErrWalletValidation)
}
