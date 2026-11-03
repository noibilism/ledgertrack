package services

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

type provLookup struct {
	LegacyLookupService
	client    *models.Client
	owner     *models.Client
	conflicts []string
}

func (p provLookup) Client(context.Context, string) (*models.Client, error) {
	if p.client == nil {
		return nil, ErrLegacyNotFound
	}
	return p.client, nil
}

func (p provLookup) ClientByID(context.Context, string) (*models.Client, error) {
	if p.owner == nil {
		return nil, ErrLegacyNotFound
	}
	return p.owner, nil
}

func (p provLookup) ClientConflicts(context.Context, repositories.ClientIdentity) ([]string, error) {
	return p.conflicts, nil
}

type provClients struct {
	repositories.ClientRepository
	created *models.Client
}

func (p *provClients) GetByNumber(context.Context, string) (*models.Client, error) {
	return nil, postgres.ErrNotFound
}

func (p *provClients) Create(_ context.Context, c *models.Client) error {
	p.created = c
	return nil
}

type provAccounts struct {
	repositories.AccountRepository
	existing []models.Account
	taken    map[string]bool
	created  []*models.Account
}

func (p *provAccounts) GetByNumber(context.Context, string) (*models.Account, error) {
	return nil, postgres.ErrNotFound
}

func (p *provAccounts) List(context.Context, repositories.AccountFilter) ([]models.Account, error) {
	return p.existing, nil
}

func (p *provAccounts) Create(_ context.Context, a *models.Account) error {
	if p.taken[a.WalletID] {
		return ErrAccountAlreadyExists
	}
	p.created = append(p.created, a)
	return nil
}

type provProducts struct {
	repositories.ProductRepository
	products []models.Product
}

func (p provProducts) List(context.Context, repositories.ProductFilter) ([]models.Product, error) {
	return p.products, nil
}

func newProv(l provLookup, c *provClients, a *provAccounts, p provProducts) ProvisioningService {
	return NewProvisioningService(l, c, a, p)
}

func TestCreateClientIndividual(t *testing.T) {
	t.Parallel()
	clients := &provClients{}
	svc := newProv(provLookup{}, clients, &provAccounts{}, provProducts{})
	ext := uuid.NewString()
	c, owner, err := svc.CreateClient(context.Background(), BankmanClientInput{
		ClientType: "individual", ExternalID: ext, Email: "a@x.com", PhoneNumber: "+234",
		FirstName: "Ada", LastName: "Obi", DateOfBirth: "1990-01-02", BankmanID: "bm-1",
	})
	require.NoError(t, err)
	require.Nil(t, owner)
	require.Equal(t, models.ClientStatusActive, c.Status)
	require.Equal(t, models.ClientTypeIndividual, c.Type)
	require.Equal(t, ext, c.KYCData["legacy_external_id"])
	require.Equal(t, "bm-1", c.KYCData["legacy_source_id"])
	require.NotEmpty(t, clients.created.ClientNumber)

	_, _, err = newProv(provLookup{conflicts: []string{"email", "phone_number"}}, &provClients{}, &provAccounts{}, provProducts{}).
		CreateClient(context.Background(), BankmanClientInput{ClientType: "individual", Email: "a@x.com", FirstName: "A", LastName: "B", DateOfBirth: "1990-01-02"})
	require.ErrorIs(t, err, ErrProvisioningValidation)
	require.ErrorContains(t, err, "email, phone_number")

	for name, in := range map[string]BankmanClientInput{
		"bad type":        {ClientType: "other"},
		"missing names":   {ClientType: "individual"},
		"individual+biz":  {ClientType: "individual", FirstName: "A", LastName: "B", DateOfBirth: "1990-01-02", BusinessName: "x"},
		"bad external id": {ClientType: "individual", ExternalID: "nope", FirstName: "A", LastName: "B", DateOfBirth: "1990-01-02"},
	} {
		_, _, err := svc.CreateClient(context.Background(), in)
		require.ErrorIs(t, err, ErrProvisioningValidation, name)
	}
}

func TestCreateClientOrganizationNeedsAnIndividualOwner(t *testing.T) {
	t.Parallel()
	owner := &models.Client{ID: uuid.New(), Type: models.ClientTypeIndividual}
	in := BankmanClientInput{ClientType: "organization", OwnerClientID: "bm-owner", BusinessName: "Acme", BusinessRegistrationNumber: "RC1", TaxIdentificationNumber: "T1"}

	clients := &provClients{}
	c, got, err := newProv(provLookup{owner: owner}, clients, &provAccounts{}, provProducts{}).CreateClient(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, owner.ID, got.ID)
	require.Equal(t, models.ClientTypeCorporate, c.Type)
	require.Equal(t, owner.ID.String(), c.KYCData["owner_client_id"])

	_, _, err = newProv(provLookup{}, &provClients{}, &provAccounts{}, provProducts{}).CreateClient(context.Background(), in)
	require.ErrorIs(t, err, ErrProvisioningValidation)

	corp := &models.Client{ID: uuid.New(), Type: models.ClientTypeCorporate}
	_, _, err = newProv(provLookup{owner: corp}, &provClients{}, &provAccounts{}, provProducts{}).CreateClient(context.Background(), in)
	require.ErrorIs(t, err, ErrProvisioningValidation, "the owner must be an individual")
}

func TestCreateAccount(t *testing.T) {
	t.Parallel()
	client := &models.Client{ID: uuid.New(), ClientNumber: "CL-1"}
	product := models.Product{ID: uuid.New(), Code: "WAL-NGN", Currency: "NGN", Status: models.ProductStatusActive, Description: "wallet [legacy_source_id=bm-prod]"}
	base := BankmanAccountInput{ExternalID: "e", ProductID: "bm-prod", AccountName: "Main", AccountType: "main"}

	t.Run("creates with the allocated id and the plain wallet id", func(t *testing.T) {
		t.Parallel()
		accounts := &provAccounts{}
		in := base
		in.ID = uuid.NewString()
		a, _, replayed, err := newProv(provLookup{client: client}, &provClients{}, accounts, provProducts{products: []models.Product{product}}).CreateAccount(context.Background(), in)
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, in.ID, a.ID.String())
		require.Equal(t, "client-CL-1-WAL-NGN", a.WalletID)
		require.Equal(t, "NGN", a.Currency)
		require.Equal(t, models.AccountStatusActive, a.Status)
		require.Len(t, a.AccountNumber, 10)
	})

	t.Run("a reserve wallet in a country gets its own wallet id", func(t *testing.T) {
		t.Parallel()
		in := base
		in.AccountType, in.CountryCode = "reserve", ptr("ng")
		a, _, _, err := newProv(provLookup{client: client}, &provClients{}, &provAccounts{}, provProducts{products: []models.Product{product}}).CreateAccount(context.Background(), in)
		require.NoError(t, err)
		require.Equal(t, "client-CL-1-WAL-NGN-reserve-ng", a.WalletID)
	})

	t.Run("the shadow id is kept when the caller allocated none", func(t *testing.T) {
		t.Parallel()
		in := base
		in.BankmanID = "bm-acct-5"
		a, _, _, err := newProv(provLookup{client: client}, &provClients{}, &provAccounts{}, provProducts{products: []models.Product{product}}).CreateAccount(context.Background(), in)
		require.NoError(t, err)
		require.Equal(t, "bm-acct-5", a.Metadata["legacy_source_id"])
	})

	t.Run("an existing matching wallet is returned, an R2 wallet is not", func(t *testing.T) {
		t.Parallel()
		main := models.Account{ID: uuid.New(), ProductID: product.ID, Status: "active", AccountType: ptr("main")}
		r2 := models.Account{ID: uuid.New(), ProductID: product.ID, Status: "active", AccountType: ptr("main"), R2RelationshipPublicID: ptr("rel")}
		accounts := &provAccounts{existing: []models.Account{r2, main}}
		a, _, replayed, err := newProv(provLookup{client: client}, &provClients{}, accounts, provProducts{products: []models.Product{product}}).CreateAccount(context.Background(), base)
		require.NoError(t, err)
		require.True(t, replayed)
		require.Equal(t, main.ID, a.ID)
		require.Empty(t, accounts.created)
	})

	t.Run("a taken wallet id falls back to one with the account id", func(t *testing.T) {
		t.Parallel()
		accounts := &provAccounts{taken: map[string]bool{"client-CL-1-WAL-NGN": true}}
		in := base
		in.ID = uuid.NewString()
		a, _, _, err := newProv(provLookup{client: client}, &provClients{}, accounts, provProducts{products: []models.Product{product}}).CreateAccount(context.Background(), in)
		require.NoError(t, err)
		require.Equal(t, "client-CL-1-WAL-NGN-"+in.ID[:8], a.WalletID)
	})

	t.Run("validation", func(t *testing.T) {
		t.Parallel()
		retired := product
		retired.Status = models.ProductStatusRetired
		cases := map[string]struct {
			l  provLookup
			p  []models.Product
			in func(*BankmanAccountInput)
		}{
			"unknown client":   {provLookup{}, []models.Product{product}, func(*BankmanAccountInput) {}},
			"unknown product":  {provLookup{client: client}, nil, func(*BankmanAccountInput) {}},
			"inactive product": {provLookup{client: client}, []models.Product{retired}, func(*BankmanAccountInput) {}},
			"bad type":         {provLookup{client: client}, []models.Product{product}, func(in *BankmanAccountInput) { in.AccountType = "x" }},
			"bad country":      {provLookup{client: client}, []models.Product{product}, func(in *BankmanAccountInput) { in.CountryCode = ptr("NG") }},
			"blank name":       {provLookup{client: client}, []models.Product{product}, func(in *BankmanAccountInput) { in.AccountName = " " }},
			"bad status":       {provLookup{client: client}, []models.Product{product}, func(in *BankmanAccountInput) { in.Status = "frozen" }},
			"bad allocated id": {provLookup{client: client}, []models.Product{product}, func(in *BankmanAccountInput) { in.ID = "nope" }},
		}
		for name, tc := range cases {
			in := base
			tc.in(&in)
			_, _, _, err := newProv(tc.l, &provClients{}, &provAccounts{}, provProducts{products: tc.p}).CreateAccount(context.Background(), in)
			require.ErrorIs(t, err, ErrProvisioningValidation, name)
		}
	})
}
