package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

type clientRepositoryStub struct {
	clients map[uuid.UUID]*models.Client
}

func newClientRepositoryStub() *clientRepositoryStub {
	return &clientRepositoryStub{
		clients: map[uuid.UUID]*models.Client{},
	}
}

func (s *clientRepositoryStub) Create(_ context.Context, client *models.Client) error {
	for _, existing := range s.clients {
		if existing.ClientNumber == client.ClientNumber {
			return postgres.ErrConstraintsFailed{}
		}
	}
	if client.ID == uuid.Nil {
		client.ID = uuid.New()
	}
	copied := *client
	s.clients[client.ID] = &copied
	return nil
}

func (s *clientRepositoryStub) Update(_ context.Context, client *models.Client) error {
	if _, ok := s.clients[client.ID]; !ok {
		return postgres.ErrNotFound
	}
	copied := *client
	s.clients[client.ID] = &copied
	return nil
}

func (s *clientRepositoryStub) Get(_ context.Context, id uuid.UUID) (*models.Client, error) {
	client, ok := s.clients[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *client
	return &copied, nil
}

func (s *clientRepositoryStub) GetByNumber(_ context.Context, clientNumber string) (*models.Client, error) {
	for _, client := range s.clients {
		if client.ClientNumber == clientNumber {
			copied := *client
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *clientRepositoryStub) List(_ context.Context, _ repositories.ClientFilter) ([]models.Client, error) {
	ret := make([]models.Client, 0, len(s.clients))
	for _, client := range s.clients {
		ret = append(ret, *client)
	}
	return ret, nil
}

type accountRepositoryStub struct {
	accounts map[uuid.UUID]*models.Account
}

func newAccountRepositoryStub() *accountRepositoryStub {
	return &accountRepositoryStub{
		accounts: map[uuid.UUID]*models.Account{},
	}
}

func (s *accountRepositoryStub) Create(_ context.Context, account *models.Account) error {
	if account.ID == uuid.Nil {
		account.ID = uuid.New()
	}
	copied := *account
	s.accounts[account.ID] = &copied
	return nil
}

func (s *accountRepositoryStub) Update(_ context.Context, account *models.Account) error {
	copied := *account
	s.accounts[account.ID] = &copied
	return nil
}

func (s *accountRepositoryStub) Get(_ context.Context, id uuid.UUID) (*models.Account, error) {
	account, ok := s.accounts[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *account
	return &copied, nil
}

func (s *accountRepositoryStub) GetByNumber(_ context.Context, accountNumber string) (*models.Account, error) {
	for _, account := range s.accounts {
		if account.AccountNumber == accountNumber {
			copied := *account
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *accountRepositoryStub) GetByWalletID(_ context.Context, walletID string) (*models.Account, error) {
	for _, account := range s.accounts {
		if account.WalletID == walletID {
			copied := *account
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *accountRepositoryStub) List(_ context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
	ret := make([]models.Account, 0, len(s.accounts))
	for _, account := range s.accounts {
		if filter.ClientID != nil && account.ClientID != *filter.ClientID {
			continue
		}
		if filter.ProductID != nil && account.ProductID != *filter.ProductID {
			continue
		}
		if filter.Status != nil && account.Status != *filter.Status {
			continue
		}
		ret = append(ret, *account)
	}
	return ret, nil
}

type kycRepositoryStub struct {
	records map[uuid.UUID]*models.KYCRecord
}

func newKYCRepositoryStub() *kycRepositoryStub {
	return &kycRepositoryStub{
		records: map[uuid.UUID]*models.KYCRecord{},
	}
}

func (s *kycRepositoryStub) Create(_ context.Context, record *models.KYCRecord) error {
	if record.ID == uuid.Nil {
		record.ID = uuid.New()
	}
	copied := *record
	s.records[record.ID] = &copied
	return nil
}

func (s *kycRepositoryStub) Update(_ context.Context, record *models.KYCRecord) error {
	if _, ok := s.records[record.ID]; !ok {
		return postgres.ErrNotFound
	}
	copied := *record
	s.records[record.ID] = &copied
	return nil
}

func (s *kycRepositoryStub) Get(_ context.Context, id uuid.UUID) (*models.KYCRecord, error) {
	record, ok := s.records[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *record
	return &copied, nil
}

func (s *kycRepositoryStub) ListByClient(_ context.Context, clientID uuid.UUID) ([]models.KYCRecord, error) {
	ret := make([]models.KYCRecord, 0)
	for _, record := range s.records {
		if record.ClientID == clientID {
			ret = append(ret, *record)
		}
	}
	return ret, nil
}

type dailyUsageRepositoryStub struct {
	usages map[string]*models.AccountDailyUsage
}

func newDailyUsageRepositoryStub() *dailyUsageRepositoryStub {
	return &dailyUsageRepositoryStub{
		usages: map[string]*models.AccountDailyUsage{},
	}
}

func (s *dailyUsageRepositoryStub) Create(_ context.Context, usage *models.AccountDailyUsage) error {
	if usage.ID == uuid.Nil {
		usage.ID = uuid.New()
	}
	copied := *usage
	s.usages[dailyUsageKey(usage.AccountID, usage.UsageDate)] = &copied
	return nil
}

func (s *dailyUsageRepositoryStub) Update(_ context.Context, usage *models.AccountDailyUsage) error {
	key := dailyUsageKey(usage.AccountID, usage.UsageDate)
	if _, ok := s.usages[key]; !ok {
		return postgres.ErrNotFound
	}
	copied := *usage
	s.usages[key] = &copied
	return nil
}

func (s *dailyUsageRepositoryStub) GetForDate(_ context.Context, accountID uuid.UUID, usageDate time.Time) (*models.AccountDailyUsage, error) {
	usage, ok := s.usages[dailyUsageKey(accountID, usageDate)]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *usage
	return &copied, nil
}

func dailyUsageKey(accountID uuid.UUID, usageDate time.Time) string {
	return accountID.String() + "|" + usageDate.UTC().Truncate(24*time.Hour).Format("2006-01-02")
}

type interestAccrualRepositoryStub struct {
	accruals map[uuid.UUID]*models.InterestAccrual
}

func newInterestAccrualRepositoryStub() *interestAccrualRepositoryStub {
	return &interestAccrualRepositoryStub{
		accruals: map[uuid.UUID]*models.InterestAccrual{},
	}
}

func (s *interestAccrualRepositoryStub) Create(_ context.Context, accrual *models.InterestAccrual) error {
	if accrual.ID == uuid.Nil {
		accrual.ID = uuid.New()
	}
	copied := *accrual
	s.accruals[accrual.ID] = &copied
	return nil
}

func (s *interestAccrualRepositoryStub) Update(_ context.Context, accrual *models.InterestAccrual) error {
	if _, ok := s.accruals[accrual.ID]; !ok {
		return postgres.ErrNotFound
	}
	copied := *accrual
	s.accruals[accrual.ID] = &copied
	return nil
}

func (s *interestAccrualRepositoryStub) Get(_ context.Context, id uuid.UUID) (*models.InterestAccrual, error) {
	accrual, ok := s.accruals[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *accrual
	return &copied, nil
}

func (s *interestAccrualRepositoryStub) ListByAccount(_ context.Context, accountID uuid.UUID) ([]models.InterestAccrual, error) {
	ret := make([]models.InterestAccrual, 0)
	for _, accrual := range s.accruals {
		if accrual.AccountID == accountID {
			ret = append(ret, *accrual)
		}
	}
	return ret, nil
}

type feePostingRepositoryStub struct {
	postings map[string]*models.FeePosting
}

func newFeePostingRepositoryStub() *feePostingRepositoryStub {
	return &feePostingRepositoryStub{
		postings: map[string]*models.FeePosting{},
	}
}

func (s *feePostingRepositoryStub) Create(_ context.Context, posting *models.FeePosting) error {
	if posting.ID == uuid.Nil {
		posting.ID = uuid.New()
	}
	copied := *posting
	s.postings[posting.Reference] = &copied
	return nil
}

func (s *feePostingRepositoryStub) Update(_ context.Context, posting *models.FeePosting) error {
	if _, ok := s.postings[posting.Reference]; !ok {
		return postgres.ErrNotFound
	}
	copied := *posting
	s.postings[posting.Reference] = &copied
	return nil
}

func (s *feePostingRepositoryStub) GetByReference(_ context.Context, reference string) (*models.FeePosting, error) {
	posting, ok := s.postings[reference]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *posting
	return &copied, nil
}

func (s *feePostingRepositoryStub) ListByAccount(_ context.Context, accountID uuid.UUID) ([]models.FeePosting, error) {
	ret := make([]models.FeePosting, 0)
	for _, posting := range s.postings {
		if posting.AccountID == accountID {
			ret = append(ret, *posting)
		}
	}
	return ret, nil
}

func (s *feePostingRepositoryStub) ListPendingRecovery(_ context.Context) ([]models.FeePosting, error) {
	ret := make([]models.FeePosting, 0)
	for _, posting := range s.postings {
		if posting.Status == models.FeePostingStatusPendingRecovery {
			ret = append(ret, *posting)
		}
	}
	return ret, nil
}

func TestClientServiceCreateIndividual(t *testing.T) {
	t.Parallel()

	service := NewClientService(newClientRepositoryStub(), newAccountRepositoryStub())
	client, err := service.Create(context.Background(), CreateClientInput{
		Type: "individual",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		IndividualData: &models.IndividualData{
			FirstName: "Ada",
			LastName:  "Lovelace",
		},
	})
	require.NoError(t, err)
	require.Equal(t, models.ClientStatusPending, client.Status)
	require.Equal(t, models.KYCStatusPending, client.KYCStatus)
	require.Equal(t, 0, client.KYCLevel)
	require.Contains(t, client.ClientNumber, "CL-")
}

func TestClientServiceActivateRequiresVerifiedKYC(t *testing.T) {
	t.Parallel()

	clientRepo := newClientRepositoryStub()
	clientService := NewClientService(clientRepo, newAccountRepositoryStub())

	client, err := clientService.Create(context.Background(), CreateClientInput{
		Type: "individual",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		IndividualData: &models.IndividualData{
			FirstName: "Ada",
			LastName:  "Lovelace",
		},
	})
	require.NoError(t, err)

	_, err = clientService.Activate(context.Background(), client.ID)
	require.ErrorIs(t, err, ErrClientKYCRequirement)
}

func TestKYCServiceVerifyPromotesClientLevel(t *testing.T) {
	t.Parallel()

	clientRepo := newClientRepositoryStub()
	clientService := NewClientService(clientRepo, newAccountRepositoryStub())
	kycRepo := newKYCRepositoryStub()
	kycService := NewKYCService(clientRepo, kycRepo)

	client, err := clientService.Create(context.Background(), CreateClientInput{
		Type: "individual",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		IndividualData: &models.IndividualData{
			FirstName:        "Ada",
			LastName:         "Lovelace",
			NationalIDNumber: "NIN-123",
		},
	})
	require.NoError(t, err)

	record, err := kycService.Submit(context.Background(), client.ID, SubmitKYCInput{
		Level:   1,
		Payload: map[string]any{"national_id_number": "NIN-123"},
	})
	require.NoError(t, err)

	verified, err := kycService.Verify(context.Background(), client.ID, record.ID, VerifyKYCInput{
		Verifier: "ops@example.com",
	})
	require.NoError(t, err)
	require.Equal(t, models.KYCStatusVerified, verified.Status)

	updatedClient, err := clientService.Get(context.Background(), client.ID)
	require.NoError(t, err)
	require.Equal(t, 1, updatedClient.KYCLevel)
	require.Equal(t, models.KYCStatusVerified, updatedClient.KYCStatus)
}

func TestKYCServiceRejectRequiresReason(t *testing.T) {
	t.Parallel()

	clientRepo := newClientRepositoryStub()
	clientService := NewClientService(clientRepo, newAccountRepositoryStub())
	kycRepo := newKYCRepositoryStub()
	kycService := NewKYCService(clientRepo, kycRepo)

	client, err := clientService.Create(context.Background(), CreateClientInput{
		Type: "individual",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		IndividualData: &models.IndividualData{
			FirstName:        "Ada",
			LastName:         "Lovelace",
			NationalIDNumber: "NIN-123",
		},
	})
	require.NoError(t, err)

	record, err := kycService.Submit(context.Background(), client.ID, SubmitKYCInput{
		Level:   1,
		Payload: map[string]any{"national_id_number": "NIN-123"},
	})
	require.NoError(t, err)

	_, err = kycService.Reject(context.Background(), client.ID, record.ID, RejectKYCInput{})
	require.ErrorIs(t, err, ErrKYCValidation)
}

func TestClientServiceCloseBlockedByOpenAccounts(t *testing.T) {
	t.Parallel()

	clientRepo := newClientRepositoryStub()
	accountRepo := newAccountRepositoryStub()
	clientService := NewClientService(clientRepo, accountRepo)

	client, err := clientService.Create(context.Background(), CreateClientInput{
		Type: "corporate",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		CorporateData: &models.CorporateData{
			LegalName: "Formance Ltd",
		},
	})
	require.NoError(t, err)

	require.NoError(t, accountRepo.Create(context.Background(), &models.Account{
		ClientID:      client.ID,
		AccountNumber: "0000000001",
		Status:        models.AccountStatusActive,
		WalletID:      "wallet-1",
	}))

	_, err = clientService.Close(context.Background(), client.ID)
	require.ErrorIs(t, err, ErrClientHasAccounts)
}
