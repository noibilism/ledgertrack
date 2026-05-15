package v2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

type clientListResponse struct {
	Clients []models.Client `json:"clients"`
}

type kycListResponse struct {
	Records []models.KYCRecord `json:"records"`
}

type clientRepositoryForHTTPTests struct {
	clients    map[uuid.UUID]*models.Client
	lastFilter repositories.ClientFilter
}

func newClientRepositoryForHTTPTests() *clientRepositoryForHTTPTests {
	return &clientRepositoryForHTTPTests{
		clients: map[uuid.UUID]*models.Client{},
	}
}

func (s *clientRepositoryForHTTPTests) Create(_ context.Context, client *models.Client) error {
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

func (s *clientRepositoryForHTTPTests) Update(_ context.Context, client *models.Client) error {
	if _, ok := s.clients[client.ID]; !ok {
		return postgres.ErrNotFound
	}
	copied := *client
	s.clients[client.ID] = &copied
	return nil
}

func (s *clientRepositoryForHTTPTests) Get(_ context.Context, id uuid.UUID) (*models.Client, error) {
	client, ok := s.clients[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *client
	return &copied, nil
}

func (s *clientRepositoryForHTTPTests) GetByNumber(_ context.Context, clientNumber string) (*models.Client, error) {
	for _, client := range s.clients {
		if client.ClientNumber == clientNumber {
			copied := *client
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *clientRepositoryForHTTPTests) List(_ context.Context, filter repositories.ClientFilter) ([]models.Client, error) {
	s.lastFilter = filter
	ret := make([]models.Client, 0, len(s.clients))
	for _, client := range s.clients {
		if filter.Type != nil && client.Type != *filter.Type {
			continue
		}
		if filter.Status != nil && client.Status != *filter.Status {
			continue
		}
		if filter.KYCStatus != nil && client.KYCStatus != *filter.KYCStatus {
			continue
		}
		ret = append(ret, *client)
	}
	return ret, nil
}

type accountRepositoryForHTTPTests struct {
	accounts map[uuid.UUID]*models.Account
}

func newAccountRepositoryForHTTPTests() *accountRepositoryForHTTPTests {
	return &accountRepositoryForHTTPTests{
		accounts: map[uuid.UUID]*models.Account{},
	}
}

func (s *accountRepositoryForHTTPTests) Create(_ context.Context, account *models.Account) error {
	if account.ID == uuid.Nil {
		account.ID = uuid.New()
	}
	copied := *account
	s.accounts[account.ID] = &copied
	return nil
}

func (s *accountRepositoryForHTTPTests) Update(_ context.Context, account *models.Account) error {
	copied := *account
	s.accounts[account.ID] = &copied
	return nil
}

func (s *accountRepositoryForHTTPTests) Get(_ context.Context, id uuid.UUID) (*models.Account, error) {
	account, ok := s.accounts[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *account
	return &copied, nil
}

func (s *accountRepositoryForHTTPTests) GetByNumber(_ context.Context, accountNumber string) (*models.Account, error) {
	for _, account := range s.accounts {
		if account.AccountNumber == accountNumber {
			copied := *account
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *accountRepositoryForHTTPTests) GetByWalletID(_ context.Context, walletID string) (*models.Account, error) {
	for _, account := range s.accounts {
		if account.WalletID == walletID {
			copied := *account
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *accountRepositoryForHTTPTests) List(_ context.Context, filter repositories.AccountFilter) ([]models.Account, error) {
	ret := make([]models.Account, 0, len(s.accounts))
	for _, account := range s.accounts {
		if filter.ClientID != nil && account.ClientID != *filter.ClientID {
			continue
		}
		ret = append(ret, *account)
	}
	return ret, nil
}

type kycRepositoryForHTTPTests struct {
	records map[uuid.UUID]*models.KYCRecord
}

func newKYCRepositoryForHTTPTests() *kycRepositoryForHTTPTests {
	return &kycRepositoryForHTTPTests{
		records: map[uuid.UUID]*models.KYCRecord{},
	}
}

func (s *kycRepositoryForHTTPTests) Create(_ context.Context, record *models.KYCRecord) error {
	if record.ID == uuid.Nil {
		record.ID = uuid.New()
	}
	copied := *record
	s.records[record.ID] = &copied
	return nil
}

func (s *kycRepositoryForHTTPTests) Update(_ context.Context, record *models.KYCRecord) error {
	if _, ok := s.records[record.ID]; !ok {
		return postgres.ErrNotFound
	}
	copied := *record
	s.records[record.ID] = &copied
	return nil
}

func (s *kycRepositoryForHTTPTests) Get(_ context.Context, id uuid.UUID) (*models.KYCRecord, error) {
	record, ok := s.records[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *record
	return &copied, nil
}

func (s *kycRepositoryForHTTPTests) ListByClient(_ context.Context, clientID uuid.UUID) ([]models.KYCRecord, error) {
	ret := make([]models.KYCRecord, 0)
	for _, record := range s.records {
		if record.ClientID == clientID {
			ret = append(ret, *record)
		}
	}
	return ret, nil
}

type dailyUsageRepositoryForHTTPTests struct {
	usages map[string]*models.AccountDailyUsage
}

func newDailyUsageRepositoryForHTTPTests() *dailyUsageRepositoryForHTTPTests {
	return &dailyUsageRepositoryForHTTPTests{
		usages: map[string]*models.AccountDailyUsage{},
	}
}

func (s *dailyUsageRepositoryForHTTPTests) Create(_ context.Context, usage *models.AccountDailyUsage) error {
	if usage.ID == uuid.Nil {
		usage.ID = uuid.New()
	}
	copied := *usage
	s.usages[dailyUsageHTTPKey(usage.AccountID, usage.UsageDate)] = &copied
	return nil
}

func (s *dailyUsageRepositoryForHTTPTests) Update(_ context.Context, usage *models.AccountDailyUsage) error {
	key := dailyUsageHTTPKey(usage.AccountID, usage.UsageDate)
	if _, ok := s.usages[key]; !ok {
		return postgres.ErrNotFound
	}
	copied := *usage
	s.usages[key] = &copied
	return nil
}

func (s *dailyUsageRepositoryForHTTPTests) GetForDate(_ context.Context, accountID uuid.UUID, usageDate time.Time) (*models.AccountDailyUsage, error) {
	usage, ok := s.usages[dailyUsageHTTPKey(accountID, usageDate)]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *usage
	return &copied, nil
}

func dailyUsageHTTPKey(accountID uuid.UUID, usageDate time.Time) string {
	return accountID.String() + "|" + usageDate.UTC().Truncate(24*time.Hour).Format("2006-01-02")
}

func newClientAndKYCServicesForHTTPTests() (services.ClientService, services.KYCService, *clientRepositoryForHTTPTests, *kycRepositoryForHTTPTests) {
	clientRepo := newClientRepositoryForHTTPTests()
	accountRepo := newAccountRepositoryForHTTPTests()
	kycRepo := newKYCRepositoryForHTTPTests()
	return services.NewClientService(clientRepo, accountRepo), services.NewKYCService(clientRepo, kycRepo), clientRepo, kycRepo
}

func TestCreateClient(t *testing.T) {
	clientService, kycService, _, _ := newClientAndKYCServicesForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithClientService(clientService), WithKYCService(kycService))

	req := httptest.NewRequest(http.MethodPost, "/test/clients", api.Buffer(t, services.CreateClientInput{
		Type: "individual",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		IndividualData: &models.IndividualData{
			FirstName: "Ada",
			LastName:  "Lovelace",
		},
	}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	client, ok := api.DecodeSingleResponse[models.Client](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, models.ClientStatusPending, client.Status)
	require.Equal(t, models.ClientTypeIndividual, client.Type)
}

func TestListClientsAppliesFilters(t *testing.T) {
	clientService, kycService, repo, _ := newClientAndKYCServicesForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithClientService(clientService), WithKYCService(kycService))

	client, err := clientService.Create(context.Background(), services.CreateClientInput{
		Type: "corporate",
		Contact: models.ClientContact{
			Phone: "08000000000",
		},
		CorporateData: &models.CorporateData{
			LegalName: "Formance Ltd",
		},
	})
	require.NoError(t, err)

	stored, err := repo.Get(context.Background(), client.ID)
	require.NoError(t, err)
	stored.KYCStatus = models.KYCStatusVerified
	require.NoError(t, repo.Update(context.Background(), stored))

	req := httptest.NewRequest(http.MethodGet, "/test/clients?type=corporate&status=pending&kyc_status=verified", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[clientListResponse](t, rec.Body)
	require.True(t, ok)
	require.Len(t, response.Clients, 1)
	require.NotNil(t, repo.lastFilter.Type)
	require.Equal(t, "corporate", *repo.lastFilter.Type)
	require.NotNil(t, repo.lastFilter.KYCStatus)
	require.Equal(t, "verified", *repo.lastFilter.KYCStatus)
}

func TestSubmitKYCValidationError(t *testing.T) {
	clientService, kycService, _, _ := newClientAndKYCServicesForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithClientService(clientService), WithKYCService(kycService))

	client, err := clientService.Create(context.Background(), services.CreateClientInput{
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

	req := httptest.NewRequest(http.MethodPost, "/test/clients/"+client.ID.String()+"/kyc", api.Buffer(t, services.SubmitKYCInput{
		Level: 2,
	}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	errResp := api.ErrorResponse{}
	api.Decode(t, rec.Body, &errResp)
	require.EqualValues(t, common.ErrValidation, errResp.ErrorCode)
}

func TestVerifyKYCAndActivateClient(t *testing.T) {
	clientService, kycService, _, _ := newClientAndKYCServicesForHTTPTests()
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithClientService(clientService), WithKYCService(kycService))

	client, err := clientService.Create(context.Background(), services.CreateClientInput{
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

	kycResp := httptest.NewRecorder()
	kycReq := httptest.NewRequest(http.MethodPost, "/test/clients/"+client.ID.String()+"/kyc", api.Buffer(t, services.SubmitKYCInput{
		Level:   1,
		Payload: map[string]any{"national_id_number": "NIN-123"},
	}))
	router.ServeHTTP(kycResp, kycReq)
	require.Equal(t, http.StatusCreated, kycResp.Code)
	record, ok := api.DecodeSingleResponse[models.KYCRecord](t, kycResp.Body)
	require.True(t, ok)

	verifyResp := httptest.NewRecorder()
	verifyReq := httptest.NewRequest(http.MethodPost, "/test/clients/"+client.ID.String()+"/kyc/"+record.ID.String()+"/verify", api.Buffer(t, services.VerifyKYCInput{
		Verifier: "ops@example.com",
	}))
	router.ServeHTTP(verifyResp, verifyReq)
	require.Equal(t, http.StatusOK, verifyResp.Code)

	activateResp := httptest.NewRecorder()
	activateReq := httptest.NewRequest(http.MethodPost, "/test/clients/"+client.ID.String()+"/activate", nil)
	router.ServeHTTP(activateResp, activateReq)

	require.Equal(t, http.StatusOK, activateResp.Code)
	activated, ok := api.DecodeSingleResponse[models.Client](t, activateResp.Body)
	require.True(t, ok)
	require.Equal(t, models.ClientStatusActive, activated.Status)

	historyResp := httptest.NewRecorder()
	historyReq := httptest.NewRequest(http.MethodGet, "/test/clients/"+client.ID.String()+"/kyc", nil)
	router.ServeHTTP(historyResp, historyReq)
	require.Equal(t, http.StatusOK, historyResp.Code)
	history, ok := api.DecodeSingleResponse[kycListResponse](t, historyResp.Body)
	require.True(t, ok)
	require.Len(t, history.Records, 1)
}
