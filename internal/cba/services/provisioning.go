package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var (
	ErrProvisioningValidation = errors.New("provisioning validation failed")
	ErrProvisioningNotFound   = errors.New("provisioning target not found")
)

var legacyProductTag = regexp.MustCompile(`\[legacy_source_id=([^\]]+)\]`)

// BankmanClientInput is the body of Bankman's POST /clients.
type BankmanClientInput struct {
	ClientType                 string
	ExternalID                 string
	Email, PhoneNumber         string
	Address                    *models.Address
	FirstName, LastName        string
	DateOfBirth, Gender        string
	Nationality                string
	OwnerClientID              string
	BusinessName               string
	BusinessRegistrationNumber string
	BusinessRegistrationDate   string
	TaxIdentificationNumber    string
	IndustrySector             string
	// BankmanID is the id Bankman gave the client, passed by the shadow gateway so ids stay aligned.
	BankmanID string
}

// BankmanAccountInput is the body of Bankman's POST /accounts.
type BankmanAccountInput struct {
	ID          string // optional caller-allocated account id
	ExternalID  string
	ProductID   string
	AccountName string
	AccountType string
	CountryCode *string
	Status      string
	// BankmanID is the id Bankman gave the account when the caller did not allocate one.
	BankmanID string
}

// ProvisioningService creates clients and wallets the way Bankman's POST /clients and POST /accounts do.
type ProvisioningService interface {
	CreateClient(ctx context.Context, in BankmanClientInput) (client *models.Client, owner *models.Client, err error)
	CreateAccount(ctx context.Context, in BankmanAccountInput) (account *models.Account, client *models.Client, replayed bool, err error)
}

type DefaultProvisioningService struct {
	lookup   LegacyLookupService
	clients  repositories.ClientRepository
	accounts repositories.AccountRepository
	products repositories.ProductRepository
}

func NewProvisioningService(
	lookup LegacyLookupService,
	clients repositories.ClientRepository,
	accounts repositories.AccountRepository,
	products repositories.ProductRepository,
) ProvisioningService {
	return &DefaultProvisioningService{lookup: lookup, clients: clients, accounts: accounts, products: products}
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProvisioningValidation, fmt.Sprintf(format, args...))
}

func (s *DefaultProvisioningService) CreateClient(ctx context.Context, in BankmanClientInput) (*models.Client, *models.Client, error) {
	externalID := strings.ToLower(strings.TrimSpace(in.ExternalID))
	if externalID == "" {
		externalID = uuid.NewString()
	} else if _, err := uuid.Parse(externalID); err != nil {
		return nil, nil, invalid("external_id must be a UUID")
	}
	email, phone := strings.TrimSpace(in.Email), strings.TrimSpace(in.PhoneNumber)
	if len(phone) > 20 || len(email) > 255 {
		return nil, nil, invalid("email or phone_number is too long")
	}

	client := &models.Client{
		Status: models.ClientStatusActive, KYCStatus: models.KYCStatusPending,
		KYCData: map[string]any{"legacy_external_id": externalID},
		Contact: models.ClientContact{Email: email, Phone: phone, Address: in.Address},
	}
	if in.BankmanID != "" {
		client.KYCData["legacy_source_id"] = in.BankmanID
	}

	var owner *models.Client
	identity := repositories.ClientIdentity{Email: email, Phone: phone, ExternalID: externalID}
	switch strings.ToLower(in.ClientType) {
	case "individual":
		if in.OwnerClientID != "" || in.BusinessName != "" {
			return nil, nil, invalid("business fields and owner_client_id are not allowed for individual clients")
		}
		if strings.TrimSpace(in.FirstName) == "" || strings.TrimSpace(in.LastName) == "" || strings.TrimSpace(in.DateOfBirth) == "" {
			return nil, nil, invalid("first_name, last_name and date_of_birth are required for individual clients")
		}
		client.Type = models.ClientTypeIndividual
		dob, gender := strings.TrimSpace(in.DateOfBirth), strings.TrimSpace(in.Gender)
		d := &models.IndividualData{FirstName: strings.TrimSpace(in.FirstName), LastName: strings.TrimSpace(in.LastName), DateOfBirth: &dob, Nationality: in.Nationality}
		if gender != "" {
			d.Gender = &gender
		}
		client.IndividualData = d
	case "organization":
		if in.OwnerClientID == "" {
			return nil, nil, invalid("owner_client_id is required for organizations")
		}
		var err error
		owner, err = s.lookup.ClientByID(ctx, in.OwnerClientID)
		if err != nil || owner.Type != models.ClientTypeIndividual {
			return nil, nil, invalid("owner_client_id must be an existing individual client")
		}
		if in.BusinessName == "" || in.BusinessRegistrationNumber == "" || in.TaxIdentificationNumber == "" {
			return nil, nil, invalid("business_name, business_registration_number and tax_identification_number are required")
		}
		client.Type = models.ClientTypeCorporate
		date := strings.TrimSpace(in.BusinessRegistrationDate)
		client.CorporateData = &models.CorporateData{
			LegalName: strings.TrimSpace(in.BusinessName), RegistrationNumber: strings.TrimSpace(in.BusinessRegistrationNumber),
			TaxID: strings.TrimSpace(in.TaxIdentificationNumber), Industry: strings.TrimSpace(in.IndustrySector),
		}
		if date != "" {
			client.CorporateData.IncorporationDate = &date
		}
		client.KYCData["owner_client_id"] = owner.ID.String()
		identity.RegistrationNumber, identity.TaxID = client.CorporateData.RegistrationNumber, client.CorporateData.TaxID
	default:
		return nil, nil, invalid("client_type must be individual or organization")
	}

	if err := validateClient(client); err != nil {
		return nil, nil, invalid("%v", err)
	}
	conflicts, err := s.lookup.ClientConflicts(ctx, identity)
	if err != nil {
		return nil, nil, err
	}
	if len(conflicts) > 0 {
		return nil, nil, invalid("the %s has already been taken", strings.Join(conflicts, ", "))
	}

	number, err := (&DefaultClientService{clientRepository: s.clients}).generateClientNumber(ctx)
	if err != nil {
		return nil, nil, err
	}
	client.ClientNumber = number
	if err := s.clients.Create(ctx, client); err != nil {
		return nil, nil, resolveClientRepositoryError(err)
	}
	return client, owner, nil
}

// findProduct resolves a product by the id callers hold: the Bankman id of an imported product, or a native id.
func (s *DefaultProvisioningService) findProduct(ctx context.Context, id string) (*models.Product, error) {
	all, err := s.products.List(ctx, repositories.ProductFilter{})
	if err != nil {
		return nil, err
	}
	id = strings.ToLower(strings.TrimSpace(id))
	for i := range all {
		if m := legacyProductTag.FindStringSubmatch(all[i].Description); m != nil && strings.EqualFold(m[1], id) {
			return &all[i], nil
		}
	}
	for i := range all {
		if strings.EqualFold(all[i].ID.String(), id) {
			return &all[i], nil
		}
	}
	return nil, nil
}

func sameCountry(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (s *DefaultProvisioningService) CreateAccount(ctx context.Context, in BankmanAccountInput) (*models.Account, *models.Client, bool, error) {
	client, err := s.lookup.Client(ctx, in.ExternalID)
	if err != nil {
		if errors.Is(err, ErrLegacyNotFound) {
			return nil, nil, false, invalid("external_id does not exist")
		}
		return nil, nil, false, err
	}
	product, err := s.findProduct(ctx, in.ProductID)
	if err != nil {
		return nil, nil, false, err
	}
	switch {
	case product == nil:
		return nil, nil, false, invalid("product_id does not exist")
	case product.Status != models.ProductStatusActive:
		return nil, nil, false, invalid("the selected product is not active")
	case strings.TrimSpace(in.AccountName) == "" || len(in.AccountName) > 255:
		return nil, nil, false, invalid("account_name is required (max 255)")
	case in.AccountType != models.AccountTypeMain && in.AccountType != models.AccountTypeReserve:
		return nil, nil, false, invalid("account_type must be main or reserve")
	case in.CountryCode != nil && !walletCountryPattern.MatchString(*in.CountryCode):
		return nil, nil, false, invalid("country_code must be two lowercase letters")
	case in.Status != "" && in.Status != "active" && in.Status != "inactive":
		return nil, nil, false, invalid("status must be active or inactive")
	}

	existing, err := s.matching(ctx, client, product, in)
	if err != nil {
		return nil, nil, false, err
	}
	if existing != nil {
		return existing, client, true, nil
	}

	account := &models.Account{
		AccountNumber: "", ClientID: client.ID, ProductID: product.ID, Currency: product.Currency,
		Status: models.AccountStatusActive, OpenedAt: time.Now().UTC(), InterestAccrued: decimal.Zero,
		Metadata:    map[string]any{},
		AccountName: ptrString(strings.TrimSpace(in.AccountName)), AccountType: ptrString(in.AccountType), CountryCode: in.CountryCode,
	}
	now := time.Now().UTC()
	if in.Status == "inactive" {
		account.Status = models.AccountStatusPending
	} else {
		account.ActivatedAt = &now
	}
	switch {
	case strings.TrimSpace(in.ID) != "":
		id, perr := uuid.Parse(strings.TrimSpace(in.ID))
		if perr != nil {
			return nil, nil, false, invalid("id must be a UUID")
		}
		account.ID = id
	case in.BankmanID != "":
		account.Metadata["legacy_source_id"] = in.BankmanID
	}

	number, err := (&DefaultAccountService{accountRepository: s.accounts}).generateAccountNumber(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	account.AccountNumber = number

	base := fmt.Sprintf("client-%s-%s", strings.TrimSpace(client.ClientNumber), strings.TrimSpace(product.Code))
	variant := base
	if in.AccountType != models.AccountTypeMain || in.CountryCode != nil {
		variant += "-" + in.AccountType
		if in.CountryCode != nil {
			variant += "-" + *in.CountryCode
		}
	}
	candidates := []string{variant}
	if account.ID != uuid.Nil {
		candidates = append(candidates, variant+"-"+account.ID.String()[:8])
	}
	for _, wallet := range candidates {
		account.WalletID = wallet
		err = s.accounts.Create(ctx, account)
		if err == nil {
			return account, client, false, nil
		}
		if !errors.Is(resolveAccountRepositoryError(err), ErrAccountAlreadyExists) {
			return nil, nil, false, resolveAccountRepositoryError(err)
		}
		// A concurrent identical request may have won: answer with what exists.
		if again, merr := s.matching(ctx, client, product, in); merr == nil && again != nil {
			return again, client, true, nil
		}
	}
	return nil, nil, false, fmt.Errorf("%w: wallet id already used", ErrAccountAlreadyExists)
}

func ptrString(s string) *string { return &s }

func (s *DefaultProvisioningService) matching(ctx context.Context, client *models.Client, product *models.Product, in BankmanAccountInput) (*models.Account, error) {
	all, err := s.accounts.List(ctx, repositories.AccountFilter{ClientID: &client.ID})
	if err != nil {
		return nil, err
	}
	for i := range all {
		a := &all[i]
		// An R2 sub-account is never the answer to "does this merchant already have a wallet": Bankman does not
		// look at roles here, which could hand a settlement wallet to an ordinary caller.
		if a.R2RelationshipPublicID == nil && a.ProductID == product.ID && a.Status != models.AccountStatusClosed &&
			a.AccountType != nil && *a.AccountType == in.AccountType && sameCountry(a.CountryCode, in.CountryCode) {
			return a, nil
		}
	}
	return nil, nil
}
