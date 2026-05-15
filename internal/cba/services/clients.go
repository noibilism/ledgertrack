package services

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var (
	ErrClientValidation             = errors.New("client validation failed")
	ErrClientNotFound               = errors.New("client not found")
	ErrClientAlreadyExists          = errors.New("client already exists")
	ErrClientInvalidStateTransition = errors.New("client state transition is invalid")
	ErrClientKYCRequirement         = errors.New("client kyc requirement not met")
	ErrClientHasAccounts            = errors.New("client has active accounts")
)

type ClientService interface {
	Create(context.Context, CreateClientInput) (*models.Client, error)
	List(context.Context, repositories.ClientFilter) ([]models.Client, error)
	Get(context.Context, uuid.UUID) (*models.Client, error)
	Patch(context.Context, uuid.UUID, PatchClientInput) (*models.Client, error)
	Activate(context.Context, uuid.UUID) (*models.Client, error)
	Suspend(context.Context, uuid.UUID, SuspendClientInput) (*models.Client, error)
	Reactivate(context.Context, uuid.UUID) (*models.Client, error)
	Close(context.Context, uuid.UUID) (*models.Client, error)
}

type CreateClientInput struct {
	Type           string                 `json:"type"`
	Contact        models.ClientContact   `json:"contact"`
	IndividualData *models.IndividualData `json:"individual_data,omitempty"`
	CorporateData  *models.CorporateData  `json:"corporate_data,omitempty"`
}

type PatchClientInput struct {
	Contact        *models.ClientContact   `json:"contact,omitempty"`
	IndividualData **models.IndividualData `json:"individual_data,omitempty"`
	CorporateData  **models.CorporateData  `json:"corporate_data,omitempty"`
}

type SuspendClientInput struct {
	Reason string `json:"reason"`
}

type DefaultClientService struct {
	clientRepository  repositories.ClientRepository
	accountRepository repositories.AccountRepository
}

func NewClientService(
	clientRepository repositories.ClientRepository,
	accountRepository repositories.AccountRepository,
) ClientService {
	return &DefaultClientService{
		clientRepository:  clientRepository,
		accountRepository: accountRepository,
	}
}

func (s *DefaultClientService) Create(ctx context.Context, input CreateClientInput) (*models.Client, error) {
	client := &models.Client{
		Type:           normalizeClientType(input.Type),
		Status:         models.ClientStatusPending,
		KYCLevel:       0,
		KYCStatus:      models.KYCStatusPending,
		KYCData:        map[string]any{},
		Contact:        normalizeContact(input.Contact),
		IndividualData: normalizeIndividualData(input.IndividualData),
		CorporateData:  normalizeCorporateData(input.CorporateData),
	}

	if err := validateClient(client); err != nil {
		return nil, err
	}

	clientNumber, err := s.generateClientNumber(ctx)
	if err != nil {
		return nil, err
	}
	client.ClientNumber = clientNumber

	if err := s.clientRepository.Create(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return client, nil
}

func (s *DefaultClientService) List(ctx context.Context, filter repositories.ClientFilter) ([]models.Client, error) {
	return s.clientRepository.List(ctx, filter)
}

func (s *DefaultClientService) Get(ctx context.Context, id uuid.UUID) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}
	return client, nil
}

func (s *DefaultClientService) Patch(ctx context.Context, id uuid.UUID, input PatchClientInput) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	if input.Contact != nil {
		client.Contact = normalizeContact(*input.Contact)
	}
	if input.IndividualData != nil {
		client.IndividualData = normalizeIndividualData(*input.IndividualData)
	}
	if input.CorporateData != nil {
		client.CorporateData = normalizeCorporateData(*input.CorporateData)
	}

	if err := validateClient(client); err != nil {
		return nil, err
	}

	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return client, nil
}

func (s *DefaultClientService) Activate(ctx context.Context, id uuid.UUID) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	switch client.Status {
	case models.ClientStatusActive:
		return client, nil
	case models.ClientStatusPending:
		if client.KYCLevel < 1 || client.KYCStatus != models.KYCStatusVerified {
			return nil, fmt.Errorf("%w: client requires verified KYC level 1 or higher", ErrClientKYCRequirement)
		}
		client.Status = models.ClientStatusActive
	default:
		return nil, fmt.Errorf("%w: cannot activate client in status %s", ErrClientInvalidStateTransition, client.Status)
	}

	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return client, nil
}

func (s *DefaultClientService) Suspend(ctx context.Context, id uuid.UUID, input SuspendClientInput) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	switch client.Status {
	case models.ClientStatusSuspended:
		return client, nil
	case models.ClientStatusActive, models.ClientStatusPending:
		client.Status = models.ClientStatusSuspended
	default:
		return nil, fmt.Errorf("%w: cannot suspend client in status %s", ErrClientInvalidStateTransition, client.Status)
	}

	if client.KYCData == nil {
		client.KYCData = map[string]any{}
	}
	if reason := strings.TrimSpace(input.Reason); reason != "" {
		client.KYCData["suspension_reason"] = reason
	}

	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return client, nil
}

func (s *DefaultClientService) Reactivate(ctx context.Context, id uuid.UUID) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	switch client.Status {
	case models.ClientStatusActive:
		return client, nil
	case models.ClientStatusSuspended:
		if client.KYCLevel < 1 || client.KYCStatus != models.KYCStatusVerified {
			return nil, fmt.Errorf("%w: client requires verified KYC level 1 or higher", ErrClientKYCRequirement)
		}
		client.Status = models.ClientStatusActive
	default:
		return nil, fmt.Errorf("%w: cannot reactivate client in status %s", ErrClientInvalidStateTransition, client.Status)
	}

	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return client, nil
}

func (s *DefaultClientService) Close(ctx context.Context, id uuid.UUID) (*models.Client, error) {
	client, err := s.clientRepository.Get(ctx, id)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	switch client.Status {
	case models.ClientStatusClosed:
		return client, nil
	case models.ClientStatusPending, models.ClientStatusActive, models.ClientStatusSuspended:
	default:
		return nil, fmt.Errorf("%w: cannot close client in status %s", ErrClientInvalidStateTransition, client.Status)
	}

	if s.accountRepository != nil {
		accounts, err := s.accountRepository.List(ctx, repositories.AccountFilter{ClientID: &client.ID})
		if err != nil {
			return nil, err
		}
		for _, account := range accounts {
			if account.Status != models.AccountStatusClosed {
				return nil, fmt.Errorf("%w: account %s must be closed before client closure", ErrClientHasAccounts, account.AccountNumber)
			}
		}
	}

	client.Status = models.ClientStatusClosed
	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return client, nil
}

func (s *DefaultClientService) generateClientNumber(ctx context.Context) (string, error) {
	year := time.Now().UTC().Year()
	for range 10 {
		suffix, err := randomDigits(6)
		if err != nil {
			return "", fmt.Errorf("%w: unable to generate client number", ErrClientValidation)
		}
		clientNumber := fmt.Sprintf("CL-%d-%s", year, suffix)
		_, err = s.clientRepository.GetByNumber(ctx, clientNumber)
		switch {
		case err == nil:
			continue
		case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
			return clientNumber, nil
		default:
			return "", resolveClientRepositoryError(err)
		}
	}

	return "", fmt.Errorf("%w: unable to generate unique client number", ErrClientValidation)
}

func normalizeClientType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeContact(contact models.ClientContact) models.ClientContact {
	normalized := models.ClientContact{
		Email: strings.TrimSpace(contact.Email),
		Phone: strings.TrimSpace(contact.Phone),
	}
	if contact.Address != nil {
		normalized.Address = &models.Address{
			Line1:   strings.TrimSpace(contact.Address.Line1),
			Line2:   strings.TrimSpace(contact.Address.Line2),
			City:    strings.TrimSpace(contact.Address.City),
			State:   strings.TrimSpace(contact.Address.State),
			Country: strings.TrimSpace(contact.Address.Country),
			Postal:  strings.TrimSpace(contact.Address.Postal),
		}
	}
	return normalized
}

func normalizeIndividualData(input *models.IndividualData) *models.IndividualData {
	if input == nil {
		return nil
	}
	return &models.IndividualData{
		FirstName:        strings.TrimSpace(input.FirstName),
		MiddleName:       strings.TrimSpace(input.MiddleName),
		LastName:         strings.TrimSpace(input.LastName),
		DateOfBirth:      input.DateOfBirth,
		Gender:           trimOptionalString(input.Gender),
		NationalIDType:   strings.TrimSpace(input.NationalIDType),
		NationalIDNumber: strings.TrimSpace(input.NationalIDNumber),
		Nationality:      strings.TrimSpace(input.Nationality),
		Occupation:       strings.TrimSpace(input.Occupation),
		Employer:         trimOptionalString(input.Employer),
	}
}

func normalizeCorporateData(input *models.CorporateData) *models.CorporateData {
	if input == nil {
		return nil
	}
	ret := &models.CorporateData{
		LegalName:            strings.TrimSpace(input.LegalName),
		TradingName:          trimOptionalString(input.TradingName),
		RegistrationNumber:   strings.TrimSpace(input.RegistrationNumber),
		TaxID:                strings.TrimSpace(input.TaxID),
		IncorporationDate:    input.IncorporationDate,
		IncorporationCountry: strings.TrimSpace(input.IncorporationCountry),
		Industry:             strings.TrimSpace(input.Industry),
	}
	for _, owner := range input.BeneficialOwners {
		ret.BeneficialOwners = append(ret.BeneficialOwners, models.BeneficialOwner{
			Name:         strings.TrimSpace(owner.Name),
			OwnershipPct: strings.TrimSpace(owner.OwnershipPct),
			NationalID:   strings.TrimSpace(owner.NationalID),
		})
	}
	for _, signatory := range input.AuthorizedSignatories {
		ret.AuthorizedSignatories = append(ret.AuthorizedSignatories, models.AuthorizedSignatory{
			ClientID:     strings.TrimSpace(signatory.ClientID),
			Role:         strings.TrimSpace(signatory.Role),
			SigningLimit: strings.TrimSpace(signatory.SigningLimit),
		})
	}
	return ret
}

func validateClient(client *models.Client) error {
	switch client.Type {
	case models.ClientTypeIndividual:
		if client.CorporateData != nil {
			return fmt.Errorf("%w: corporate_data is not allowed for individual clients", ErrClientValidation)
		}
		if client.IndividualData == nil {
			return fmt.Errorf("%w: individual_data is required for individual clients", ErrClientValidation)
		}
		if client.IndividualData.FirstName == "" || client.IndividualData.LastName == "" {
			return fmt.Errorf("%w: individual clients require first_name and last_name", ErrClientValidation)
		}
	case models.ClientTypeCorporate:
		if client.IndividualData != nil {
			return fmt.Errorf("%w: individual_data is not allowed for corporate clients", ErrClientValidation)
		}
		if client.CorporateData == nil {
			return fmt.Errorf("%w: corporate_data is required for corporate clients", ErrClientValidation)
		}
		if client.CorporateData.LegalName == "" {
			return fmt.Errorf("%w: corporate clients require legal_name", ErrClientValidation)
		}
	default:
		return fmt.Errorf("%w: invalid client type %s", ErrClientValidation, client.Type)
	}

	switch client.Status {
	case models.ClientStatusPending, models.ClientStatusActive, models.ClientStatusSuspended, models.ClientStatusClosed:
	default:
		return fmt.Errorf("%w: invalid client status %s", ErrClientValidation, client.Status)
	}

	switch client.KYCStatus {
	case models.KYCStatusPending, models.KYCStatusVerified, models.KYCStatusRejected, models.KYCStatusExpired:
	default:
		return fmt.Errorf("%w: invalid kyc_status %s", ErrClientValidation, client.KYCStatus)
	}

	if client.KYCLevel < 0 || client.KYCLevel > 3 {
		return fmt.Errorf("%w: kyc_level must be between 0 and 3", ErrClientValidation)
	}

	return nil
}

func resolveClientRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrClientNotFound
	case errors.Is(err, postgres.ErrConstraintsFailed{}):
		return ErrClientAlreadyExists
	default:
		return err
	}
}

func trimOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed
}

func randomDigits(width int) (string, error) {
	if width <= 0 {
		return "", nil
	}
	max := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(width)), nil)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return "", err
	}
	value := n.String()
	if len(value) >= width {
		return value, nil
	}
	return strings.Repeat("0", width-len(value)) + value, nil
}
