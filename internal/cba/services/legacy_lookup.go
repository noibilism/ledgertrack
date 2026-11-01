package services

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var ErrLegacyNotFound = errors.New("legacy identifier not found")

// LegacyLookupService resolves the identifiers callers of the Bankman API hold. An id that is a LedgerTrack
// id (an account opened after cutover) resolves directly; any other id is looked up as a legacy id.
type LegacyLookupService interface {
	// Account finds an account by its LedgerTrack id or by the Bankman account id it was imported with.
	Account(ctx context.Context, id string) (*models.Account, error)
	// Client finds a client by the Bankman external_id it was imported with, or by its LedgerTrack id.
	Client(ctx context.Context, externalID string) (*models.Client, error)
	// ClientByID finds a client by the Bankman client id it was imported with, or by its LedgerTrack id.
	ClientByID(ctx context.Context, id string) (*models.Client, error)
}

type DefaultLegacyLookupService struct {
	accounts repositories.AccountRepository
	clients  repositories.ClientRepository
	legacy   repositories.LegacyLookupRepository
}

func NewLegacyLookupService(accounts repositories.AccountRepository, clients repositories.ClientRepository, legacy repositories.LegacyLookupRepository) LegacyLookupService {
	return &DefaultLegacyLookupService{accounts: accounts, clients: clients, legacy: legacy}
}

func isNotFound(err error) bool {
	return postgres.IsNotFoundError(err) || errors.Is(err, postgres.ErrNotFound)
}

func (s *DefaultLegacyLookupService) Account(ctx context.Context, id string) (*models.Account, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return nil, ErrLegacyNotFound
	}
	if parsed, err := uuid.Parse(id); err == nil {
		account, err := s.accounts.Get(ctx, parsed)
		if err == nil {
			return account, nil
		}
		if !isNotFound(err) {
			return nil, err
		}
	}
	account, err := s.legacy.AccountByLegacySourceID(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrLegacyNotFound
		}
		return nil, err
	}
	return account, nil
}

func (s *DefaultLegacyLookupService) Client(ctx context.Context, externalID string) (*models.Client, error) {
	externalID = strings.ToLower(strings.TrimSpace(externalID))
	if externalID == "" {
		return nil, ErrLegacyNotFound
	}
	client, err := s.legacy.ClientByLegacyExternalID(ctx, externalID)
	if err == nil {
		return client, nil
	}
	if !isNotFound(err) {
		return nil, err
	}
	if parsed, perr := uuid.Parse(externalID); perr == nil {
		client, err := s.clients.Get(ctx, parsed)
		if err == nil {
			return client, nil
		}
		if !isNotFound(err) {
			return nil, err
		}
	}
	return nil, ErrLegacyNotFound
}

func (s *DefaultLegacyLookupService) ClientByID(ctx context.Context, id string) (*models.Client, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return nil, ErrLegacyNotFound
	}
	client, err := s.legacy.ClientByLegacySourceID(ctx, id)
	if err == nil {
		return client, nil
	}
	if !isNotFound(err) {
		return nil, err
	}
	if parsed, perr := uuid.Parse(id); perr == nil {
		client, err := s.clients.Get(ctx, parsed)
		if err == nil {
			return client, nil
		}
		if !isNotFound(err) {
			return nil, err
		}
	}
	return nil, ErrLegacyNotFound
}
