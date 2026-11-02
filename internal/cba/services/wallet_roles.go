package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var (
	// ErrWalletResolution means the wallet to route to is missing or not provably unique.
	ErrWalletResolution   = errors.New("wallet could not be resolved")
	ErrWalletNotFound     = errors.New("wallet not found")
	ErrWalletRoleConflict = errors.New("wallet role conflict")
	ErrWalletValidation   = errors.New("wallet validation failed")
)

const (
	// WalletRoleActor is the application-visible identity of the trusted internal callers. Infrastructure
	// authenticates the service; callers cannot supply this value.
	WalletRoleActor = "private_ingress"
)

var (
	currencyPattern      = regexp.MustCompile(`^[A-Z]{3}$`)
	walletCountryPattern = regexp.MustCompile(`^[a-z]{2}$`)
	relationshipPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,180}$`)
)

type ClassifyWalletInput struct {
	ExternalID   string  `json:"external_id"`
	WalletRole   string  `json:"wallet_role"`
	Relationship *string `json:"r2_relationship_public_id"`
	Evidence     string  `json:"evidence_reference"`
	Correction   bool    `json:"correction"`
}

type CreateR2SubaccountInput struct {
	AccountID    string  `json:"account_id"`
	ExternalID   string  `json:"external_id"`
	ProductID    string  `json:"product_id"`
	AccountName  string  `json:"account_name"`
	AccountType  string  `json:"account_type"`
	CountryCode  *string `json:"country_code"`
	Relationship string  `json:"r2_relationship_public_id"`
	Evidence     string  `json:"evidence_reference"`
}

// WalletRoleService implements the wallet-role contract that Ramp Core relies on: which wallet of a merchant is
// its main wallet, which belong to an R2 relationship, and how that classification changes.
type WalletRoleService interface {
	ResolveMainWallet(ctx context.Context, externalID, currency string) (*models.Account, error)
	ResolveR2Subaccount(ctx context.Context, externalID, currency, relationship string) (*models.Account, error)
	List(ctx context.Context, externalID string) ([]models.Account, error)
	// Show returns the account only if it belongs to the client; otherwise ErrWalletNotFound.
	Show(ctx context.Context, accountID, externalID string) (*models.Account, error)
	Classify(ctx context.Context, accountID string, in ClassifyWalletInput) (account *models.Account, replayed bool, err error)
	CreateR2Subaccount(ctx context.Context, in CreateR2SubaccountInput) (account *models.Account, replayed bool, err error)
}

type DefaultWalletRoleService struct {
	lookup   LegacyLookupService
	roles    repositories.WalletRoleRepository
	accounts repositories.AccountRepository
	products repositories.ProductRepository
}

func NewWalletRoleService(
	lookup LegacyLookupService,
	roles repositories.WalletRoleRepository,
	accounts repositories.AccountRepository,
	products repositories.ProductRepository,
) WalletRoleService {
	return &DefaultWalletRoleService{lookup: lookup, roles: roles, accounts: accounts, products: products}
}

func (s *DefaultWalletRoleService) client(ctx context.Context, externalID string) (*models.Client, error) {
	c, err := s.lookup.Client(ctx, externalID)
	if err != nil {
		if errors.Is(err, ErrLegacyNotFound) {
			// Bankman validates external_id with "exists", which is a 422 and not a 404.
			return nil, fmt.Errorf("%w: external_id does not exist", ErrWalletValidation)
		}
		return nil, err
	}
	return c, nil
}

func (s *DefaultWalletRoleService) ResolveMainWallet(ctx context.Context, externalID, currency string) (*models.Account, error) {
	if !currencyPattern.MatchString(currency) {
		return nil, fmt.Errorf("%w: currency", ErrWalletValidation)
	}
	client, err := s.client(ctx, externalID)
	if err != nil {
		return nil, err
	}
	wallets, err := s.roles.EligibleWallets(ctx, client.ID, currency)
	if err != nil {
		return nil, err
	}
	// One snapshot decides everything. An R2-bound wallet is never an ordinary main wallet, whatever its role says.
	var mains, unclassified []models.Account
	for _, w := range wallets {
		if w.R2RelationshipPublicID != nil {
			continue
		}
		switch {
		case w.WalletRole != nil && *w.WalletRole == models.WalletRoleMerchantMain:
			mains = append(mains, w)
		case w.WalletRole == nil:
			unclassified = append(unclassified, w)
		}
	}
	if len(mains) == 1 {
		return &mains[0], nil
	}
	if len(mains) > 1 {
		return nil, ErrWalletResolution
	}
	// Unclassified legacy wallets are a bounded fallback, and never chosen by order.
	if len(unclassified) == 1 {
		return &unclassified[0], nil
	}
	return nil, ErrWalletResolution
}

func (s *DefaultWalletRoleService) ResolveR2Subaccount(ctx context.Context, externalID, currency, relationship string) (*models.Account, error) {
	if !currencyPattern.MatchString(currency) || !relationshipPattern.MatchString(relationship) {
		return nil, fmt.Errorf("%w: currency or relationship", ErrWalletValidation)
	}
	client, err := s.client(ctx, externalID)
	if err != nil {
		return nil, err
	}
	wallets, err := s.roles.EligibleWallets(ctx, client.ID, currency)
	if err != nil {
		return nil, err
	}
	var found []models.Account
	for _, w := range wallets {
		if w.WalletRole != nil && *w.WalletRole == models.WalletRoleR2Subaccount &&
			w.R2RelationshipPublicID != nil && *w.R2RelationshipPublicID == relationship {
			found = append(found, w)
		}
	}
	if len(found) != 1 {
		return nil, ErrWalletResolution
	}
	return &found[0], nil
}

func (s *DefaultWalletRoleService) List(ctx context.Context, externalID string) ([]models.Account, error) {
	client, err := s.client(ctx, externalID)
	if err != nil {
		return nil, err
	}
	return s.roles.ListByClient(ctx, client.ID)
}

func (s *DefaultWalletRoleService) Show(ctx context.Context, accountID, externalID string) (*models.Account, error) {
	client, err := s.client(ctx, externalID)
	if err != nil {
		return nil, err
	}
	account, err := s.lookup.Account(ctx, accountID)
	if err != nil {
		if errors.Is(err, ErrLegacyNotFound) {
			return nil, ErrWalletNotFound
		}
		return nil, err
	}
	if account.ClientID != client.ID {
		return nil, ErrWalletNotFound
	}
	return account, nil
}

func (s *DefaultWalletRoleService) Classify(ctx context.Context, accountID string, in ClassifyWalletInput) (*models.Account, bool, error) {
	if in.WalletRole != models.WalletRoleMerchantMain && in.WalletRole != models.WalletRoleR2Subaccount {
		return nil, false, fmt.Errorf("%w: wallet_role", ErrWalletValidation)
	}
	if in.WalletRole == models.WalletRoleR2Subaccount && (in.Relationship == nil || !relationshipPattern.MatchString(*in.Relationship)) {
		return nil, false, fmt.Errorf("%w: r2_relationship_public_id is required for r2_subaccount", ErrWalletValidation)
	}
	if strings.TrimSpace(in.Evidence) == "" || len(in.Evidence) > 255 {
		return nil, false, fmt.Errorf("%w: evidence_reference", ErrWalletValidation)
	}
	client, err := s.client(ctx, in.ExternalID)
	if err != nil {
		return nil, false, err
	}
	account, err := s.lookup.Account(ctx, accountID)
	if err != nil {
		if errors.Is(err, ErrLegacyNotFound) {
			return nil, false, ErrWalletNotFound
		}
		return nil, false, err
	}
	out, replayed, err := s.roles.Classify(ctx, repositories.ClassifyInput{
		AccountID: account.ID, ClientID: client.ID, Role: in.WalletRole, Relationship: in.Relationship,
		Correction: in.Correction, Actor: WalletRoleActor, Evidence: in.Evidence,
	})
	return out, replayed, mapRoleError(err)
}

func (s *DefaultWalletRoleService) CreateR2Subaccount(ctx context.Context, in CreateR2SubaccountInput) (*models.Account, bool, error) {
	accountID, err := uuid.Parse(in.AccountID)
	if err != nil {
		return nil, false, fmt.Errorf("%w: account_id", ErrWalletValidation)
	}
	productID, err := uuid.Parse(in.ProductID)
	if err != nil {
		return nil, false, fmt.Errorf("%w: product_id", ErrWalletValidation)
	}
	name := strings.TrimSpace(in.AccountName)
	switch {
	case name == "" || len(name) > 255:
		return nil, false, fmt.Errorf("%w: account_name", ErrWalletValidation)
	case in.AccountType != models.AccountTypeMain && in.AccountType != models.AccountTypeReserve:
		return nil, false, fmt.Errorf("%w: account_type", ErrWalletValidation)
	case in.CountryCode != nil && !walletCountryPattern.MatchString(*in.CountryCode):
		return nil, false, fmt.Errorf("%w: country_code", ErrWalletValidation)
	case !relationshipPattern.MatchString(in.Relationship):
		return nil, false, fmt.Errorf("%w: r2_relationship_public_id", ErrWalletValidation)
	case strings.TrimSpace(in.Evidence) == "" || len(in.Evidence) > 255:
		return nil, false, fmt.Errorf("%w: evidence_reference", ErrWalletValidation)
	}

	client, err := s.client(ctx, in.ExternalID)
	if err != nil {
		return nil, false, err
	}
	product, err := s.products.Get(ctx, productID)
	if err != nil {
		if isNotFound(err) {
			return nil, false, fmt.Errorf("%w: product_id does not exist", ErrWalletValidation)
		}
		return nil, false, err
	}
	if product.Status != models.ProductStatusActive {
		return nil, false, fmt.Errorf("%w: the selected product is not active", ErrWalletRoleConflict)
	}

	number, err := s.newAccountNumber(ctx)
	if err != nil {
		return nil, false, err
	}
	out, replayed, err := s.roles.CreateR2Subaccount(ctx, repositories.CreateR2SubaccountInput{
		AccountID: accountID, AccountNumber: number,
		// One wallet per client and product is the rule for ordinary wallets. An R2 wallet is told apart by its
		// relationship, so it gets a wallet id of its own and never collides with the main wallet.
		WalletID: fmt.Sprintf("client-%s-%s-r2-%s", strings.TrimSpace(client.ClientNumber), strings.TrimSpace(product.Code), in.Relationship),
		Client:   client, Product: product, AccountName: name, AccountType: in.AccountType, CountryCode: in.CountryCode,
		Relationship: in.Relationship, Actor: WalletRoleActor, Evidence: in.Evidence,
	})
	return out, replayed, mapRoleError(err)
}

func (s *DefaultWalletRoleService) newAccountNumber(ctx context.Context) (string, error) {
	for range 10 {
		number, err := randomDigits(10)
		if err != nil {
			return "", fmt.Errorf("%w: unable to generate account number", ErrWalletValidation)
		}
		_, err = s.accounts.GetByNumber(ctx, number)
		switch {
		case err == nil:
			continue
		case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
			return number, nil
		default:
			return "", err
		}
	}
	return "", fmt.Errorf("%w: unable to generate unique account number", ErrWalletValidation)
}

func mapRoleError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, repositories.ErrWalletRoleConflict):
		return fmt.Errorf("%w: %s", ErrWalletRoleConflict, strings.TrimPrefix(err.Error(), repositories.ErrWalletRoleConflict.Error()+": "))
	case errors.Is(err, repositories.ErrWalletNotFound):
		return ErrWalletNotFound
	default:
		return err
	}
}
