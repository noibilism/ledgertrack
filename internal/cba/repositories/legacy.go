package repositories

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

// LegacyLookupRepository finds accounts and clients by the identifiers they had in the system they were
// imported from. The importer keeps the legacy account id in the account metadata (legacy_source_id) and the
// legacy client external id in the KYC data (legacy_external_id); both are indexed.
type LegacyLookupRepository interface {
	AccountByLegacySourceID(ctx context.Context, legacyID string) (*models.Account, error)
	ClientByLegacyExternalID(ctx context.Context, externalID string) (*models.Client, error)
	// ClientByLegacySourceID finds a client by the id it had in the legacy system (not its external_id).
	ClientByLegacySourceID(ctx context.Context, legacyID string) (*models.Client, error)
	// ClientConflicts names the fields (email, phone, registration_number, tax_id, external_id) that another
	// client already uses. Empty inputs are ignored.
	ClientConflicts(ctx context.Context, c ClientIdentity) ([]string, error)
	// ActiveWalletStats counts the active wallets and their owners per currency.
	ActiveWalletStats(ctx context.Context) ([]WalletStat, error)
}

// WalletStat is one row of the active-wallet summary.
type WalletStat struct {
	Currency string `bun:"currency"`
	Wallets  int    `bun:"wallets"`
	Clients  int    `bun:"clients"`
}

type BunLegacyLookupRepository struct {
	db bun.IDB
}

func NewLegacyLookupRepository(db bun.IDB) *BunLegacyLookupRepository {
	return &BunLegacyLookupRepository{db: db}
}

func (r *BunLegacyLookupRepository) AccountByLegacySourceID(ctx context.Context, legacyID string) (*models.Account, error) {
	account := &models.Account{}
	err := r.db.NewSelect().Model(account).
		Where("(metadata ->> 'legacy_source_id') = ?", legacyID).
		Limit(1).
		Scan(ctx)
	return account, postgres.ResolveError(err)
}

func (r *BunLegacyLookupRepository) ClientByLegacyExternalID(ctx context.Context, externalID string) (*models.Client, error) {
	client := &models.Client{}
	err := r.db.NewSelect().Model(client).
		Where("(kyc_data ->> 'legacy_external_id') = ?", externalID).
		Limit(1).
		Scan(ctx)
	return client, postgres.ResolveError(err)
}

func (r *BunLegacyLookupRepository) ClientByLegacySourceID(ctx context.Context, legacyID string) (*models.Client, error) {
	client := &models.Client{}
	err := r.db.NewSelect().Model(client).
		Where("(kyc_data ->> 'legacy_source_id') = ?", legacyID).
		Limit(1).
		Scan(ctx)
	return client, postgres.ResolveError(err)
}

func (r *BunLegacyLookupRepository) ActiveWalletStats(ctx context.Context) ([]WalletStat, error) {
	var out []WalletStat
	err := r.db.NewSelect().
		TableExpr("_system.accounts").
		ColumnExpr("currency, count(*) as wallets, count(distinct client_id) as clients").
		Where("status = 'active'").
		GroupExpr("currency").
		OrderExpr("currency").
		Scan(ctx, &out)
	return out, postgres.ResolveError(err)
}

// ClientIdentity is the set of values that must be unique across clients.
type ClientIdentity struct {
	Email, Phone, RegistrationNumber, TaxID, ExternalID string
}

func (r *BunLegacyLookupRepository) ClientConflicts(ctx context.Context, c ClientIdentity) ([]string, error) {
	checks := []struct{ name, value, expr string }{
		{"email", c.Email, "lower(contact ->> 'email') = lower(?)"},
		{"phone_number", c.Phone, "(contact ->> 'phone') = ?"},
		{"business_registration_number", c.RegistrationNumber, "(corporate_data ->> 'registration_number') = ?"},
		{"tax_identification_number", c.TaxID, "(corporate_data ->> 'tax_id') = ?"},
		{"external_id", c.ExternalID, "(kyc_data ->> 'legacy_external_id') = ?"},
	}
	var found []string
	for _, ch := range checks {
		if ch.value == "" {
			continue
		}
		exists, err := r.db.NewSelect().TableExpr("_system.clients").Where(ch.expr, ch.value).Exists(ctx)
		if err != nil {
			return nil, postgres.ResolveError(err)
		}
		if exists {
			found = append(found, ch.name)
		}
	}
	return found, nil
}
