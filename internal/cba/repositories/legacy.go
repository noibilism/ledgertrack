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
