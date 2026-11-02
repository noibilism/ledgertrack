package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

var (
	// ErrWalletRoleConflict is returned when a request contradicts the role or identity already recorded.
	ErrWalletRoleConflict = errors.New("wallet role conflict")
	// ErrWalletNotFound is returned when the account does not exist or does not belong to the client. The two are
	// deliberately indistinguishable, to prevent cross-merchant discovery.
	ErrWalletNotFound = errors.New("wallet not found")
)

type ClassifyInput struct {
	AccountID    uuid.UUID
	ClientID     uuid.UUID
	Role         string  // merchant_main or r2_subaccount
	Relationship *string // required for r2_subaccount
	Correction   bool
	Actor        string
	Evidence     string
}

type CreateR2SubaccountInput struct {
	AccountID     uuid.UUID
	AccountNumber string
	WalletID      string
	Client        *models.Client
	Product       *models.Product
	AccountName   string
	AccountType   string
	CountryCode   *string
	Relationship  string
	Actor         string
	Evidence      string
}

// WalletRoleRepository changes and reads wallet roles. Every change runs in one transaction with the
// account rows locked, and is audited.
type WalletRoleRepository interface {
	Classify(ctx context.Context, in ClassifyInput) (account *models.Account, replayed bool, err error)
	CreateR2Subaccount(ctx context.Context, in CreateR2SubaccountInput) (account *models.Account, replayed bool, err error)
	ListByClient(ctx context.Context, clientID uuid.UUID) ([]models.Account, error)
	// EligibleWallets are the active main wallets of a client in a currency, with every role.
	EligibleWallets(ctx context.Context, clientID uuid.UUID, currency string) ([]models.Account, error)
}

type BunWalletRoleRepository struct {
	db *bun.DB
}

func NewWalletRoleRepository(db *bun.DB) *BunWalletRoleRepository {
	return &BunWalletRoleRepository{db: db}
}

func strEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (r *BunWalletRoleRepository) Classify(ctx context.Context, in ClassifyInput) (*models.Account, bool, error) {
	var (
		out      *models.Account
		replayed bool
	)
	err := r.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		account := &models.Account{}
		if err := tx.NewSelect().Model(account).Where("id = ?", in.AccountID).For("UPDATE").Scan(ctx); err != nil {
			if errors.Is(postgres.ResolveError(err), postgres.ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
				return ErrWalletNotFound
			}
			return err
		}
		if account.ClientID != in.ClientID {
			return ErrWalletNotFound
		}

		var requestedRelationship *string
		if in.Role == models.WalletRoleR2Subaccount {
			requestedRelationship = in.Relationship
		}
		currentRole, currentRelationship := account.WalletRole, account.R2RelationshipPublicID

		if currentRole != nil && *currentRole == models.WalletRoleR2Subaccount {
			if in.Role != models.WalletRoleR2Subaccount || !strEq(currentRelationship, requestedRelationship) {
				return fmt.Errorf("%w: R2 relationship ownership is immutable", ErrWalletRoleConflict)
			}
			out, replayed = account, true
			return nil
		}
		if currentRole != nil && *currentRole == models.WalletRoleMerchantMain {
			if in.Role == models.WalletRoleMerchantMain {
				out, replayed = account, true
				return nil
			}
			if !in.Correction {
				return fmt.Errorf("%w: wallet role is already set; use an explicit correction with evidence", ErrWalletRoleConflict)
			}
		}

		if in.Role == models.WalletRoleR2Subaccount {
			bound, err := r.relationshipWallet(ctx, tx, *requestedRelationship, account.Currency)
			if err != nil {
				return err
			}
			if bound != nil && bound.ID != account.ID {
				return fmt.Errorf("%w: R2 relationship is already bound for this currency", ErrWalletRoleConflict)
			}
		}

		role := in.Role
		account.WalletRole = &role
		account.R2RelationshipPublicID = requestedRelationship
		if _, err := tx.NewUpdate().Model(account).
			Column("wallet_role", "r2_relationship_public_id").WherePK().Exec(ctx); err != nil {
			return err
		}
		action := "classified"
		if currentRole != nil {
			action = "corrected"
		}
		if err := r.audit(ctx, tx, account, in.Actor, action, currentRole, currentRelationship, in.Evidence); err != nil {
			return err
		}
		out = account
		return nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, false, fmt.Errorf("%w: R2 relationship is already bound for this currency", ErrWalletRoleConflict)
		}
		return nil, false, err
	}
	return out, replayed, nil
}

func (r *BunWalletRoleRepository) CreateR2Subaccount(ctx context.Context, in CreateR2SubaccountInput) (*models.Account, bool, error) {
	run := func() (*models.Account, bool, error) {
		var (
			out      *models.Account
			replayed bool
		)
		err := r.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
			existing := &models.Account{}
			err := tx.NewSelect().Model(existing).Where("id = ?", in.AccountID).For("UPDATE").Scan(ctx)
			switch {
			case err == nil:
				out, err = replayOrFail(existing, in)
				replayed = true
				return err
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}

			bound, err := r.relationshipWallet(ctx, tx, in.Relationship, in.Product.Currency)
			if err != nil {
				return err
			}
			if bound != nil {
				out, err = replayOrFail(bound, in)
				replayed = true
				return err
			}

			now := time.Now().UTC()
			role, relationship, accountType, name := models.WalletRoleR2Subaccount, in.Relationship, in.AccountType, in.AccountName
			account := &models.Account{
				ID:                     in.AccountID,
				AccountNumber:          in.AccountNumber,
				ClientID:               in.Client.ID,
				ProductID:              in.Product.ID,
				Currency:               in.Product.Currency,
				Status:                 models.AccountStatusActive,
				WalletID:               in.WalletID,
				OpenedAt:               now,
				ActivatedAt:            &now,
				Metadata:               map[string]any{},
				AccountName:            &name,
				AccountType:            &accountType,
				CountryCode:            in.CountryCode,
				WalletRole:             &role,
				R2RelationshipPublicID: &relationship,
			}
			if _, err := tx.NewInsert().Model(account).Returning("*").Exec(ctx); err != nil {
				return err
			}
			if err := r.audit(ctx, tx, account, in.Actor, "created", nil, nil, in.Evidence); err != nil {
				return err
			}
			out = account
			return nil
		})
		return out, replayed, err
	}

	out, replayed, err := run()
	if err != nil && isUniqueViolation(err) {
		// A competing request won the race. It is a replay only if its identity matches exactly.
		return run()
	}
	return out, replayed, err
}

func replayOrFail(a *models.Account, in CreateR2SubaccountInput) (*models.Account, error) {
	if a.WalletRole == nil || *a.WalletRole != models.WalletRoleR2Subaccount ||
		!strEq(a.R2RelationshipPublicID, &in.Relationship) ||
		a.ID != in.AccountID || a.ClientID != in.Client.ID || a.ProductID != in.Product.ID ||
		!strEq(a.AccountType, &in.AccountType) || !strEq(a.CountryCode, in.CountryCode) {
		return nil, fmt.Errorf("%w: R2 wallet identity conflicts with an existing account", ErrWalletRoleConflict)
	}
	return a, nil
}

func (r *BunWalletRoleRepository) relationshipWallet(ctx context.Context, tx bun.Tx, relationship, currency string) (*models.Account, error) {
	account := &models.Account{}
	err := tx.NewSelect().Model(account).
		Where("r2_relationship_public_id = ?", relationship).
		Where("currency = ?", currency).
		For("UPDATE").Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return account, err
}

func (r *BunWalletRoleRepository) audit(ctx context.Context, tx bun.Tx, a *models.Account, actor, action string, prevRole, prevRel *string, evidence string) error {
	row := &models.AccountWalletRoleAudit{
		ID:                             uuid.New(),
		AccountID:                      a.ID,
		ActorService:                   actor,
		Action:                         action,
		PreviousWalletRole:             prevRole,
		PreviousR2RelationshipPublicID: prevRel,
		WalletRole:                     a.WalletRole,
		R2RelationshipPublicID:         a.R2RelationshipPublicID,
		EvidenceReference:              evidence,
	}
	_, err := tx.NewInsert().Model(row).Exec(ctx)
	return err
}

func (r *BunWalletRoleRepository) ListByClient(ctx context.Context, clientID uuid.UUID) ([]models.Account, error) {
	out := make([]models.Account, 0)
	err := r.db.NewSelect().Model(&out).Where("client_id = ?", clientID).OrderExpr("opened_at asc, id asc").Scan(ctx)
	return out, postgres.ResolveError(err)
}

func (r *BunWalletRoleRepository) EligibleWallets(ctx context.Context, clientID uuid.UUID, currency string) ([]models.Account, error) {
	out := make([]models.Account, 0)
	err := r.db.NewSelect().Model(&out).
		Where("client_id = ?", clientID).
		Where("currency = ?", currency).
		Where("status = ?", models.AccountStatusActive).
		Where("account_type = ?", models.AccountTypeMain).
		OrderExpr("opened_at asc, id asc").
		Scan(ctx)
	return out, postgres.ResolveError(err)
}

func isUniqueViolation(err error) bool {
	return errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{})
}
