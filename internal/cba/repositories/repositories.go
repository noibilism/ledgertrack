package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
)

type ProductFilter struct {
	Category *string
	Currency *string
	Status   *string
}

type ClientFilter struct {
	Type      *string
	Status    *string
	KYCStatus *string
}

type AccountFilter struct {
	ClientID  *uuid.UUID
	ProductID *uuid.UUID
	Status    *string
}

type ProductRepository interface {
	Create(context.Context, *models.Product) error
	Update(context.Context, *models.Product) error
	Get(context.Context, uuid.UUID) (*models.Product, error)
	GetByCode(context.Context, string) (*models.Product, error)
	List(context.Context, ProductFilter) ([]models.Product, error)
}

type ClientRepository interface {
	Create(context.Context, *models.Client) error
	Update(context.Context, *models.Client) error
	Get(context.Context, uuid.UUID) (*models.Client, error)
	GetByNumber(context.Context, string) (*models.Client, error)
	List(context.Context, ClientFilter) ([]models.Client, error)
}

type AccountRepository interface {
	Create(context.Context, *models.Account) error
	Update(context.Context, *models.Account) error
	Get(context.Context, uuid.UUID) (*models.Account, error)
	GetByNumber(context.Context, string) (*models.Account, error)
	GetByWalletID(context.Context, string) (*models.Account, error)
	List(context.Context, AccountFilter) ([]models.Account, error)
}

type KYCRepository interface {
	Create(context.Context, *models.KYCRecord) error
	Update(context.Context, *models.KYCRecord) error
	Get(context.Context, uuid.UUID) (*models.KYCRecord, error)
	ListByClient(context.Context, uuid.UUID) ([]models.KYCRecord, error)
}

type InterestAccrualRepository interface {
	Create(context.Context, *models.InterestAccrual) error
	Update(context.Context, *models.InterestAccrual) error
	Get(context.Context, uuid.UUID) (*models.InterestAccrual, error)
	ListByAccount(context.Context, uuid.UUID) ([]models.InterestAccrual, error)
}

type FeePostingRepository interface {
	Create(context.Context, *models.FeePosting) error
	Update(context.Context, *models.FeePosting) error
	GetByReference(context.Context, string) (*models.FeePosting, error)
	ListByAccount(context.Context, uuid.UUID) ([]models.FeePosting, error)
	ListPendingRecovery(context.Context) ([]models.FeePosting, error)
}

type DailyUsageRepository interface {
	Create(context.Context, *models.AccountDailyUsage) error
	Update(context.Context, *models.AccountDailyUsage) error
	GetForDate(context.Context, uuid.UUID, time.Time) (*models.AccountDailyUsage, error)
}

type BunProductRepository struct {
	db bun.IDB
}

type BunClientRepository struct {
	db bun.IDB
}

type BunAccountRepository struct {
	db bun.IDB
}

type BunKYCRepository struct {
	db bun.IDB
}

type BunInterestAccrualRepository struct {
	db bun.IDB
}

type BunFeePostingRepository struct {
	db bun.IDB
}

type BunDailyUsageRepository struct {
	db bun.IDB
}

func NewProductRepository(db bun.IDB) *BunProductRepository {
	return &BunProductRepository{db: db}
}

func NewClientRepository(db bun.IDB) *BunClientRepository {
	return &BunClientRepository{db: db}
}

func NewAccountRepository(db bun.IDB) *BunAccountRepository {
	return &BunAccountRepository{db: db}
}

func NewKYCRepository(db bun.IDB) *BunKYCRepository {
	return &BunKYCRepository{db: db}
}

func NewInterestAccrualRepository(db bun.IDB) *BunInterestAccrualRepository {
	return &BunInterestAccrualRepository{db: db}
}

func NewFeePostingRepository(db bun.IDB) *BunFeePostingRepository {
	return &BunFeePostingRepository{db: db}
}

func NewDailyUsageRepository(db bun.IDB) *BunDailyUsageRepository {
	return &BunDailyUsageRepository{db: db}
}

func (r *BunProductRepository) Create(ctx context.Context, product *models.Product) error {
	setUUID(&product.ID)
	_, err := r.db.NewInsert().Model(product).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunProductRepository) Update(ctx context.Context, product *models.Product) error {
	product.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().
		Model(product).
		Column("code", "name", "description", "category", "currency", "status", "rules", "interest_config", "fee_schedule", "updated_at").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunProductRepository) Get(ctx context.Context, id uuid.UUID) (*models.Product, error) {
	product := &models.Product{}
	err := r.db.NewSelect().Model(product).Where("id = ?", id).Scan(ctx)
	return product, postgres.ResolveError(err)
}

func (r *BunProductRepository) GetByCode(ctx context.Context, code string) (*models.Product, error) {
	product := &models.Product{}
	err := r.db.NewSelect().Model(product).Where("code = ?", code).Scan(ctx)
	return product, postgres.ResolveError(err)
}

func (r *BunProductRepository) List(ctx context.Context, filter ProductFilter) ([]models.Product, error) {
	products := make([]models.Product, 0)
	query := r.db.NewSelect().Model(&products).OrderExpr("created_at desc")
	if filter.Category != nil {
		query = query.Where("category = ?", *filter.Category)
	}
	if filter.Currency != nil {
		query = query.Where("currency = ?", *filter.Currency)
	}
	if filter.Status != nil {
		query = query.Where("status = ?", *filter.Status)
	}
	err := query.Scan(ctx)
	return products, postgres.ResolveError(err)
}

func (r *BunClientRepository) Create(ctx context.Context, client *models.Client) error {
	setUUID(&client.ID)
	_, err := r.db.NewInsert().Model(client).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunClientRepository) Update(ctx context.Context, client *models.Client) error {
	client.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().
		Model(client).
		Column("client_number", "type", "status", "kyc_level", "kyc_status", "kyc_data", "contact", "individual_data", "corporate_data", "updated_at").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunClientRepository) Get(ctx context.Context, id uuid.UUID) (*models.Client, error) {
	client := &models.Client{}
	err := r.db.NewSelect().Model(client).Where("id = ?", id).Scan(ctx)
	return client, postgres.ResolveError(err)
}

func (r *BunClientRepository) GetByNumber(ctx context.Context, clientNumber string) (*models.Client, error) {
	client := &models.Client{}
	err := r.db.NewSelect().Model(client).Where("client_number = ?", clientNumber).Scan(ctx)
	return client, postgres.ResolveError(err)
}

func (r *BunClientRepository) List(ctx context.Context, filter ClientFilter) ([]models.Client, error) {
	clients := make([]models.Client, 0)
	query := r.db.NewSelect().Model(&clients).OrderExpr("created_at desc")
	if filter.Type != nil {
		query = query.Where("type = ?", *filter.Type)
	}
	if filter.Status != nil {
		query = query.Where("status = ?", *filter.Status)
	}
	if filter.KYCStatus != nil {
		query = query.Where("kyc_status = ?", *filter.KYCStatus)
	}
	err := query.Scan(ctx)
	return clients, postgres.ResolveError(err)
}

func (r *BunAccountRepository) Create(ctx context.Context, account *models.Account) error {
	setUUID(&account.ID)
	_, err := r.db.NewInsert().Model(account).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunAccountRepository) Update(ctx context.Context, account *models.Account) error {
	_, err := r.db.NewUpdate().
		Model(account).
		Column("account_number", "client_id", "product_id", "currency", "status", "wallet_id", "freeze_debits", "activated_at", "closed_at", "last_activity_at", "interest_accrued", "metadata").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunAccountRepository) Get(ctx context.Context, id uuid.UUID) (*models.Account, error) {
	account := &models.Account{}
	err := r.db.NewSelect().Model(account).Where("id = ?", id).Scan(ctx)
	return account, postgres.ResolveError(err)
}

func (r *BunAccountRepository) GetByNumber(ctx context.Context, accountNumber string) (*models.Account, error) {
	account := &models.Account{}
	err := r.db.NewSelect().Model(account).Where("account_number = ?", accountNumber).Scan(ctx)
	return account, postgres.ResolveError(err)
}

func (r *BunAccountRepository) GetByWalletID(ctx context.Context, walletID string) (*models.Account, error) {
	account := &models.Account{}
	err := r.db.NewSelect().Model(account).Where("wallet_id = ?", walletID).Scan(ctx)
	return account, postgres.ResolveError(err)
}

func (r *BunAccountRepository) List(ctx context.Context, filter AccountFilter) ([]models.Account, error) {
	accounts := make([]models.Account, 0)
	query := r.db.NewSelect().Model(&accounts).OrderExpr("opened_at desc")
	if filter.ClientID != nil {
		query = query.Where("client_id = ?", *filter.ClientID)
	}
	if filter.ProductID != nil {
		query = query.Where("product_id = ?", *filter.ProductID)
	}
	if filter.Status != nil {
		query = query.Where("status = ?", *filter.Status)
	}
	err := query.Scan(ctx)
	return accounts, postgres.ResolveError(err)
}

func (r *BunKYCRepository) Create(ctx context.Context, record *models.KYCRecord) error {
	setUUID(&record.ID)
	_, err := r.db.NewInsert().Model(record).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunKYCRepository) Update(ctx context.Context, record *models.KYCRecord) error {
	_, err := r.db.NewUpdate().
		Model(record).
		Column("client_id", "level", "status", "submitted_at", "verified_at", "expires_at", "verifier", "reason", "documents", "payload").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunKYCRepository) Get(ctx context.Context, id uuid.UUID) (*models.KYCRecord, error) {
	record := &models.KYCRecord{}
	err := r.db.NewSelect().Model(record).Where("id = ?", id).Scan(ctx)
	return record, postgres.ResolveError(err)
}

func (r *BunKYCRepository) ListByClient(ctx context.Context, clientID uuid.UUID) ([]models.KYCRecord, error) {
	records := make([]models.KYCRecord, 0)
	err := r.db.NewSelect().
		Model(&records).
		Where("client_id = ?", clientID).
		OrderExpr("submitted_at desc").
		Scan(ctx)
	return records, postgres.ResolveError(err)
}

func (r *BunInterestAccrualRepository) Create(ctx context.Context, accrual *models.InterestAccrual) error {
	setUUID(&accrual.ID)
	_, err := r.db.NewInsert().Model(accrual).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunInterestAccrualRepository) Update(ctx context.Context, accrual *models.InterestAccrual) error {
	_, err := r.db.NewUpdate().
		Model(accrual).
		Column("account_id", "accrual_date", "balance_basis", "rate", "amount", "posted", "posted_reference", "metadata").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunInterestAccrualRepository) Get(ctx context.Context, id uuid.UUID) (*models.InterestAccrual, error) {
	accrual := &models.InterestAccrual{}
	err := r.db.NewSelect().Model(accrual).Where("id = ?", id).Scan(ctx)
	return accrual, postgres.ResolveError(err)
}

func (r *BunInterestAccrualRepository) ListByAccount(ctx context.Context, accountID uuid.UUID) ([]models.InterestAccrual, error) {
	accruals := make([]models.InterestAccrual, 0)
	err := r.db.NewSelect().
		Model(&accruals).
		Where("account_id = ?", accountID).
		OrderExpr("accrual_date desc").
		Scan(ctx)
	return accruals, postgres.ResolveError(err)
}

func (r *BunFeePostingRepository) Create(ctx context.Context, feePosting *models.FeePosting) error {
	setUUID(&feePosting.ID)
	_, err := r.db.NewInsert().Model(feePosting).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunFeePostingRepository) Update(ctx context.Context, feePosting *models.FeePosting) error {
	_, err := r.db.NewUpdate().
		Model(feePosting).
		Column("account_id", "event_type", "reference", "linked_reference", "amount", "currency", "status", "metadata").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunFeePostingRepository) GetByReference(ctx context.Context, reference string) (*models.FeePosting, error) {
	feePosting := &models.FeePosting{}
	err := r.db.NewSelect().Model(feePosting).Where("reference = ?", reference).Scan(ctx)
	return feePosting, postgres.ResolveError(err)
}

func (r *BunFeePostingRepository) ListByAccount(ctx context.Context, accountID uuid.UUID) ([]models.FeePosting, error) {
	feePostings := make([]models.FeePosting, 0)
	err := r.db.NewSelect().
		Model(&feePostings).
		Where("account_id = ?", accountID).
		OrderExpr("created_at desc").
		Scan(ctx)
	return feePostings, postgres.ResolveError(err)
}

func (r *BunFeePostingRepository) ListPendingRecovery(ctx context.Context) ([]models.FeePosting, error) {
	feePostings := make([]models.FeePosting, 0)
	err := r.db.NewSelect().
		Model(&feePostings).
		Where("status = ?", models.FeePostingStatusPendingRecovery).
		OrderExpr("created_at asc").
		Scan(ctx)
	return feePostings, postgres.ResolveError(err)
}

func (r *BunDailyUsageRepository) Create(ctx context.Context, usage *models.AccountDailyUsage) error {
	setUUID(&usage.ID)
	_, err := r.db.NewInsert().Model(usage).Returning("*").Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunDailyUsageRepository) Update(ctx context.Context, usage *models.AccountDailyUsage) error {
	usage.UpdatedAt = time.Now().UTC()
	_, err := r.db.NewUpdate().
		Model(usage).
		Column("account_id", "usage_date", "debit_amount", "credit_amount", "debit_count", "credit_count", "last_reference", "updated_at").
		WherePK().
		Returning("*").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (r *BunDailyUsageRepository) GetForDate(ctx context.Context, accountID uuid.UUID, usageDate time.Time) (*models.AccountDailyUsage, error) {
	usage := &models.AccountDailyUsage{}
	err := r.db.NewSelect().
		Model(usage).
		Where("account_id = ?", accountID).
		Where("usage_date = ?", usageDate.UTC().Truncate(24*time.Hour)).
		Scan(ctx)
	return usage, postgres.ResolveError(err)
}

func setUUID(id *uuid.UUID) {
	if *id == uuid.Nil {
		*id = uuid.New()
	}
}
