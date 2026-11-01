package system

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/pkg/features"
)

// GetMigrator creates a Migrator configured with the package's system schema migrations for the given database.
// It appends the system schema option to any provided migration options, registers all system migrations, and returns the configured *migrations.Migrator.
func GetMigrator(db bun.IDB, options ...migrations.Option) *migrations.Migrator {

	// configuration table has been removed, we keep the model to keep migrations consistent but the table is not used anymore.
	type configuration struct {
		bun.BaseModel `bun:"_system.configuration,alias:configuration"`

		Key     string    `bun:"key,type:varchar(255),pk"`
		Value   string    `bun:"value,type:text"`
		AddedAt time.Time `bun:"addedAt,type:timestamp"`
	}

	options = append(options, migrations.WithSchema(SchemaSystem))

	migrator := migrations.NewMigrator(db, options...)
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "Init schema",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					create table _system.ledgers (
						ledger varchar primary key,
						addedat timestamp,
						bucket varchar(255)
					)
				`)
					if err != nil {
						return err
					}

					_, err = tx.NewCreateTable().
						Model((*configuration)(nil)).
						Exec(ctx)
					return postgres.ResolveError(err)
				})
			},
		},
		migrations.Migration{
			Name: "Add ledger, bucket naming constraints 63 chars",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						alter column ledger type varchar(63),
						alter column bucket type varchar(63);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add ledger metadata",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						add column if not exists metadata jsonb;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Fix empty ledger metadata",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						update _system.ledgers
						set metadata = '{}'::jsonb
						where metadata is null;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add ledger state",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						add column if not exists state varchar(255) default 'initializing';
	
						update _system.ledgers
						set state = 'in-use'
						where state = '';
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add features column",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					add column if not exists features jsonb;
				`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Rename ledger column to name",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					rename column ledger to name;
				`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add sequential id on ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create sequence _system.ledger_sequence;
							
						alter table _system.ledgers 
						add column id bigint default nextval('_system.ledger_sequence');
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add aggregate_objects pg aggregator",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, aggregateObjects)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Remove ledger state column",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						drop column state;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Remove configuration table",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						drop table _system.configuration;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Generate addedat of table ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					alter column addedat type timestamp without time zone;

					alter table _system.ledgers
					alter column addedat set default (now() at time zone 'utc');

					alter table _system.ledgers
					rename column addedat to added_at;
				`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "add pgcrypto",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create extension if not exists pgcrypto
						with schema public;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Configure features for old ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					update _system.ledgers
					set features = ?
					where features is null;
				`, features.DefaultFeatures)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add state column to ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					add column state varchar(255) default 'initializing';
				`, features.DefaultFeatures)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add json_compact function",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, jsonCompact)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "set default metadata on ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						alter column metadata set default '{}'::jsonb;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "add pipelines",
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					create table _system.exporters (
					    id varchar,
					    driver varchar,
					    config varchar,
					    created_at timestamp,
					    
					    primary key(id)   
					);

					create table _system.pipelines (
					    id varchar,
					    ledger varchar,
					    exporter_id varchar references _system.exporters (id) on delete cascade,
					    created_at timestamp,
					    enabled bool,
					    last_log_id bigint,
					    error varchar,
					    version int,
					    
					    primary key(id)
					);
					create unique index on _system.pipelines (ledger, exporter_id);
				`)
				return err
			},
		},
		migrations.Migration{
			Name: "Add deleted_at column to ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						add column if not exists deleted_at timestamp;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add CBA domain tables",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.products (
							id uuid primary key default gen_random_uuid(),
							code varchar(64) not null unique,
							name varchar(255) not null,
							description text,
							category varchar(128) not null,
							currency varchar(16) not null,
							status varchar(32) not null check (status in ('draft', 'active', 'retired')),
							rules jsonb not null default '{}'::jsonb,
							interest_config jsonb,
							fee_schedule jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc')
						);
						create index if not exists idx_products_category_currency_status on _system.products(category, currency, status);

						create table if not exists _system.clients (
							id uuid primary key default gen_random_uuid(),
							client_number varchar(64) not null unique,
							type varchar(32) not null check (type in ('individual', 'corporate')),
							status varchar(32) not null check (status in ('pending', 'active', 'suspended', 'closed')),
							kyc_level integer not null default 0,
							kyc_status varchar(32) not null default 'pending' check (kyc_status in ('pending', 'verified', 'rejected', 'expired')),
							kyc_data jsonb not null default '{}'::jsonb,
							contact jsonb not null default '{}'::jsonb,
							individual_data jsonb,
							corporate_data jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc')
						);
						create index if not exists idx_clients_type_status_kyc on _system.clients(type, status, kyc_level, kyc_status);

						create table if not exists _system.accounts (
							id uuid primary key default gen_random_uuid(),
							account_number varchar(32) not null unique,
							client_id uuid not null references _system.clients(id),
							product_id uuid not null references _system.products(id),
							currency varchar(16) not null,
							status varchar(32) not null check (status in ('pending', 'active', 'dormant', 'suspended', 'closed')),
							wallet_id varchar(255) not null unique,
							freeze_debits boolean not null default false,
							opened_at timestamp without time zone not null default (now() at time zone 'utc'),
							activated_at timestamp without time zone,
							closed_at timestamp without time zone,
							last_activity_at timestamp without time zone,
							interest_accrued numeric(20,8) not null default 0,
							metadata jsonb not null default '{}'::jsonb
						);
						create index if not exists idx_accounts_client_status on _system.accounts(client_id, status);
						create index if not exists idx_accounts_product_status on _system.accounts(product_id, status);
						create index if not exists idx_accounts_last_activity on _system.accounts(last_activity_at);

						create table if not exists _system.kyc_records (
							id uuid primary key default gen_random_uuid(),
							client_id uuid not null references _system.clients(id),
							level integer not null,
							status varchar(32) not null check (status in ('pending', 'verified', 'rejected', 'expired')),
							submitted_at timestamp without time zone not null,
							verified_at timestamp without time zone,
							expires_at timestamp without time zone,
							verifier varchar(255),
							reason text,
							documents jsonb not null default '[]'::jsonb,
							payload jsonb not null default '{}'::jsonb
						);
						create index if not exists idx_kyc_records_client on _system.kyc_records(client_id);
						create index if not exists idx_kyc_records_client_status_level on _system.kyc_records(client_id, status, level);

						create table if not exists _system.interest_accruals (
							id uuid primary key default gen_random_uuid(),
							account_id uuid not null references _system.accounts(id),
							accrual_date date not null,
							balance_basis numeric(20,8) not null,
							rate numeric(12,8) not null,
							amount numeric(20,8) not null,
							posted boolean not null default false,
							posted_reference varchar(255),
							metadata jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							constraint interest_accruals_account_date_unique unique (account_id, accrual_date)
						);
						create index if not exists idx_interest_accruals_posted_date on _system.interest_accruals(posted, accrual_date);

						create table if not exists _system.fee_postings (
							id uuid primary key default gen_random_uuid(),
							account_id uuid not null references _system.accounts(id),
							event_type varchar(64) not null,
							reference varchar(255) not null unique,
							linked_reference varchar(255) not null,
							amount numeric(20,8) not null,
							currency varchar(16) not null,
							status varchar(64) not null,
							metadata jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc')
						);
						create index if not exists idx_fee_postings_account on _system.fee_postings(account_id);
						create index if not exists idx_fee_postings_linked_reference on _system.fee_postings(linked_reference);

						create table if not exists _system.account_daily_usages (
							id uuid primary key default gen_random_uuid(),
							account_id uuid not null references _system.accounts(id),
							usage_date date not null,
							debit_amount numeric(20,8) not null default 0,
							credit_amount numeric(20,8) not null default 0,
							debit_count bigint not null default 0,
							credit_count bigint not null default 0,
							last_reference varchar(255),
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc'),
							constraint account_daily_usages_account_date_unique unique (account_id, usage_date)
						);
						create index if not exists idx_account_daily_usages_usage_date on _system.account_daily_usages(usage_date);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add currencies registry table",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.currencies (
							code varchar(16) primary key,
							precision integer not null,
							enabled boolean not null default true,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc')
						);

						insert into _system.currencies (code, precision, enabled)
						values
							('USD', 2, true),
							('EUR', 2, true),
							('BTC', 8, true),
							('NGN', 2, true),
							('GHS', 2, true),
							('KES', 2, true),
							('ZMW', 2, true)
						on conflict (code) do update
						set precision = excluded.precision,
							enabled = excluded.enabled,
							updated_at = (now() at time zone 'utc');
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add channel fee config and revenue tables",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.channel_fee_configs (
							id uuid primary key default gen_random_uuid(),
							channel_id varchar(255) not null unique,
							currency varchar(16) not null,
							enabled boolean not null default true,
							user_fee jsonb not null default '{}'::jsonb,
							processing_fee jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc')
						);
						create index if not exists idx_channel_fee_configs_currency on _system.channel_fee_configs(currency);

						create table if not exists _system.channel_fee_config_audits (
							id uuid primary key default gen_random_uuid(),
							channel_id varchar(255) not null,
							actor varchar(255),
							action varchar(32) not null,
							before jsonb,
							after jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc')
						);
						create index if not exists idx_channel_fee_config_audits_channel on _system.channel_fee_config_audits(channel_id, created_at desc);

						create table if not exists _system.channel_fee_records (
							id uuid primary key default gen_random_uuid(),
							channel_id varchar(255) not null,
							currency varchar(16) not null,
							wallet_id varchar(255),
							reference varchar(255) not null,
							ledger_tx_id bigint,
							channel_tx_id bigint,
							revenue_tx_id bigint,
							occurred_at timestamp without time zone not null default (now() at time zone 'utc'),
							total_amount bigint not null,
							principal_amount bigint not null,
							user_fee_amount bigint not null,
							processing_fee_amount bigint not null,
							net_revenue_amount bigint not null,
							metadata jsonb not null default '{}'::jsonb
						);
						create index if not exists idx_channel_fee_records_channel_time on _system.channel_fee_records(channel_id, occurred_at desc);
						create index if not exists idx_channel_fee_records_time on _system.channel_fee_records(occurred_at desc);
						create index if not exists idx_channel_fee_records_reference on _system.channel_fee_records(reference);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add GL accounts registry table",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.gl_accounts (
							id uuid primary key default gen_random_uuid(),
							code varchar(64) not null,
							name varchar(255) not null,
							type varchar(16) not null check (type in ('asset', 'liability', 'equity', 'income', 'expense')),
							normal_side varchar(8) not null check (normal_side in ('debit', 'credit')),
							currency varchar(16) not null,
							parent_code varchar(64),
							ledger varchar(255) not null,
							address varchar(512) not null check (address like 'gl:%'),
							status varchar(16) not null default 'active' check (status in ('active', 'closed')),
							metadata jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc'),
							constraint gl_accounts_code_unique unique (code),
							constraint gl_accounts_ledger_address_unique unique (ledger, address),
							constraint gl_accounts_parent_fk foreign key (parent_code) references _system.gl_accounts(code),
							constraint gl_accounts_not_own_parent check (parent_code is null or parent_code <> code)
						);
						create index if not exists idx_gl_accounts_currency_status on _system.gl_accounts(currency, status);
						create index if not exists idx_gl_accounts_type on _system.gl_accounts(type);
						create index if not exists idx_gl_accounts_parent on _system.gl_accounts(parent_code);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add transfers table for multi-ledger wallet transfers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.transfers (
							id uuid primary key default gen_random_uuid(),
							reference varchar(255) not null,
							idempotency_key varchar(255) not null default '',
							request_hash varchar(64) not null,
							user_ledger varchar(63) not null,
							wallet_id varchar(255) not null,
							currency varchar(16) not null,
							kind varchar(32) not null check (kind in ('debit', 'release_pay')),
							state varchar(32) not null check (state in (
								'created', 'user_posted', 'channel_posted', 'revenue_posted',
								'completed', 'failed', 'failed_needs_review'
							)),
							amount bigint not null check (amount > 0),
							channel_id varchar(255) not null default '',
							channel_amount bigint not null default 0,
							user_fee bigint not null default 0,
							processing_fee bigint not null default 0,
							net_revenue bigint not null default 0,
							user_metadata jsonb not null default '{}'::jsonb,
							user_tx_id bigint,
							channel_tx_id bigint,
							revenue_tx_id bigint,
							result jsonb not null default '{}'::jsonb,
							attempts integer not null default 0,
							last_error text not null default '',
							lease_owner varchar(255),
							lease_until timestamp without time zone,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc'),
							completed_at timestamp without time zone,
							constraint transfers_reference_unique unique (reference)
						);
						create index if not exists idx_transfers_recovery
							on _system.transfers(updated_at)
							where state in ('created', 'user_posted', 'channel_posted', 'revenue_posted');
						create index if not exists idx_transfers_needs_review
							on _system.transfers(updated_at)
							where state = 'failed_needs_review';
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add currencies used by Bankman products",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					// Precision 2 for every code, including XOF and XAF, to match the
					// decimal(19,4) values Bankman holds. do nothing on conflict so a
					// precision an operator already set is never overwritten.
					_, err := tx.ExecContext(ctx, `
						insert into _system.currencies (code, precision, enabled)
						values
							('XOF', 2, true),
							('XAF', 2, true),
							('TZS', 2, true),
							('MZN', 2, true),
							('ZAR', 2, true)
						on conflict (code) do nothing;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add fee overrides and tax rules tables",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.fee_overrides (
							id uuid primary key default gen_random_uuid(),
							scope varchar(16) not null check (scope in ('client', 'account')),
							client_id uuid,
							account_id uuid,
							product_id uuid,
							currency varchar(16) not null,
							event varchar(16) not null check (event in ('debit', 'credit', 'transfer')),
							rules jsonb not null,
							effective_from timestamp without time zone not null,
							effective_to timestamp without time zone,
							status varchar(16) not null default 'active' check (status in ('active', 'revoked')),
							metadata jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc'),
							constraint fee_overrides_target check (
								(scope = 'client' and client_id is not null and account_id is null) or
								(scope = 'account' and account_id is not null and client_id is null and product_id is null)
							),
							constraint fee_overrides_period check (effective_to is null or effective_to > effective_from)
						);
						create index if not exists idx_fee_overrides_client on _system.fee_overrides(client_id, event) where client_id is not null;
						create index if not exists idx_fee_overrides_account on _system.fee_overrides(account_id, event) where account_id is not null;

						create table if not exists _system.tax_rules (
							id uuid primary key default gen_random_uuid(),
							country varchar(2) not null,
							code varchar(32) not null,
							name varchar(255) not null,
							event varchar(16) not null check (event in ('debit', 'credit', 'transfer')),
							base varchar(16) not null check (base in ('fee', 'amount')),
							rate varchar(32) not null,
							min_amount varchar(32),
							max_amount varchar(32),
							payable_accounts jsonb not null,
							effective_from timestamp without time zone not null,
							effective_to timestamp without time zone,
							status varchar(16) not null default 'active' check (status in ('active', 'revoked')),
							metadata jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc'),
							constraint tax_rules_period check (effective_to is null or effective_to > effective_from)
						);
						create index if not exists idx_tax_rules_lookup on _system.tax_rules(country, event, status);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add reserve rules table",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.reserve_rules (
							id uuid primary key default gen_random_uuid(),
							scope varchar(16) not null check (scope in ('client', 'account')),
							client_id uuid,
							account_id uuid,
							product_id uuid,
							currency varchar(16) not null,
							percentage varchar(32) not null,
							hold_hours integer not null check (hold_hours between 1 and 8760),
							effective_from timestamp without time zone not null,
							effective_to timestamp without time zone,
							status varchar(16) not null default 'active' check (status in ('active', 'revoked')),
							metadata jsonb not null default '{}'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc'),
							constraint reserve_rules_target check (
								(scope = 'client' and client_id is not null and account_id is null) or
								(scope = 'account' and account_id is not null and client_id is null and product_id is null)
							),
							constraint reserve_rules_period check (effective_to is null or effective_to > effective_from)
						);
						create index if not exists idx_reserve_rules_client on _system.reserve_rules(client_id) where client_id is not null;
						create index if not exists idx_reserve_rules_account on _system.reserve_rules(account_id) where account_id is not null;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add business calendars and reserve rule credit types",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table if not exists _system.business_calendars (
							id uuid primary key default gen_random_uuid(),
							code varchar(64) not null unique,
							name varchar(255) not null,
							timezone varchar(64) not null,
							working_days jsonb not null,
							open_time varchar(5) not null,
							close_time varchar(5) not null,
							holidays jsonb not null default '[]'::jsonb,
							created_at timestamp without time zone not null default (now() at time zone 'utc'),
							updated_at timestamp without time zone not null default (now() at time zone 'utc')
						);
						alter table _system.reserve_rules
							add column if not exists credit_types jsonb not null default '[]'::jsonb,
							add column if not exists hold_mode varchar(16) not null default 'calendar',
							add column if not exists calendar_id uuid references _system.business_calendars(id);
						alter table _system.reserve_rules
							add constraint reserve_rules_hold_mode check (
								(hold_mode = 'calendar' and calendar_id is null) or
								(hold_mode = 'business' and calendar_id is not null)
							);
					`)
					return err
				})
			},
		},
	)

	return migrator
}

func Migrate(ctx context.Context, db *bun.DB, options ...migrations.Option) error {
	return GetMigrator(db, options...).Up(ctx)
}

const aggregateObjects = `
create or replace function public.jsonb_concat(a jsonb, b jsonb) returns jsonb
    as 'select $1 || $2'
    language sql
    immutable
    parallel safe
;

create or replace aggregate public.aggregate_objects(jsonb)
(
    sfunc = public.jsonb_concat,
    stype = jsonb,
    initcond = '{}'
);
`

const jsonCompact = `
CREATE OR REPLACE FUNCTION public.json_compact(p_json JSON,
                                               p_step INTEGER DEFAULT 0)
RETURNS JSON
AS $$
DECLARE
  v_type TEXT;
  v_text TEXT := '';
  v_indent INTEGER;
  v_key TEXT;
  v_object JSON;
  v_count INTEGER;
BEGIN
  p_step := coalesce(p_step, 0);
  -- Object or array?
  v_type := json_typeof(p_json);

  IF v_type = 'object' THEN
    -- Start object
    v_text := '{';
    SELECT count(*) - 1 INTO v_count
    FROM json_object_keys(p_json);
    -- go through keys, add them and recurse over value
    FOR v_key IN (SELECT json_object_keys(p_json))
    LOOP
      v_text := v_text || to_json(v_key)::TEXT || ':' || public.json_compact(p_json->v_key, p_step + 1);
      IF v_count > 0 THEN
        v_text := v_text || ',';
        v_count := v_count - 1;
      END IF;
      --v_text := v_text || E'\n';
    END LOOP;
    -- Close object
    v_text := v_text || '}';
  ELSIF v_type = 'array' THEN
    -- Start array
    v_text := '[';
    v_count := json_array_length(p_json) - 1;
    -- go through elements and add them through recursion
    FOR v_object IN (SELECT json_array_elements(p_json))
    LOOP
      v_text := v_text || public.json_compact(v_object, p_step + 1);
      IF v_count > 0 THEN
        v_text := v_text || ',';
        v_count := v_count - 1;
      END IF;
      --v_text := v_text || E'\n';
    END LOOP;
    -- Close array
    v_text := v_text || ']';
  ELSE -- A simple value
    v_text := v_text || p_json::TEXT;
  END IF;
  IF p_step > 0 THEN RETURN v_text;
  ELSE RETURN v_text::JSON;
  END IF;
END;
$$ LANGUAGE plpgsql;
`
