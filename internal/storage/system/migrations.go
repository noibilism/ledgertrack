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
