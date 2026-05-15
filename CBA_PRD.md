# Core Banking Application (CBA) PRD

## Goal

Add a Core Banking Application layer on top of the existing LedgerTrack wallet wrapper without changing the ledger core.

The new CBA layer introduces `Products`, `Clients`, `KYC`, and `Accounts` as first-class domain entities while preserving the existing wallet, channel, lien, and ledger primitives as the underlying financial mechanics.

This document is a repo-aligned design for implementation. It is intentionally layered on top of the current codebase patterns:

- HTTP transport in `internal/api/v2`
- Business/domain logic in a new `internal/cba`
- Persistence through the existing Bun + embedded migrator model in `internal/storage`
- Background processing through the existing Fx worker lifecycle and cron-based scheduling

No ledger-core rewrite is in scope.

## Existing System Constraints

The current system already provides:

- Ledger-scoped API routes under `/v2/{ledger}/...`
- Wallet wrapper routes under `/v2/ledgertrack/wallets/...`
- Channel routes under `/v2/ledgertrack/channels/...`
- Postgres-backed storage with embedded migrations
- Worker patterns using Fx lifecycle hooks and cron schedules

The current wallet layer remains the source of truth for balances. The new Account layer must call wallet-domain logic internally and must not duplicate balance state in CBA tables.

## Repo Alignment Decisions

These design choices are selected to match the current repository:

- Client-specific details use JSONB:
  - `individual_data jsonb`
  - `corporate_data jsonb`
- Migration style follows the current repo pattern:
  - embedded Go migrators
  - forward-only migration registration
  - SQL and Bun-compatible DDL in package-owned migrators
- Fee income and interest expense GL accounts are config-driven
- Delivery is phased, not all-at-once

## Migration Convention And Rollback Story

This repository uses forward-only embedded Go migrations registered in code under `internal/storage/system` and `internal/storage/bucket`.

The CBA layer will follow that convention.

That means:

- no standalone down-migration files
- no alternative migration runner
- schema evolution is append-only at the migration level

Rollback story:

- application rollback happens by deploying an older binary
- schema rollback is operational, not migration-driven
- destructive incidents recover from database backup or snapshot
- non-destructive fixes use compensating forward migrations
- behavior that must be halted after schema rollout should be guarded by code-path disablement or feature flags rather than pretending the schema can be rolled back safely

In short: the repo's migration contract is forward-only, and CBA will match it explicitly.

## Architectural Principle

The CBA layer is a domain facade above the wallet wrapper.

Conceptually:

- `Product` defines rules and pricing
- `Client` represents the customer or institution
- `KYC` governs lifecycle eligibility
- `Account` is the customer-facing banking object
- `Wallet` remains the underlying balance engine

The mapping is:

- One CBA `Account` maps to one deterministic LedgerTrack `wallet_id`
- Account credit/debit/lien/release operations delegate to wallet service functions inside the process
- Account balance, history, and statement are derived from the wallet layer

The CBA layer owns:

- eligibility checks
- lifecycle state
- rule enforcement
- fee and interest orchestration
- KYC gating
- audit metadata linking

The wallet layer continues to own:

- append-only financial postings
- concurrency-safe balance movement
- lien mechanics
- ledger-derived history and statement timelines

## Proposed Package Layout

Add a new domain package tree:

```text
internal/cba/
  models/
  repositories/
  services/
  rules/
  scheduler/
  config/
```

Supporting transport and storage additions:

- `internal/api/v2/controllers_cba_*.go`
- `internal/storage/system` extensions for CBA reference data tables
- optional helper adapter package to bridge CBA services to wallet operations without HTTP round-trips

## Persistence Strategy

### Why system schema

Products, clients, accounts, KYC records, accruals, and fee postings are business-domain records, not bucket-local ledger data.

They should live in the shared system schema, not in per-ledger bucket schemas.

That means CBA migrations should extend the `_system` schema alongside existing ledger metadata tables rather than the per-bucket migration tree.

### New tables

#### products

- `id uuid primary key`
- `code varchar unique not null`
- `name varchar not null`
- `description text`
- `category varchar not null`
- `currency varchar not null`
- `status varchar not null`
- `rules jsonb not null default '{}'::jsonb`
- `interest_config jsonb`
- `fee_schedule jsonb`
- `created_at timestamp not null default now() at time zone 'utc'`
- `updated_at timestamp not null default now() at time zone 'utc'`

Recommended indexes:

- unique index on `code`
- index on `(category, currency, status)`

#### clients

- `id uuid primary key`
- `client_number varchar unique not null`
- `type varchar not null`
- `status varchar not null`
- `kyc_level integer not null default 0`
- `kyc_status varchar not null default 'pending'`
- `kyc_data jsonb not null default '{}'::jsonb`
- `contact jsonb not null default '{}'::jsonb`
- `individual_data jsonb`
- `corporate_data jsonb`
- `created_at timestamp not null default now() at time zone 'utc'`
- `updated_at timestamp not null default now() at time zone 'utc'`

Recommended indexes:

- unique index on `client_number`
- index on `(type, status, kyc_level, kyc_status)`

#### accounts

- `id uuid primary key`
- `account_number varchar unique not null`
- `client_id uuid not null`
- `product_id uuid not null`
- `currency varchar not null`
- `status varchar not null`
- `wallet_id varchar unique not null`
- `freeze_debits boolean not null default false`
- `opened_at timestamp not null default now() at time zone 'utc'`
- `activated_at timestamp`
- `closed_at timestamp`
- `last_activity_at timestamp`
- `interest_accrued numeric(20,8) not null default 0`
- `metadata jsonb not null default '{}'::jsonb`

Recommended constraints:

- foreign key `client_id -> clients.id`
- foreign key `product_id -> products.id`

Recommended indexes:

- unique index on `account_number`
- unique index on `wallet_id`
- index on `(client_id, status)`
- index on `(product_id, status)`
- index on `last_activity_at`

#### kyc_records

- `id uuid primary key`
- `client_id uuid not null`
- `level integer not null`
- `status varchar not null`
- `submitted_at timestamp not null`
- `verified_at timestamp`
- `expires_at timestamp`
- `verifier varchar`
- `reason text`
- `documents jsonb not null default '[]'::jsonb`
- `payload jsonb not null default '{}'::jsonb`

Indexes:

- index on `client_id`
- index on `(client_id, status, level)`

#### interest_accruals

- `id uuid primary key`
- `account_id uuid not null`
- `accrual_date date not null`
- `balance_basis numeric(20,8) not null`
- `rate numeric(12,8) not null`
- `amount numeric(20,8) not null`
- `posted boolean not null default false`
- `posted_reference varchar`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamp not null default now() at time zone 'utc'`

Indexes:

- unique index on `(account_id, accrual_date)`
- index on `(posted, accrual_date)`

#### fee_postings

- `id uuid primary key`
- `account_id uuid not null`
- `event_type varchar not null`
- `reference varchar not null`
- `linked_reference varchar not null`
- `amount numeric(20,8) not null`
- `currency varchar not null`
- `status varchar not null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamp not null default now() at time zone 'utc'`

Indexes:

- index on `account_id`
- unique index on `reference`
- index on `linked_reference`

#### account_daily_usages

- `id uuid primary key`
- `account_id uuid not null`
- `usage_date date not null`
- `debit_amount numeric(20,8) not null default 0`
- `credit_amount numeric(20,8) not null default 0`
- `debit_count bigint not null default 0`
- `credit_count bigint not null default 0`
- `last_reference varchar`
- `created_at timestamp not null default now() at time zone 'utc'`
- `updated_at timestamp not null default now() at time zone 'utc'`

Indexes:

- unique index on `(account_id, usage_date)`
- index on `usage_date`

## Domain Models

### Product

Product is a configurable definition for opening accounts.

Important rules:

- category remains free-form
- currency must map to an enabled wallet currency
- status transitions:
  - `draft -> active -> retired`
- only draft products are fully editable
- active products only allow whitelisted patch fields
- retired products cannot be used for new accounts

In this codebase, "enabled wallet currency" means:

- the product currency must exist in the same wallet currency registry used by the current wallet handlers
- that registry is now loaded from the `_system.currencies` table at application startup
- product creation and activation must validate against that registry, not against a separate CBA-only currency list

Implementation note:

- Product and Account validation should keep using the shared internal currency registry so wallet, CBA, and API validation do not drift from the database-backed source of truth

### Client

Client is the customer record.

Important lifecycle:

- create as `pending`
- activate only after minimum KYC level
- suspend/reactivate for operational controls
- close only when no active non-zero-balance accounts remain

### KYC

KYC is first-class and append-oriented.

Each submission creates a new `kyc_record`.

Current client KYC state is projected from the latest approved or terminal record into:

- `clients.kyc_level`
- `clients.kyc_status`
- `clients.kyc_data`

Default policy:

- L0: name + phone
- L1: + national ID number
- L2: + government ID document + proof of address
- L3: + biometric or in-person verification

These defaults should be configurable in code and config, but the initial implementation can ship with deterministic built-ins.

### Account

Account is the banking abstraction customers see.

It wraps one wallet and one product.

Key invariants:

- exactly one `wallet_id` per account
- wallet balance is never duplicated as authoritative state
- account operations validate product rules and account status before wallet movement

Deterministic `wallet_id` format:

```text
client-{client_number}-{product_code}
```

Notes:

- this creates a stable auditable mapping
- currency is derived from product and already encoded in ledger account paths under the wallet wrapper

## Out Of Scope

The CBA layer in this prompt does not define or redesign authentication or authorization.

Specifically out of scope:

- who is allowed to call each new endpoint
- role mapping for ops, compliance, client service, or system actors
- tenant isolation or per-client access-control policy

The implementation should continue to use the repo's existing auth middleware and request-context patterns, but no new authn/authz model is introduced by this PRD.

## Service Boundaries

### Product service

Responsibilities:

- create/list/read/patch/activate/retire products
- validate product JSON shapes and defaults
- expose a resolved rule set used by Account service

### Client service

Responsibilities:

- create/read/update clients
- activate/suspend/reactivate/close clients
- enforce client lifecycle preconditions

### KYC service

Responsibilities:

- submit KYC
- verify KYC
- reject KYC
- list KYC history
- update client-level KYC projection

### Account service

Responsibilities:

- open/activate/suspend/freeze/dormant/reactivate/close accounts
- derive deterministic wallet IDs
- call wallet operations internally
- enforce product and account rules
- calculate and apply inline transaction fees
- link wallet transaction references for auditability

### Wallet adapter

The CBA layer should call wallet mechanics internally, not via HTTP.

Recommended implementation:

- extract reusable wallet-domain functions behind an internal service interface
- let both wallet HTTP handlers and CBA Account service use the same internal adapter

This avoids:

- self-HTTP calls
- inconsistent request validation logic
- duplicated balance-movement code

## API Surface

All endpoints remain under `/v2/ledgertrack`.

### Products

- `POST /v2/ledgertrack/products`
- `GET /v2/ledgertrack/products`
- `GET /v2/ledgertrack/products/{id}`
- `PATCH /v2/ledgertrack/products/{id}`
- `POST /v2/ledgertrack/products/{id}/activate`
- `POST /v2/ledgertrack/products/{id}/retire`

### Clients

- `POST /v2/ledgertrack/clients`
- `GET /v2/ledgertrack/clients`
- `GET /v2/ledgertrack/clients/{id}`
- `PATCH /v2/ledgertrack/clients/{id}`
- `POST /v2/ledgertrack/clients/{id}/activate`
- `POST /v2/ledgertrack/clients/{id}/suspend`
- `POST /v2/ledgertrack/clients/{id}/reactivate`
- `POST /v2/ledgertrack/clients/{id}/close`

### KYC

- `POST /v2/ledgertrack/clients/{id}/kyc`
- `POST /v2/ledgertrack/clients/{id}/kyc/{kycID}/verify`
- `POST /v2/ledgertrack/clients/{id}/kyc/{kycID}/reject`
- `GET /v2/ledgertrack/clients/{id}/kyc`

### Accounts

- `POST /v2/ledgertrack/accounts`
- `GET /v2/ledgertrack/accounts`
- `GET /v2/ledgertrack/accounts/{id}`
- `POST /v2/ledgertrack/accounts/{id}/activate`
- `POST /v2/ledgertrack/accounts/{id}/suspend`
- `POST /v2/ledgertrack/accounts/{id}/freeze`
- `POST /v2/ledgertrack/accounts/{id}/dormant`
- `POST /v2/ledgertrack/accounts/{id}/reactivate`
- `POST /v2/ledgertrack/accounts/{id}/close`
- `POST /v2/ledgertrack/accounts/{id}/credit`
- `POST /v2/ledgertrack/accounts/{id}/debit`
- `POST /v2/ledgertrack/accounts/{id}/lien`
- `POST /v2/ledgertrack/accounts/{id}/lien/release`
- `GET /v2/ledgertrack/accounts/{id}/balance`
- `GET /v2/ledgertrack/accounts/{id}/history`
- `GET /v2/ledgertrack/accounts/{id}/statement`

## Rule Enforcement

Rule enforcement happens in Account service before wallet movement.

## Daily Limit Enforcement Strategy

Daily debit and credit limits will use denormalized daily counters stored in `_system.account_daily_usages`, not computed-from-history reads.

Why this choice:

- limit checks happen in the write path and must stay cheap
- wallet history is authoritative for balance movement, but it is the wrong hot path for repeated daily-limit scans
- fee-linked and multi-step CBA operations need a deterministic domain-level view of what already counted against a limit
- computed-from-history would force repeated ledger queries plus filtering rules on every write and would be harder to lock safely under concurrency

Enforcement model:

1. account operation begins inside a Postgres transaction for CBA state changes
2. load or create today's `account_daily_usages` row for the account
3. lock that row `FOR UPDATE`
4. evaluate single and daily product limits
5. execute the wallet movement
6. if wallet movement succeeds, increment the daily counters in the same CBA transaction

If the wallet call fails, the counters are not advanced.

Daily usage is CBA-domain enforcement state, not a balance source of truth.

### Open account

Checks:

- product exists and is `active`
- client exists and is eligible
- client KYC level satisfies product `requires_kyc_level`
- client type is allowed by product
- opening deposit satisfies `min_opening_balance`

Open flow:

1. load product and client
2. derive account number and wallet ID
3. create account row in `pending`
4. create underlying wallet through internal wallet service
5. if opening deposit > 0, perform account credit with deterministic reference
6. set `opened_at`, `last_activity_at`

### Credit

Checks:

- account status allows credits
- product `allow_credits = true`
- single and daily credit limits
- max balance rule after credit if configured

### Debit

Checks:

- account status is active and not frozen/suspended/closed/dormant
- product `allow_debits = true`
- single and daily debit limits
- available-balance based result must not violate:
  - `min_balance`
  - `max_balance` not applicable after debit
  - negative balance policy

### Lien / release

Checks:

- same account status protections as debit
- lien amount must be valid under available funds
- release logic maps directly to wallet lien-release mechanics

## Fee Handling

### Config

Add CBA config values for GL wallet addresses or wallet-source abstractions:

- `CBA_FEE_INCOME_ACCOUNT`
- `CBA_INTEREST_EXPENSE_ACCOUNT`
- optional scheduler toggles and batch sizes

### Inline transaction fees

For debit/credit/transfer-like operations:

1. compute fee from product `fee_schedule`
2. perform main wallet movement
3. perform fee wallet movement with linked metadata
4. persist `fee_postings` row referencing both business reference and fee reference

Metadata should include:

- `cba_account_id`
- `cba_operation`
- `linked_reference`
- `fee_event`

Important note:

The current wallet multi-ledger flow is not atomic across chained writes. The CBA layer must treat multi-step financial orchestration as reconcilable rather than pretending it is a single ACID ledger operation.

### Failure mode: main debit succeeds but fee fails

This is an explicitly supported failure mode and must not be hidden.

If the main debit succeeds in the wallet layer but the linked fee posting fails:

- the account balance reflects the main debit only
- the customer-facing transaction is considered `posted_with_fee_pending` at the CBA domain level
- a `fee_postings` row is created or updated with:
  - `status = pending_recovery`
  - `linked_reference = <main reference>`
  - metadata describing the failure cause and retry state

Operational behavior:

- the API returns a clear non-success outcome for the overall CBA operation
- the response must state that the principal movement posted but fee collection did not complete
- no silent success is allowed

What reconciles it:

- a fee-recovery background process scans `fee_postings.status = pending_recovery`
- it retries the fee posting using the deterministic fee reference
- because the fee reference is stable, retries remain idempotent
- once the fee posting succeeds, the row becomes `posted`
- if recovery is abandoned after policy-defined retries, the row becomes `writeoff_required` for manual ops handling

What the account looks like during this state:

- wallet balance equals the principal transaction result only
- account lifecycle remains unchanged unless business policy later escalates repeated fee failures
- statements and history should eventually surface the principal posting and the fee posting as separate but linked entries through metadata

## Interest Handling

Use background jobs, not synchronous request-time posting.

### Daily accrual job

For each active account with interest enabled:

1. read end-of-day account balance from wallet source
2. compute daily accrual
3. write `interest_accruals`
4. increment `accounts.interest_accrued`

### Posting job

On posting boundary:

1. gather unposted accruals
2. compute posting amount
3. credit account through wallet service
4. use configured interest expense GL source
5. mark accrual rows posted and zero down the running accrued bucket

### Interest precision and rounding policy

Interest uses high-precision decimal math inside the CBA layer and rounds only at posting time into wallet minor units.

Policy:

- accrual calculation precision:
  - use decimal math, not float math
  - compute daily accrual at 12 decimal places
- accrual storage precision:
  - store `interest_accruals.amount`, `interest_accruals.balance_basis`, and `accounts.interest_accrued` as `numeric(20,8)` in Postgres for bounded but high enough precision
- posting precision:
  - when interest is actually credited into the wallet, convert from accrued decimal amount into wallet minor units using the wallet currency precision
- rounding mode:
  - round half up when converting from accrued decimal amount to wallet-postable minor units

Where rounding happens:

- no rounding to wallet minor units during daily accrual computation
- daily accrual rows keep fractional carry
- rounding happens once, at posting time, when generating the wallet credit amount
- any residual fractional remainder that cannot yet be posted stays in `accounts.interest_accrued` and carries forward into the next posting boundary

This keeps interest behavior deterministic while preserving the wallet layer's integer minor-unit contract.

## Dormancy Handling

Dormancy should be determined from:

- `last_activity_at`
- product `dormancy_days`

Job behavior:

1. scan active accounts with dormancy policy
2. compare current date with `last_activity_at`
3. mark account `dormant` when threshold exceeded

Manual dormant/reactivate endpoints remain supported.

## Scheduling Pattern

Follow the repository's worker pattern:

- Fx module
- lifecycle OnStart/OnStop
- cron.Schedule-based loops

Add lightweight runners under `internal/cba/scheduler` for:

- interest accrual
- interest posting
- maintenance fees
- dormancy detection

These should be wired into:

- embedded worker mode through `serve --worker`
- dedicated worker mode through the existing worker command composition

## Idempotency

All account-level financial operations require deterministic references.

Strategy:

- inbound request may provide business reference
- CBA derives internal references for linked fee and interest postings

Reference pattern examples:

- main operation: provided business reference
- fee posting: `{reference}:fee:{event}`
- opening deposit: `account-open:{account_id}:opening-deposit`
- interest posting: `interest:{account_id}:{period}`

The wallet layer remains the source of truth for dedupe of financial intent through reference/idempotency semantics.

## Handler Style

New handlers should match current `internal/api/v2` conventions:

- request structs in handler files
- `api.BadRequest` for validation
- common error mapping helpers
- `api.Created`, `api.Ok`, `api.NoContent`, `api.RenderCursor`
- no HTTP self-calls

Responses should remain compatible with the existing top-level `data` convention.

## OpenAPI And Postman

Update:

- `openapi.yaml`
- likely `openapi/v2.yaml` as the concrete v2 route document already used by the repo
- `postman_collection.json` with a new `CBA` folder

Suggested Postman scenarios:

- create product
- create client
- submit and verify KYC
- activate client
- open account with opening deposit
- debit account with fee
- accrue/post interest example

## Test Plan

### Unit tests

- product rule evaluation
- KYC eligibility checks
- account lifecycle guards
- fee calculation
- interest calculation
- deterministic wallet/account/reference derivation

### Integration tests

- product + client + KYC + account open flow
- account debit with fee linkage
- account history/statement wrapping wallet history correctly
- scheduler posting interest into wallet ledger

### Manual/collection tests

Extend Postman collection with CBA examples built on top of existing wallet flow.

## Milestone Plan

### Milestone 1: Design and persistence

- add `CBA_PRD.md`
- add system-schema migrations
- add CBA models and repositories

### Milestone 2: Products

- product service
- product handlers
- product tests

### Milestone 3: Clients and KYC

- client service
- KYC service
- lifecycle handlers
- tests

### Milestone 4: Accounts

- open/activate/suspend/freeze/dormant/reactivate/close
- account-to-wallet adapter
- account credit/debit/lien/release/balance/history/statement
- tests

### Milestone 5: Fees and interest

- fee engine
- accrual and posting jobs
- maintenance fees
- tests

### Milestone 6: Documentation and API artifacts

- OpenAPI updates
- Postman `CBA` folder
- README updates

## Risks To Manage

- current wallet multi-ledger flows are not fully atomic; CBA orchestration must be explicit and auditable
- repo migration style is forward-only, so destructive rollback expectations must be documented carefully
- daily limit enforcement needs a deterministic query strategy against persisted CBA operations and/or wallet history
- interest math needs a single precision policy to avoid rounding drift across accrual and posting

## Non-Goals

The first CBA implementation does not add:

- external file storage for KYC documents
- a new ledger core
- direct balance columns as source of truth
- FX, treasury, or settlement engines beyond existing primitives
- a new authentication or authorization model

## Implementation Stop Point

After this PRD is approved, implementation should proceed in milestones starting with:

1. system-schema migrations for CBA tables
2. product domain and handlers
3. client and KYC domain
4. account layer on top of wallet mechanics

No code changes beyond this document are implied by the PRD itself.
