<p align="center">
  <img src="https://formance01.b-cdn.net/Github-Attachements/banners/ledger-readme-banner.webp" alt="ledger" width="100%" />
</p>

# LedgerTrack

**LedgerTrack** is a fork of [Formance Ledger](https://github.com/formancehq/ledger) that adds a Core Banking Application (CBA) layer and a wallet API on top of the programmable double-entry ledger core. It is designed for fintechs, neobanks, and remittance providers that need configurable banking products, customer-facing accounts, and multi-currency wallets without rebuilding the underlying accounting engine.

The ledger core is preserved as-is. LedgerTrack adds two layers above it: a wallet wrapper that exposes wallets, channels, liens, and revenue separation as first-class primitives, and a CBA layer that exposes products, clients, KYC, accounts, lifecycle controls, interest, fees, and reporting.

---

## Core Banking Application

The CBA layer turns LedgerTrack into a banking platform rather than a raw ledger. Every banking object is backed by a ledger primitive, so the accounting guarantees of Formance flow through end to end.

### Products

Products are configurable account-type definitions. There is no fixed list of product types; categories are free-form, and rules, interest configuration, and fee schedules are expressed as JSON.

Each product carries:

* **Rules**: minimum opening balance, minimum and maximum balance, allowed transaction types, KYC requirements, eligible client types, dormancy thresholds, and transaction limits (per-transaction and daily).
* **Interest configuration**: type (none, simple, compound, tiered), rate, accrual frequency, posting frequency, and tier definitions where applicable.
* **Fee schedule**: maintenance fees, transaction fees (flat or percentage), and penalty fees.

Products have a lifecycle of `draft → active → retired`. Only draft products are fully editable; retired products cannot be used to open new accounts, but existing accounts continue to operate.

### Clients

Clients are first-class customer records, supporting both individuals and corporates with type-specific data:

* **Individuals**: name, date of birth, national ID, nationality, occupation, employer.
* **Corporates**: legal name, registration number, tax ID, incorporation details, industry, beneficial owners, authorized signatories.

Clients follow a lifecycle of `pending → active → suspended → closed`, with guards on each transition (for example, a client cannot be closed while holding non-zero-balance accounts).

### KYC

KYC is append-oriented. Each submission creates a new record, and the latest verified or terminal record is projected onto the client for fast lookups. Levels 0 through 3 progressively require more documentation, and account opening gates on the client's KYC level meeting the product's requirement.

### Accounts

Accounts are the customer-facing banking object. Each account wraps exactly one LedgerTrack wallet (with a deterministic `wallet_id` derived from `client_number` and `product_code`) and references one product.

All financial operations route through the wallet layer, but the account layer enforces product rules first: balance limits, transaction limits, status checks, KYC checks, and eligibility. Account responses include `wallet_info` so consumers can see the backing wallet ID, currency, available and lien addresses, and current balance in a single read.

Account lifecycle states:

* **pending**: created but not yet activated.
* **active**: fully operational.
* **suspended**: blocks credits and debits.
* **freeze**: blocks debits only, credits still allowed.
* **dormant**: marked inactive (manually or by the dormancy job).
* **closed**: terminal; requires zero balance or a sweep to a GL.

### Interest and Fees

Interest accrues daily through a background job, computed against end-of-day balances per the product's interest configuration. Postings happen on the configured frequency (monthly, quarterly, annually, or at maturity) by crediting the account through the wallet layer from a configured interest-expense GL.

Maintenance fees post on schedule. Transaction fees apply inline during credits, debits, and transfers, with linked metadata so the main movement and the fee posting reconcile cleanly. Penalty fees trigger on rule violations. All fees flow to a configured fee-income GL.

### Reporting

JSON reporting endpoints cover the common operational and compliance needs:

* Client portfolio across all their accounts.
* Account statement with enriched fee and interest postings.
* Daily transaction summary.
* Interest and fee summaries.

---

## Wallet Layer

The wallet layer sits between the CBA layer and the ledger core. It is the foundation that makes accounts possible, and it is also usable directly for any system that needs multi-currency wallets without the full banking domain.

### Wallets as Primitives

Wallets are created with a single API call and identified deterministically (for example, `user123-USD`). There is no account-path design or Numscript template to write.

### Channels and Multi-Ledger Routing

LedgerTrack maintains three logical ledgers in parallel:

* **User ledger**: tracks individual user balances.
* **Channel ledger**: tracks liquidity with external payment providers or payout partners.
* **Revenue ledger**: captures fees and FX margin automatically.

A single debit call can atomically debit the user, debit the channel, and book the difference as revenue, with everything linked by metadata for traceability and reconciliation.

### Liens and Holds

Funds can be moved from `available` to a `lien` sub-account when a transaction is in flight, then released as either `pay` (finalize) or `cancel` (return to available). This is the right primitive for cross-border transfers, card authorizations, and any operation where funds need to be held before settlement.

### Currency Registry

Enabled wallet currencies are sourced from a database-backed registry (`_system.currencies`) rather than hardcoded or read from environment variables. The registry is hydrated at startup by both the `serve` and `worker` processes and is used by wallet validation, balance aggregation, product validation, account validation, interest rounding, and fee posting.

---

## Quick Start

### Local Development

Start the server:

```bash
export POSTGRES_URI="postgresql://ledger:ledger@localhost:5432/ledger?sslmode=disable"
go run main.go serve
```

Create the `ledgertrack` ledger:

```bash
curl -X POST http://localhost:3068/v2/ledgertrack
```

Create a wallet:

```bash
curl -X POST http://localhost:3068/v2/ledgertrack/wallets \
  -H 'Content-Type: application/json' \
  -d '{"userID": "u1", "currency": "USD"}'
```

Credit it:

```bash
curl -X POST http://localhost:3068/v2/ledgertrack/wallets/u1-USD/credit \
  -H 'Content-Type: application/json' \
  -d '{"amount": 1000, "reference": "ref-001"}'
```

### Onboarding a Customer

Configure a product:

```bash
curl -X POST http://localhost:3068/v2/ledgertrack/products \
  -H 'Content-Type: application/json' \
  -d '{
    "code": "SAV-USD-001",
    "name": "Personal Savings USD",
    "category": "savings",
    "currency": "USD",
    "rules": {
      "min_opening_balance": "0",
      "min_balance": "0",
      "allow_debits": true,
      "allow_credits": true,
      "requires_kyc_level": 1
    }
  }'
```

Create a client:

```bash
curl -X POST http://localhost:3068/v2/ledgertrack/clients \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "individual",
    "contact": { "phone": "+15550001111" },
    "individual_data": {
      "first_name": "Ada",
      "last_name": "Lovelace"
    }
  }'
```

Submit and verify KYC, then activate the client and open the account:

```bash
curl -X POST http://localhost:3068/v2/ledgertrack/clients/{clientID}/kyc \
  -H 'Content-Type: application/json' \
  -d '{ "level": 1, "documents": [] }'

curl -X POST http://localhost:3068/v2/ledgertrack/clients/{clientID}/kyc/{kycID}/verify
curl -X POST http://localhost:3068/v2/ledgertrack/clients/{clientID}/activate

curl -X POST http://localhost:3068/v2/ledgertrack/accounts \
  -H 'Content-Type: application/json' \
  -d '{
    "client_id": "{clientID}",
    "product_id": "{productID}",
    "opening_deposit": 1000
  }'
```

### Deployment

For Ubuntu servers, run the included deploy script:

```bash
sudo ./deploy.sh
```

This installs dependencies, configures the database connection, sets up a Systemd service, and pre-creates payment channels.

---

## Architecture

### Layering

The system is organized in three layers, each with a single concern:

1. **Formance core**: append-only postings, concurrency-safe balance movement, ledger guarantees.
2. **Wallet layer**: wallets, channels, liens, revenue separation, history, statements.
3. **CBA layer**: products, clients, KYC, accounts, rule enforcement, interest and fee orchestration, reporting.

The CBA layer never bypasses the wallet layer, and the wallet layer never bypasses Formance. Balance is always read from the wallet, never duplicated as authoritative state on the account row. Every financial movement is a ledger entry.

### Worker and Scheduler

The worker process runs CBA scheduled jobs alongside the existing ledger workers:

* Interest accrual (daily)
* Interest posting (per product configuration)
* Maintenance fee processing
* Dormancy detection

The API and worker can run as separate processes or together with the embedded worker:

```bash
go run main.go serve --worker
```

---

## API Reference

### Wallets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET    | `/v2/ledgertrack/currencies` | List configured wallet currencies |
| POST   | `/v2/ledgertrack/wallets` | Create a wallet |
| POST   | `/v2/ledgertrack/wallets/{walletID}/credit` | Credit funds |
| POST   | `/v2/ledgertrack/wallets/{walletID}/debit` | Debit funds (supports `channelID` for routing) |
| POST   | `/v2/ledgertrack/wallets/{walletID}/lien` | Lock funds |
| POST   | `/v2/ledgertrack/wallets/{walletID}/lien/release` | Release lien (pay or cancel) |
| GET    | `/v2/ledgertrack/wallets/{walletID}/history` | Transaction history |
| GET    | `/v2/ledgertrack/wallets/{walletID}/statement` | Statement |

### Channels

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | `/v2/ledgertrack/channels` | Register a channel |
| POST   | `/v2/ledgertrack/channels/{channelID}/credit` | Add channel liquidity |
| GET    | `/v2/ledgertrack/channels/{channelID}` | Channel info and balance |
| GET    | `/v2/ledgertrack/channels/{channelID}/fees/config` | Get channel fee config (user fee + processing fee) |
| PUT    | `/v2/ledgertrack/channels/{channelID}/fees/config` | Upsert channel fee config (supports flat, percentage, combined, tiered, layered) |
| GET    | `/v2/ledgertrack/channels/{channelID}/fees/audits` | Fee config audit log |
| GET    | `/v2/ledgertrack/channels/fees/configs` | List fee configs |

### Products

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | `/v2/ledgertrack/products` | Create product (status: draft) |
| GET    | `/v2/ledgertrack/products` | List products |
| GET    | `/v2/ledgertrack/products/{productID}` | Get product |
| PATCH  | `/v2/ledgertrack/products/{productID}` | Update product |
| POST   | `/v2/ledgertrack/products/{productID}/activate` | Activate |
| POST   | `/v2/ledgertrack/products/{productID}/retire` | Retire |

### Clients

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | `/v2/ledgertrack/clients` | Create client |
| GET    | `/v2/ledgertrack/clients` | List clients |
| GET    | `/v2/ledgertrack/clients/{clientID}` | Get client |
| PATCH  | `/v2/ledgertrack/clients/{clientID}` | Update client |
| POST   | `/v2/ledgertrack/clients/{clientID}/activate` | Activate |
| POST   | `/v2/ledgertrack/clients/{clientID}/suspend` | Suspend |
| POST   | `/v2/ledgertrack/clients/{clientID}/reactivate` | Reactivate |
| POST   | `/v2/ledgertrack/clients/{clientID}/close` | Close |
| GET    | `/v2/ledgertrack/clients/{clientID}/accounts` | List client's accounts |

### KYC

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | `/v2/ledgertrack/clients/{clientID}/kyc` | Submit KYC |
| GET    | `/v2/ledgertrack/clients/{clientID}/kyc` | KYC history |
| POST   | `/v2/ledgertrack/clients/{clientID}/kyc/{kycID}/verify` | Verify |
| POST   | `/v2/ledgertrack/clients/{clientID}/kyc/{kycID}/reject` | Reject |

### Accounts

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | `/v2/ledgertrack/accounts` | Open account |
| GET    | `/v2/ledgertrack/accounts` | List accounts |
| GET    | `/v2/ledgertrack/accounts/{accountID}` | Get account (includes `wallet_info`) |
| GET    | `/v2/ledgertrack/accounts/{accountID}/balance` | Balance |
| GET    | `/v2/ledgertrack/accounts/{accountID}/history` | Transaction history |
| GET    | `/v2/ledgertrack/accounts/{accountID}/statement` | Statement (with fees and interest) |
| POST   | `/v2/ledgertrack/accounts/{accountID}/credit` | Credit |
| POST   | `/v2/ledgertrack/accounts/{accountID}/debit` | Debit |
| POST   | `/v2/ledgertrack/accounts/{accountID}/lien` | Place lien |
| POST   | `/v2/ledgertrack/accounts/{accountID}/lien/release` | Release lien |
| POST   | `/v2/ledgertrack/accounts/{accountID}/activate` | Activate |
| POST   | `/v2/ledgertrack/accounts/{accountID}/suspend` | Suspend |
| POST   | `/v2/ledgertrack/accounts/{accountID}/freeze` | Freeze debits |
| POST   | `/v2/ledgertrack/accounts/{accountID}/dormant` | Mark dormant |
| POST   | `/v2/ledgertrack/accounts/{accountID}/reactivate` | Reactivate |
| POST   | `/v2/ledgertrack/accounts/{accountID}/close` | Close |

### Reporting

All reporting endpoints return JSON and are scoped to the `ledgertrack` ledger.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET    | `/v2/ledgertrack/reports/clients/{clientID}/portfolio` | Client portfolio across accounts |
| GET    | `/v2/ledgertrack/reports/accounts/{accountID}/statement` | Enriched account statement |
| GET    | `/v2/ledgertrack/reports/transactions/daily?date=YYYY-MM-DD` | Daily transaction summary |
| GET    | `/v2/ledgertrack/reports/interest-fees` | Interest and fee summaries |
| GET    | `/v2/ledgertrack/reports/finance/trial-balance?currency=USD` | Trial balance snapshot (per currency) |
| GET    | `/v2/ledgertrack/reports/finance/balance-sheet?currency=USD` | Balance sheet snapshot (per currency) |
| GET    | `/v2/ledgertrack/reports/finance/pnl?currency=USD&startTime=...&endTime=...` | Profit and loss (per currency, period) |
| GET    | `/v2/ledgertrack/reports/finance/cash-flow?currency=USD&startTime=...&endTime=...` | Cash flow (per currency, period) |
| POST   | `/v2/ledgertrack/reports/channels/revenue` | Channel revenue summary (gross fees, processing cost, net revenue) |
| POST   | `/v2/ledgertrack/reports/channels/revenue/timeseries` | Channel revenue timeseries (day/week/month) |
| POST   | `/v2/ledgertrack/reports/channels/revenue/export` | Export channel revenue reports (CSV/PDF) |
| POST   | `/v2/ledgertrack/reports/channels/dashboard` | Dashboard metrics (current month net, top channels, MoM trend) |

The full Formance API surface remains available alongside these additions.

---

## Testing

### CBA Functional Suite

A Docker-backed end-to-end scenario exercises the live CBA API surface in `test/e2e/api_cba_functional_test.go`. It covers:

* Ledger creation
* Currency listing
* Product lifecycle
* Client lifecycle
* KYC submit, verify, reject, and history
* Account opening, lifecycle, and transactions
* Client-owned accounts listing
* Reporting endpoints

Run it with:

```bash
go test -tags it ./test/e2e -ginkgo.focus='CBA functional API tests' -count=1
```

### Prerequisites

The `test/e2e` suite uses the repository's Docker-based test harness for Postgres, NATS, and ClickHouse. Docker must be available locally before running tests tagged with `it`.

---

## Documentation

* **CBA domain specification**: [`CBA_PRD.md`](CBA_PRD.md)
* **Wallet wrapper behavior**: [`WalletPRD.md`](WalletPRD.md)
* **OpenAPI document**: [`openapi/v2.yaml`](openapi/v2.yaml)
* **Postman collection**: [`postman_collection.json`](postman_collection.json)

---

## Acknowledgements

LedgerTrack is built on [Formance Ledger](https://github.com/formancehq/ledger). The Formance team built the core engine that makes this project possible, and the ledger guarantees that flow through every layer of LedgerTrack are theirs.

---

**License**: Apache 2.0 (inherited from Formance Ledger)
