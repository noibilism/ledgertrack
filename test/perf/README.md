# Performance Testing (Accounts)

This folder contains a simple load generator for CBA account actions:

- credit
- debit
- lien
- lien release

## Load Generator

Source: [main.go](file:///Library/WebServer/Documents/errandman/bankman/ledger/test/perf/accounts_rps/main.go)

It provisions 100 accounts (product + clients + KYC + accounts) once, writes the seed file to:

- `/tmp/ledgertrack_accounts_perf_seed.json`

Then it runs a fixed-rate mixed workload using a worker pool and reports latency percentiles per action.

## Key Finding (Perf Failure Root Cause)

At high concurrency, account `credit`/`debit` originally posted against a single shared control account:

- `system:control:{currency}`

This became a hot-spot: concurrent requests contended on the same account rows and caused request pileups, timeouts, and server-side `context canceled` errors.

## Fix: Control Account Sharding

CBA account `credit` and `debit` now shard the system control account when `CBA_SYSTEM_CONTROL_SHARDS > 1`:

- default (unset/invalid): `system:control:{currency}`
- sharded: `system:control:{currency}:{shard}`

`shard` is derived from a stable hash of the wallet/account seed.

Implementation: [controllers_cba_accounts.go](file:///Library/WebServer/Documents/errandman/bankman/ledger/internal/api/v2/controllers_cba_accounts.go)

## Other Changes Applied

- HTTP request/response body logging is opt-in (kept off for perf runs) via `LEDGER_HTTP_LOG=true|1` in [serve.go](file:///Library/WebServer/Documents/errandman/bankman/ledger/cmd/serve.go).
- `GET /v2/{ledger}/accounts/{id}/balance` for CBA reads from volumes-with-balances instead of aggregated balances (avoids a heavier query path) in [controllers_cba_accounts.go](file:///Library/WebServer/Documents/errandman/bankman/ledger/internal/api/v2/controllers_cba_accounts.go).
- The load generator issues valid lien release payloads (`mode=RELEASE`) and serializes operations per account to avoid false-negative 400s in [main.go](file:///Library/WebServer/Documents/errandman/bankman/ledger/test/perf/accounts_rps/main.go).

## Benchmark Result (After Fix)

Target:

- mixed workload: credit/debit/lien/lien_release
- 100 accounts
- 1000 RPS
- 2s client timeout

Validated run:

- achieved: ~1000 RPS
- failures: 0
- sample output: `/tmp/ledgertrack_accounts_perf_1000rps_5s_final.json`

## How To Run

### 1) Start Postgres

Ensure Postgres is running and `POSTGRES_URI` points to it, e.g.:

```bash
export POSTGRES_URI='postgresql://ledger:ledger@127.0.0.1:5432/ledger?sslmode=disable'
```

Note: local Postgres in this environment reports `max_connections = 100`, so keep `--postgres-max-open-conns` at or below 100 unless you increase Postgres config.

### 2) Start LedgerTrack (recommended perf flags)

```bash
CBA_SYSTEM_CONTROL_SHARDS=256 \
go run . serve \
  --bind 127.0.0.1:3068 \
  --postgres-uri "$POSTGRES_URI" \
  --postgres-max-open-conns 100 \
  --postgres-max-idle-conns 100 \
  --auth-enabled=false \
  --auto-upgrade \
  --experimental-features \
  --experimental-numscript-interpreter
```

Important: request/response body logging should stay disabled during perf runs. It is opt-in via `LEDGER_HTTP_LOG=true|1`.

### 3) Seed (first time only)

```bash
go run ./test/perf/accounts_rps \
  -base-url 'http://127.0.0.1:3068' \
  -ledger ledgertrack \
  -token local-no-auth \
  -accounts 100 \
  -seed-only
```

### 4) Run 1000 RPS mixed workload

```bash
go run ./test/perf/accounts_rps \
  -base-url 'http://127.0.0.1:3068' \
  -ledger ledgertrack \
  -token local-no-auth \
  -run-only \
  -accounts 100 \
  -workers 1200 \
  -rps 1000 \
  -duration 30s \
  -request-timeout 2s \
  -out /tmp/ledgertrack_accounts_perf_1000rps_30s.json
```
