# LedgerTrack Postgres backup, PITR and restore drill

Why this matters: migrations are forward-only (`CBA_PRD.md`, "Migration Convention And Rollback Story"). There is no down migration. Rolling back a bad schema change or a destructive incident means deploying the older binary and restoring the database from a backup or snapshot. The backup is the rollback.

Everything LedgerTrack owns is in one Postgres database: schema `_system` (ledgers, CBA tables, channels, pipelines) and one schema per bucket (default `_default`) holding `logs`, `transactions`, `moves`, `accounts`, `accounts_volumes`, `logs_blocks`. They must be restored together, from the same instant. Never restore only one schema.

## 1. Targets to set

Proposed starting values. They are decisions for the CTO (see the end).

| Target | Value | Meaning |
| --- | --- | --- |
| RPO | 5 minutes or less, ideally under 1 minute | Maximum acceptable loss of committed postings. Needs WAL archiving (PITR). Volume snapshots alone give hours. |
| RTO | 60 minutes | Time from "decide to restore" to API serving and verified. |
| Backup retention | 14 days PITR window, 35 days of daily full backups, 12 monthly | Covers late-discovered corruption and audit requests. |
| Restore drill | Quarterly, and after every major version upgrade | Untested backups do not count. |
| Off-platform copy | Daily logical dump to storage in a different account/region | Protects against account lockout, volume loss, or deletion of the project. |

Reconciliation covers the gap: after any restore to a point in the past, replay or reconcile the postings made between the restore point and the incident from the upstream posting services' own records (they hold idempotency keys, so replays are safe).

## 2. What to enable

### Option A: Railway Postgres

Railway documents a "Back Up and Restore Postgres" guide with two mechanisms. I could only read search snippets of it, not the full page, so everything below marked (verify) must be confirmed in the dashboard and docs before you rely on it.

- **Scheduled volume backups** (documented): snapshots of the whole volume, incremental and copy-on-write. A restore stages a new volume mounted at the original path and keeps the old one until you deploy the change. These are crash-consistent snapshots, not logical dumps. Granularity is the schedule, so RPO is the schedule interval. Verify the available schedules (daily, weekly, monthly), retention, plan requirement, and whether the restored volume boots cleanly with Postgres crash recovery.
- **Point-in-time recovery** (documented as available): "restores to any timestamp in the archive window". Verify: how it is enabled, the archive window length, where WAL is stored, whether it works for your Postgres template and version, the maximum restore granularity, and whether a PITR restore creates a new service or overwrites the current one. Enable it before go-live.
- Do not assume anything not listed here: for example standby replicas, automatic failover, cross-region copies, or encryption of backups are all (verify).
- Add your own **daily `pg_dump` custom-format dump** from a scheduled job to object storage outside Railway (verify the Railway cron/ bucket options, or run it from a CI runner over a TCP proxy, which needs the DB reachable, so prefer a job inside the project). Command:

```
pg_dump --format=custom --no-owner --compress=9 --file=ledgertrack-$(date -u +%Y%m%dT%H%M%SZ).dump "$DATABASE_URL"
```

  Encrypt the dump before upload (for example `age` or the bucket's server-side encryption with a key you control) and keep the key somewhere other than the bucket.

### Option B: External managed Postgres (RDS, Cloud SQL, Neon, Supabase, Aiven, Crunchy)

Prefer this for the ledger of record if procurement allows. Enable:

- Automated backups with PITR (continuous WAL archiving). Retention 14 days minimum (RDS allows up to 35).
- Multi-AZ or HA standby for availability (separate from backup).
- Storage encryption with a customer-managed key, TLS enforced, deletion protection on, final snapshot on delete.
- Cross-region or cross-account snapshot copy, daily.
- Alerts on backup failure, WAL archive lag and replication lag.

Required extensions: `pgcrypto` (the schema calls `public.digest`) and `uuid`/`gen_random_uuid` (built in from Postgres 13). Check your provider allows `pgcrypto`.

### Option C: Self-managed Postgres (pgBackRest or WAL-G)

Continuous archiving to object storage with a full backup weekly and incremental daily, `archive_timeout=60s` to bound RPO. Most control, most operational work. Only choose it if you will own the on-call.

### Settings that apply to all options

- Postgres 14 or newer. Keep the restore target on the same major version as the backup.
- `synchronous_commit=on` (default). Do not turn it off on the ledger DB: you would trade RPO for latency.
- Test restores into a scratch instance, never over production, until the drill is signed off.
- Take a manual backup immediately before every release that adds a migration, and note its id in the release record. Because migrations are forward-only, this is the only way back.
- Run the worker (async log hashing, schedulers) as a single instance and stop it before and during a restore.

## 3. Restore drill runbook

Run this in a scratch environment. For a real incident, the same steps apply with the production names, and steps 1 and 2 happen first.

Roles: incident lead (decides, records times), DB operator (does the restore), verifier (runs queries). Use the template in section 4 and write down the wall clock time at every step marked T.

### Step 0. Prepare (before the drill, once)

- Note the target restore point `T_target` (for example "now minus 30 minutes"), and record the current last log id and last transaction id of each ledger on the source (the queries in step 4, run against production read-only). This is your expected value.
- Confirm you have the exact binary version that matches the schema (`goose_db_version` max `version_id` per schema).

### Step 1. Declare and freeze (T0)

1. Stop writes: scale the LedgerTrack API and worker to zero (or revoke the posting services' write tokens). Posting services must queue and retry with the same idempotency keys.
2. Record `T0` and, if this is a real incident, the time of the last known good state (`T_loss_start`).

### Step 2. Restore (T1 start, T2 end)

Pick the matching case:

- **PITR**: restore to `T_target` into a new instance/service. Record the restored instance id and the exact recovery target time reported.
- **Volume snapshot (Railway)**: restore the chosen backup in the Backups tab, review the staged change, deploy (verify the steps in the current UI). The previous volume stays mounted-off, keep it until the drill is accepted.
- **Dump**: create an empty database, then

```
createdb ledgertrack_restore
psql -d ledgertrack_restore -c 'create extension if not exists pgcrypto;'
pg_restore --no-owner --jobs=2 --dbname=ledgertrack_restore ledgertrack-<ts>.dump
```

Wait for the database to accept connections and finish crash or WAL recovery (`select pg_is_in_recovery();` returns `f`).

### Step 3. Start LedgerTrack against the restored DB, read-only first (T3)

Start one API instance with the new `POSTGRES_URI` but do not enable the posting services. Use the same binary as the backup's schema. Do not pass `--auto-upgrade` in a drill: a restore must not migrate on its own. If the binary is newer than the schema, run `ledger migrate` deliberately and record it.

Start the worker only after verification passes.

### Step 4. Verification queries (T4 start)

Run with `psql -d <restored db>`. Replace `_default` with each bucket from `select distinct bucket from _system.ledgers;`. These queries are derived from the migrations in `internal/storage/system/migrations.go` and `internal/storage/bucket/migrations`. They were not executed against a live database when this document was written (no Postgres was available to the author), so the first drill must confirm them and fix any column name drift.

**4.1 Migration state (schema matches the binary)**

```sql
select 'system' as schema, max(version_id) as version, bool_and(is_applied) as all_applied from _system.goose_db_version where version_id > 0
union all
select '_default', max(version_id), bool_and(is_applied) from _default.goose_db_version where version_id > 0;
```

Expect both `all_applied = t` and versions equal to the production values.

**4.2 Last log id, last transaction id, counts, per ledger. Compare to the expected values from step 0 (for PITR, to the values at `T_target`)**

```sql
select l.ledger,
       l.count as logs,
       l.last_log_id,
       l.last_log_date,
       t.count as transactions,
       t.last_tx_id
from (select ledger, count(*), max(id) as last_log_id, max(date) as last_log_date from _default.logs group by ledger) l
left join (select ledger, count(*), max(id) as last_tx_id from _default.transactions group by ledger) t using (ledger)
order by l.ledger;
```

Log ids are contiguous per ledger apart from rare holes from rolled back transactions, so also check for gaps count:

```sql
select ledger, max(id) - count(*) + 1 as missing_ids from _default.logs group by ledger;
```

A small positive number can be legitimate (aborted sequence use); a large or growing one is not.

**4.3 Log hash chain**

The logs table carries a `hash` per row (sync mode, default) chained to the previous row of the same ledger. Recompute it with the schema's own function and list mismatches (expect zero rows):

```sql
with ordered as (
  select l as log, lag(l.hash) over (partition by l.ledger order by l.id) as prev_hash
  from _default.logs l
)
select (o.log).ledger, (o.log).id
from ordered o
where (o.log).hash is distinct from _default.compute_hash(o.prev_hash, o.log)
limit 20;
```

This scans the whole table. On large tables run it on the restored copy only, or restrict with `where l.ledger = '<name>' and l.id >= <n>` inside the CTE (the first row of the range then has no `prev_hash`, so start one row earlier and ignore it). If a ledger uses async hashing (`features[HASH_LOGS]=ASYNC`), the per-row hash is not the proof; check block continuity instead:

```sql
select ledger, count(*) as blocks, max(to_id) as hashed_up_to
from _default.logs_blocks group by ledger;
```

and confirm each `from_id` equals the previous block's `to_id` and `hashed_up_to` is at or below the last log id (the worker catches up after start).

**4.4 Sum of balances per asset equals zero, per ledger**

Double entry means total inputs equal total outputs for every asset in a ledger:

```sql
select ledger, asset, sum(input) as inputs, sum(output) as outputs, sum(input - output) as net
from _default.accounts_volumes
group by ledger, asset
having sum(input - output) <> 0;
```

Expect zero rows. Cross-check against the movement table, which is independent of the volumes cache:

```sql
select ledger, asset,
       sum(case when is_source then -amount else amount end) as net
from _default.moves
group by ledger, asset
having sum(case when is_source then -amount else amount end) <> 0;
```

Expect zero rows. Then confirm the cache equals the movements (expect zero rows):

```sql
select v.ledger, v.accounts_address, v.asset, v.input - v.output as cached, m.net as from_moves
from _default.accounts_volumes v
join (
  select ledger, account_address, asset,
         sum(case when is_source then -amount else amount end) as net
  from _default.moves group by 1, 2, 3
) m on m.ledger = v.ledger and m.account_address = v.accounts_address and m.asset = v.asset
where v.input - v.output <> m.net
limit 20;
```

**4.5 `_system` row counts**

Compare to production (or to the pre-restore counts, allowing for the rows created after `T_target`):

```sql
select 'ledgers' t, count(*) from _system.ledgers
union all select 'configuration', count(*) from _system.configuration
union all select 'exporters', count(*) from _system.exporters
union all select 'pipelines', count(*) from _system.pipelines
union all select 'products', count(*) from _system.products
union all select 'clients', count(*) from _system.clients
union all select 'accounts', count(*) from _system.accounts
union all select 'kyc_records', count(*) from _system.kyc_records
union all select 'interest_accruals', count(*) from _system.interest_accruals
union all select 'fee_postings', count(*) from _system.fee_postings
union all select 'account_daily_usages', count(*) from _system.account_daily_usages
union all select 'currencies', count(*) from _system.currencies
union all select 'channel_fee_configs', count(*) from _system.channel_fee_configs
union all select 'channel_fee_config_audits', count(*) from _system.channel_fee_config_audits
union all select 'channel_fee_records', count(*) from _system.channel_fee_records
order by 1;
```

**4.6 CBA to ledger consistency (wallet layer)**

Every CBA account must have a backing wallet that exists in the ledger. Spot check through the API once it is up (read token):

```
GET /v2/ledgertrack/accounts/{accountID}   # wallet_info.available_balance
GET /v2/ledgertrack/reports/finance/trial-balance
```

The trial balance must balance (debits equal credits). Compare two or three accounts to the values recorded in step 0.

### Step 5. Accept or reject (T4 end)

All of 4.1 to 4.4 must pass and 4.5 must match within the expected delta. If any fails, stop, keep everything, and escalate. Do not "fix up" data. Try the next older restore point instead.

### Step 6. Cut over (T5)

1. Point the production `POSTGRES_URI` (or the service reference) at the restored database. Keep the old database intact and read-only for forensic work.
2. Start the API with `LEDGERTRACK_REQUIRE_AUTH=true` and your normal auth settings. Confirm 401 without a token and 200 with one.
3. Start the worker, wait for `logs_blocks` and schedulers to catch up.
4. Re-enable the posting services. They replay queued requests using their idempotency keys.
5. Reconcile the window between the restore point and the freeze: compare each posting service's records against `select ... from _default.transactions where timestamp > <T_target>`.
6. Record `T_ready` (first successful posting).

## 4. Measurement template

Copy one row per drill. RTO is `T_ready - T0`. Measured RPO is `T_incident (or freeze time) - latest log date found in the restored DB`.

| Date | Drill id | Method (PITR / snapshot / dump) | DB size | T0 freeze | T1 restore start | T2 restore end | T3 API up | T4 verified | T5 cut over | T_ready | Measured RTO | Measured RPO | Queries 4.1 to 4.5 pass | Issues found | Operator |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | | | | | | | |

Targets for comparison: RTO 60 min, RPO 5 min. If a drill misses a target, open an action item and rerun within 30 days.

## 5. Failure notes

- Restoring only `_system` or only a bucket schema breaks the link between CBA accounts and wallet addresses. Always restore the whole database.
- A restore that is newer than your binary's schema needs `ledger migrate`. A restore older than your binary also needs it. A binary older than the schema must not be started against it (forward-only: the old binary may misbehave). Keep binaries tagged by version for exactly this reason.
- Bucket deletion (`DELETE /v2/_/buckets/{bucket}`) soft deletes first, with a cleanup retention period configured on the worker (`--worker-bucket-cleanup-retention-period`). Restore of a soft deleted bucket is `POST /v2/_/buckets/{bucket}/restore` before the retention expires. This is not a backup.
