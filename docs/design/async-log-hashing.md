# Asynchronous log hashing for the wallet ledgers

Status: implemented on branch `phase1/async-hash`, off by default.
Audience: CTO (decision), engineers (merge and review), operators (runbook).

## 1. Summary

Every write to a ledger ends with one log row. With `HASH_LOGS=SYNC` (the default) the hash of that row
depends on the hash of the previous row, so every writer of a ledger takes one advisory lock before
inserting its log and keeps it until commit. All writes of a ledger therefore commit one after the other.
Benchmarks showed this lock is the throughput ceiling of the wallet ledger (about 85-154 TPS with `SYNC`,
268-658 TPS with `HASH_LOGS=DISABLED`, same load, same pool).

This change adds an opt-in mode, `--wallet-ledger-hash-mode=ASYNC`, for the wallet ledgers
(`ledgertrack`, `channels-*`, `revenue-*`):

* the commit path no longer computes a hash and no longer takes the lock;
* a background hasher in the worker chains the logs a moment later, **with the same hash function and
  the same chain format as SYNC**, so the result is byte for byte what SYNC would have produced;
* a verifier (`hashchain verify`) recomputes the chain and reports tampering and the not yet chained tail;
* the chain head can be exported to storage outside the database (anchoring) to detect a full rewrite.

Nothing changes unless the flag is set. This is a relaxation of the audit property, described precisely
in section 5. Read section 8 before turning it on.

## 2. What ASYNC did before this change

`HASH_LOGS` has three values: `SYNC`, `ASYNC`, `DISABLED` (`pkg/features/features.go:47`). Reading the code:

| Question | Answer | Where |
| --- | --- | --- |
| Is the per ledger advisory lock taken in ASYNC? | No. It is taken only when the feature is `SYNC`. | `internal/storage/ledger/logs.go:57-58` |
| Is the hash computed in the commit path in ASYNC? | No. The `set_log_hash_<id>` trigger is only created for `SYNC` ledgers. | `internal/storage/bucket/default_bucket.go:216-229` |
| Does the Go side check hashes in ASYNC? | No, only for `SYNC`. | `internal/controller/ledger/controller_default.go:361` |
| How are log ids allocated? | `nextval()` of a per ledger sequence, inside the INSERT. No lock. | `internal/storage/ledger/logs.go:90-91` |
| What is hashed, then? | Nothing per log: `logs.hash` stays NULL. A cron job in the worker (default every minute) calls `create_blocks`, which writes one hash per block of up to 1000 logs in `logs_blocks`. | `internal/storage/worker_async_block.go:85,112`, `internal/storage/bucket/migrations/38-logs-async-hash-procedure/up.sql` |
| Can an ASYNC ledger be created? | Only with `--experimental-features` (any non default feature is rejected otherwise). | `internal/controller/system/controller.go:210-214` |

I ran it to confirm: 40 concurrent transactions on an ASYNC ledger gave 40 logs with a NULL hash and one
`logs_blocks` row covering ids 0..40.

So ASYNC already removed the lock and the synchronous hash. It was not usable for an audited ledger:

1. **No per log hash and no verifier.** A block hash is a hash of the concatenation of the block's logs,
   with a different formula than the per log chain (no separators, different encoding of the date). Nothing in
   the repo verifies it.
2. **It can silently skip logs.** `create_block` selects `id > previous_block.max_log_id` (migration 38, line 38). Ids are
   allocated at insert time but visible at commit time, so without the lock a log with a lower id can commit after a block
   that already covers a higher id. That log is never hashed by any block. The hole is permanent and undetectable.
3. **Out of order visibility was never handled** for the consumers of the log ids (section 6).
4. It needs `--experimental-features`, which opens every ledger feature (including `DISABLED`) to anyone who can call the
   ledger creation API.

## 3. What this change does

### 3.1 Commit path

With `HASH_LOGS=ASYNC` the commit path is the existing ASYNC one: no trigger, no lock, id from the sequence.
Nothing in `InsertLog` changed. Log id allocation is a `nextval` inside the INSERT, which is not transactional and
does not serialize writers, so there is no critical section to shrink. The visible consequence (ids become visible
out of order) is dealt with by the hasher and documented in section 6.

### 3.2 The hasher (`internal/storage/loghash`)

A loop in the worker (default every 500 ms) lists the ASYNC ledgers and chains their pending logs in batches
(default 500) with the existing SQL function `compute_hash` (the one the SYNC trigger uses), in one
`REPEATABLE READ` transaction per batch:

1. `pg_try_advisory_xact_lock` on `log-hasher:<ledger id>`: several hashers (replicas) never work on one ledger at the
   same time. Writers never take this lock.
2. Read the head of the chain: the highest id with a hash, and that hash.
3. Read the next pending ids and decide how many can be chained (below).
4. `UPDATE logs SET hash = ...` for exactly those ids, in id order, from the head hash. The number of updated rows must
   equal the number expected, otherwise the transaction is rolled back.

**Which ids can be chained.** The chain is in id order. Ids contiguous with the head are always safe. A hole in the ids
is either a rolled back transaction (permanent, one per deadlock retry) or a transaction still in flight (the log will
appear later). The hasher advances over a hole only when it is final:

* when it sees a hole it records the snapshot horizon (`pg_snapshot_xmax`) and the highest visible id;
* the holes at or below that id are final once the oldest running transaction (`pg_snapshot_xmin`) has moved past the
  recorded horizon (every transaction that was running then has ended) and `--worker-log-hasher-gap-grace` (1 s) has
  passed.

This gives a bounded lag (about the grace plus the longest write transaction) whatever the rate of holes, and it
never skips a log whose transaction was in flight. A long running write transaction anywhere in the PostgreSQL instance
delays the chain (it shows in the lag metric). It never makes the chain wrong.

One window is covered by the grace period only, not by the transaction horizon: between `nextval` and the moment the
inserting transaction gets a transaction id, inside one statement. A pause of the PostgreSQL backend longer than the
grace period in that window could make a log appear below the head. This is detected by the verifier
(`UNCHAINED_BELOW_HEAD`). For the wallet write path the transaction has already written rows before the log, so it already has
a transaction id and the window does not exist.

The hasher has no persistent state. After a restart it reads the head from the table and continues. It never
rehashes a log (`WHERE hash IS NULL`).

### 3.3 Productization for the wallet ledgers

* New flag / env `--wallet-ledger-hash-mode` / `WALLET_LEDGER_HASH_MODE` (`SYNC`, `ASYNC`, empty = unchanged; `DISABLED` is
  rejected). On `serve` and `worker`.
* When the server creates `ledgertrack`, `channels-*` or `revenue-*` (all creation sites use the default configuration)
  the flag sets `HASH_LOGS` for that ledger. This is done in `CreateLedger` of the system controller, after the
  experimental features gate: `--experimental-features` is not needed and is not turned on by this flag. A caller that
  sets `features` explicitly is not affected and still needs the gate. Only `HASH_LOGS` changes.
* Existing ledgers are never touched by the flag. To move an existing ledger: `hashchain set-mode` (runbook).
* `--worker-async-block-hasher-disabled` turns off the legacy block job, which has the defect in 2.2 and is redundant.
  Set it on deployments that use ASYNC.
* Worker flags: `--worker-log-hasher-interval`, `-batch-size`, `-gap-grace`, `-alert-lag`.

### 3.4 Verifier and monitoring

`ledgertrack hashchain verify --ledger ledgertrack [--anchor-id N --anchor-hash HEX] [--max-lag 60s] [--json]`

Walks the logs by id, checks for each hashed log that `hash == compute_hash(stored hash of the previous hashed log, content)`, and reports:
logs verified, the first mismatch (id, reason, stored and expected hash), the number of problems, the chain head (id and hash),
holes in the ids (informational), and the unhashed tail (count, id range, oldest date, lag in seconds). Exit codes:
0 intact, 2 broken (or anchor mismatch), 3 tail older than `--max-lag`, 1 error. Read only.

`hashchain status --ledger L [--max-lag D]`: cheap (head, backlog size, lag), no recomputation. This is the command to
schedule, because it still works when the worker is down.

Metrics (OpenTelemetry, per ledger): `ledger.log_hasher.lag_seconds`, `ledger.log_hasher.unhashed_logs`,
`ledger.log_hasher.head_id`, counter `ledger.log_hasher.hashed_logs`.
Log line: `ALERT log hash chain lag: ledger=... lag=...` every 30 s while the lag is above `--worker-log-hasher-alert-lag`
(default 60 s). An info line with head, backlog and lag is logged once a minute.

Alert thresholds, with the reasoning: normal lag is 1-3 s (grace + interval + longest write transaction).
**Warn at 30 s, page at 60 s sustained for 2 minutes.** The 60 s default of the ALERT log line is the page level.
Because a dead worker produces no log line at all, also alert on `hashchain status --max-lag 60s` run from outside
(cron / Railway job), and on the absence of the metric.

## 4. Hash format and compatibility

The hasher calls `compute_hash(previous_hash, log)`, the function used by the SYNC trigger (migration 47). The tests
build the same logs in a SYNC ledger and an ASYNC ledger and require every hash to be equal. A ledger can move from
SYNC to ASYNC and back and the chain stays one continuous chain that the verifier validates end to end
(tested). Export/import between modes was not tested.

## 5. Audit semantics, before and after

### Before (SYNC)

* A log's hash is computed in the same transaction that creates the log. When a write is acknowledged, the log is already
  in the chain.
* There is no window with logs outside the chain. An attacker with database write access who edits or deletes a
  committed log breaks every hash after it, unless he recomputes all of them. The hash of the head is the only thing that
  needs protecting.

### After (ASYNC)

* A log is acknowledged to the client when it commits, with `hash = NULL`. It joins the chain typically 1-3 seconds later
  (the lag metric).
* **The window.** The logs after the head (the "unhashed tail") are not covered by any tamper evidence. In the window an
  attacker with write access to the database can edit, delete or insert tail logs, and the chain will then be built over
  the modified content, with no trace in the chain. The same attacker could not do that with SYNC for acknowledged logs
  without rewriting the chain. The window is one lag long in normal operation. If the worker is down or blocked, the window
  grows with the lag until someone notices. The monitoring in 3.4 is what bounds it.
* Before the head the guarantees are the same as SYNC, with two additions that SYNC does not need:
  * a log inserted below the head after the chain moved past (backdated insert) has no hash: reported `UNCHAINED_BELOW_HEAD`;
  * a hole in the ids is accepted when it is final; holes are indistinguishable from rolled back transactions. An attacker
    who deletes a log and does nothing else is detected (the next link breaks). Deleting the very last logs before the
    head, and the head itself, is only detected with an anchor (section 7), as in SYNC.
* What an attacker with write access can do **outside** the window is unchanged from SYNC: edit a log and recompute every
  following hash up to the head. The chain alone does not detect it (the verifier says OK). Only an anchor held outside the
  database detects it. Anchoring is as important in SYNC as in ASYNC.
* What the chain never covered, in any mode: the tables other than `logs` (accounts, volumes, moves, transactions) are not
  hashed. The chain protects the log, from which they can be rebuilt.
* The application itself (not an attacker) never reads `hash` in ASYNC and never fails a write because of it.

### Net change in one sentence

For each acknowledged write, SYNC proves "this log was chained when I acknowledged it". ASYNC proves "this log was chained
within about the lag", and anything after the head is unprotected until then.

## 6. Ordering relaxation: what in this repo depends on ids becoming visible in order

Without the lock, log id N+1 can become visible before id N (N is in flight). The hasher handles that (3.2). These
readers rely on `id > last seen` and can **permanently miss** a late visible log (the same is true with
`HASH_LOGS=DISABLED`, SYNC was hiding it):

| Reader | Where | Effect |
| --- | --- | --- |
| Replication pipelines / exporters | `internal/replication/pipeline.go:83-84` (`id > LastLogID`) | A late log is never exported. **Do not attach exporters to an ASYNC ledger** until the pipeline is limited to the chain head (a log with an id at or below the head is final). Not implemented here. |
| Anyone paging `GET /logs` with a stored last id | API clients | Same. A client that polls "ids greater than the last one I saw" can miss a log. Use the transactions endpoints, or lag behind by a few seconds. |
| Event bus | `internal/bus` (not read in detail) | Events are published by the controller after the write; I did not verify their order against log ids. Assume a consumer can see N+1 before N. |
| `Export` / `Import` | `controller_default.go` (`Export` reads ascending pages) | An export during writes can miss a log that commits during the export. Export a quiet ledger or compare with the verifier. |
| `ErrInvalidHash` on import | `controller_default.go:361` | Only for SYNC target ledgers; an import in order into a SYNC ledger is unaffected. |

Nothing else in the repo (transactions, balances, accounts, idempotency, reference checks) uses log ids for ordering.
Idempotency keys and references are enforced by unique indexes / their own lock, not by the log lock. Removing the log
lock removes the incidental serialization of all writes of a ledger: writes that touch the same accounts still
serialize on account rows and can deadlock, which the existing retry handles (the benchmark shows it works).

## 7. Anchoring (recommended, not implemented)

The chain proves internal consistency. It cannot prove that the whole chain was not rewritten. Periodically store the head
outside the database, where the application and the database administrators cannot rewrite it.

Procedure (to automate as a scheduled job, e.g. every 15 minutes and once a day):

1. `ledgertrack hashchain status --ledger ledgertrack --json` gives `headId` and `headHash`. For a stronger anchor
   run `hashchain verify --json` first and anchor only if it exits 0.
2. Write `{ledger, headId, headHash, observedAt}` to a store with write-once semantics held by another party/account: an
   S3 bucket with Object Lock in compliance mode, or an append-only log signed by a key that is not on the server
   (for example a daily signed digest emailed to compliance, or published to a transparency log).
3. Regularly (daily) run `hashchain verify --anchor-id <headId> --anchor-hash <headHash>` with a **previous** anchor.
   If the log at that id has a different hash, the history was rewritten after the anchor was taken.
4. Keep the anchors for the retention period of the ledger.

The tail is not covered by an anchor until the head passes it, so anchors reduce the exposure to the window but the window
stays open for the lag. A shorter anchoring interval narrows what could be rewritten consistently.

## 8. Statement for the CTO: risk accepted by enabling this

Turning on `ASYNC` for the wallet ledgers accepts the following:

1. **A tamper-evidence gap of about 1-3 seconds in normal operation, unbounded if the hasher stops.** During the gap,
   acknowledged payment records are not covered by the hash chain. It is bounded only by monitoring and by someone acting
   on the alert.
2. **Any claim that "every acknowledged transaction is hash chained at acknowledgement" is no longer true.** If a regulator,
   auditor, partner bank or the CBN examination (or a contract) relies on that exact property, check the wording before the
   switch. The honest wording is "hash chained within seconds, verified continuously, with the head anchored externally".
3. **The protection against a privileged insider or an attacker with database write access is not stronger than SYNC outside
   the window and weaker inside it.** Neither mode protects against a full chain rewrite without an external anchor. If
   you turn ASYNC on, do the anchoring at the same time.
4. **Readers that follow log ids can miss late logs** (section 6): exporters/replication pipelines and clients that poll
   "after the last id".
5. **New moving part.** The worker must run. A stopped worker does not stop payments, it widens the gap. This is a deliberate
   choice (availability over audit freshness) and the alternative (refuse writes when the chain lags) was not built.
6. It is not a security control against a compromised application either: an attacker who controls the application can write
   any log, in any mode.

What it does not change: balances and idempotency checks, API contracts, double-entry integrity, or the data that is
stored. The verifier makes the audit state observable, which SYNC never offered.

Not verified here: behaviour at 1,200 TPS on the production PostgreSQL, a real regulator's reading of the wording above
(needs compliance/legal), behaviour with several worker replicas under load (two concurrent hashers are tested for
correctness only), and anything involving exporters.

## 9. Operations runbook

### Enable on a new deployment

1. Set `WALLET_LEDGER_HASH_MODE=ASYNC` on the API service and on the worker service.
2. Run a worker (`serve --worker` or the `worker` command). Set `WORKER_ASYNC_BLOCK_HASHER_DISABLED=true`.
3. Start; the first request that creates `ledgertrack`, `channels-<ccy>`, `revenue-<ccy>` creates it with `HASH_LOGS=ASYNC`.
4. Schedule `hashchain status --max-lag 60s` per ledger (exit code 3 pages) and the anchoring job.

### Move an existing SYNC ledger (the production `ledgertrack`)

1. Deploy the new binary, worker running, **flag unset**. Nothing changes.
2. Take an anchor: `hashchain verify --ledger ledgertrack --json`, store `headId`/`headHash` outside the database.
3. Quiet period: `ledgertrack hashchain set-mode --ledger ledgertrack --mode ASYNC` (short exclusive lock on the logs table).
4. Restart every API and worker process (the ledger configuration is cached).
5. Watch `hashchain status` until lag settles at a few seconds. `hashchain verify --anchor-id <id> --anchor-hash <hash>` from step 2
   must pass.
6. Do the same for each `channels-*` and `revenue-*` ledger.

### Roll back to SYNC

`hashchain set-mode --ledger L --mode SYNC`. It refuses while any log is unchained (wait for the hasher), then recreates the trigger.
Restart all processes. Stop writes (or accept a short block) during the switch: the command takes a lock on the logs table.

### Alerts

| Signal | Meaning | Action |
| --- | --- | --- |
| `lag_seconds` > 30 | hasher behind | check worker is running and its logs |
| `lag_seconds` > 60 sustained 2 min, or ALERT log line, or `status` exit 3 | tamper-evidence gap is open | page; follow below |
| `verify` exit 2 | chain broken or anchor mismatch | treat as a security incident: snapshot the database, do not run any fix, preserve the report (`--json`), compare with the last anchor, escalate |
| `UNCHAINED_BELOW_HEAD` | a log appeared or lost its hash below the head | same as above, unless the cause is known (see the one window in 3.2) |

Lag does not go down:

1. Is the worker running (`ledger.log_hasher.hashed_logs` increasing)? Restart it. A restart loses nothing.
2. `status` shows `unhashed` growing but the hasher is running: it is waiting for a hole to become final, which waits for a
   long transaction. In PostgreSQL: `select pid, xact_start, state, query from pg_stat_activity where xact_start < now() - interval '30 seconds' order by xact_start;`
   (idle in transaction sessions and prepared transactions: `select * from pg_prepared_xacts;`). Terminate the old one. The cause may be in another
   database of the same instance.
3. Errors in the worker log (`chaining logs: ...`): "concurrent modification" means somebody else writes the `hash` column.
   Stop and investigate before restarting.

### Notes

* `verify` reads the whole ledger. For a large ledger run it from a replica, off peak.
* The metrics callback exists only in processes that run the worker.
* The legacy `logs_blocks` table is left in place and not used by this mode.

## 10. Tests

* `internal/storage/loghash` (needs a PostgreSQL, `LEDGERTRACK_TEST_PG_URI=postgres://user@host:port/postgres?sslmode=disable`, skipped otherwise):
  async chain equals the SYNC trigger chain; restart resumes without rehashing; two hashers at once; an in flight lower id is waited
  for and chain order is id order; a rolled back id is skipped once final; tampering (content, date, deleted row, erased hash,
  backdated insert) is detected at the right id; a consistent full rewrite is only detected by the anchor; tail and lag are reported;
  `set-mode` both ways.
* `internal/controller/system`: the flag applies to wallet ledgers only, never mutates the shared defaults, never overrides an
  explicit choice, keeps the experimental gate for explicit features.
* The tests that use docker (existing integration tests) were not run here.

## 11. Local measurements and what to retest

Setup: 2 vCPU VM shared with another agent running builds and its own benchmarks at the same time (load average 9-20 on 2
cores during the runs), PostgreSQL 16 on the same VM, `fsync=off`, API pool `--postgres-max-open-conns 30` for both modes,
`ledgertrack-bench` from `phase1/bench-harness`, closed loop (`-rate 0`), 400 wallets, one currency, 12-15 s per run after 3 s of
warmup. SYNC and ASYNC servers (separate databases) were up together and runs were alternated so that both saw the same ambient load.
**Treat throughput numbers as shape only.**

Where the backends wait (sampled every 200 ms from `pg_stat_activity`, active backends only) is the useful result:

| Workload | Mode | Samples waiting on the ledger advisory lock |
| --- | --- | --- |
| lien only, 16/32/64 in flight (9 SYNC runs) | SYNC | 47% to 77% of active backend samples (465/606, 1017/1845, 1072/1705, 1210/1395, 1097/3682, 925/3540, 1231/2525, 879/3089, 1303/3105) |
| same | ASYNC | 0 in every run |
| credit + debit, 16/32/64 | SYNC | 0 (they wait earlier, see below) |
| credit + debit | ASYNC | 0 |

Throughput of the same runs (2xx per second, higher is better):

| Run | SYNC | ASYNC |
| --- | --- | --- |
| lien, 16 in flight | 55, 26 | 63, 65 |
| lien, 32 in flight | 47, 55, 52, 23, 46 | 54, 56, 60, 55, 41 |
| lien, 64 in flight | 55, 26, 52, 51 | 54, 56, 65, 39 |
| credit+debit, 16 | 44, 34 | 39, 50 |
| credit+debit, 32 | 37, 17 | 40, 22 |
| credit+debit, 64 | 41 | 44 |

Reading:

* The advisory lock is gone from the ASYNC commit path. In SYNC it is more than half of what active lien backends wait on.
* **Throughput did not show a reliable gain on this machine.** Medians are slightly higher for ASYNC on lien (about 52-55
  against 46-52) but the spread inside one mode is larger than the difference. The box is CPU bound by the other tenant, which
  hides a lock that is not on the CPU. I cannot confirm here the 2-4x that was seen with `HASH_LOGS=DISABLED`. This needs the Railway run.
* **Credit and debit do not benefit until the control account hot row is fixed.** Both modes spend their time in
  `Lock:transactionid` (a row lock held by another transaction): every wallet credit/debit updates `system:control:{CCY}`
  (`docs/benchmarking.md`, "sharding and the wallet routes"). That row lock comes before the log insert, so the log lock
  never queues. Expect ASYNC to lift lien (and anything that does not touch the control account) first; credit/debit need the
  control account sharding / hot row work in `phase1/channel-hot-rows` as well. The gain of `DISABLED` that was measured earlier
  depended on the mix.
* Full mix on the ASYNC ledger (`credit=35,debit=35,lien=10,lien_release=8,channel_debit=12`, 32 in flight, 25 s): consistency check PASS
  (400 wallets, 0 mismatches, 0 negative balances, control delta equals expected, conservation 0, 0 unexpected 409).
  Afterwards `hashchain verify`: 7,561 logs verified, no mismatch, unhashed tail none, lag 0.0 s, 0 id holes.

Retest on Railway (production-like PostgreSQL, API and PostgreSQL not sharing 2 cores with other work):

1. `lien=100`, then `credit=50,debit=50`, then the gate mix at 1,260 TPS open loop, SYNC against ASYNC, same pool, 10 minutes each.
2. Sample `pg_stat_activity.wait_event` during the run, the lock evidence is more robust than TPS.
3. Watch `ledger.log_hasher.lag_seconds` during the run: the hasher must keep up at the target rate (the hasher does a batch of 500 in
   about 25 ms here; 1,200 TPS needs 2.4 batches per second), and look at its share of database CPU, because it rewrites every
   log row once (an UPDATE per log, so an extra row version per log: watch table bloat and autovacuum on `logs`).
4. Run `hashchain verify` on the benchmark ledger at the end, and with `--anchor-id/--anchor-hash` taken mid run.
5. Kill the worker for 2 minutes during a run, restart it, and check the backlog drains and verify passes.
