# Hot rows on the channel transfer path

Status: implemented on branch `phase1/channel-hot-rows`, opt-in, defaults unchanged. Last updated 2026-10-30.

## Problem

Once `system:control` was sharded (`CBA_SYSTEM_CONTROL_SHARDS=16`), `channel_debit` became the tail of the wallet benchmark: the only operation that timed out at 10 s, and the full mix stayed at 50 to 63 TPS while mixes without it reached 85 to 154 TPS. A channel transfer is up to four ledger transactions plus a Postgres insert, and three of those ledger transactions end in rows that every transfer of a currency updates.

## Trace of one channel debit

`POST /wallets/{id}/debit` with `channelID` (`internal/api/v2/controllers_wallets.go:303`). `lien/release` with mode PAY (`:712`) and the CBA account routes (`controllers_cba_accounts.go:347`, `:614`) write the same channel and revenue legs. Amounts: principal `P` (channelAmount), user fee `F`, processing fee `C`, wallet debit `P + F`.

| # | Ledger | Posting | Code | Accounts whose `accounts_volumes` row is locked |
|---|---|---|---|---|
| 1 | `ledgertrack` | `users:{u}:wallets:{CCY}:available` -> `system:control:{CCY}[:n]` | `controllers_wallets.go:389-428` | user available (per user), control shard (per shard) |
| 2 | `channels-{CCY}` | `channel:{id}` (unbounded overdraft) -> `world` | `controllers_wallets.go:474-490` | **`channel:{id}`** (per channel), **`world`** (all transfers of the currency) |
| 3a | `revenue-{CCY}` | `world` -> `revenue:accumulated` (reference `{ref}-user-fee`) | `postRevenueEntries`, `controllers_wallets.go:57-125` | **`world`**, **`revenue:accumulated`** (all transfers of the currency) |
| 3b | `revenue-{CCY}` | `world` -> `revenue:channel_processing_cost` (`{ref}-processing-fee`, only if `C > 0`) | same | **`world`**, **`revenue:channel_processing_cost`** (all transfers of the currency) |
| 4 | Postgres | `_system.channel_fee_records` insert (fee record) | `channelFeeConfigService.Record` | none (append) |

Shared across all transfers of a currency (hot): `world` in `channels-{CCY}`, `world` in `revenue-{CCY}` (written twice per transfer), `revenue:accumulated`, `revenue:channel_processing_cost`, `channel:{id}` (shared by every transfer on one channel), the control shard unless `CBA_SYSTEM_CONTROL_SHARDS` is set. Per user: the wallet accounts, which the in-process wallet locker already serialises (`wallet_locker.go:94`).

Every leg is its own database transaction: a transfer with both fees is 4 ledger commits and 1 fee record insert, each separately committed and not atomic with the others.

### What a ledger transaction locks, in order

`internal/controller/ledger/log_process.go:36` `runTx` begins, then:

1. `machine.Execute` reads source balances with `GetBalances` (`internal/storage/ledger/balances.go:20-81`): it inserts zero rows `ON CONFLICT DO NOTHING` and selects the **source** accounts `FOR UPDATE`, ordered by address. `world` is never read (`internal/machine/vm/machine.go:156`), and a source with `allowing unbounded overdraft` is not balance-checked.
2. `CommitTransaction` (`transactions.go:28`) calls `UpdateVolumes` (`volumes.go:20-60`): one `INSERT ... ON CONFLICT (ledger, accounts_address, asset) DO UPDATE` with a row per touched account, sorted by address (`internal/transaction.go:129-185`). This is where **source and destination, including `world`, get their row lock**. It is held until commit. Consistent address order is what keeps these updates from deadlocking.
3. `InsertTransaction`, `InsertMoves` (moves history is ON; its triggers read the previous move of each account).
4. `upsertTransactionAccounts` (`controller_default.go:500`): touches `accounts` rows only for new accounts or changed metadata.
5. `InsertLog` (`internal/storage/ledger/logs.go:55-60`): with `HASH_LOGS=SYNC`, the default, `pg_advisory_xact_lock(ledger.ID)` is taken here and held until commit. This is the per ledger lock; it is held for the tail of the transaction only.
6. `Commit` (`log_process.go:62`).

On an idle box a single credit measured with `log_min_duration_statement=0`: `UpdateVolumes` at t+8 ms, advisory lock at t+20 ms, commit at t+24 ms. So a hot row is held for about 16 ms of a 24 ms transaction while the per ledger advisory lock is held for about 4 ms. A hot row therefore caps a ledger at roughly `1 / (row hold time)` writes per second, well below what the advisory lock alone would allow. With two revenue transactions per transfer on the same `world` row, the revenue ledger caps channel transfers at about half of that, which is the order of magnitude seen on Railway.

Not addressed here: the per ledger advisory lock (owned by the async hash work), `channel:{id}` itself, and the fixed per transaction cost (moves triggers, log insert, fsync).

## What this change does

Two opt-in flags on `serve`. Both default to `1`, which is the historical layout byte for byte (the unit test `TestChannelDebitLegsAreUnchangedByDefault` pins it).

| Flag | Leg 2 becomes | Legs 3a/3b become |
|---|---|---|
| `--revenue-shards N` | unchanged | `world:{s}` -> `revenue:accumulated:{s}` and `world:{s}` -> `revenue:channel_processing_cost:{s}` |
| `--channel-world-shards N` | `channel:{id}` -> `world:{s}` | unchanged |

`s = fnv32a(business reference) mod N` (`internal/hotrows`). The code is in one small package that the handlers, the reports and (after merge) the transfer executor all call.

Why the counter side is sharded together with the revenue accounts: sharding only `revenue:accumulated` leaves the single `world` row of `revenue-{CCY}` in every transaction, so nothing gets better. That was measured (arm B below). A sharded counter account is not the special `world` account, so postings from it carry `allowing unbounded overdraft` (`hotrows.SourceClause`). Per shard the pair `world:{s}` / `revenue:*:{s}` nets to zero together with the others: the ledger still balances, and `sum(world, world:*) = -sum(revenue accounts)` in `revenue-{CCY}`, `sum(world, world:*) = -sum(channel:*)` in `channels-{CCY}`.

`channel:{id}` is not sharded. Splitting it would change what the channel balance means (the overdraft warning in the debit response, `GET` channel balance, the `channel:` totals in the reports). With C channels per currency the ceiling after this change is about C times a single row's rate. If one channel dominates, that row is the next bottleneck.

### Determinism and recovery (atomic-transfers)

The shard is a pure function of the **business reference** (`req.Reference`, never `{ref}-user-fee`) and N. Every leg of one transfer, an inline retry, a client retry with the same reference and the recovery runner compute the same accounts. The ledger refuses a second use of a reference, so a retry is a duplicate and posts nothing; `TestRetryOfARevenueLegLandsOnTheSameShard` and `TestShardingIsADeterministicFunctionOfTheReference` cover it, and `TestShardForIsStable` pins the hash because changing it would move in-flight retries.

Merging with `phase1/atomic-transfers` needs three small edits in `internal/transfers/executor.go` (that branch was only read, not changed):

```go
// PostChannel
Destination: hotrows.ChannelSink(t.Reference),

// PostRevenue: use the business reference t.Reference (not the leg reference) for the shard
Source:             hotrows.RevenueSource(t.Reference),
Destination:        hotrows.RevenueDestination(entry.base, t.Reference), // entry.base: hotrows.AccumulatedBase / hotrows.ProcessingCostBase
UnboundedOverdraft: hotrows.IsUnboundedSource(hotrows.RevenueSource(t.Reference)),

// samePosting: a replayed leg may sit on another shard than the one the current N picks
p.Source == m.Source || hotrows.EquivalentAccount(p.Source, m.Source)   // and the same for Destination
```

The last edit matters when N changes while a transfer is mid-flight: the first attempt posted on shard 3, the recovery run (after a restart with another N) computes shard 9, the ledger answers "duplicate reference", and `samePosting` would otherwise report `ErrReferenceMismatch` and park the transfer in `failed_needs_review`. `hotrows.EquivalentAccount` treats `revenue:accumulated`, `revenue:accumulated:N`, `world`, `world:N` (and the processing cost account) as the same logical account. Also note that `phase1/atomic-transfers` posts the user leg to `system:control:{CCY}` unsharded; the merged executor should use `systemControlAddress(walletID, currency)` like the handler does after `phase1/wallet-perf`.

## Reporting impact

Anything that read `revenue:accumulated` by exact address undercounts once shards are on. Found and changed:

| Reader | Before | Now |
|---|---|---|
| Trial balance, line `revenue:accumulated` (`internal/cba/services/finance_reporting.go`) | `Match("address", "revenue:accumulated")`: exact | `Match("address", "revenue:accumulated:")` |
| Profit and loss, "wallet revenue" (same file) | `Match("account", "revenue:accumulated")` and `p.Destination == "revenue:accumulated"` | `Match` on the base **and** on the `:` pattern, `hotrows.IsShardOf` on postings |
| `test/e2e-python/e2e_functional_test.py` revenue postings check | destination equal | base or `:N` |

The report output is unchanged: the same line labelled `revenue:accumulated`, the same numbers.

Why a plain match is wrong, and why the two filters differ (verified against a live Postgres, see below). `aggregatedBalance` uses `query.Match("address", x)`. The store turns an address without an empty segment into an exact comparison (`filterAccountAddress`, `internal/storage/ledger/utils.go:23`), so `revenue:accumulated` never matches `revenue:accumulated:3`. A trailing empty segment makes the pattern partial with no length constraint for **balances**, so `revenue:accumulated:` matches the base and every shard. The **transactions** filter (`filterAccountAddressOnTransactions`, `transactions.go:271`) counts the trailing empty segment as a segment, so there `revenue:accumulated:` matches only 3-segment addresses and **misses the base address**. The profit and loss query therefore matches both. Live: transactions with `account=revenue:accumulated` returned 39 transactions touching only the base, `account=revenue:accumulated:` returned 87 touching only shards. The system control account has the same exact-match flaw with `CBA_SYSTEM_CONTROL_SHARDS`; that is fixed on `phase1/bench-harness` (`systemControlPattern`) and not duplicated here.

Not affected: balance sheet and cash flow (they do not read revenue), channel revenue reports (read `_system.channel_fee_records`), per channel balance endpoints (`channel:{id}` is not sharded), `revenue:fee_income` and `revenue:interest_expense` (posted by the scheduler, not on this path).

Check before turning shards on: any exporter, ClickHouse reconciliation, GL mapping or alert that keys on `source == "world"` or an exact `revenue:accumulated` in `channels-*` and `revenue-*` ledgers. Postings from these ledgers will show `world:{s}` and `revenue:accumulated:{s}`. The default ledgertrack `world` (credits, lien release) is untouched.

Tests (fail without the reader fix): `internal/cba/services/finance_reporting_revenue_shards_test.go` builds the same 80 postings as unsharded, 16 shards, one shard that is not 0, and a mix of base plus 3 and 4 and 16 shards, with fakes that mirror both store filters, and asserts the trial balance and profit and loss give 600 in every layout. Without the fix 3 of the 4 layouts fail (215 and 0 instead of 600).

## Rollout

Turn on: set `--revenue-shards 16` (and `--channel-world-shards 16`) on `serve` and restart. New postings use shards, old balances stay on the unsharded accounts, and every reader sums both. Nothing is migrated. Verified live on one database: shards off, then 16, then 4 (see "Live rollout check"): trial balance and profit and loss equalled the books after every phase, and each ledger netted to zero.

Change N: allowed at any time. History stays where it was posted and readers sum the base and all shards regardless of the current N. In-flight transfers: see the `EquivalentAccount` note above; without that edit, drain `_system.transfers` (no non terminal rows) before changing N.

Turn off: set the flags back to `1`. New postings return to the base accounts; shard balances stay and are still read. To fold them back, post one balanced transaction per shard in `revenue-{CCY}` (`send [CCY/2 X] (source = @revenue:accumulated:N destination = @revenue:accumulated)` and the same for `world:N` -> `world`), which is optional and not automated.

Replicas: the flags only matter on processes that accept channel writes (`serve`). The worker does not post these accounts. Use the same value on every `serve` replica; mixed values are safe (all addresses are read), but the load spreads accordingly.

Suggested first value: 16, as for the control account. More shards than concurrent in-flight transfers per currency gains nothing.

## Risks

1. Gain is bounded by what is left. With `HASH_LOGS=SYNC` every write on a ledger still passes the per ledger advisory lock, and `channel:{id}` is still shared. This change removes the row serialisation, not those.
2. `world:{s}` and `revenue:*:{s}` are new addresses. Consumers keyed on exact addresses (above) must be checked; schema enforcement `strict` with a chart of accounts would reject unknown addresses (default is `audit`).
3. Sharded counter accounts hold negative balances by design (like `world`), and `channel:{id}` already does.
4. Changing N mid-flight without the `samePosting` edit can park a transfer for review.
5. The reports read more rows when many shards exist (16 extra accounts per currency; negligible).
6. The profit and loss query still pages through every transaction of the period; that cost is unrelated to sharding.

## Option: fewer commits per transfer (not implemented as a default)

Arm E in the local measurements is option 1 (4 to 3 commits for a transfer with both fees, throwaway patch): +18% TPS and a slightly lower p99 on top of D, on a noisy box. Two ways, in increasing order of size:

1. **Revenue legs in one multi-posting transaction** (same ledger, no topology change). `user-fee` and `processing-fee` become one transaction with two postings, reference `{ref}-revenue`. A transfer with both fees goes from 4 ledger commits to 3 and takes one less per ledger lock section. Cost: the per leg references (`{ref}-user-fee`, `{ref}-processing-fee`) disappear, so `RevenueTxID`, the atomic-transfers executor (`samePosting` requires one posting, two references) and anything that reads `revenue_tx_id` change. Needs a flag and a reader audit; it is a reference-scheme change, so it should be its own change.
2. **Channel and revenue legs in one ledger per currency** (a combined `settlement-{CCY}` or channel and revenue accounts in the existing ledger). Legs 2 and 3 become one transaction (`channel:{id} -> world:{s}`, `world:{s} -> revenue:accumulated:{s}` in one ledger), so a transfer is 2 ledger commits (user leg plus settlement) instead of 4, and the cross-ledger non-atomicity between channel and revenue disappears; only the user leg and the settlement remain to reconcile. This is the topology change the atomic-transfers note already discusses, with the same migration and reporting cost, so it is documented here and not built. Hot rows in that ledger are the same set as today and the same shards apply.

Neither changes existing data, and neither is enabled by default.

## Local measurements

Harness: `cmd/ledgertrack-bench` from `phase1/bench-harness` (copied in, not committed), closed loop, 400 wallets uniform, 1 currency (USD), 4 channels, flat fees 50 and 20, so a channel debit is 4 ledger transactions. Mixes: "heavy" `channel_debit=70,credit=10,debit=10,lien=5,lien_release=5`; "full" `credit=35,debit=35,lien=10,lien_release=8,channel_debit=12`. Server `serve --worker --experimental-features` with `CBA_SYSTEM_CONTROL_SHARDS=16`, throwaway PG 16.15 on port 5566 with default durability (fsync on, `synchronous_commit` on, about 0.3 ms per fsync), pool of 100. Each cell: fresh database, 4 s warmup, 15 s measured, a fresh server; two repetitions interleaved A, D, A, D. Hash mode DISABLED vs SYNC is `HASH_LOGS` on `perf`, `channels-USD` and `revenue-USD`, set when the ledgers are created (DISABLED stands in for the finished async hash work). Every one of the 30 runs passed the harness consistency check with the sharded layouts, including `revenue:accumulated` and `revenue:channel_processing_cost` read through the prefix pattern.

Arms: **A** baseline (`--revenue-shards 1 --channel-world-shards 1`). **B** revenue credit side sharded only, `world` kept (`--revenue-shards 16` with a throwaway patch that keeps `world` as the source). **C** `--revenue-shards 16`, channel `world` kept. **D** both flags 16. **E** D plus both revenue entries of a transfer posted as one two-posting transaction (throwaway patch, not in this branch).

**Read these numbers for shape only.** The VM has 2 vCPUs and was shared with other agents running their own benchmarks the whole time (about 1.6 of 2 cores busy outside my processes by my accounting, which also counts loopback softirq). Absolute throughput is therefore CPU bound at roughly 17 TPS for the heavy mix in every arm and does not reflect Railway. What the box can show is where requests wait: the sampled number of backends waiting on a row lock or the advisory lock, and the tail latency of `channel_debit`. Waiters are the average count of backends in `Lock:transactionid` or `Lock:advisory` wait, sampled about every 0.4 s from `pg_stat_activity` during the measured window.

Mean of two runs per cell (per run rows are at the end of this section):

| cfg | hash | mix | conc | n | TPS all (mean) | chan_debit p99 ms (mean) | chan_debit max ms (mean) | row-lock waiters (mean) | advisory waiters (mean) | foreign cores (mean) |
|---|---|---|---|---|---|---|---|---|---|---|
| A | DISABLED | full | 32 | 2 | 44.8 | 6661 | 6661 | 9.02 | 0.00 | 1.60 |
| D | DISABLED | full | 32 | 2 | 43.1 | 2839 | 2839 | 0.70 | 0.00 | 1.60 |
| A | SYNC | full | 32 | 2 | 36.2 | 4260 | 4260 | 8.62 | 7.21 | 1.58 |
| D | SYNC | full | 32 | 2 | 37.1 | 3066 | 3066 | 8.16 | 7.93 | 1.58 |
| A | DISABLED | heavy | 16 | 2 | 17.7 | 2834 | 3022 | 7.76 | 0.00 | 1.58 |
| D | DISABLED | heavy | 16 | 2 | 18.9 | 1589 | 1713 | 0.47 | 0.00 | 1.62 |
| A | DISABLED | heavy | 32 | 2 | 16.4 | 6259 | 7689 | 18.82 | 0.00 | 1.56 |
| B | DISABLED | heavy | 32 | 2 | 16.2 | 7570 | 7906 | 18.18 | 0.00 | 1.56 |
| C | DISABLED | heavy | 32 | 2 | 17.5 | 6914 | 7818 | 12.76 | 1.26 | 1.60 |
| D | DISABLED | heavy | 32 | 2 | 18.5 | 2851 | 2952 | 0.72 | 0.00 | 1.60 |
| E | DISABLED | heavy | 32 | 2 | 21.9 | 2556 | 2898 | 0.92 | 0.00 | 1.90 |
| A | SYNC | heavy | 16 | 2 | 16.2 | 2916 | 3228 | 7.58 | 0.07 | 1.57 |
| D | SYNC | heavy | 16 | 2 | 17.7 | 1638 | 1693 | 0.41 | 1.83 | 1.59 |
| A | SYNC | heavy | 32 | 2 | 15.4 | 6980 | 7280 | 18.48 | 0.05 | 1.56 |
| D | SYNC | heavy | 32 | 2 | 19.4 | 4326 | 4517 | 4.43 | 5.50 | 1.60 |

What it shows:

* **Row waits are the dominant wait before the change and nearly vanish after it.** Heavy mix, 32 in flight, hash off: 18.8 backends waiting on row locks on average (of 32 requests), down to 0.7 with both flags. `channel_debit` p99 6.3 s to 2.9 s, max 7.7 s to 3.0 s; the 10 s timeouts seen on Railway are consistent with this tail. With 16 in flight: p99 2.8 s to 1.6 s. Full mix, hash off: p99 6.7 s to 2.8 s.
* **Sharding only the credit side does nothing (B).** 18.2 waiters, p99 7.6 s, same as A. The single `world` row in `revenue-{CCY}` is what serialises the revenue legs, so the counter side must move with it.
* **Both ledgers need it (C vs D).** Fixing only the revenue ledger leaves 12.8 waiters and the p99 near baseline, because every transfer still updates `world` in `channels-{CCY}`. D has both.
* **Throughput moved by +5% to +13% in the heavy mix, which is inside this box's noise** (CPU bound, see above). Do not quote it. The model below isolates the lock structure from CPU.
* **With `HASH_LOGS=SYNC` the gain is smaller and the wait moves.** D at 32 in flight: row waiters 18.5 to 4.4, but advisory lock waiters 0.05 to 5.5, p99 7.0 s to 4.3 s. At 16 in flight p99 2.9 s to 1.6 s. The per ledger advisory lock becomes the next serial point, which is what the async hash work removes. The two changes are complementary; in the full mix with SYNC there is no visible difference (p99 4.3 s vs 3.1 s on two noisy runs, row waiters 8.6 vs 8.2) because most waits there are the `ledgertrack` ledger's advisory lock and the user rows, not this path.
* **E: one commit fewer (4 to 3) is worth about as much again.** 21.9 vs 18.5 TPS and p99 2.6 s vs 2.9 s at 32 in flight, with more outside load during E (1.9 vs 1.6 cores), so the direction is reliable and the size is not.

### Lock structure model (no CPU noise)

Because TPS on this box is CPU bound, the same transaction shape was replayed with `pgbench` on the same Postgres: three transactions per transfer (channels, revenue user fee, revenue processing fee), each doing the real `INSERT ... ON CONFLICT DO UPDATE` on two `accounts_volumes` style rows and then holding its locks for 12 ms (`pg_sleep`, no CPU; the 12 ms is the measured hold of row locks before commit on an idle box, 16 ms of a 24 ms transaction, rounded down), 4 channels, 32 clients, 15 s. Files: not committed (about 40 lines of SQL).

| Arm | Transfers per second |
|---|---|
| A: everything on `world`, `revenue:accumulated`, `revenue:channel_processing_cost` | 34.7 |
| B: revenue credit side sharded, `world` kept | 35.0 |
| C: revenue fully sharded, channel `world` kept | 68.9 |
| D: revenue and channel `world` sharded (16) | 240.6 (bound by the 2 vCPUs of the load generator and server) |

The same ordering as the real runs. A is `1 / (3 x 12 ms)` because all three transactions queue on a `world` row; this is the same order as the 50 to 63 TPS ceiling seen on Railway for a mix containing channel debits. D is limited by the number of channels (4 rows) and by CPU, not by locks.

### Live rollout check

One database, three server restarts with the harness in between, `--revenue-shards` / `--channel-world-shards` set to 1, then 16, then 4 (environment variables `REVENUE_SHARDS` and `CHANNEL_WORLD_SHARDS` were also checked to work). After each phase:

| Phase | books: `revenue:accumulated` base + shards | `/reports/finance/trial-balance` revenue line | `/reports/finance/pnl` wallet revenue | sum of all balances in `revenue-USD` and in `channels-USD` |
|---|---|---|---|---|
| 1 (shards off) | 1950 | 1950 | 1950 | 0 and 0 |
| 2 (16) | 3700 (1950 base + 1750 in 15 shards) | 3700 | 3700 | 0 and 0 |
| 3 (4) | 6300 | 6300 | 6300 | 0 and 0 |

and the harness consistency check passed in every phase. The two filter behaviours the reports depend on, checked on the same database: aggregated balances for `revenue:accumulated` returned 1950 (base only), for `revenue:accumulated:` returned 6300 (base and all shards); transactions with `account=revenue:accumulated` returned the 39 base transactions, with `account=revenue:accumulated:` the 87 shard transactions and none of the base.

Not run: the Docker based integration tests (`internal/storage/...`, `test/e2e`), because Docker is not available in this environment. Everything that needs the real store was checked by hand on Postgres as above, and the unit tests mirror both store filters with fakes (flagged in the test comments, so drift is visible).

### What to retest on Railway

1. A and D of the same mix (channel heavy and full), 16/32/64 in flight, on the real `HASH_LOGS` setting of `channels-*` and `revenue-*`, then again after async hash lands. Compare channel debit p99 and timeouts first, TPS second. Watch `pg_stat_activity` for `Lock:transactionid` (row) against `Lock:advisory` (per ledger) to see which one is left.
2. Several channel counts (1, 4, 16). With few channels `channel:{id}` is the next cap and sharding the counter side will not lift it past C times a row's rate.
3. Trial balance and profit and loss before and after the restart with shards on, on the production data: totals must not move, and the sum of `world` and `world:*` must equal the negative of the revenue accounts.
4. The CBA account routes (`/accounts/{id}/debit` with a channel) post the same legs and were changed the same way but were not driven by the harness here.
5. The harness itself: `cmd/ledgertrack-bench/check.go` and `setup.go` read `revenue:accumulated` and `revenue:channel_processing_cost` by exact address. For these runs they were patched locally to read `revenue:accumulated:` and `revenue:channel_processing_cost:` (prefix patterns, which also match the base address when reading balances). Without that change its revenue check reports a mismatch as soon as shards are on.

### All runs

| seq | cfg | hash | mix | conc | TPS all | TPS chan_debit | chan p50 ms | chan p99 ms | chan max ms | timeouts | row-lock waiters | advisory waiters | foreign cores | check |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | A | DISABLED | heavy | 16 | 17.4 | 12.0 | 1212 | 3146 | 3260 | 0 | 7.36 | 0.00 | 1.58 | PASS |
| 2 | D | DISABLED | heavy | 16 | 18.7 | 12.9 | 1098 | 1720 | 1953 | 0 | 0.49 | 0.00 | 1.66 | PASS |
| 3 | A | DISABLED | heavy | 32 | 16.3 | 11.2 | 2490 | 6685 | 8060 | 0 | 18.25 | 0.00 | 1.57 | PASS |
| 4 | D | DISABLED | heavy | 32 | 18.8 | 13.3 | 2130 | 2851 | 2866 | 0 | 0.59 | 0.00 | 1.6 | PASS |
| 5 | A | SYNC | heavy | 16 | 16.3 | 11.2 | 1262 | 3080 | 3277 | 0 | 7.77 | 0.07 | 1.57 | PASS |
| 6 | D | SYNC | heavy | 16 | 17.1 | 12.2 | 1212 | 1540 | 1584 | 0 | 0.39 | 1.93 | 1.59 | PASS |
| 7 | A | SYNC | heavy | 32 | 14.8 | 10.3 | 2589 | 6750 | 7089 | 0 | 18.53 | 0.04 | 1.56 | PASS |
| 8 | D | SYNC | heavy | 32 | 18.7 | 12.7 | 2228 | 4522 | 4681 | 0 | 4.51 | 5.82 | 1.6 | PASS |
| 9 | A | DISABLED | heavy | 16 | 18.0 | 12.7 | 1081 | 2523 | 2784 | 0 | 8.17 | 0.00 | 1.58 | PASS |
| 10 | D | DISABLED | heavy | 16 | 19.1 | 13.3 | 1049 | 1458 | 1473 | 0 | 0.46 | 0.00 | 1.59 | PASS |
| 11 | A | DISABLED | heavy | 32 | 16.5 | 12.1 | 2130 | 5833 | 7318 | 0 | 19.40 | 0.00 | 1.56 | PASS |
| 12 | D | DISABLED | heavy | 32 | 18.2 | 13.8 | 2130 | 2851 | 3037 | 0 | 0.86 | 0.00 | 1.6 | PASS |
| 13 | A | SYNC | heavy | 16 | 16.1 | 11.3 | 1327 | 2753 | 3179 | 0 | 7.38 | 0.07 | 1.57 | PASS |
| 14 | D | SYNC | heavy | 16 | 18.3 | 12.8 | 1114 | 1737 | 1802 | 0 | 0.44 | 1.73 | 1.59 | PASS |
| 15 | A | SYNC | heavy | 32 | 15.9 | 11.5 | 2359 | 7209 | 7472 | 0 | 18.43 | 0.07 | 1.55 | PASS |
| 16 | D | SYNC | heavy | 32 | 20.0 | 13.1 | 2064 | 4129 | 4353 | 0 | 4.34 | 5.18 | 1.6 | PASS |
| 17 | B | DISABLED | heavy | 32 | 16.9 | 12.1 | 2163 | 6816 | 7249 | 0 | 18.20 | 0.00 | 1.57 | PASS |
| 18 | C | DISABLED | heavy | 32 | 18.1 | 12.3 | 2048 | 8061 | 8912 | 0 | 14.80 | 0.00 | 1.6 | PASS |
| 19 | E | DISABLED | heavy | 32 | 21.8 | 15.7 | 1802 | 2425 | 2796 | 0 | 0.89 | 0.00 | 1.9 | PASS |
| 20 | A | DISABLED | full | 32 | 44.3 | 5.0 | 2916 | 6450 | 6450 | 0 | 7.82 | 0.00 | 1.59 | PASS |
| 21 | D | DISABLED | full | 32 | 44.1 | 4.3 | 2294 | 2796 | 2796 | 0 | 0.76 | 0.00 | 1.6 | PASS |
| 22 | A | SYNC | full | 32 | 37.6 | 4.2 | 1327 | 4877 | 4877 | 0 | 8.39 | 7.11 | 1.59 | PASS |
| 23 | D | SYNC | full | 32 | 38.3 | 3.9 | 1114 | 2795 | 2795 | 0 | 8.00 | 8.05 | 1.58 | PASS |
| 24 | B | DISABLED | heavy | 32 | 15.5 | 10.7 | 2523 | 8323 | 8562 | 0 | 18.15 | 0.00 | 1.56 | PASS |
| 25 | C | DISABLED | heavy | 32 | 16.8 | 11.2 | 2130 | 5767 | 6724 | 0 | 10.72 | 2.51 | 1.59 | PASS |
| 26 | E | DISABLED | heavy | 32 | 22.0 | 15.5 | 1851 | 2687 | 3001 | 0 | 0.94 | 0.00 | 1.9 | PASS |
| 27 | A | DISABLED | full | 32 | 45.3 | 4.8 | 3572 | 6872 | 6872 | 0 | 10.22 | 0.00 | 1.6 | PASS |
| 28 | D | DISABLED | full | 32 | 42.1 | 5.3 | 2228 | 2882 | 2882 | 0 | 0.63 | 0.00 | 1.59 | PASS |
| 29 | A | SYNC | full | 32 | 34.8 | 4.5 | 1409 | 3643 | 3643 | 0 | 8.84 | 7.32 | 1.58 | PASS |
| 30 | D | SYNC | full | 32 | 35.9 | 4.2 | 1130 | 3336 | 3336 | 0 | 8.33 | 7.81 | 1.58 | PASS |
