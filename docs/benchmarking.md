# Benchmarking LedgerTrack

Goal: show that LedgerTrack sustains at least 3x Bankman's breaking point (about 400 TPS), so
**>= 1,200 TPS for 10 minutes** with acceptable latency and no data errors, before the ledger
migration.

`cmd/ledgertrack-bench` drives the wallet API (`/v2/{ledger}/wallets/...`) and checks the books
afterwards. The older generators (`cmd/wallet-load-test`, `cmd/wallet-concurrent-debit-test`,
`cmd/wallet-idempotency-test`) are closed loop with fixed workers and report no percentiles. Use
this one for decisions. `test/perf/accounts_rps` drives the CBA account routes
(`/v2/{ledger}/accounts/{id}/credit|debit|lien`), which is a different code path (see the next
section).

## Read this first: sharding and the wallet routes

`CBA_SYSTEM_CONTROL_SHARDS` only affects the CBA account routes. It does **not** apply to the wallet
routes this harness drives:

| Route | Control account used | Where |
| --- | --- | --- |
| `POST /wallets/{id}/credit` | `system:control:{CCY}` (always) | `internal/api/v2/controllers_wallets.go:232` |
| `POST /wallets/{id}/debit` (and channel debit) | `system:control:{CCY}` (always) | `internal/api/v2/controllers_wallets.go:388` |
| `POST /accounts/{id}/credit` | `systemControlAddress(...)`, sharded | `internal/api/v2/controllers_cba_accounts.go:303` |
| `POST /accounts/{id}/debit` | `systemControlAddress(...)`, sharded | `internal/api/v2/controllers_cba_accounts.go:402` |
| opening deposit, interest scheduler | `system:control:{CCY}` (always) | `controllers_cba_accounts.go:936`, `internal/cba/scheduler/engine.go:67,80` |

Lien and lien release do not touch the control account. So with the wallet routes, every credit and
debit in a currency still serialises on one account, whatever the shard setting. Consequences for
the test plan:

* Running the shard matrix (1, 8, 16, 32) with wallet-route traffic should show **no change**. Run
  it anyway as the control experiment: a flat result confirms the hot spot is the single control
  account and that the env var alone cannot fix wallet traffic.
* Real shard effects can only be measured on the CBA account routes (`test/perf/accounts_rps`,
  documented in `test/perf/README.md`), or after the wallet routes are changed to call
  `systemControlAddress`. That change is not part of this harness.
* The finance reports (trial balance, balance sheet, cash flow) now sum `system:control:{CCY}` and
  all `system:control:{CCY}:{n}` shards. Before that fix they missed shard balances.

## Build and run

```bash
go build -o ledgertrack-bench ./cmd/ledgertrack-bench
./ledgertrack-bench -h
```

Smoke test (30 seconds, 200 TPS):

```bash
./ledgertrack-bench -url http://127.0.0.1:3068/v2 -ledger bench \
  -rate 200 -warmup 5s -duration 30s -wallets 500 -currencies USD,NGN
```

Gate run (the decision run):

```bash
./ledgertrack-bench -url https://ledgertrack.internal/v2 -auth-token "$TOKEN" -ledger bench-all \
  -rate 1260 -warmup 60s -duration 10m -concurrency 512 \
  -wallets 10000 -currencies USD,NGN,GHS,KES -dist zipf -zipf-s 1.2 \
  -mix credit=35,debit=35,lien=10,lien_release=8,channel_debit=12 \
  -shards 1 -label "shards=1 zipf mixed all-ccy-one-ledger" \
  -min-tps 1200 -min-window-tps 1140 -max-p99 250ms -max-error-rate 0.001 -max-insufficient-rate 0.01 \
  -json-out results/s1-zipf.json -csv-out results/s1-zipf.csv
```

Prerequisites on the host that runs the harness and the helper scripts around it: the Go toolchain
to build it, plus `jq` and `bc` (`apt-get update && apt-get install -y jq bc`) for the shell
snippets that read the JSON report and compute rates. The harness binary itself needs neither.

Run the harness from a different host than the server, in the same network zone. It is light, but
on a shared small box it competes with the server and distorts the numbers.

### What the flags mean

* `-rate N` is open loop: arrivals are scheduled at N per second regardless of how fast the server
  answers. Latency is measured from the **intended** start time, so a stall shows up as latency
  instead of silently lowering the load (no coordinated omission). `-rate 0` is closed loop: each
  of `-concurrency` workers sends the next request when the previous one returns. Use it to find
  the ceiling, not to validate an SLO.
* `-concurrency` caps in-flight requests. Size it to at least rate x p99 (1,260 x 0.25s is about
  315, default 256 is too low for the gate run, use 512). If arrivals queue behind the cap, the wait
  counts as latency. Arrivals beyond `-max-backlog` are dropped and counted as errors.
* `-warmup` sends traffic but excludes it from every number.
* `-setup-concurrency` is the parallelism of wallet creation, funding and baseline reads (default
  32). **Use `-setup-concurrency 8` against a remote or small server** (Railway included). With 32
  parallel setup calls on a small Postgres the setup itself can be shed with 503 or time out,
  and a half funded wallet set makes the run meaningless. `-check-concurrency` (default 32) is the
  same knob for the read back after the run.
* `-max-uncertain-frac` (default 0.01) is the share of operations with an unknown outcome the
  consistency check tolerates, see below.
* `-dist zipf` makes a few wallets hot (rank 0 gets roughly 20% of traffic at `-zipf-s 1.2`);
  `-dist uniform` spreads evenly. `-wallets` are spread round-robin over `-currencies`, all in the
  one `-ledger`.
* `-mix` weights: `credit`, `debit`, `lien`, `lien_release` (release only), `lien_pay` (PAY mode
  with channel and fees), `channel_debit` (wallet debit plus channel and revenue ledger legs).
  Releases consume a lien the harness created earlier; when none is on hold the draw runs as a
  lien and is reported as `fallback_to_lien`.
* Every request has a unique reference (`runid-op-seq`). `-idempotency-key` also sends it as an
  `Idempotency-Key` header, which adds server work, so leave it off unless you want that cost in.
* `-auth-token` sends `Authorization: Bearer ...` (or set `LEDGERTRACK_BENCH_TOKEN`).

### Setup and rerun behaviour

Setup is idempotent. It creates the ledger and `revenue-{CCY}` ledgers if missing, creates
`-channels` channels per currency with a flat fee config, calls `POST /wallets`, and tops each
wallet up to `-fund`. A never-funded wallet is funded with a fixed reference, so a repeated or
concurrent setup returns 409 instead of funding twice. Reuse `-prefix` (default `bench`) to reuse
wallets across runs; the consistency check works from balances read after setup, so reruns are
valid. Do not run two harness instances against the **same** ledger and prefix at once, and do not
let other traffic touch the benchmark accounts during a run.

### Output and exit codes

The table goes to stdout. `-json-out` and `-csv-out` take a path or `-` for stdout (the table then
goes to stderr). JSON has totals, per-op stats, a per-second timeline, gate results and the
consistency check. CSV has one row per op plus `ALL`.

Response classes: `2xx`, `402_insufficient` (also a 400 carrying `INSUFFICIENT_FUND`, which is
what lien returns), `409_conflict`, `4xx_other`, `5xx`, `timeout`, `network`, `dropped`, `shed`. An
insufficient-funds answer is an expected outcome for debit, lien and channel debit and is not an
error; for credit and releases it is. 409s are always errors, because references are unique.

`shed` is a 503 whose body carries the code `OVERLOADED`: the server's write limiter refused the
request before reading or writing anything (it also sends `Retry-After`). It is load shedding
working as designed, so it is **not** an error, does not count against `-max-error-rate`, and is
**not** an uncertain operation (nothing was committed). It is reported on its own: `shed` and
`shed_rate` in the JSON, a `shed` column in the table and CSV. A high shed rate means the server
was past its admission limit; the achieved TPS is what the server accepted, so read the two
together. Any other 503, or a 5xx without `OVERLOADED`, stays a `5xx` error with an unknown
outcome.

When no gate flag is given the gates are **not evaluated**: the table says so,
`gates_evaluated` is `false` and `gates_passed` is `null` (not `true`), and the exit code is not
affected. A script that needs a verdict must pass at least one gate flag and require
`gates_passed == true`.

| Exit code | Meaning |
| --- | --- |
| 0 | ran, no gate failed (or none was evaluated), consistency passed |
| 1 | usage error or setup failure |
| 2 | an SLO gate failed |
| 3 | the consistency check failed, including too many uncertain operations (takes priority over 2) |

Definitions to keep results comparable:

* Achieved TPS is the number of 2xx responses per second over the measured window. Channel debit
  and lien pay are one API call but 2 to 4 ledger transactions, so the report also gives an
  estimated ledger tx/s. Quote both, and say which one you compare with Bankman.
* Latency percentiles cover every attempt in the window (including errors and timeouts), measured
  from the intended start. `latency_ok_ms` is 2xx only. `service_time_ms` is time on the wire
  without queueing; a large gap between the two means the harness or the server's accept queue is
  saturated.
* `min_10s_window_tps` is the lowest 10 second average by completion time. It is the sustained
  check: a 10 minute average can hide a minute of stalls.
* Histogram resolution is about 1.6% of the value.

## Consistency check

After the run the harness reads balances back and fails (exit 3) if any of these break:

1. **Per wallet**: available and lien equal baseline + what the harness expects from acknowledged
   operations.
2. **No negative balances** on any wallet available or lien account.
3. **System control**: delta of `system:control:{CCY}` plus all shards (read with the prefix
   pattern `system:control:{CCY}:`) equals expected.
4. **Channels and revenue**: each channel balance, `revenue:accumulated` and
   `revenue:channel_processing_cost` deltas equal expected (both read by prefix, so the base account
   and every `:N` shard written with `--revenue-shards` are summed) (fees are taken from the response
   metadata, so fee config is honoured).
5. **Conservation**: control + sum(available) + sum(lien) + `world` deltas equal exactly 0 per
   currency, independent of the harness bookkeeping.
6. **No duplicate handling**: zero 409s, since every reference is unique.

Operations with an unknown outcome (timeout, 5xx, network error, a channel leg failing after the
wallet leg) are added to a tolerance for the affected accounts and listed as `uncertain_ops`. The
tolerance only stops the check from reporting false corruption for a few ambiguous operations. It
is not allowed to grow without limit: the check **fails** (exit 3, failure text starting with
`INCONCLUSIVE`) when `uncertain_ops` is more than `-max-uncertain-frac` (default `0.01`, 1%) of the
operations sent. The base is every operation that got an answer or an error, warmup included,
without the `shed` ones. With enough unknown outcomes the tolerance is wider than the amounts the
check is looking for, so a pass would prove nothing. Do not raise the flag to make a run pass; fix
the cause (5xx, timeouts, capacity) and rerun. `-max-uncertain-frac -1` disables the cap and is
only for debugging. If you see `uncertain_ops > 0` with a passing check, note it in the results.

## SLO gates

| Gate | Flag | Value | Why |
| --- | --- | --- | --- |
| Throughput | `-min-tps` | 1200 | the 3x target; drive with `-rate 1260` so the 2xx count can reach 1200 |
| Sustained | `-min-window-tps` | 1140 | every 10s window at least 95% of target |
| Duration | `-duration` | 10m (after 60s warmup) | sustained, not a burst |
| p99 latency | `-max-p99` | 250ms overall | proposal: at 1,200 TPS a quarter second keeps wallet calls usable for card and transfer flows; revisit once the first real numbers exist |
| Error rate | `-max-error-rate` | 0.001 (0.1%) | includes 5xx, timeouts, 409, unexpected 4xx and dropped arrivals |
| Rejections | `-max-insufficient-rate` | 0.01 | keeps a run from passing on cheap 402s |
| Consistency | `-check` (default on) | pass | zero tolerance for lost or duplicated money |

Also read per-op rows. Proposed per-op p99 ceilings to check by eye: single-leg ops 250ms,
`channel_debit` and `lien_pay` 500ms (they cross three ledgers). The harness gates only the overall
p99 for now.

## Scenarios for the decision matrix

Keep everything else equal between runs (hardware, Postgres settings, `-wallets 10000`, 10 minute
duration, same mix). Restart the server between shard settings so the env var takes effect, and
record it with `-shards` and `-label`. Start every row with a 2 minute closed-loop probe
(`-rate 0 -duration 2m`) to see the ceiling, then the open-loop gate run.

Server settings are not part of the harness but belong in the record: `CBA_SYSTEM_CONTROL_SHARDS`,
Postgres `max_connections` and `--postgres-max-open-conns`, `LEDGER_HTTP_LOG` (keep off), server
and Postgres instance sizes, version and commit.

| # | Scenario | Server | Harness |
| --- | --- | --- | --- |
| 1 | Baseline, bankman parity | shards 1 | `-rate 400 -dist uniform` |
| 2 | Hot keys, shards 1 | shards 1 | `-rate 1260 -dist zipf` |
| 3 | Hot keys, shards 8 | `CBA_SYSTEM_CONTROL_SHARDS=8` | same as 2, `-shards 8` |
| 4 | Hot keys, shards 16 | 16 | same, `-shards 16` |
| 5 | Hot keys, shards 32 | 32 | same, `-shards 32` |
| 6 | Uniform keys at 1, 8, 16, 32 | each | same as 2 with `-dist uniform` |
| 7 | Mixed workload (default mix) | best of 1/8/16/32 | gate command above |
| 8 | Credit/debit only | best | `-mix credit=50,debit=50` isolates the control account |
| 9 | Lien heavy | best | `-mix lien=40,lien_release=40,credit=10,debit=10` (no control account) |
| 10 | Channel heavy | best | `-mix channel_debit=60,credit=25,debit=15` stresses multi-ledger writes |
| 11 | Everything in one ledger | best | `-ledger bench-all -currencies USD,NGN,GHS,KES` |
| 12 | One ledger per currency | best | four harness instances, one per currency (below) |
| 13 | Ramp / headroom | best | `-rate 1800` for 5 minutes, then closed loop with `-concurrency 64,128,256,512,1024` |

Per the section above, rows 3 to 6 are expected to match row 2 for wallet traffic. Add the same
rows for the CBA account routes with `test/perf/accounts_rps` where sharding does apply.

**All currencies in one ledger vs per currency (rows 11 and 12).** In row 11 the harness puts
every wallet and every `system:control:{CCY}` account in `-ledger`. The channel and revenue legs
always go to the existing per-currency ledgers `channels-{CCY}` and `revenue-{CCY}`. For row 12 run
one instance per currency in parallel, each with its own ledger, prefix and a quarter of the rate:

```bash
for c in USD NGN GHS KES; do
  ./ledgertrack-bench -url "$URL" -auth-token "$TOKEN" -ledger "bench-$c" -prefix "bench-$c" \
    -currencies "$c" -wallets 2500 -rate 315 -warmup 60s -duration 10m -concurrency 160 \
    -dist zipf -shards 1 -label "per-ccy-ledger $c" -json-out "results/r12-$c.json" &
done; wait
```

Sum `total.ok_tps` over the four JSON files and take the worst p99 as the row's numbers. If row 12
beats row 11 at equal total load, the per-ledger serialisation (log ordering inside one ledger) is
a bottleneck in addition to the control account. That is a hypothesis to test, not a finding.

## Numbers to record per run

From the JSON (paths in brackets), one line per scenario in the results sheet:

* label, commit, shards, dist, mix, ledger layout, wallets, target rate, concurrency
* achieved TPS `[total.ok_tps]`, offered TPS `[offered_tps]`, estimated ledger tx/s
  `[total.ledger_tx_per_sec_est]`, slowest 10s window `[min_10s_window_tps]`
* p50, p90, p95, p99, max overall `[total.latency_ms]` and for `channel_debit` and `debit`
  `[ops[].latency_ms]`, plus service time p99 `[service_time_ms.p99]`
* error rate `[total.error_rate]` with class counts `[total.classes]`, dropped arrivals
  `[dropped_arrivals]`, insufficient-funds rate `[insufficient_funds_rate]`
* consistency result `[consistency_check.passed]` and `uncertain_ops`
* gate results `[gates]` and exit code
* server side during the run: Postgres CPU, active connections, lock waits on `accounts` and
  `moves`, server CPU and memory, GC pauses. The harness cannot see these and the shard result is
  not interpretable without them.

Run each decision scenario at least twice and keep both; ignore a run whose first minute shows
connection or cache warmup effects outside the warmup window.

## Known limits of the harness

* It drives wallet routes only. It does not call the CBA account routes, so it cannot measure the
  effect of `CBA_SYSTEM_CONTROL_SHARDS` (see above).
* `world` is read as a single account per asset in the wallet ledger. Other traffic in that ledger
  breaks the conservation check; use a dedicated ledger.
* Wallet creation (`POST /wallets`) does not persist anything today, so the first credit creates
  the account.
* The harness reads every wallet asset as `{CCY}/2`. The server derives the asset from the currency
  registry precision (`currency.AssetFor`), which is 2 for USD, NGN, GHS and KES. Do not use a
  currency with another precision (USDT is 6, JPY is 0) with this harness: its balance reads would
  look at the wrong asset. Amounts and `-fund` are in minor units of a 2 decimal currency.
* It has been validated against an in-memory fake of the API only (unit tests), never against a
  live LedgerTrack with Postgres.
