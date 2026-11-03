# bankman-shadow

A small proxy that sits between Bankman's callers (opsman, middleman, gateman) and Bankman during the
LedgerTrack parallel run.

- Every call is forwarded to Bankman and Bankman's answer is returned untouched. The callers see no change.
- After Bankman has answered, a copy of the call is replayed against LedgerTrack's `/bankman/api/v1`, in order
  per account, and the two answers are compared.
- Differences are logged (`shadow_diff` JSON lines on stdout) and kept in a ring of the last 500, with counters.

The replay never blocks or fails the real call: it is queued, has a timeout, and a dead LedgerTrack only
fills the queue (calls are dropped and counted when a partition is full).

## Callers

Each service changes its Bankman base URL once, to this gateway. Both `/v1/...` (opsman) and `/api/v1/...`
(middleman, gateman) are understood; the part after `/v1` is what is replayed. Gateman's `/health` has no
version prefix and is forwarded as is.

## What is mirrored

All calls except `/health`. A Bankman 5xx is not replayed (its outcome is unknown; the daily reconciliation
covers it). For creations the gateway sends `X-Bankman-Resource-Id` with the id Bankman returned
(`data.id` for clients and accounts, `data.transaction_id` for deposits, withdrawals and liens), so ids are
the same in both systems and stay the same after cutover.

## Ordering

A call is assigned to a partition by account id (from the path). A lien release or release-and-withdraw
is assigned to the account its lien was placed on, learned from Bankman's reply to the placement. Within
a partition calls run one at a time in arrival order. A replay that fails while Bankman succeeded (404 or 5xx)
is retried with backoff, which covers a prerequisite still waiting on another partition.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `BANKMAN_URL` | required | Bankman base, `scheme://host[:port]` |
| `BANKMAN_HOST` | none | Host header to send to Bankman, if its ingress routes on it |
| `LEDGERTRACK_URL` | required | LedgerTrack base |
| `LEDGERTRACK_PREFIX` | `/bankman/api/v1` | compat API prefix |
| `LISTEN` | `:8080` | listen address |
| `SHADOW_WORKERS` | `32` | partitions replayed in parallel |
| `SHADOW_QUEUE_SIZE` | `10000` | per partition; a full queue drops and counts |
| `SHADOW_TIMEOUT_SECONDS` | `10` | per replayed call |
| `SHADOW_RETRIES` | `5` | retries for a replay that fails while Bankman succeeded |
| `SHADOW_READ_SAMPLE` | `1` | share of GETs replayed (0 to 1) |
| `SHADOW_IGNORE_FIELDS` | `created_at,updated_at,approved_at,timestamp,last_activity_at,account_number,message_id` | JSON keys left out of comparisons |
| `SHADOW_MAX_BODY` | `1048576` | bytes captured per request or reply; a bigger reply is passed through and not compared |
| `SHADOW_ADMIN_TOKEN` | none | bearer token for `/shadow/*`; without it those routes answer 404 |
| `SHADOW_DISABLED` | `false` | `true` starts it as a plain proxy |
| `CUTOVER_HOLD_SECONDS` | `30` | longest a write waits during a hold; after that it gets a 503 with `Retry-After: 1`, which opsman and middleman retry |
| `SHADOW_REVERSE_IGNORE_FIELDS` | `id,transaction_id` | extra keys left out of comparisons once Bankman is the copy, because it mints its own ids |

## Admin (bearer `SHADOW_ADMIN_TOKEN`)

- `GET /shadow/metrics`: counters (mirrored, matched, mismatched, errors, dropped, retried, skipped), queue
  depth and the largest replay lag in milliseconds.
- `GET /shadow/diffs`: the last 500 differences and replay errors.
- `POST /shadow/pause`, `POST /shadow/resume`: the kill switch. Paused, it is a plain proxy.

Numbers are compared as decimals (`"1500.0000"`, `1500` and `"1500"` are equal). Bodies that are not JSON are
compared as text.

## Cutover

Bankman is primary until you switch. The sequence, each step a call with the admin token:

1. `POST /shadow/cutover/hold`: new writes wait (up to `CUTOVER_HOLD_SECONDS`, then a retryable 503). Reads
   keep going to the primary.
2. `GET /shadow/cutover/status` until `drained` is true: no write in flight and every write replay finished.
3. Run `bankman-reconcile` with `GATEWAY_URL` and `SHADOW_ADMIN_TOKEN` set. It compares every account and
   posts its result to `POST /shadow/reconcile`. A run only counts if it started after the drain.
4. `POST /shadow/cutover/switch?to=ledgertrack`. It answers 409 with the reason unless the hold is drained and
   the reconciliation is fresh, covers accounts and has zero differences.
5. `POST /shadow/cutover/release`: held writes go to LedgerTrack, and Bankman now receives the copy of every
   write (reverse shadow), compared with the id keys above ignored.
6. Smoke tests. To go back, repeat the same sequence with `to=bankman`; the reconciliation is required again.

`GET /shadow/cutover/status?to=ledgertrack` also says whether a switch would be accepted and why not.

## Not here yet

Calls that bring a caller's own auth to LedgerTrack are assumed to be accepted by it, as in the shadow phase.

## Run and test

```
go run ./tools/bankman-shadow
go test -race ./tools/bankman-shadow/ ./tools/bankman-reconcile/
```

`tools/bankman-reconcile` reads Bankman's database (`BANKMAN_DSN`, use a read replica) and
`GET /accounts/{id}` on LedgerTrack (`LEDGERTRACK_URL`, `LEDGERTRACK_TOKEN`) and compares available, actual and
locked amounts and currency per account. It prints each difference as a JSON line, then a summary, and exits 1
if anything differs.
