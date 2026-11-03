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

## Admin (bearer `SHADOW_ADMIN_TOKEN`)

- `GET /shadow/metrics`: counters (mirrored, matched, mismatched, errors, dropped, retried, skipped), queue
  depth and the largest replay lag in milliseconds.
- `GET /shadow/diffs`: the last 500 differences and replay errors.
- `POST /shadow/pause`, `POST /shadow/resume`: the kill switch. Paused, it is a plain proxy.

Numbers are compared as decimals (`"1500.0000"`, `1500` and `"1500"` are equal). Bodies that are not JSON are
compared as text.

## Not here yet

Reading the cutover (hold writes, drain the queue, switch primary) and the daily reconciler that compares
balances account by account. Both are the next pieces.

## Run and test

```
go run ./tools/bankman-shadow
go test -race ./tools/bankman-shadow/
```
