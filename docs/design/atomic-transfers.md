# Atomic channel transfers (wallet debit and lien release with pay)

Status: implemented on branch `phase1/atomic-transfers`. Owner: platform. Last updated 2026-10-29.

## Problem

`POST /wallets/{id}/debit` and `POST /wallets/{id}/lien/release` (mode `PAY`) with a `channelID` write to up to three Formance ledgers and then to a Postgres table:

1. user ledger (`ledgertrack`): user available (or lien) to `system:control:{CCY}` (or `world`)
2. `channels-{CCY}`: channel account to `world`
3. `revenue-{CCY}`: one or two postings (`{ref}-user-fee`, `{ref}-processing-fee`)
4. `_system.channel_fee_records` row

Each ledger commits in its own database transaction, so there is no atomicity. A failure after leg 1 returned an error to the client while the user stayed debited and nothing would ever finish legs 2 to 4. CBA_PRD.md (Fee Handling) already says this flow must be treated as reconcilable, not ACID.

## Chosen approach: durable transfer record plus idempotent legs (saga with forward recovery)

Every leg already uses the same business reference, and a ledger rejects a duplicate reference, so each leg is individually idempotent. What was missing is durable intent and something that finishes the work. We add `_system.transfers`, written before the first ledger write. The handler drives the legs inline; a cron runner finishes anything left behind.

We only ever roll forward. We never reverse the principal automatically: reversing a customer debit is a business decision and a second money movement that can itself fail. A transfer that cannot be completed after N attempts goes to `failed_needs_review` for a human.

### State machine

```
            insufficient funds / definitive ledger rejection on leg 1
   created ------------------------------------------------------------> failed        (nothing posted, terminal)
      | leg 1 ok (user ledger)
      v
  user_posted --(no channel leg needed)--------------------------------> completed
      | leg 2 ok (channels-CCY)
      v
 channel_posted --(no fees)-------------------------------------------> completed
      | leg 3 ok (revenue-CCY, 1-2 postings)
      v
 revenue_posted
      | leg 4 ok (fee record, deduplicated by reference)
      v
  completed                                  any non-terminal state --(attempts >= max)--> failed_needs_review
```

`failed` is not in the original list. It is needed so that a rejected request (402 insufficient funds) is not retried by the recovery runner and later debits the user. A client may retry a `failed` reference (the transfer is re-armed with the new parameters), which matches today's behaviour where a rejected request leaves no trace.

Recovery of a stale `created` row re-drives leg 1 (idempotent by reference). We chose completing the recorded intent over guessing "it never posted": the guess cannot be made safely (leg 1 may commit after the check), while re-driving can never duplicate.

### Concurrency

Ownership is a lease on the row (`lease_owner`, `lease_until`), taken with a single conditional `UPDATE ... RETURNING`. The recovery runner picks rows with `SELECT ... FOR UPDATE SKIP LOCKED` and sets the lease in the same statement, so two workers never get the same row. State changes are `UPDATE ... WHERE state = <from> AND lease_owner = <me>`, so a driver that lost its lease cannot move state. Ledger idempotency is the second line of defence: even a stalled driver that wakes up after its lease expired can only replay legs that already exist.

### HTTP contract

Success is unchanged: `201` with `txid, timestamp, postings, metadata, balance_before, balance_after[, warning]`.

| Situation | Status | errorCode |
|---|---|---|
| Rejected before anything posted (insufficient funds, validation, reference used by a different request) | as today (402 debit, 400, 409) | as today |
| Principal posted, later legs pending (inline failure) | 500 | `TRANSFER_PENDING_COMPLETION` |
| Retry arrives while another driver holds the lease | 409 | `TRANSFER_IN_PROGRESS` |
| Retry of a transfer in `failed_needs_review` | 500 | `TRANSFER_NEEDS_REVIEW` |
| Retry of a `completed` transfer, same parameters | 201 + `Idempotency-Hit: true`, stored body | n/a |
| Same reference, different parameters | 409 | `CONFLICT` |

The pending body message states that the principal debit posted, names the transfer reference and txid, and says that retrying with the same reference or waiting for recovery completes it. It is deliberately not a 2xx so clients cannot treat it as success. A fee-record failure alone (all money legs done) stays a 201 with the existing `warning`, and the runner writes the record later.

## Failure modes

| Event | State left behind | Outcome |
|---|---|---|
| Crash before the transfer row commits | no row, nothing posted | client retries as new |
| Crash after row, before leg 1 commits | `created` | runner re-drives leg 1; posts once |
| Crash after leg 1 commit, before state update | `created` (ledger has tx) | re-drive: duplicate reference, existing tx read and verified, advance |
| Crash after leg 2 / leg 3 / between the two revenue postings | `user_posted` / `channel_posted` / partial | re-drive: done legs are duplicate-reference no-ops, rest posted |
| Crash after leg 4 write, before `completed` | `revenue_posted` | fee record deduplicated by reference, then completed |
| Leg returns a transient error | unchanged, `last_error` set | 500 pending now; runner retries with backoff |
| Duplicate request while in flight | unchanged | 409 `TRANSFER_IN_PROGRESS`, no ledger call |
| Retry storm on a pending transfer | unchanged | one driver at a time (lease); client-driven re-drives are rate limited by a cooldown and do not consume the attempt budget |
| Recovery runner and a live retry race | unchanged | one wins the lease; the other gets in-progress or finds the row completed |
| Existing reference is found with different postings | `failed_needs_review` (or `failed` if leg 1) | never overwritten, human review |
| Max attempts exhausted | `failed_needs_review` | log line and `transfers.needs_review` counter |
| Postgres `_system` down at intake | nothing posted | 500, nothing to reconcile |

## Alternative evaluated: one ledger transaction per currency

Put user wallets, channel accounts and revenue accounts in one ledger per currency and post all legs as postings of a single ledger transaction. Atomicity is then free and the recovery machinery goes away.

- Migration cost (high): `ledgertrack` holds all currencies today; splitting it per currency changes wallet addressing, statements, CBA runner config (`worker-cba-ledger-name`), the perf harness and every reader. Existing `channels-{CCY}` and `revenue-{CCY}` balances and history would have to be re-based into the new ledgers, with reference namespaces merged (today the same reference appears in three ledgers). A cheaper variant (channel and revenue accounts added to `ledgertrack` itself) avoids splitting but still moves history and changes reporting queries.
- Hot accounts (medium): every transfer would touch `system:control:{CCY}`, the revenue accounts and the channel account in one transaction, serialising on those rows. This repo already shards `system:control` (`CBA_SYSTEM_CONTROL_SHARDS`); revenue and channel accounts would need the same.
- Reporting (positive): one trial balance per currency, and channel to revenue movements become real double entry instead of two ledgers each balancing against `world`.
- Risk: a large one-shot financial data migration is exactly what we want to avoid before the external VAPT.

Recommendation: ship the durable transfer record now (additive, no data migration, reversible by turning off the runner), and treat the single-ledger model as a later re-platforming decision once the volume and reporting needs justify it. The transfer table stays useful then as the intake and audit record.

## Known residual windows

- Between leg 1 and the last leg the books are inconsistent by design. The window is bounded by the grace period plus backoff and is visible in `_system.transfers`.
- `failed_needs_review` rows require a person; nothing reverses automatically.
- The fee record is deduplicated with an advisory lock plus existence check rather than a unique index, because existing data may already contain duplicate references.
