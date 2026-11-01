# Bankman compatibility API

Serves the money-moving part of Bankman's HTTP contract at `/bankman/api/v1`, so the Cray middleware and snowman
can move to LedgerTrack by changing one base URL. It has no ledger logic: each request is translated and
dispatched in process through the v2 router to the native account endpoints, so authentication, account status,
limits, reserves and fees apply exactly as for a native call. Authentication is the same bearer token as `/v2`.

Enabled whenever the CBA module is on. Nothing to configure; replicating to prod is deploying the same build
and running the importer (`tools/bankman-migration`, `docs/import`) so the legacy ids exist.

## Endpoints

| Bankman call | Maps to |
|---|---|
| `POST /transactions/deposits/{account_id}` | `POST /v2/ledgertrack/accounts/{id}/credit` |
| `POST /transactions/withdrawals/{account_id}` | `.../debit` |
| `POST /transactions/liens/{account_id}` | `.../lien` |
| `PUT /transactions/liens/{lien_id}` | `.../lien/release`, mode `release_only` |
| `POST /transactions/liens/{lien_id}/release-and-withdraw` | `.../lien/release`, mode `release_and_debit` |
| `POST /transactions/reversal` and `POST /transactions/{id}/reversals` | `POST /v2/ledgertrack/transactions/{id}/revert` |

Not in this version (next PRs): reads (accounts by `external_id`/`client_id`, account, transaction, history,
products, client), `internal/wallet-operations`, R2 settlement and wallet-role endpoints.

## Behaviour callers should know

- **Ids.** `{account_id}` is a LedgerTrack account id or the Bankman account id the importer stored
  (`legacy_source_id`). `external_id` is the Bankman client id (`legacy_external_id`). An account that does not
  belong to the client in `external_id` answers 404.
- **Amounts** are major units. Values with more decimals than the currency allows are rounded half-even
  (Bankman's behaviour), not rejected. The layer always sends LedgerTrack a decimal string, because a bare
  integer there means minor units.
- **Idempotency** is `transaction_reference`, required. A repeat answers 200 with `idempotent: true` and the same
  `transaction_id`.
- **`transaction_id`** is a UUIDv5 of the reference, stored as transaction metadata `bankman_transaction_id`.
  Imported liens are found by their Bankman id (`legacy_source_id`).
- **Lien release** is synchronous here and answers 202 with `status: completed` (Bankman queues it; the
  middleware only checks for 2xx). A lien releases once, whichever way: the release reference is derived from
  the lien, so a second release with any caller reference answers `idempotent: true`. The amount is the lien's.
- **Errors.** Unknown client, account, lien or transaction is 404. Business rule failures (insufficient funds,
  frozen account, limits) are 422 with the message. Server faults are 500 without detail.
- **Reversal** uses the native revert. It does not undo daily usage counters.

## Assumptions to confirm against the PHP callers

- Envelope `{"status":"success"|"error","message","data"}` (the status words are assumed).
- Withdrawal `account_balance` keys: `previous_available_balance`, `current_available_balance`,
  `previous_ledger_balance`, `current_ledger_balance`. Ledger balance equals available here (lien not added).
- Bankman's release webhook to sendman is not replicated.
- Transaction ids are UUID strings; confirm the column types in snowman.
