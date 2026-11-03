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
| `POST /transactions/{id}/reversals` | `POST /v2/ledgertrack/transactions/{id}/revert` |
| `POST /transactions/reversal` | not served: 404, because Bankman has no such route and callers get a 404 there today |

| `GET /accounts?external_id=` or `?client_id=` | account list for the client, with balances |
| `GET /accounts/{id}` | one account with balances |

| `GET /transactions/{id}` | one transaction in Bankman's history row shape |
| `GET /transactions/{id}/status` | always `completed`, no job or webhook |
| `GET /transactions/accounts/{account_id}/history` | account history, page based |
| `GET /transactions/accounts/all-history` | wallet transactions of every account, newest first, page based |
| `GET /products` | every product; `id` is the Bankman product id for imported products |
| `GET /reports/aggregated-merchant-balances` | per-currency wallet totals over active accounts |
| `GET /health` (also `/bankman/health`) | public, `{status:"ok", service, timestamp}` |

Notes on the newer endpoints:

- `GET /products`: an imported product carries its Bankman id in a `[legacy_source_id=...]` tag in the
  description (written by the importer). The tag is stripped from `description` and used as `id`.
  `currency_name` is the currency code, `charges` is empty, and the loan-only fields are null, because
  LedgerTrack products have no equivalent. Check what the callers read before relying on those.
- `GET /reports/aggregated-merchant-balances`: totals come from the ledger (`users::wallets:{CCY}:available` and
  `:lien`). Bankman also returns a per-product breakdown; LedgerTrack cannot sum a ledger address pattern by
  product, so `products` is an empty list for now.
- `all-history` selects wallet transactions with the `users::wallets::available` address pattern, so fees and
  settlement movements are not listed. Same filter support and page limit as the per-account history.
- `/health` is public. If the callers' base URL ends in `/api/v1`, the gateway must map their `/health` to it.

## Client and account creation

| Bankman call | Behaviour |
|---|---|
| `POST /clients` | creates an `active` client (Bankman has no client status; compliance is handled elsewhere). `individual` maps to an individual client, `organization` to a corporate client owned by an existing individual (`owner_client_id`, the Bankman client id). `email`, `phone_number`, registration number, tax id and `external_id` must be unique (422). `external_id` is generated when absent. |
| `POST /accounts` | creates an active wallet. `product_id` is the Bankman product id (or a native id) and the product must be active. A caller-allocated `id` becomes the account id. A matching wallet (same client, product, `account_type` and `country_code`, not closed) is returned with 200 and "Wallet account retrieved" instead of creating a second one, as in Bankman. R2 sub-accounts never count as a match. `status: inactive` creates the wallet `pending`. |

- Wallet ids: `client-{number}-{product}` for a main wallet without a country, otherwise
  `client-{number}-{product}-{type}[-{cc}]`. If that is taken the first 8 characters of the account id are added.
- The shadow gateway sends `X-Bankman-Resource-Id` with the id Bankman gave a client or an account. It is stored
  as `legacy_source_id`, so the id callers see is the same before and after cutover. Nothing else should send it.
  It is not authenticated separately yet (see the open items in the cutover plan).
- Uniqueness checks on email, phone, registration number, tax id and external id scan the clients table. They
  are unindexed, which is fine for creation volumes but should not be used in a hot path.
- Differences from Bankman: a new wallet answers `0.0000` for both balances (Bankman answers a bare `0.00`), and
  `inactive` is stored as `pending`.

Not in this version (next PRs): `internal/wallet-operations`, R2 settlement.

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

- Envelope `{"status":"success"|"error","message","data"}` is confirmed against Bankman's `ResponseUtils`.
- Withdrawal `account_balance` keys: `previous_available_balance`, `current_available_balance`,
  `previous_ledger_balance`, `current_ledger_balance`. Ledger balance equals available here (lien not added).
- Account reads: `id` is the Bankman id for imported accounts (so callers keep their stored ids) and the
  LedgerTrack id for new ones. `available_balance`, `actual_balance` (available plus held) and `locked_amount`
  are strings with four decimals, like Bankman's `decimal:4` casts. `account_name`, `account_type`,
  `country_code` come from the account columns (null until set). `client_id` is the Bankman client id.
  R2 sub-accounts are left out of the list unless `allwallets=true`, as in Bankman. An unfiltered list
  answers 422 instead of dumping every account.
- Transaction rows follow Bankman's `transformTransaction()`. The type comes from the LedgerTrack operation
  (credit/opening deposit is `deposit`, debit `withdrawal`, lien `lien`, release `lien_release`, a revert made
  through this layer `reversal`, anything else `transfer`). `id` is the Bankman-style UUID for transactions made
  through this layer, the Bankman id for imported liens and opening balances, otherwise the ledger id.
  `balances.locked` and `balances.ledger` are only filled when the transaction touched the lien address;
  otherwise they are null, because LedgerTrack records post-commit volumes only for touched accounts.
- History is cursor based natively, so page N is reached by following N-1 cursors (pages above 50 are refused),
  and `total` and `last_page` are null. `has_more_pages` is accurate. Supported filters: `page`, `per_page`
  (max 100), `start_date`, `end_date`, `status` (only `completed` matches), `currency`. `transaction_type`,
  `search`, `external_id`, `source_account_id` and `destination_account_id` answer 422 rather than being
  ignored, since ignoring them would return rows the caller excluded.
- `account_name` and the client `email` in the nested account objects are null.
- Bankman's release webhook to sendman is not replicated.
- Transaction ids are UUID strings; confirm the column types in snowman.

## Wallet roles (`/internal/accounts`)

These are the endpoints Ramp Core uses to pick the wallet a movement goes to. They return Bankman's shapes
and status codes. Authentication is the same bearer token as the rest of the layer; the audit actor is always
`private_ingress`, never caller supplied.

| Method and path | Answers |
| --- | --- |
| `GET /internal/accounts/resolved-main-wallet?external_id&currency` | `{status, data:{id, currency, wallet_role}}` (no message). 409 `wallet_selection_ambiguous_or_missing` |
| `GET /internal/accounts/resolved-r2-subaccount-wallet?external_id&currency&r2_relationship_public_id` | same shape, exact owner-scoped match |
| `GET /internal/accounts/wallet-classifications?external_id` | every wallet of the client with `account_type`, `status`, `wallet_role` (`unknown` when unclassified), `r2_relationship_public_id` |
| `GET /internal/accounts/{account}/wallet-classification?external_id` | one wallet; another client's account is 404 |
| `PUT /internal/accounts/{account}/wallet-classification` | classify; a replay answers the stored classification |
| `POST /internal/accounts/r2-subaccounts` | 201 when created, 200 when the same request is replayed; keeps the caller's `account_id` |

Rules, all enforced in one transaction with the account rows locked and an audit row written:

- A wallet's role is immutable once set. `merchant_main` to `r2_subaccount` needs `correction: true`; nothing
  moves a wallet out of `r2_subaccount`.
- A relationship binds one wallet per currency (unique index). Concurrent creates create it once.
- The main wallet resolver only considers active, same-currency, `main` wallets that are not bound to an R2
  relationship. Exactly one `merchant_main` wins; otherwise exactly one unclassified wallet is used; anything
  else is a 409. It never takes "the first wallet".
- The new wallet id is `client-{client_number}-{product_code}-r2-{relationship}`, so it never collides with the
  client's main wallet.
- Unknown `external_id` or `product_id` is 422 (Bankman validates them with `exists`), a non-owned account 404.

Columns added to `_system.accounts`: `account_name`, `account_type`, `country_code`, `wallet_role`,
`r2_relationship_public_id`, plus `_system.account_wallet_role_audits`. The importer does not carry roles yet
(next PR), so imported accounts read as `unknown` until it does.
