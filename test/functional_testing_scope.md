You are writing a Python end-to-end functional test script for LedgerTrack (https\://github.com/noibilism/ledgertrack), a fork of Formance Ledger that adds a wallet wrapper, a CBA (Core Banking Application) layer, and a Channel Fees & Revenue Reporting layer on top of the programmable double-entry ledger core.

&#x20;

The script's job is to exercise the live HTTP API surface against a running LedgerTrack instance and verify that every layer works end to end. It is not a unit test, not a load test, and not a soak test. It is a single linear functional run that an operator can execute against a fresh deployment to confirm the system is healthy and correct.

Reference these docs before writing code:

- WalletPRD.md
- CBA\_PRD.md
- CHANNEL\_FEES\_PRD.md
- openapi/v2.yaml
- postman\_collection.json

# Deliverables

1. A single Python file: e2e\_functional\_test.py
2. A requirements.txt listing exact pinned versions of dependencies.
3. A README\_E2E.md documenting how to run the script, environment variables, and how to interpret output.

Place all three under /test/e2e-python/ in the repo.

# Dependencies

- requests (HTTP client)
- python-dateutil (date math)
- rich (pretty terminal output; optional but strongly preferred)
- decimal from the standard library (do NOT use float anywhere for money)

No test framework. Plain Python. The script is its own runner: it prints section banners, runs steps, asserts inline, prints pass/fail, and exits non-zero if anything fails.

# Configuration

The script reads environment variables with sensible defaults:

- LEDGERTRACK\_BASE\_URL (default: http\://localhost:3068)
- LEDGERTRACK\_LEDGER (default: ledgertrack)
- LEDGERTRACK\_ADMIN\_TOKEN (optional; used as bearer token if set)
- LEDGERTRACK\_FINANCE\_TOKEN (optional; used for reporting endpoints if set)
- LEDGERTRACK\_AUDIT\_TOKEN (optional; used for audit log endpoints if set)
- E2E\_RUN\_ID (default: auto-generated UUID, used as a suffix for all created resource names/references so reruns don't collide)
- E2E\_CLEANUP (default: "true"; when "false", skips the cleanup phase so an operator can inspect state in the database after the run)

Print the resolved config at the top of the run so an operator can verify what the script targeted.

# Output

Use rich.console.Console if available; otherwise fall back to plain print. Every section starts with a banner. Every step prints:

- a one-line description of what it's doing
- the HTTP method, path, and response status
- a green PASS or red FAIL marker
- on failure: the request body, response body, and the assertion that tripped, then exit non-zero

At the end, print a summary table: section, steps run, steps passed, total wall time. Exit 0 if all sections passed, 1 otherwise.

# Money handling

All money values in this script are Decimal. Convert API responses to Decimal at the boundary. Never use float. Compare Decimals with Decimal('0.00001') tolerance only where rounding is documented in the spec; everywhere else, exact equality.

Amounts in API calls are in minor units (e.g. cents). The script should have a small helper to convert between major (Decimal) and minor (int) units per currency.

# Test plan

The script runs the following sections in order. Each section's success is a precondition for the next.

## Section 0: Preflight

- GET base URL health (try /\_info or root; whatever the codebase exposes)
- POST /v2/{LEDGERTRACK\_LEDGER} to ensure the ledger exists (idempotent)
- GET /v2/ledgertrack/currencies and confirm at least USD, NGN, and GBP are configured. If not, fail with a clear message telling the operator to seed currencies first.

## Section 1: Wallet & Channel layer

Goal: prove the foundation works.

Steps:

1. Create a channel for USD called "test-channel-usd-{run\_id}".
2. Credit the channel with $1,000,000 of liquidity.
3. GET the channel and assert balance == $1,000,000.
4. Create a user wallet "wallet-user-A-{run\_id}" in USD.
5. Credit the wallet with $5,000 (reference: "wallet-credit-A-{run\_id}-1").
6. GET wallet history; assert the credit appears, amount and reference match.
7. Place a $1,200 lien on the wallet (reference: "lien-A-{run\_id}-1").
8. GET wallet balance/history; assert available = $3,800, lien = $1,200.
9. Release the lien as "pay" through the test channel, no fee (reference matches step 7); assert wallet available = $3,800, lien = $0, channel balance reduced by $1,200.
10. Place a $500 lien and release it as "cancel"; assert wallet available = $3,800 (no change), lien = $0.

Math check at end of section:

- Wallet history sum == current balance.
- Channel debit on the pay-release == lien amount.
- No phantom postings.

## Section 2: CBA layer

Goal: open a real customer account end to end.

Steps:

1. Create a product:
   - code: "SAV-USD-{run\_id}"
   - category: "savings"
   - currency: USD
   - rules: requires\_kyc\_level=1, min\_opening\_balance=0, allow\_debits=true, allow\_credits=true, single\_debit\_limit=$2,000
   - fee\_schedule: transaction\_fees with a flat $0.50 fee on debits
2. Activate the product.
3. Create an individual client with first\_name, last\_name, phone, etc.
4. Attempt to open an account for this client BEFORE KYC; assert the call fails with a 4xx and a clear "KYC level insufficient" type error.
5. Submit KYC at level 1 for the client.
6. Verify the KYC record.
7. Activate the client.
8. Open an account for this client and product with opening\_deposit=$1,000.
9. GET the account; assert:
   - status == active
   - wallet\_id is deterministic and matches expected pattern ({client\_number}-{product\_code} or as documented in CBA\_PRD.md)
   - wallet\_info.available\_balance == $1,000
10. Credit the account with $500 (no channel, no fee yet); assert balance == $1,500.
11. Debit the account with $200 (no channel) and assert:
    - account balance == $1,300 - $0.50 product fee = $1,299.50
    - a fee\_posting row equivalent appears in account history with the $0.50 fee, linked to the main debit by metadata
12. Attempt to debit the account with $5,000; assert 4xx (over limit, insufficient balance, or both).
13. Suspend the account; attempt a debit; assert 4xx.
14. Reactivate the account; debit $100; assert success.
15. Freeze the account; attempt a debit; assert 4xx. Attempt a credit; assert success (freeze blocks debits only).
16. Reactivate, run a small credit and debit, then call:
    - GET /accounts/{id}/balance
    - GET /accounts/{id}/history
    - GET /accounts/{id}/statement Assert that history and statement entries reconcile: starting balance
    * sum(credits) - sum(debits) - sum(fees) == current balance.

Math check at end of section:

- Sum of all wallet movements for the underlying wallet matches the account balance.
- Every fee posting has a linked main posting via metadata.transaction\_id.

## Section 3: Channel Fees & Revenue Reporting layer

Goal: prove fee resolution, revenue calculation, and reporting all work.

Steps:

1. Create a fee config for the test channel + USD + direction=debit:
   - platform\_fee\_model: combined ($0.30 flat + 2.9% percentage)
   - processing\_fee\_model: percentage 1.5% with min $0.10
   - tier\_basis: per\_transaction
   - integration\_mode: "sum" (sum channel platform fee with product fee)
2. Activate the config; assert the audit log records "created" then "activated" with the actor.
3. Run a $1,000 debit on the account from Section 2, routed through the test channel.

   Hand-calculated expected fees (the script computes these alongside the API and asserts they match):
   - Product fee (from Section 2): $0.50 flat
   - Channel platform fee: $0.30 + (2.9% of $1000) = $0.30 + $29.00 = $29.30
   - Total platform fee charged to user: $0.50 + $29.30 = $29.80
   - Channel processing cost: 1.5% of $1000 = $15.00 (above $0.10 min)
   - Net revenue: $29.80 - $15.00 = $14.80
4. After the debit, assert:
   - Account balance reduced by $1000 + $29.80 = $1029.80
   - A channel\_revenue\_entries row exists for this transaction with the exact values computed above (Decimal exact equality)
   - The breakdown JSONB explains the computation
   - Three linked postings exist by metadata.transaction\_id: main debit, platform fee, processing cost
5. Run a $50 debit through the same channel. Hand-calculate expected fees and assert they match.
6. Run a $5 debit (small enough that 1.5% processing fee would be under the $0.10 min). Assert processing\_fee == $0.10 exactly.
7. Create a SECOND fee config for the same channel/currency/direction with a tiered model:
   - tiers: 3.5% for $0-$1000, 2.5% for $1000-$10000, 1.5% above
   - tier\_basis: per\_transaction
   - integration\_mode: "override" Activate it (assert the previous config is automatically retired per the "at most one active" invariant).
8. Run a $1,500 debit. Hand-calculated platform fee:
   - 3.5% on first $1000 = $35.00
   - 2.5% on next $500 = $12.50
   - Total: $47.50 Assert the channel\_revenue\_entries row matches exactly.
9. Run a $50,000 debit (top up the account first if needed). Hand- calculate the tiered fee across all three bands. Assert exact match.
10. Reporting:
    - GET /v2/ledgertrack/reports/channels/revenue?from=...\&to=... \&granularity=day for the run's date range Assert gross\_revenue, platform\_fee\_total, processing\_cost\_total, net\_revenue, transaction\_count match the sum of the manually tracked values from steps 3-9.
    - GET .../revenue/by-channel; assert the test channel appears with correct totals.
    - GET .../revenue/comparison?compare\_to=prior\_period; assert response shape (prior period will be zero, current period matches).
    - GET .../revenue/export.csv; parse the CSV and assert rows match the channel\_revenue\_entries records.
    - GET .../revenue/export.pdf; assert HTTP 200 and Content-Type is application/pdf and body starts with %PDF (don't validate render).
11. Admin dashboard:
    - GET /v2/ledgertrack/admin/dashboard/revenue-summary?period=current\_day
    - Assert current\_period\_net\_revenue matches the sum from this run (tolerance: $0 since we just transacted).
    - Assert the test channel appears in top\_channels.
12. Audit log:
    - GET the fee config's audit log endpoint.
    - Assert at least: created, activated, retired (for the first config), created, activated (for the second config). Verify actor and previous\_state/new\_state are present.
13. Negative cases:
    - Attempt to create a fee config without admin role (use a different token or no token). Assert 401/403.
    - Attempt to fetch the revenue report without finance/admin role. Assert 401/403.
    - Attempt to fetch the audit log without audit role. Assert 401/403. (Skip these if role tokens are not configured; print SKIPPED with a clear reason.)
14. Partial-posting failure mode (best-effort):
    - This requires a way to induce a fee posting failure. If the codebase has a debug/fault-injection flag, use it; otherwise mark this step SKIPPED with a TODO comment in the script and a note in README\_E2E.md. Do NOT skip the rest of the section.

Math check at end of section:

- Sum of net\_revenue from channel\_revenue\_entries for this run == reporting endpoint's net\_revenue for the run's date range.
- For every entry: platform\_fee - processing\_fee == net\_revenue (exact Decimal equality).
- For every entry: there exist three postings in the underlying ledger with the same metadata.transaction\_id (verifiable via wallet history on the involved wallets).

## Section 4: Cross-layer reconciliation

Goal: prove the three layers agree about money.

Steps:

1. Compute, from wallet history alone, the total debits and total credits to the account from Section 2 across the entire run.
2. Compute the same from the account's own history endpoint.
3. Assert they match exactly.
4. Compute the sum of all platform fees from channel\_revenue\_entries for the run's date range.
5. Assert it equals the sum of fee postings to the fee\_income GL for the same range (GET the GL wallet's history and filter).
6. Compute the sum of all processing fees from channel\_revenue\_entries.
7. Assert it equals the sum of postings to the channel\_cost GL for the same range.

## Section 5: Cleanup (skippable)

If E2E\_CLEANUP == "true":

- Close the account (will require zero balance; debit/sweep first).
- Close the client.
- Retire the product.
- Retire any active fee configs created during the run.

Do NOT delete the test channel or the GL accounts (they may be shared).

Print a summary line: "Cleanup complete" or "Cleanup skipped (E2E\_CLEANUP=false)".

# Implementation notes

- Use a single requests.Session with a shared base URL and default headers (Content-Type: application/json, optional Bearer tokens).
- Implement a small helper class LedgerTrackClient that wraps the endpoints used. Each method returns the parsed JSON or raises on non-2xx with the request/response context attached to the exception.
- Track all created resource IDs in a dict so cleanup and assertions can reference them.
- For Decimal handling: write a json\_to\_decimal helper that recursively converts any string that looks like a number into Decimal at the boundary. Keep amounts in their native form (minor units int OR major units Decimal) and document which functions use which.
- Hand-calculated expected values must be computed in the script as Decimal expressions inline so a reader can verify the math by reading the code. Don't hardcode the expected number as a literal; compute it the same way a human would and assert against the API.
- Use retries with exponential backoff for transient 5xx errors only (max 3 retries). Never retry 4xx; those are real failures.
- Idempotency: every financial operation uses a reference that includes E2E\_RUN\_ID. Reruns of the same run ID will be no-ops at the ledger level but the script should still fail cleanly on the assertion that it can't, for example, double-credit a wallet.

# README\_E2E.md content

Cover:

- Prerequisites (running LedgerTrack instance, currencies seeded, at least one GL account configured for fees, channel exists or will be created)
- How to install requirements
- How to run with default config
- How to run against a remote instance
- How to interpret output
- Known limitations (e.g. the partial-posting failure mode requires fault injection which may not be available)
- How to extend the script for new features

# Constraints

- Don't use any test framework (no pytest, unittest). The script is standalone.
- Don't write a CLI argument parser. Environment variables only.
- Don't shell out. Pure Python.
- Don't write to disk except for the CSV/PDF download files, which go under /tmp/ledgertrack-e2e-{run\_id}/ and are deleted in cleanup.
- The script must be safe to run repeatedly against a long-lived environment. It must not leave clutter in shared tables beyond what cleanup removes.

# Order of work

1. Read the four reference docs and the existing API surface.
2. Sketch the LedgerTrackClient class with method stubs for every endpoint the script will hit. Stop and confirm with me.
3. Implement Section 0 and Section 1. Verify they pass against a running instance.
4. Implement Section 2. Verify.
5. Implement Section 3. Verify.
6. Implement Sections 4 and 5. Verify.
7. Write README\_E2E.md.
8. Final pass: ensure the script runs cleanly end to end, exits 0 on success, exits 1 with clear diagnostics on any assertion failure.

Ask clarifying questions before coding if any of the following are unclear from the codebase:

- The exact wallet\_id derivation formula for accounts.
- The exact GL wallet identifiers for fee\_income and channel\_cost.
- Whether role tokens are JWT, opaque, or query params.
- Whether the dashboard summary endpoint exists yet or is part of the in-progress fee structure work (if not yet built, mark its section steps as SKIPPED with a note).
- Whether there's a fault-injection flag for the partial-posting test.

