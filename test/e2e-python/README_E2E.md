# LedgerTrack Python E2E Functional Test

This folder contains a standalone end-to-end functional test runner for a live LedgerTrack deployment.

It exercises:
- Wallets + channels (liquidity, lien, lien release PAY + cancel)
- CBA (products, clients, KYC, accounts)
- Channel fees + revenue reporting (fee config, per-transaction fee metadata, reports, CSV/PDF export, dashboard metrics)
- Cross-layer reconciliation (wallet/account history vs revenue ledger postings)

## Prerequisites

- A running LedgerTrack instance reachable via HTTP.
- Currencies seeded and enabled in `_system.currencies`:
  - USD
  - NGN
- A token that can call the v2 API (bearer token). The runner exits early if `LEDGERTRACK_ADMIN_TOKEN` is not set.

## Install

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python e2e_functional_test.py
```

Run against a remote deployment:

```bash
LEDGERTRACK_BASE_URL="https://your-host" \
LEDGERTRACK_ADMIN_TOKEN="..." \
python e2e_functional_test.py
```

## Environment Variables

- `LEDGERTRACK_BASE_URL` (default: `http://localhost:3068`)
- `LEDGERTRACK_LEDGER` (default: `ledgertrack`)
- `LEDGERTRACK_ADMIN_TOKEN` (required for authenticated deployments; bearer token)
- `LEDGERTRACK_FINANCE_TOKEN` (optional; bearer token)
- `LEDGERTRACK_AUDIT_TOKEN` (optional; bearer token)
- `E2E_RUN_ID` (default: generated UUID)
- `E2E_CLEANUP` (default: `true`)

## Output

- Prints the resolved configuration at startup.
- Prints a banner per section, then one line per step:
  - `{description} | {METHOD} {PATH} -> {STATUS} | PASS/FAIL | {seconds}`
- On failure, prints request/response details and exits non-zero.
- At the end, prints a summary table (when rich is available).

## Artifacts

- CSV/PDF exports are downloaded to `/tmp/ledgertrack-e2e-{run_id}/`.
- If `E2E_CLEANUP=true`, the directory is deleted during cleanup.

## Known Limitations / Notes

- This runner uses the wallet endpoints to exercise the channel-fee engine (wallet debit with `channelID` + `channelAmount`). It does not currently assert channel-fee behavior on CBA account debits.
- Product transaction-fee postings on CBA account debit are asserted only if `fee_posting` transactions appear in account history; otherwise the check is marked SKIPPED.
- Role-based (claims/roles) negative-case assertions are marked SKIPPED because the current auth middleware does not expose roles/claims for true RBAC enforcement.
- Partial-posting failure testing is marked SKIPPED because no fault-injection toggle is configured.
