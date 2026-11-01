# Bankman to LedgerTrack migration tooling

Everything needed to produce the import files from a Bankman MySQL database. The same steps run against the sample database and production; only the connection changes.

## Files
- `export_bankman.py`: reads Bankman (read-only) and writes the `ledgertrack-import` JSON Lines files plus `gl_accounts.csv`, `export_issues.csv` and `finance_followup.csv`.
- `coa.py`: the standard chart of accounts (115 accounts per currency) the exporter writes to `gl_accounts.csv`.
- `sample/schema.sql`, `sample/seed.py`: a synthetic Bankman database with the real column definitions, for dry runs and tests. It holds no real data.

## Run
```
export MYSQL_HOST=... MYSQL_PORT=3306 MYSQL_USER=<read-only user> MYSQL_DB=bankman
export MYSQL_PWD=...                      # read by the mysql client, never put on a command line
python3 export_bankman.py ./export        # a unix socket works too: MYSQL_SOCK=/path/mysql.sock
```
Requires `python3` and the `mysql` client. The script only runs SELECT statements.

Then import with `ledgertrack-import` (see `docs/import`), starting with a dry run, and load `gl_accounts.csv` through `POST /v2/{ledger}/gl/accounts/import`.

## What the export decides
- No client is excluded and no account is suspended. Accounts keep their Bankman status. The real KYC status stays in `kyc_source`.
- Amounts are rounded half-even to cents. Every difference is a row in `export_issues.csv`.
- `external_id` of each client (the key callers send) is exported and stored on the client as `legacy_external_id`.
- Open liens are exported one per Bankman lien transaction, with the Bankman transaction id as `source_id`, so a release by lien transaction id can be resolved after cutover. A lien is open when it completed and no release row points at it through `original_transaction_id`. An account uses this detail only when it adds up to `locked_amount`; otherwise it gets one aggregate lien and an `lien_detail_mismatch` or `lien_detail_missing` issue. Check how production Bankman links a release to its lien before relying on the detail; the fallback keeps balances right either way.

## Before production
- Review `export_issues.csv` and `finance_followup.csv` with Finance.
- Rerun the export from a quiet Bankman (or a replica at a known time) and import into an empty LedgerTrack database. The import is idempotent but the new `legacy_external_id` field is written when a client is created.
