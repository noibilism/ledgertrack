# bankman-bootstrap

One-shot job that loads bankmax from Bankman and switches the shadow gateway on. DevOps sets
environment variables only. No manual export, import or reconcile.

Required: LEDGERTRACK_URL, BANKMAN_DB_HOST, BANKMAN_DB_USER (read-only), BANKMAN_DB_PASSWORD, BANKMAN_DB_NAME.
Optional: BANKMAN_DB_PORT (3306), LEDGERTRACK_LEDGER (ledgertrack), LEDGERTRACK_TOKEN, GATEWAY_URL,
SHADOW_ADMIN_TOKEN (needed with GATEWAY_URL), MODE (bootstrap|reconcile), ENABLE_GATEWAY (true),
WAIT_SECONDS (600), BOOTSTRAP_TOLERANCE (0.005), RECONCILE_TOLERANCE (0.02), BOOTSTRAP_FORCE.

Flow: wait for bankmax, create the ledger, export (SELECT only), import GL and accounts, verify,
reconcile every account, write the `bankman_bootstrap` ledger marker, resume the gateway.
Re-runs skip the load when the marker is set and only reconcile. The gateway is never switched on
after a failed reconcile. Exit 0 verified, 1 failed, 64 missing variables.

MODE=reconcile runs only the reconcile (use it as a daily cron service).
Build from the repo root: docker build -f tools/bankman-bootstrap/Dockerfile .
