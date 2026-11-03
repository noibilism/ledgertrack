#!/usr/bin/env bash
# bankman-bootstrap: copies Bankman's current state into bankmax and proves it, with no manual step.
# Everything is configured by environment variables. Bankman is only ever read.
#
#   MODE=bootstrap (default)  wait for bankmax, create the ledger, export Bankman, load the chart of
#                             accounts, import, verify, reconcile, then switch the shadow gateway on.
#   MODE=reconcile            compare every Bankman account with bankmax (run daily as a cron job).
#
# Safe to run again: every step is idempotent. Exit code 0 only when the end state is verified.
set -euo pipefail

MODE="${MODE:-bootstrap}"
log() { printf '%s [bootstrap] %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { log "FAILED: $*"; exit 1; }

need() {
  local missing=()
  for v in "$@"; do [ -n "${!v:-}" ] || missing+=("$v"); done
  if [ "${#missing[@]}" -gt 0 ]; then
    log "missing required variables: ${missing[*]}"
    exit 64
  fi
}

need LEDGERTRACK_URL BANKMAN_DB_HOST BANKMAN_DB_USER BANKMAN_DB_PASSWORD BANKMAN_DB_NAME

LT="${LEDGERTRACK_URL%/}"
LEDGER="${LEDGERTRACK_LEDGER:-ledgertrack}"
GW="${GATEWAY_URL:-}"; GW="${GW%/}"
WORKDIR="${WORKDIR:-/tmp/bootstrap}"
mkdir -p "$WORKDIR"
curl_auth=()
if [ -n "${LEDGERTRACK_TOKEN:-}" ]; then
  curl_auth=(-H "Authorization: Bearer ${LEDGERTRACK_TOKEN}")
  export LEDGERTRACK_AUTH_TOKEN="$LEDGERTRACK_TOKEN"   # read by ledgertrack-import
fi
export LEDGERTRACK_URL="$LT"

# Bankman's database, read-only. The mysql client and the exporter read these.
export MYSQL_HOST="$BANKMAN_DB_HOST" MYSQL_PORT="${BANKMAN_DB_PORT:-3306}" MYSQL_USER="$BANKMAN_DB_USER" \
       MYSQL_DB="$BANKMAN_DB_NAME" MYSQL_PWD="$BANKMAN_DB_PASSWORD"
export BANKMAN_DSN="${BANKMAN_DB_USER}:${BANKMAN_DB_PASSWORD}@tcp(${BANKMAN_DB_HOST}:${MYSQL_PORT})/${BANKMAN_DB_NAME}"

wait_for() { # name, url, seconds
  local name="$1" url="$2" secs="$3" i=0
  until curl -fsS -m 5 -o /dev/null "${curl_auth[@]}" "$url"; do
    i=$((i + 5)); [ "$i" -ge "$secs" ] && die "$name did not answer at $url within ${secs}s"
    sleep 5
  done
  log "$name is up"
}

reconcile() { # tolerance
  log "reconciling every Bankman account against bankmax (tolerance $1)"
  local rc=0
  RECONCILE_TOLERANCE="$1" bankman-reconcile | tee "$WORKDIR/reconcile.jsonl" || rc=$?
  return "$rc"
}

enable_gateway() {
  if [ -n "$GW" ] && [ "${ENABLE_GATEWAY:-true}" = "true" ]; then
    need SHADOW_ADMIN_TOKEN
    log "switching the shadow gateway on"
    curl -fsS -X POST -H "Authorization: Bearer ${SHADOW_ADMIN_TOKEN}" "$GW/shadow/resume" >/dev/null || die "could not resume the gateway at $GW"
    log "gateway is mirroring"
  else
    log "GATEWAY_URL not set or ENABLE_GATEWAY=false: gateway left as it is"
  fi
}

wait_for bankmax "$LT/_info" "${WAIT_SECONDS:-600}"

if [ "$MODE" = "reconcile" ]; then
  reconcile "${RECONCILE_TOLERANCE:-0.02}" || die "reconciliation failed (differences or a read error, see lines above)"
  log "reconciliation clean"
  exit 0
fi
[ "$MODE" = "bootstrap" ] || die "MODE must be bootstrap or reconcile, got $MODE"

# 1. The ledger
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${curl_auth[@]}" "$LT/v2/$LEDGER" || true)
case "$code" in
  204|200) log "ledger $LEDGER created" ;;
  *) curl -fsS -o /dev/null "${curl_auth[@]}" "$LT/v2/$LEDGER" && log "ledger $LEDGER already exists" \
       || die "cannot create or read ledger $LEDGER (HTTP $code)" ;;
esac

# Already loaded? Then do not import again: Bankman has moved on since, and a second import would conflict.
loaded=$(curl -fsS "${curl_auth[@]}" "$LT/v2/$LEDGER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"].get("metadata",{}).get("bankman_bootstrap",""))' 2>/dev/null || true)
if [ -n "$loaded" ] && [ "${BOOTSTRAP_FORCE:-false}" != "true" ]; then
  log "bankmax was already loaded from Bankman at $loaded: skipping export and import (BOOTSTRAP_FORCE=true overrides)"
  reconcile "${RECONCILE_TOLERANCE:-0.02}" || die "reconciliation failed: the shadow gateway was NOT switched on"
  enable_gateway
  log "bootstrap complete (nothing to load)"
  exit 0
fi

# 2. Export Bankman (SELECT only)
OUT="$WORKDIR/export"; rm -rf "$OUT"
log "exporting Bankman from $BANKMAN_DB_HOST/$BANKMAN_DB_NAME"
python3 /opt/migration/export_bankman.py "$OUT" || die "export failed"
for f in products clients accounts balances liens; do
  [ -f "$OUT/$f.jsonl" ] && log "exported $f: $(wc -l < "$OUT/$f.jsonl") rows"
done
for f in export_issues.csv finance_followup.csv; do
  [ -f "$OUT/$f" ] && log "$f: $(($(wc -l < "$OUT/$f") - 1)) rows (kept in the job's output below)"
done
[ -f "$OUT/export_issues.csv" ] && sed -n '1,200p' "$OUT/export_issues.csv" | sed 's/^/[export_issues] /'
[ -f "$OUT/finance_followup.csv" ] && sed -n '1,200p' "$OUT/finance_followup.csv" | sed 's/^/[finance_followup] /'

# 3. Chart of accounts
GLURL="$LT/v2/$LEDGER/gl/accounts/import"
log "chart of accounts: dry run"
curl -fsS -X POST "${curl_auth[@]}" -H 'Content-Type: text/csv' --data-binary @"$OUT/gl_accounts.csv" "$GLURL?dry_run=true" \
  | tee "$WORKDIR/gl_dry.json"; echo
log "chart of accounts: commit"
curl -fsS -X POST "${curl_auth[@]}" -H 'Content-Type: text/csv' --data-binary @"$OUT/gl_accounts.csv" "$GLURL" \
  | tee "$WORKDIR/gl_commit.json"; echo

# 4. Import, then read everything back
IMP=(ledgertrack-import)
COMMON=(--input "$OUT" --url "$LT" --ledger "$LEDGER" --state "$WORKDIR/import-state.json")
log "import: validate"; "${IMP[@]}" validate --input "$OUT" || die "validate found errors"
log "import: plan";     "${IMP[@]}" plan "${COMMON[@]}"      || die "plan found conflicts"
log "import: apply";    "${IMP[@]}" apply "${COMMON[@]}" --yes || die "apply failed"
log "import: verify";   "${IMP[@]}" verify "${COMMON[@]}" --deep --out "$WORKDIR/reconciliation.csv" || die "verify found differences"

# 5. Reconcile against Bankman itself. 0.005 covers the half-cent from rounding 4 decimals to cents.
reconcile "${BOOTSTRAP_TOLERANCE:-0.005}" || die "baseline reconciliation failed: the shadow gateway was NOT switched on"

# 6. Remember that the load is done, so a restart of this job never imports twice, then switch the gateway on
curl -fsS -X PUT "${curl_auth[@]}" -H 'content-type: application/json' \
  -d "{\"bankman_bootstrap\":\"$(date -u +%FT%TZ)\"}" "$LT/v2/$LEDGER/metadata" >/dev/null || die "could not record the bootstrap marker"
enable_gateway
log "bootstrap complete"
