# LedgerTrack deployment hardening checklist

Use with `docs/security/auth.md` (authentication) and `docs/ops/backup-restore.md` (recovery). Tick every item before LedgerTrack holds real money. Items marked (verify) depend on platform behaviour that was not confirmed from here.

## 1. Network exposure

- [ ] No public domain or TCP proxy on the LedgerTrack service or on Postgres. In Railway: no generated domain, no custom domain, no TCP proxy (check the service Networking tab and `railway domain` list).
- [ ] LedgerTrack is reachable only by the Cray posting services. The Railway private network is flat inside an environment (any service can call any other by hostname), so:
  - [ ] LedgerTrack and its Postgres live in their own project or environment, with no unrelated services (verify that environments are isolated networks for your plan).
  - [ ] Auth is on (`AUTH_ENABLED=true`, `LEDGERTRACK_REQUIRE_AUTH=true`). Network position is not a control.
- [ ] If an external client ever needs access, put an authenticating, rate limiting edge in front (section 6), never the raw service.
- [ ] The worker gRPC port (`ledger worker`, default `:8081`) uses insecure credentials and no auth. Run the worker embedded (`--worker`) in the API process, or make sure the gRPC port is not routable by anything else.
- [ ] `/_/metrics` (only with `--otel-metrics-keep-in-memory`) and `/_/info` are unauthenticated. Keep metrics off or scrape through a private collector.
- [ ] `--debug` is off. It exposes `/_/debug/pprof` unauthenticated and writes request bodies to a file. `LEDGERTRACK_REQUIRE_AUTH=true` refuses to start with it on.

## 2. TLS

- [ ] Clients reach LedgerTrack over TLS. On the private network Railway traffic is not TLS by default (verify), and LedgerTrack serves plain HTTP on `0.0.0.0:3068`. Choose one: terminate TLS in a sidecar/proxy in the same container, use a mesh or WireGuard overlay, or accept plain HTTP on the private network and record that as a risk with the CTO's sign-off. Bearer tokens are credentials, so do not send them over a network you do not trust.
- [ ] Postgres connection uses TLS: `sslmode=require` at minimum, `verify-full` with the CA when the provider supports it. Set it in `POSTGRES_URI` (flag `--postgres-uri`). For Railway Postgres, verify whether the private endpoint offers TLS and whether the template's server has SSL enabled.
- [ ] The issuer URL (`AUTH_ISSUER`) is `https://`. The JWKS is fetched over it at startup and key rotation relies on it.
- [ ] Minimum TLS 1.2 at any edge, HSTS if a browser ever calls it.

## 3. Secrets

- [ ] Secrets only in the platform's secret/variable store, never in the repo, `docker-compose.yml`, `deploy.sh` defaults, images or build logs. The Cray Processing Engine project already has findings about GitLab tokens appearing in build logs (gateman, opsman), so do not repeat that here: pass secrets as runtime variables or build secrets (`--secret` mounts), not `ARG`/`ENV` or echoed commands. Run a secret scan (section 7) over history before go-live.
- [ ] Secrets inventory with owner and rotation date: DB password, `POSTGRES_URI`, each posting service client secret, issuer signing key, backup encryption key.
- [ ] Rotation: DB password and client secrets every 90 days or on staff change; issuer signing key per `docs/security/auth.md` section 5. Rotation is rehearsed, not theoretical.
- [ ] Separate secrets per environment. Staging never shares credentials, issuer or database with production.
- [ ] Variable references (for example `${{Postgres.DATABASE_URL}}` in Railway) are fine, but confirm the resolved value does not appear in logs (`railway logs` search for the host and password).
- [ ] No test or admin tokens in `test/e2e-python` configs that point at production.

## 4. Database

- [ ] LedgerTrack has its own Postgres instance. Not shared with other Cray services (gateman, snowman, opsman, ClickHouse feeders), because a shared instance means shared superuser blast radius, shared connection limits, and a restore of one rolls back the other.
- [ ] Least privilege DB users:
  - Migration user (DDL, `create schema`, `create extension` once) used only by `ledger migrate`/`--auto-upgrade` runs.
  - Runtime user for `ledger serve` and `ledger worker`: `connect`, `usage` and DML (`select, insert, update, delete`) on `_system` and bucket schemas, `usage, select, update` on sequences, `execute` on the schema functions. No superuser, no `createdb`, no `createrole`, no `drop` on schemas. Note: the migrator creates schemas and functions, and ledger creation can create new bucket schemas, so the runtime user needs `create` on the database if you create ledgers or buckets at runtime. Decide per environment: either pre-create buckets with the migration user, or accept `create` for the runtime user. Test with a non-owner user in staging before relying on this (the author has not run it).
  - A read-only user for reporting, backup verification and the reconciliation project.
- [ ] `pgcrypto` installed once by a privileged user; `public.digest` is used by the hash functions.
- [ ] `pg_hba`/provider rules: only the LedgerTrack service and the backup job can connect. No `0.0.0.0/0`.
- [ ] Encryption at rest on the volume or disk (verify on Railway), TLS in transit (section 2).
- [ ] Connection limits sized: API pool plus worker plus migrations plus backup job below `max_connections`.
- [ ] `statement_timeout` and `idle_in_transaction_session_timeout` set on the runtime role.
- [ ] Backups and PITR enabled and a restore drill passed (`docs/ops/backup-restore.md`).
- [ ] Row level data is not editable by hand. Any manual SQL on `logs`, `transactions`, `moves` or `accounts_volumes` is forbidden outside a documented incident, and requires a second person.

## 5. Logging and redaction

- [ ] `LEDGER_HTTP_LOG` unset and `--debug` off. Both append full request and response bodies to `requests.log` in the working directory (`assembleFinalRouter` in `cmd/serve.go`), which would contain balances, account numbers, client PII and bearer tokens' downstream data. The startup guard refuses them when `LEDGERTRACK_REQUIRE_AUTH=true`.
- [ ] The container filesystem is not persisted or shipped anywhere that would keep an old `requests.log`; delete any that exists.
- [ ] Never log the `Authorization` header at the edge or in the proxy. Redact `authorization`, `cookie`, `set-cookie`, `x-api-key` and any `*token*`, `*secret*`, `password` fields in log pipelines.
- [ ] Redact or hash in logs: account numbers, BVN/NIN/ID numbers from KYC payloads, phone numbers, emails, `kyc_data`, `contact`, `individual_data`, `corporate_data`. LedgerTrack's own logs should not contain them; confirm by posting a test client and grepping the logs.
- [ ] JSON logs on (`--json-formatting-logger`, env `JSON_FORMATTING_LOGGER=true`) so the log pipeline can redact by field.
- [ ] Log retention set (for example 90 days hot, 1 year archived), access to logs restricted to the on-call and security roles.
- [ ] Alerts: spikes of 401, repeated failed auth from one source, any 5xx rate over 1%, `DELETE /v2/_/buckets/*`, `POST /v2/*/logs/import`, `POST /v2/_/exporters`, any use of the write scope outside posting services.
- [ ] Because LedgerTrack does not record the caller identity, every posting service sets a source tag in transaction metadata and a deterministic idempotency key (`docs/security/auth.md` section 5).

## 6. Edge and rate limiting

LedgerTrack has no built-in rate limiting. Put it at the edge (Caddy, nginx, Envoy, API gateway, or Cloudflare if a public edge is ever added):

- [ ] Per-client rate limit on mutating routes (POST, PUT, PATCH, DELETE), for example 50 requests per second per service credential, burst 100, tuned from load tests. 429 on excess.
- [ ] Lower limits on destructive and admin routes: `DELETE /v2/_/buckets/*`, `/v2/_/exporters`, `POST /v2/*/logs/import`, `POST /v2/*/_bulk` (also bound `--bulk-max-size`, default 100).
- [ ] Maximum request body size (for example 1 MB, larger for bulk if needed) and request timeouts.
- [ ] Allowlist of paths actually used by Cray. Deny the rest, notably `/_/debug`, `/_/metrics`, `/v2/_/*` from posting services.
- [ ] Block or strip hop headers you do not use, and do not forward client supplied `X-Forwarded-*` unvalidated.
- [ ] CORS: LedgerTrack allows any origin with credentials. If no browser calls it, drop CORS headers at the edge and reject browser `Origin` requests.
- [ ] Idempotency: posting services always send an idempotency key so retries after 429/timeouts never double post.

## 7. Dependency and image scanning

Run in CI and before each release. Commands from the repo root:

```
# Go vulnerabilities in reachable code (preferred signal)
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...

# Known CVEs in the module graph, any language in the repo
osv-scanner --recursive .

# Outdated direct dependencies (review, do not blindly upgrade)
go list -m -u all | grep '\['

# Static analysis for security smells
go install github.com/securego/gosec/v2/cmd/gosec@latest
gosec -exclude-generated ./...

# Container image and filesystem (OS packages, Go binary, secrets, misconfig)
trivy image --severity HIGH,CRITICAL --exit-code 1 <image:tag>
trivy fs --scanners vuln,secret,misconfig .

# Secret scan over full git history (before go-live and in CI)
gitleaks detect --source . --log-opts="--all"

# Wallet UI (separate JS dependency tree)
cd wallet-ui && npm audit --omit=dev --audit-level=high
```

- [ ] Fail the build on HIGH or CRITICAL findings with a reachable code path; record accepted exceptions with an owner and expiry.
- [ ] Pin the base image by digest, rebuild weekly, scan the built image, not only the source.
- [ ] `renovate.json` is present: confirm it is active and that security updates are auto-opened.
- [ ] Verify the `go-libs` auth and `oidc` packages in use (`v3.5.0`) against its security advisories at each upgrade, since all authentication is delegated to them.
- [ ] Build from a clean checkout in CI, not from a developer machine, and keep the SBOM (`trivy image --format cyclonedx`).

## 8. Runtime and process

- [ ] Container runs as a non-root user with a read-only root filesystem where possible (`requests.log` writes would then fail closed).
- [ ] Resource limits set; health probe path is `/_healthcheck` (not `/_info`, which now requires a token).
- [ ] One worker instance only (async hash blocks, schedulers, interest accrual); API instances can scale.
- [ ] `--auto-upgrade` off in production; run `ledger migrate` as a deliberate release step after a backup (forward-only migrations, see `CBA_PRD.md`).
- [ ] Deploys go through the pipeline's quality gate; no direct `deploy.sh` to production hosts with default secrets.
- [ ] Staging mirrors production settings, including `LEDGERTRACK_REQUIRE_AUTH=true`, so config drift is caught before release.

## 9. Sign-off

| Area | Owner | Date | Evidence link |
| --- | --- | --- | --- |
| Network exposure | | | |
| TLS | | | |
| Secrets | | | |
| Database | | | |
| Logging | | | |
| Edge and rate limits | | | |
| Scanning | | | |
| Backup drill | | | |
