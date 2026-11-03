# LedgerTrack authentication and authorization

Status: verified against the code at branch `phase1/auth-ops-hardening` (go-libs `v3.5.0`).
Scope: how the API is protected, how to turn it on, how Cray should wire it, and what it does not do.

## 1. The short version

- Authentication is **off by default**. With no flags set, `auth.NewNoAuth()` is injected and every request is accepted. Anyone who can reach port 3068 can credit, debit and read everything.
- Every HTTP route group in `/v2` and `/` (v1) is mounted **inside** the auth middleware. There is no money or data mutating route outside it. The only unauthenticated HTTP paths are the operational ones in section 5.
- When auth is on, the check is: valid signature from the configured issuer, matching `iss`, not expired, and (optionally) a `<service>:read` or `<service>:write` scope. That is all. There is no audience check, no per-route scope, no caller identity in logs.
- Set `LEDGERTRACK_REQUIRE_AUTH=true` in production. The process then refuses to start unless auth is fully configured (section 4).
- On Railway the private network is flat, so network position is not a control. Auth must be on even if the service has no public domain.

## 2. How auth works in this codebase

Wiring:

1. `cmd/serve.go` registers the flags with `auth.AddFlags(cmd.Flags())` and the module with `auth.FXModuleFromFlags(cmd)` (go-libs `auth/cli.go`, `auth/module.go`).
2. If `auth-enabled` is false, the module provides `NewNoAuth()`, whose `Authenticate` always returns `true`.
3. If it is true, the module does OIDC discovery at startup (`GET {issuer}/.well-known/openid-configuration`, retried up to `auth-read-key-set-max-retries` times), reads `jwks_uri`, builds a remote key set, and provides `NewJWTAuth(keySet, issuer, service, checkScopes)`. If the issuer is unreachable the process fails to start (fail closed).
4. `internal/api/module.go` injects the `auth.Authenticator` into `api.NewRouter`, which passes it to both `v2.NewRouter` and `v1.NewRouter`.
5. Each router does `router.Group(func(r){ r.Use(auth.Middleware(authenticator)); ... })`. The middleware returns `401` with an empty body when `Authenticate` returns an error or `false`.

What `JWTAuth.Authenticate` checks (go-libs `auth/auth.go`):

| Check | Behaviour |
| --- | --- |
| Header | `Authorization: Bearer <jwt>` (prefix `bearer` or `Bearer`). Anything else is 401. |
| Signature | Must verify against the issuer JWKS. Accepted algorithms: RS256, ES256, PS256 (the comment in the library says RS256 only, the code allows these three). `alg=none` is rejected. |
| Issuer | `iss` must equal `AUTH_ISSUER` as an exact string. A trailing slash mismatch fails every request. |
| Expiry | `exp` is required. A token with no `exp` is rejected. No clock skew allowance. |
| Scopes | Only if `auth-check-scopes` is true. Reads (`GET`, `HEAD`, `OPTIONS`, `TRACE`) need `<service>:read` or `<service>:write`. Every other method needs `<service>:write`. `<service>` is the `auth-service` flag value. |
| Not checked | `aud` (audience), `nbf`, `iat`, `sub`, `client_id`. There is no audience flag. |

Consequences you need to plan around:

- **No audience binding.** Any token from the same issuer with the right scope string is accepted. Use an issuer (or a dedicated realm/tenant) that only issues tokens for LedgerTrack, or make the scope string unique (`ledgertrack:write`).
- **Scopes are method based, not route based.** `ledgertrack:write` allows every mutating route, including `POST /v2/_bulk`, `POST /v2/{ledger}/logs/import`, `DELETE /v2/_/buckets/{bucket}` and exporter management. `ledgertrack:read` allows all GETs, including reports and client PII. Some read-only endpoints are POST (`/reports/channels/*`, `/logs/export`) and therefore need the write scope.
- **No caller identity.** The authenticator returns a bool. Claims are discarded, so the subject is not in the request log, the ledger log or the CBA audit rows. Put the calling service name in transaction metadata and `reference`/idempotency keys in the posting services, and keep the edge access log.
- **Revocation is by expiry.** There is no introspection or revocation list. Keep token lifetime short or rotate the signing key.
- Tracing, request logging and CORS run before the auth middleware (`internal/api/router.go`). CORS currently allows any origin with credentials. This is harmless for bearer tokens but should not be relied on as a control.

## 3. Route group coverage

`/v2` routes are in `internal/api/v2/routes.go` (one group, line 43 onward). v1 routes (served at `/` on the same port) are in `internal/api/v1/routes.go`. The CBA account routes (`/accounts/{addr}/credit` and the other lifecycle routes) and the `/reports` routes are wrapped in `ledgertrackOnly`: the ledger in the URL must be literally `ledgertrack`, otherwise they return 404 (after auth).

Protected = inside `auth.Middleware`. All rows are verified by `internal/api/v2/routes_auth_test.go` and `internal/api/v1/routes_auth_test.go`.

| Route group | Paths (prefix `/v2` unless noted) | Mutates money or data | Protected |
| --- | --- | --- | --- |
| Server info | `GET /_info` | no | yes |
| Buckets | `DELETE /_/buckets/{bucket}`, `POST /_/buckets/{bucket}/restore` | yes (destructive) | yes |
| Exporters (only if `--experimental-exporters`) | `/_/exporters` CRUD | yes | yes |
| Ledgers | `GET /`, `POST /{ledger}`, `GET /{ledger}`, `PUT/DELETE /{ledger}/metadata` | yes | yes |
| Bulk | `POST /{ledger}/_bulk` | yes (many transactions) | yes |
| Ledger info, stats, schema | `GET /{ledger}/_info`, `/stats`, `/schema`, `POST /schema/{version}` | schema insert | yes |
| Pipelines (only if exporters on) | `/{ledger}/pipelines` incl. start, stop, reset | yes | yes |
| Logs | `GET /{ledger}/logs`, `POST /logs/import`, `POST /logs/export` | import yes | yes |
| Native accounts | `GET/HEAD /{ledger}/accounts`, `POST /accounts/{addr}/metadata`, `DELETE .../metadata/{key}` | metadata | yes |
| Native transactions | `POST /{ledger}/transactions`, `POST /transactions/{id}/revert`, `POST/DELETE .../metadata`, GET and HEAD | yes | yes |
| Balances, volumes, currencies | `GET /aggregate/balances`, `/volumes`, `/currencies` | no | yes |
| Wallets | `POST /{ledger}/wallets`, `/wallets/{id}/credit`, `/debit`, `/lien`, `/lien/release`, GETs | yes | yes |
| Channels | `POST /{ledger}/channels`, `/channels/{id}/credit`, GETs | yes | yes |
| Channel fees (if service wired) | `GET/PUT /channels/{id}/fees/config`, `GET /channels/fees/configs`, audits | yes (fee config) | yes |
| CBA accounts (`ledgertrack` ledger) | `POST /{ledger}/accounts`, `/accounts/{addr}/credit`, `/debit`, `/lien`, `/lien/release`, `/activate`, `/suspend`, `/freeze`, `/dormant`, `/reactivate`, `/close`, balance, history, statement | yes | yes |
| Products (if service wired) | `/{ledger}/products` create, patch, activate, retire, GETs | yes | yes |
| Clients and KYC (if services wired) | `/{ledger}/clients` create, patch, activate, suspend, reactivate, close, `/kyc` submit, verify, reject | yes | yes |
| Reports | `GET /{ledger}/reports/clients/{id}/portfolio`, `/accounts/{addr}/statement`, `/transactions/daily`, `/interest-fees` | no (PII and balances) | yes |
| Finance reports (GL) | `GET /{ledger}/reports/finance/trial-balance`, `/balance-sheet`, `/pnl`, `/cash-flow` | no | yes |
| Channel revenue reports | `POST /{ledger}/reports/channels/revenue`, `/revenue/timeseries`, `/revenue/export`, `/dashboard` | no | yes |
| v1 API (root, no prefix) | `GET /_info` and `/{ledger}/...` transactions, accounts metadata, balances, logs, stats | yes | yes |

Not behind the auth middleware (added in `cmd/serve.go`, `assembleFinalRouter`):

| Path | Content | Exposure |
| --- | --- | --- |
| `GET /_healthcheck`, `/_/healthcheck` | health status | always, harmless, use for probes |
| `GET /_/info` | server name and version | always |
| `GET /_/metrics` | in-memory OTLP metrics | only with `--otel-metrics-keep-in-memory` |
| `/_/debug/pprof/*` | Go profiler | only with `--debug`. Never in production. |
| gRPC `:8081` (`ledger worker` command only; `serve` without `--worker` is the client) | replication server, insecure credentials, no auth | only on a standalone worker process. Do not expose. |

Change made on this branch: v1 `GET /_info` was registered outside the middleware and returned every ledger name plus the server version. It is now inside. Probes must use `/_healthcheck`.

Finding about the fork-added routes (wallets, channels, products, clients, accounts, reports): they were already inside the single `/v2` auth group. Nothing needed to be moved. The real exposure is the default `NoAuth`, not a routing gap.

## 4. Enforcing auth

Flags and environment variables (a flag `auth-issuer` is read from env `AUTH_ISSUER`; dashes become underscores, upper case, via `service.BindEnvToCommand`):

| Flag | Env | Default | Set to |
| --- | --- | --- | --- |
| `--auth-enabled` | `AUTH_ENABLED` | `false` | `true` |
| `--auth-issuer` | `AUTH_ISSUER` | empty | exact `iss` value of your tokens, e.g. `https://auth.cray.internal/` |
| `--auth-check-scopes` | `AUTH_CHECK_SCOPES` | `false` | `true` |
| `--auth-service` | `AUTH_SERVICE` | empty | `ledgertrack` (scopes become `ledgertrack:read` and `ledgertrack:write`) |
| `--auth-read-key-set-max-retries` | `AUTH_READ_KEY_SET_MAX_RETRIES` | `10` | keep, or lower for faster failure |
| (new) | `LEDGERTRACK_REQUIRE_AUTH` | `false` | `true` in production |

Production environment:

```
AUTH_ENABLED=true
AUTH_ISSUER=https://<your issuer>/
AUTH_CHECK_SCOPES=true
AUTH_SERVICE=ledgertrack
LEDGERTRACK_REQUIRE_AUTH=true
```

`LEDGERTRACK_REQUIRE_AUTH=true` makes `ledger serve` exit before opening any connection unless: auth is enabled, an issuer is set, scope checking is on, a service name is set, `--debug` is off and `LEDGER_HTTP_LOG` is unset. The error lists every problem. A non-boolean value (for example `ture`) also fails, so a typo cannot silently disable it. With the variable unset or `false`, behaviour is unchanged.

Quick check after deploy (no token must give 401, a read token must not be able to post):

```
curl -s -o /dev/null -w '%{http_code}\n' $BASE/v2/_info                                   # 401, no token
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $READ_TOKEN" -X POST $BASE/v2/ledgertrack/wallets/x/credit   # 401, read scope can not write
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $WRITE_TOKEN" $BASE/v2/_info   # 200
```

## 5. Recommended topology for Cray

Goal: only Cray posting services can reach LedgerTrack, and each one proves who it is.

1. **One credential per caller.** Create one OAuth2 client (client credentials grant) per posting service, for example `svc-gateman`, `svc-snowman`, `svc-opsman`, plus one for humans/ops tooling. Never share a client between services. Each service fetches a short-lived access token (10 to 15 minutes) and caches it until about 60 seconds before `exp`.
2. **Scopes.** Posting services get `ledgertrack:write`. Reporting and reconciliation jobs get `ledgertrack:read` only. This is the full extent of least privilege the library supports. If you need "service A may only call credit on wallets", the library cannot do it; use an authenticating proxy with a path allowlist (section 6, option B) or split into separate ledgers and issuers.
3. **Keep write credentials rare.** Anything that can mint `ledgertrack:write` can also delete buckets and import logs. Restrict who can create clients in the issuer, and never hand a write token to a UI.
4. **Network.** No public domain on LedgerTrack. On Railway, the flat private network means every service in the project can reach it by hostname, so do not put unrelated services (or anything running third-party code) in the same project/environment. Railway docs state that each environment has its own isolated private network (verify for your plan), so a dedicated project or environment limits who can reach it. Prefer a dedicated Railway project or environment for LedgerTrack and Postgres, with only the posting services deployed there or reaching it through an authenticated edge (verify whether Railway offers cross-project private networking or per-service ingress rules for your plan; do not assume it does).
5. **Defence in depth.** Auth is the control, network position is not. Add edge rate limiting and request logging in front (see `docs/security/deployment-hardening.md`).
6. **Attribution.** Since LedgerTrack drops the caller identity, make every posting service set `metadata.source_service` and a deterministic idempotency key/`reference` on each call, so the ledger log can be traced back to a caller.
7. **Rotation.** Rotate each client secret at least every 90 days. Rotate the issuer signing key by publishing the new key in the JWKS first, then switching signing, then removing the old key after the longest token lifetime.

## 6. If there is no OIDC provider

The library needs exactly one thing from the issuer: `GET {issuer}/.well-known/openid-configuration` returning JSON with `jwks_uri`, a JWKS with the signing keys, and JWTs whose `iss` equals the configured issuer, with `exp` and a space separated `scope` claim, signed RS256/ES256/PS256. Options, best first:

**A. Use a real IdP.** Self-hosted Keycloak, Zitadel or Ory Hydra, or a managed one (Auth0, Cognito, Entra ID). Gives client credentials, rotation, revocation of clients, audit of token issuance. Cost: another stateful service to run and secure (if self-hosted, give it its own Postgres and its own project, not the ledger DB). Check that your IdP puts custom scopes such as `ledgertrack:write` in the `scope` claim of access tokens and that the access token is a JWT (not opaque).

**B. Authenticating reverse proxy in front, LedgerTrack auth disabled.** Caddy, nginx or Envoy validates a token or an mTLS client certificate and forwards. Pros: no OIDC needed, can enforce per-path, per-method allowlists per client (finer than the library), can add rate limits. Cons: LedgerTrack itself is then unauthenticated, so the proxy must be the only way in. On a flat network that means the proxy and LedgerTrack must share a container or host and LedgerTrack must bind loopback only (`--bind 127.0.0.1:3068`), with the proxy as the only listener. If they are separate Railway services, any other service in the project can bypass the proxy by calling LedgerTrack directly, so this option is not safe there. `LEDGERTRACK_REQUIRE_AUTH=true` will (correctly) refuse to start in this mode, so you must leave it unset and accept the weaker guarantee. Also loses the library scope check.

**C. Small static JWT issuer.** Host two static files on a tiny web service, `/.well-known/openid-configuration` (`{"issuer": "...", "jwks_uri": ".../jwks.json"}`) and `/jwks.json`, and mint tokens yourself with a private key held in a secret store. Two variants:
   - C1, long-lived tokens minted offline per service (30 to 90 days). Simple, no runtime dependency except the static file service at LedgerTrack startup. Weak: a leaked token works until expiry, revocation means rotating the signing key and re-issuing everything.
   - C2, a ~100 line token endpoint implementing `client_credentials` with a per-client secret and an allowed-scope list, issuing 10 minute tokens. Closer to a real IdP, but you now own security-critical code (secret storage, comparison, rate limiting, key rotation) with no audit trail beyond what you build.
   In both, the signing key is as powerful as a root credential. Do not let posting services hold it; only the issuer does. Prefer ES256 or RS256 with a 2048+ bit key and set `kid`.

**D. Network-only (mTLS or private network).** Not acceptable here: the Railway private network is flat and LedgerTrack's own auth would be off.

Recommendation: A with a managed provider if procurement allows; otherwise C2 hosted outside the LedgerTrack project, with C1 only as a stopgap for the first weeks. Avoid B on Railway unless proxy and LedgerTrack are one container.

## 7. Known gaps and follow-ups

- No audience check, no per-route scopes, no caller identity in logs (library limits). Fix requires a fork of go-libs `auth` or a wrapper authenticator in `internal/api` that also stores claims in the request context; not done on this branch.
- `/_/metrics` and `/_/info` are unauthenticated by design; `--otel-metrics-keep-in-memory` should stay off unless the port is not reachable by others.
- Worker gRPC (`:8081`) uses insecure credentials with no auth. Only run it where the API process can reach it and nothing else can.
- CORS allows any origin with credentials. Tighten at the edge if a browser UI is ever served.
