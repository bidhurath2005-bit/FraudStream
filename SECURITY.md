# FraudStream — Security

FraudStream is an **educational project that runs on one host with synthetic data**. Its controls are designed for
that deployment model: every port bound to `127.0.0.1`, services on a private Docker network, no real customer data.
The detailed Phase 12 review, with reproductions and before/after evidence, is in
[docs/phase12/SECURITY_REVIEW.md](docs/phase12/SECURITY_REVIEW.md).

**Reporting a problem:** open an issue in the repository describing the class of problem. Do not include working
exploit code, and never include real credentials or `.env` contents.

## 1. Authentication

| Control | Implementation (`backend/app/security/auth.py`) |
|---|---|
| Password storage | Argon2id (`argon2-cffi` defaults); never returned by any endpoint |
| Password policy | ≥ 12 characters, at least 3 of: lowercase, uppercase, digit, symbol |
| Unknown usernames | verified against a dummy hash, so timing does not reveal whether an account exists |
| Access token | JWT HS256, `exp` and `sub` required, `type=access`, default lifetime 30 min (`ACCESS_TOKEN_MINUTES`); `JWT_SECRET` must be ≥ 32 characters and is mandatory in production |
| Refresh token | 48 random bytes, stored only as SHA-256; **rotated on every use**; re-use of a rotated token revokes all of the user's tokens; cookie `HttpOnly`, `SameSite=Strict`, path `/api/v1/auth`, 7 days |
| `Secure` cookie flag | off by default (`SECURE_COOKIES=false`) because the dashboard is plain HTTP on 127.0.0.1. **Set it to true behind HTTPS.** |
| Failed logins | `401 "invalid username or password"` (no distinction between causes) and a `LOGIN_FAILED` audit row with the client address |
| Live feed | SSE accepts the bearer header (used by the dashboard) or a `?token=` parameter for EventSource clients. The parameter is discouraged because URLs can be logged. |

## 2. Role-based access control

Two roles, enforced **on the server** by FastAPI dependencies that run before the request body is parsed:

| Capability | ANALYST | ADMIN |
|---|---|---|
| Dashboards, transactions, analytics, model registry, drift checks, infrastructure status | ✓ | ✓ |
| Alert triage and investigations (only items unassigned or assigned to themselves) | ✓ | ✓ (any item) |
| Re-open a closed alert or investigation | — | ✓ |
| Simulation Lab, ingestion | ✓ | ✓ |
| Reports: 5 types | ✓ | ✓ |
| Report: system activity (audit) | — | ✓ |
| Change risk rules, risk scoring, drift settings | — (403, audited as `CONFIG_CHANGE_DENIED`) | ✓ |
| Create or modify users; upload datasets; train, activate or roll back models | — | ✓ |
| Global audit trail (`GET /audit`) | — | ✓ |

The dashboard hides or disables what a role cannot do, but **the API rejects it regardless of the UI**. Tests check
both the UI state and the API status code.

## 3. Rate limiting

| Limit | Default | Key |
|---|---|---|
| Login | 10 / minute (`LOGIN_RATE_LIMIT_PER_MINUTE`) | client address |
| Other API calls | 1,200 / minute (`API_RATE_LIMIT_PER_MINUTE`) | valid access token; invalid or missing token → client address |

Counters live in Redis in the distributed profile and fall back to per-process counters while Redis is unavailable;
`/system/status` reports which is active. nginx **sets** `X-Forwarded-For` to the connecting address instead of
appending to a client-supplied value. Before Phase 12 it appended, which let a client choose its own address and
bypass the login limit (fixed and verified: 10 × 401, then 5 × 429).

## 4. Input and configuration validation

- All request bodies are Pydantic models. Configuration, report and user bodies forbid unknown fields (422).
- Configuration values are **strict**: `"40"` or `"yes"` are rejected rather than coerced. Numbers must be finite and
  within per-parameter bounds (e.g. rule `count` a whole number 1–1000, `kmh` (0, 20000], risk thresholds
  `0 < medium < high ≤ 100`, drift `0 < moderate < significant ≤ 10`, `window_rows` 50–100,000).
- Shared bounds on query and path integers (`app/api/params.py`) mean hostile values return 422, never 500.
  A test probes every GET route with 12 hostile values.
- SQL is built with SQLAlchemy expressions and bound parameters only; injection strings are treated as data (tested).
- No endpoint opens a user-supplied path. Uploads are stored under a content-hash name; report downloads look up a
  UUID.
- CSV exports neutralise spreadsheet formulas: cells starting with `= + - @ TAB CR` get a `'` prefix. This applies to
  the report exports and the alerts CSV, because `customer_id` is free text from ingestion.
- Report periods must be timezone-aware, with `start < end` and at most 366 days.

## 5. Optimistic concurrency on configuration

`PATCH /risk/rules/{code}`, `PUT /risk/settings` and `PUT /settings/{key}` **require** `expected_updated_at` (the
`updated_at` value the client loaded):

1. non-admin → 403 (audited);
2. invalid values → 422 with the reason (audited);
3. version missing → **428** (audited);
4. version stale → **409** (audited);
5. otherwise the change is applied in one transaction, with `SELECT … FOR UPDATE` on PostgreSQL. The previous
   configuration is kept on any failure.

`null` is accepted only for an item that has never been stored. Full sequence:
[docs/API_DOCUMENTATION.md §9](docs/API_DOCUMENTATION.md#9-risk-configuration-and-settings-optimistic-concurrency).

## 6. Audit logging

`audit_logs` records logins and failed logins, alert and investigation actions, every successful configuration change
(field, old → new value), every **rejected** (`RULE_UPDATE_REJECTED`, `RISK_SETTINGS_REJECTED`,
`SETTING_UPDATE_REJECTED`, with reason, attempted payload ≤ 1,000 characters and status code) and **denied**
(`CONFIG_CHANGE_DENIED`) attempt, report generation and failure, report purges and model actions. No-op saves are not
recorded as changes. The audit trail is admin-only. Schema-level rejections of a body that is not a JSON object are
answered 422 by FastAPI before the handler runs and are therefore **not** audited.

## 7. Container and network security

- Non-root processes: API and consumer (uid 10001), Spark (10001), nginx (101), Kafka (1000). PostgreSQL and Redis
  drop root after their entrypoint. No container is privileged or adds capabilities.
- Host ports bound to `127.0.0.1` only. **The API is not published**; nginx is the single HTTP entry point.
- `ml/artifacts` and `data/synthetic` are mounted **read-only**. Spark mounts only `backend/`, `ml/`, `streaming/` and
  `simulator/`, so it cannot read `.env` (fixed in Phase 12 and checked by a container test).
- Spark UI kill actions are disabled. The UI itself is unauthenticated and bound to 127.0.0.1.
- Every long-running service has a health check, a memory limit and `restart: unless-stopped`. Dependencies and base
  images are pinned to exact versions.
- Response headers: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`;
  nginx `server_tokens off`.

## 8. Secret handling

- Secrets exist only in `.env`, which is created by `scripts/new_env.py` with random values. `.env` and `.env.*` are
  gitignored (except `.env.example`, which holds placeholders only) and excluded from every Docker build context.
- Secrets reach containers as environment variables. Container tests assert that no image's baked-in environment
  contains a secret and that system-information endpoints return none.
- `scripts/scan_secrets.py` searches a directory or ZIP for the actual secret values from `.env` files
  (`--strict` also flags secret-bearing file names).
- Backups contain password and refresh-token hashes, so they are written **outside** the repository by default
  (`../FraudStream-backups`). `.env` is never included in a backup.
- `app-init` creates accounts only if they do not exist and never resets a password.

## 9. Automated security checks (CI)

Defined in `.github/workflows/ci.yml` (job `security`, on every PR and push to `main`) and `full-stack.yml`. Details
and local reproduction: [CONTRIBUTING.md §11](CONTRIBUTING.md#11-continuous-integration).

| Check | Tool | Blocking? |
|---|---|---|
| Forbidden files in the repository (env files, `.dev-credentials`, databases, dumps, backups, keys), Argon2 hashes and private keys inside files, required `.gitignore` rules, placeholder-only `.env.example` | `scripts/ci/check_repo_hygiene.py` | yes |
| Secrets in the code **and the whole Git history** | gitleaks 8.30.1 (container image pinned by digest; output redacted) | yes |
| Python dependency advisories (`backend/requirements*.txt`, `infrastructure/spark/requirements-spark.txt`) | pip-audit 2.10.1 (PyPI / OSV advisory data) | yes, for advisories **not** in the reviewed baseline |
| JavaScript dependency advisories (`frontend/package-lock.json`, production and dev) | `npm audit` | yes, same baseline gate |
| Base-image OS packages of the built app and frontend images | Trivy 0.76.2 (image pinned by digest), HIGH/CRITICAL with an available fix | **no, report only** (weekly / manual `full-stack.yml`) |
| GitHub Actions supply chain | every third-party action pinned to a commit SHA; Dependabot proposes updates; workflows run with `permissions: contents: read` | — |

One tool per concern: Trivy is limited to OS packages (`--pkg-types os`), because pip-audit and npm audit already
gate the application libraries. CI uses **no repository secrets**. The integration and E2E stacks get random
credentials generated on the runner (`scripts/ci/ci_env.py`), masked with `::add-mask::` and never written to the
repository. The fixed passwords in `backend/tests/conftest.py` and the integration tests (e.g. `Admin-Pass-12345`) are
**test-only** values for throw-away SQLite or temporary databases. They are not used by any deployed stack.

## 10. Dependency findings (reviewed 2026-11-16, Phase 15)

Dependencies stay pinned; CI never upgrades them on its own. In Phase 15 each advisory in the Phase 14 baseline (123)
was read and classified, the applicable ones were fixed by upgrading, and the full regression was re-run (results in
[DEVELOPMENT_STATUS.md](DEVELOPMENT_STATUS.md), Phase 15). `.github/security/audit-baseline.json` now holds the
**18** remaining entries, a strict subset of the reviewed 123 (no entry was added). CI still blocks anything new.

### 10.1 Fixed (105 advisories)

| Package | Before → after | Advisories | Applicable to FraudStream? | Evidence |
|---|---|---|---|---|
| **PyJWT** | 2.10.1 → **2.15.1** | 17 | **Two were reachable**: unknown `crit` header accepted (GHSA-752w-5fwx-jx9f) and signatures with non-Base64URL characters accepted (GHSA-hxm8-2xgr-2p9m). Both need the server's HMAC secret to forge, so they enable token *malleability*, not forgery. The other 15 concern `PyJWK`/`PyJWKClient`, JWK import, mixed HMAC+asymmetric allow-lists or PEM handling, none of which FraudStream uses (fixed secret, `algorithms=["HS256"]`). PYSEC-2025-183 (key length) is disputed; the app enforces ≥ 32 characters. | `backend/tests/test_phase15_jwt.py` (21 tests): **2 fail on 2.10.1, all pass on 2.15.1**. alg `none`/HS384/HS512, wrong or empty secret, tampering, missing `exp`/`sub`, expiry, token type, untrusted role claim, malformed headers. |
| **Starlette** (via **FastAPI**) | 0.41.3 → **1.10.0** (FastAPI 0.115.6 → **0.163.1**) | 7 | Partly: Host-header / request-URL reconstruction (nginx sets `Host`), form-limit and multipart DoS on the admin upload. `StaticFiles` / `FileResponse` Range and Windows UNC issues: not used. | Full backend suite unchanged (203 passed, 2 skipped, as before). Starlette is pinned explicitly because FastAPI only sets a lower bound. `HTTP_422_UNPROCESSABLE_ENTITY` → `…_CONTENT` (same value; the old name is deprecated). |
| **python-multipart** | 0.0.19 → **0.0.33** | 6 | Partly: DoS parsing on the admin-only dataset upload; the path-traversal one needs `UPLOAD_KEEP_FILENAME`, not used. | upload validation tests pass |
| **Pillow** | 11.0.0 → **13.0.0** | 55 | Low: only reportlab uses it, for images the application embeds itself (no user-supplied images). reportlab 4.2.5 is kept (requires `pillow>=9`). | every PDF report test passes (incl. the CJK font test in the Linux app image) |
| **pytest** | 8.3.4 → **9.2.0** | 1 | Test tooling only (`/tmp/pytest-of-<user>` on shared UNIX hosts). | suite passes |
| **react-router-dom** | 6.28.1 → **6.30.6** | 4 | **Yes, one path**: the post-login redirect used `location.state.from`, so `http://host//evil.example` became a protocol-relative redirect. Also hardened in the app: `safeRedirect()` in `pages/Login.tsx` accepts only `/…` paths without `//`, backslash or control characters. | `frontend/src/test/phase15.test.tsx` (14 tests) |
| **vite / postcss / vitest / playwright** (dev only) | vite 5.4.11 → **5.4.21**, postcss 8.4.49 → **8.5.33**, vitest 2.1.8 → **2.1.9**, @playwright/test 1.49.1 → **1.65.1** | 22 | Development and test tooling only; not in the production bundle. | Vitest, typecheck, build, both E2E profiles |

Base image: `fraudstream-frontend:local` moved from `nginxinc/nginx-unprivileged:1.27-alpine` (Alpine 3.21.3,
**68** HIGH/CRITICAL, 3 critical) to `1.30.6-alpine3.24` pinned by digest, plus `apk upgrade --no-cache` within the
3.24 branch at build time. The unpatched new base still had 13 fixable findings (brotli, curl, expat, libxslt, musl,
zlib); after the upgrade step Trivy 0.76.2 reports **0** findings at any severity. The upgrade step makes a rebuild
pick up later 3.24 patches, so the image is reproducible per build date, not bit-for-bit. `fraudstream-app:local`
(Debian 12.15): **0** HIGH/CRITICAL. The Spark image was not changed (see 10.2).

### 10.2 Remaining (18 advisories, in the baseline)

| Package | Advisories | Classification | Reasoning | Plan |
|---|---|---|---|---|
| **pyarrow 18.1.0** (Spark image only) | 1 (PYSEC-2026-113 / GHSA-rgxp-2hwp-jwgg, fixed 23.0.1) | **Not applicable**, deferred | Use-after-free when reading an Arrow IPC **file** with pre-buffering. PySpark's `mapInPandas` / `applyInPandasWithState` exchange Arrow IPC **streams** between the JVM and Python workers; FraudStream never reads Arrow files, and all input is its own Kafka traffic. | Upgrade with the next Spark image rebuild (3.75 GB image + Spark integration suite). |
| **react-router 6.30.6** | 2 (GHSA-wrjc-x8rr-h8h6 backslash open redirect; GHSA-337j-9hxr-rhxg SSR hydration) | **Mitigated / not applicable**, deferred | The fix exists only in 7.18 (major migration). Backslash redirect: the one dynamic redirect target goes through `safeRedirect()`, which rejects backslashes; every other `<Link>` target is a fixed internal prefix. SSR hydration: FraudStream is a client-only SPA (no SSR). | React Router 7 migration. |
| **vite 5.4.21** + esbuild 0.21.5 | 9 | **Accepted (dev only)** | Dev-server issues: `server.fs` bypasses, launch-editor on Windows, cross-origin requests. The dev server is bound to 127.0.0.1 and not used in any container. Production is static files served by nginx. | Toolchain refresh (Vite ≥ 6.4.4). |
| **vitest 2.1.9**, `@vitest/mocker`, tinypool, vite-node | 6 (2 critical labelled) | **Accepted (dev only)** | The critical ones need the Vitest UI/API server listening (not installed or enabled; tests run with `vitest run`) or attacker-controlled tinypool options. vitest 3.x still bundles the affected tinypool; the fix is vitest ≥ 4.1.12, which needs Vite ≥ 6. | Same toolchain refresh. |

How to re-check: [CONTRIBUTING.md §11](CONTRIBUTING.md#11-continuous-integration) (pip-audit, npm audit,
`scripts/ci/audit_gate.py`).

## 11. Known limitations (not fixed)

- **No TLS** anywhere: browser → nginx, nginx → API, and services → PostgreSQL, Redis and Kafka. Before exposing the
  dashboard beyond localhost, terminate TLS at nginx and set `SECURE_COOKIES=true`.
- **Kafka has no authentication or ACLs** (PLAINTEXT). Anything that can reach `127.0.0.1:9092` can produce and
  consume. Redis and PostgreSQL use passwords only.
- No Content-Security-Policy header; no read-only root filesystems, `cap_drop: [ALL]` or custom seccomp/AppArmor
  profiles; no CPU limits.
- JWT secret rotation invalidates every session (no key ids). No account lockout beyond the rate limit, and no MFA.
- The audit trail is an ordinary table: anyone with database credentials could alter it. There is no hash chaining or
  WORM storage.
- **No high availability**: single host, single Kafka broker (RF=1), single PostgreSQL and Redis.
- Backups are unencrypted local directories with no off-site copy or schedule.
