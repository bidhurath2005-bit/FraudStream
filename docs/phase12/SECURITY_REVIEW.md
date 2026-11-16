# FraudStream — Phase 12 security review

Scope: authentication, authorization, input validation, secrets/configuration, containers. Method: code review +
reproduction on the **isolated** stack (`fraudstream-p12`) or in the backend test client; every fix has a test.
Severity is relative to this project's deployment model (single host, services on a private Docker network, all host
ports bound to 127.0.0.1, synthetic data).

## 1. Findings fixed in Phase 12

| # | Finding | Severity | Evidence | Fix | Test |
|---|---|---|---|---|---|
| S1 | **Login rate limit bypass / spoofable client IP.** nginx *appended* to a client-supplied `X-Forwarded-For`; uvicorn runs with `--forwarded-allow-ips *`, and uvicorn 0.32.1 (`get_trusted_client_host`) then returns the **left-most** entry — the value the client chose. A new value per attempt = a new login bucket (unlimited password guessing), and `LOGIN_FAILED` audit rows record the forged IP. | High | reproduced (§3): 15 attempts, all processed, 0 × 429 | nginx **sets** `X-Forwarded-For $remote_addr` (both API locations). The API is not published, so nginx is the only client-facing hop. | `scripts/probe_login_rate_limit.py` before/after (§3); container test suite |
| S2 | **Spark container could read every live secret.** It bind-mounted the whole repository (`./:/app:ro`), including `.env` (JWT secret, DB/Redis passwords, account passwords). | Medium | `docker exec … test -e /app/.env` succeeded | Mount only `backend/`, `ml/`, `streaming/`, `simulator/` and the health-check script, read-only. | `test_spark_container_cannot_read_the_env_file` |
| S3 | **Optimistic concurrency could be bypassed** by omitting `expected_updated_at` on `PATCH /risk/rules/{code}`, `PUT /risk/settings`, `PUT /settings/{key}` (silent overwrite of a concurrent change). | Medium | code + 11C limitation | Version now **mandatory**: values are validated first (422 unchanged), then a missing version → **428 Precondition Required**, stale → 409; `null` is accepted only for a setting that was never saved. Rejections are audited (`*_REJECTED`, `status_code` 428). The dashboard always sent it; 9 requests in older backend tests (5 in `test_models_and_ops.py`, 4 in `test_config_changes.py`) were updated to send it (no assertion changed). | `test_configuration_updates_without_a_version_are_rejected_428_and_audited`, `test_null_version_…`, `test_invalid_values_are_still_reported_422_…` |
| S4 | **CSV formula injection in the legacy alerts export** (`GET /reports/alerts.csv`, Investigation Center "Export CSV"). `customer_id`/`account_id` are free text (1–64 chars) set by whoever ingests an event; `=HYPERLINK(…)` reached the CSV unchanged. The Phase 11C report exports already had the guard. | Medium | test reproduces it | Same guard as 11C (`csv_value`: leading `= + - @ TAB CR` → `'` prefix) on every text column. | `test_alerts_csv_neutralises_formulas_from_ingested_text` |
| S5 | **HTTP 500 for out-of-range integers** on 11 GET routes (`page=10^20`, ids `10^21`, `minutes`, `ids=`): the value reached the driver (SQLite `OverflowError`; PostgreSQL `integer out of range`). `limit=-1` on `/drift/history` and `/simulation/runs` meant *unlimited* on SQLite. | Low | probe of every GET route × 12 hostile values | Shared bounds (`app/api/params.py`): page 1–100,000, ids 1–2,147,483,647 (PostgreSQL integer), `minutes` 1–43,200, `limit ≥ 1`. | `test_no_get_route_answers_5xx_for_hostile_parameter_values` (all routes, all params), `test_negative_and_zero_limits_…` |
| S6 | **API rate-limit bucket per arbitrary bearer string.** The key was the last 24 characters of the `Authorization` header, so random bearer values each got a fresh 1,200/min bucket. | Low | code | Valid access token → per-token bucket (unchanged); missing/invalid token → per-client-address bucket. | `test_invalid_bearer_tokens_fall_back_to_the_client_address_bucket` |
| S7 | Spark UI (no authentication, 127.0.0.1 only) allowed killing jobs/stages. | Low | Spark default | `spark.ui.killEnabled=false`. | config |
| R1 | (integrity) **Redis outage silently changed Spark's scoring rules**: the runtime-config watcher fell back to `ml/artifacts/runtime_config.json` (a stale copy written by a host-run API on 2026-11-14) or to built-in defaults. | Medium | code | Keep the last configuration loaded from Redis (`source = "redis (last known; Redis unavailable)"`). | `test_runtime_watcher_keeps_the_last_redis_config_during_a_redis_outage`, container Redis-outage test |
| R2 | (status) With Redis down the API reported the processor as **"NOT RUNNING"** while Spark kept processing. | Low | code | `UNKNOWN (Redis unavailable: processor heartbeats cannot be read)`; header shows it as a warning. | `test_redis_error_is_reported_as_unknown_not_as_not_running`, `phase12.test.tsx` |

## 2. Verified, no change needed

- **Passwords:** Argon2id (`argon2-cffi` defaults), ≥ 12 characters with ≥ 3 character classes, constant-time path for
  unknown users (verification against a dummy hash), never returned by any endpoint (`/users` has no hash — tested).
- **Tokens:** HS256 JWT, `exp` and `sub` required, `type=access`, 30-minute lifetime; refresh tokens are random,
  stored as SHA-256, rotated on every use, re-use revokes all of the user's tokens; refresh cookie `HttpOnly`,
  `SameSite=Strict`, path `/api/v1/auth`. `Secure` is off because the dashboard is plain HTTP on 127.0.0.1 (see §4).
- **Login rate limit:** 10/min per client (Redis-backed, in-process fallback reported in `/system/status`); enforced
  (probe in §3 after the S1 fix).
- **RBAC:** configuration changes, model activation, user management, dataset upload, audit trail and system-activity
  reports are admin-only on the server (dependency runs before body parsing, so an analyst gets 403 for any body);
  denials are audited (`CONFIG_CHANGE_DENIED`). Covered by existing and Phase 11C tests.
- **SQL injection:** all queries are SQLAlchemy expressions / bound parameters; the only f-string SQL in the scripts
  uses catalog table names, never input. Injection strings in filters are treated as data (test: 0 rows, table intact).
- **Path traversal:** no endpoint opens a user-supplied path; report downloads look up a UUID and a section id from the
  stored report; dataset uploads keep only the base name for provenance and store as `<kind>-<sha256[:12]>.csv`.
  Traversal strings → 404/422 (test).
- **Unexpected JSON fields:** request models forbid extra fields (`POST /reports`, rule/setting updates) → 422 (test).
- **Invalid time ranges:** reports reject naive datetimes, `start ≥ end`, > 366 days (11C tests); analytics windows bounded.
- **Secrets:** `scripts/scan_secrets.py` — no secret value from `.env` / `.env.isolated` in any repository file or in
  the backups directory (the dump holds hashes only); `.env*` are gitignored and dockerignored; no secret in any image
  environment (container test); system-information endpoints contain no secret (test); the Spark UI environment page
  (110 KB) contains none of the secret values (checked).
- **Containers:** API, consumer (uid 10001), Spark (10001), frontend (101), Kafka (1000) run as non-root; PostgreSQL
  (70) and Redis (999) servers drop root after their entrypoint. No container is privileged, none adds capabilities.
  Host ports: 127.0.0.1 only; the API is not published. Model artifacts and datasets mounted read-only; every
  long-running service has a health check, a memory limit and `restart: unless-stopped`. Dependencies are pinned.

## 3. Reproduction of S1 (before / after)

`scripts/probe_login_rate_limit.py --base http://127.0.0.1:18080 --attempts 15` (isolated stack, default limit
10/min, wrong password for random non-existent users, fresh rate-limit window for each series):

| Build | No `X-Forwarded-For` | New forged `X-Forwarded-For` per attempt |
|---|---|---|
| before (nginx appended) | 10 × 401, then **5 × 429** | **15 × 401, 0 × 429 — limit bypassed**; audit rows recorded the forged addresses (`198.51.100.1` … `.15`) |
| after (nginx sets `$remote_addr`) | 10 × 401, 5 × 429 | 10 × 401, **5 × 429** — limit enforced |

Evidence: `xff-probe-before-fix.txt`, `xff-probe-after-fix.txt` (restore-evidence folder, outside the repository).

## 4. Remaining limitations (not fixed; documented)

- **No TLS** anywhere (browser → nginx, nginx → API, services → PostgreSQL/Redis/Kafka). Acceptable only because every
  port is bound to 127.0.0.1 and the services share a private bridge network on one host. Behind any real network,
  terminate TLS at nginx and set `SECURE_COOKIES=true`.
- Kafka has **no authentication/ACLs** (PLAINTEXT listeners); anything that can reach 127.0.0.1:9092 can produce and
  consume. Redis and PostgreSQL use passwords only.
- No Content-Security-Policy header; no read-only root filesystems, no `cap_drop: [ALL]`, no seccomp/AppArmor profile
  beyond Docker defaults; no CPU limits.
- JWT secret rotation invalidates all sessions (no key ids); no account lockout beyond the rate limit; no MFA.
- Spark UI is unauthenticated (localhost only).
- Single host, single broker (RF=1), single PostgreSQL and Redis: **no high availability**, no replication.
- The audit trail is an ordinary table: an attacker with database credentials could alter it (no hash chaining /
  WORM storage).
