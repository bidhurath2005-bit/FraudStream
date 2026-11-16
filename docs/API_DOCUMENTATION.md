# FraudStream — API documentation

The API is a FastAPI application (`backend/app/main.py`, routers in `backend/app/api/routes/`). Everything in this
document was checked against the generated OpenAPI schema and the handler code. The live, machine-readable schema is
served by the running application:

| | Distributed profile (through nginx) | Host-run API |
|---|---|---|
| Base URL | `http://127.0.0.1:8080/api/v1` | `http://127.0.0.1:8000/api/v1` |
| Swagger UI | `http://127.0.0.1:8080/api/docs` | `http://127.0.0.1:8000/api/docs` |
| OpenAPI JSON | `/api/openapi.json` | same |

In the containerised stacks the API is **not** published on the host. All requests go through nginx on the same
origin as the dashboard.

---

## 1. Conventions

### Authentication

- `POST /auth/login` returns a short-lived **access token** (JWT, HS256, default 30 min) and sets a **refresh token**
  cookie (`HttpOnly`, `SameSite=Strict`, path `/api/v1/auth`, default 7 days).
- Send the access token on every other request: `Authorization: Bearer <access_token>`.
- `POST /auth/refresh` exchanges the cookie for a new access token **and rotates the refresh token**. Presenting a
  refresh token that was already rotated revokes **all** of that user's refresh tokens (theft detection).
- Public endpoints: only `GET /health` and `POST /auth/login` / `/auth/refresh` / `/auth/logout`.

### Roles

| Role | Meaning |
|---|---|
| **Any** | any authenticated user (`ANALYST` or `ADMIN`) |
| **Admin** | `ADMIN` only; others receive `403 {"detail": "administrator role required"}` |

The role check runs **before** the request body is parsed, so an analyst receives 403 whatever the body contains.
Denied *configuration* changes are written to the audit trail as `CONFIG_CHANGE_DENIED`.

### Rate limits

| Limit | Default | Key | Response |
|---|---|---|---|
| Login | 10 / minute | client address (set by nginx to the connecting address) | `429 {"detail": "too many login attempts"}` + `Retry-After` |
| All other `/api/v1/*` except `/health` | 1,200 / minute | valid access token; otherwise client address | `429 {"detail": "rate limit exceeded"}` + `Retry-After` |

Limits are counted in Redis in the distributed profile, with an in-process fallback while Redis is unavailable.
`GET /system/status` reports which backend is active.

### Response and error format

- JSON everywhere except CSV/PDF downloads and the SSE stream.
- Timestamps are ISO-8601 UTC.
- Paginated lists return `{"items": [...], "total": N, "page": P, "size": S}`.
- Errors: `{"detail": "<message>"}`. Request-validation errors (FastAPI/Pydantic) return HTTP 422 with:

  ```json
  {"detail": "validation error",
   "errors": [{"loc": ["query", "size"], "msg": "Input should be less than or equal to 200", "type": "less_than_equal"}]}
  ```

- Every response carries `X-Request-ID`, plus `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` and
  `Referrer-Policy: no-referrer`.
- Unhandled errors return `500 {"detail": "internal server error"}` with no internal details.

### Status codes used

| Code | When |
|---|---|
| 200 / 201 / 202 / 204 | success; 201 for created resources, 202 for accepted ingestion/training, 204 for logout and deletes |
| 401 | missing, invalid or expired token; wrong credentials (`"invalid username or password"`) |
| 403 | wrong role; acting on an alert or investigation assigned to someone else; re-opening a closed item as an analyst |
| 404 | unknown id or code |
| 409 | illegal state transition; stale `expected_updated_at`; alert already linked; model action while `ml/artifacts` is read-only; download of a FAILED report |
| 413 / 415 | dataset upload too large / not a CSV |
| 422 | invalid body or parameter values (including out-of-range integers, unknown fields where the model forbids them, invalid configuration values) |
| 428 | configuration update without `expected_updated_at` |
| 429 | rate limit |
| 503 | `/health` while the database is down or the API is starting; Kafka unavailable for ingestion or simulation in the distributed profile |

### Shared parameter bounds

Defined in `backend/app/api/params.py`. Out-of-range values return 422, never 500:

| Parameter | Bounds |
|---|---|
| `page` | 1 – 100,000 |
| integer path ids | 1 – 2,147,483,647 |
| analytics `minutes` (period) | 1 – 43,200 (30 days); `/analytics/timeline` and `/analytics/velocity` ≤ 1,440; `/analytics/geo` ≤ 10,080 |
| `limit` | ≥ 1 everywhere (values above an endpoint's cap are clamped) |

---

## 2. Health and system

| Method & path | Role | Description |
|---|---|---|
| `GET /health` | public | Liveness/readiness. 200 if the database answers, otherwise 503. |
| `GET /system/status` | Any | Profile, stream processor (from live heartbeats), database type, Redis state, live-feed broker, rate-limiter backend, loaded models, runtime-config publish status, start time, version. No secrets or environment variables. |

`GET /health` response:

```json
{"status": "ok", "database": "up", "model_loaded": true, "version": "1.0.0",
 "profile": "distributed", "stream_processor": "Spark Structured Streaming", "time": "2026-11-16T07:29:37Z"}
```

`status` is `ok`, `degraded` (database up, model not loaded) or `down`. `stream_processor` is
`Spark Structured Streaming`, `python-kafka processor (non-Spark)`, `NOT RUNNING`, `UNKNOWN (Redis unavailable: …)`,
or `in-process` in the local profile.

## 3. Authentication and users

| Method & path | Role | Body / parameters | Response |
|---|---|---|---|
| `POST /auth/login` | public | `{"username": str(1–64), "password": str(1–256)}` | `{"access_token", "token_type": "bearer", "expires_in": 1800, "user": {...}}` + refresh cookie. Failed attempts → 401 and a `LOGIN_FAILED` audit row. |
| `POST /auth/refresh` | cookie | — | same as login; rotates the cookie |
| `POST /auth/logout` | cookie | — | 204; revokes the refresh token |
| `GET /auth/me` | Any | — | `{"id", "username", "full_name", "role", "is_active", "created_at", "last_login_at"}` |
| `GET /users` | Any | — | list of users (no password hashes); analysts need it to display assignees |
| `POST /users` | Admin | `{"username": [A-Za-z0-9_.-]{3,64}, "full_name"?, "password": ≥ 12 chars with 3 of lower/upper/digit/symbol, "role": "ADMIN"\|"ANALYST"}` (extra fields → 422) | 201 user |
| `PATCH /users/{user_id}` | Admin | any of `full_name`, `role`, `is_active`, `password` | user |

## 4. Transactions and live stream

| Method & path | Role | Parameters / body | Notes |
|---|---|---|---|
| `GET /transactions` | Any | `page`, `size` (1–200, default 25), `customer_id`, `merchant_category`, `risk_category` (LOW/MEDIUM/HIGH), `min_risk` (0–100), `status`, `q` (≤ 64), `since`, `until`, `sort` (`processed_at`\|`event_time`\|`amount`\|`risk_score`), `order` | paginated transactions with their risk score |
| `GET /transactions/{tx_pk}` | Any | — | transaction + full `prediction` (model/anomaly scores, components, triggered rules, explanation, model version, inference ms) |
| `POST /transactions/ingest` | Any | one event matching the shared schema (`streaming/schemas/events.py`) | **Local:** scored synchronously, returns the decision. **Distributed:** produced to `transactions.raw` and acknowledged by Kafka → `202 {"profile": "distributed", "status": "accepted", "topic", "event_id"}`. Invalid events → 422 and a dead-letter row (`api-ingest`). Kafka down → 503. |
| `GET /stream/transactions` | Any (token) | `Authorization: Bearer` (preferred) or `?token=` | **Server-Sent Events.** Events: `connected` (sent once the subscription is active), `transaction` (scored event), keep-alive comment every 15 s. Through nginx the stream is unbuffered with a 1 h read timeout. |

Transaction fields: `id, event_id, transaction_id, customer_id, account_id, merchant_id, amount, currency, event_time,
processed_at, transaction_type, merchant_category, channel, device_id, ip_address, city, country, lat, lon,
location_simulated, status, synthetic, source, profile, pipeline_latency_ms, sim_is_suspicious, sim_scenario,
risk_score, risk_category, detection_method, recommended_action, model_score`.

`sim_is_suspicious` and `sim_scenario` are **simulator ground truth** kept for evaluation. They are never model inputs.

## 5. Simulation Lab

All endpoints require **Any** role. In the distributed profile they produce to Kafka (503 if Kafka is down, with no
local fallback); in the local profile they run the in-process pipeline.

| Method & path | Body / parameters |
|---|---|
| `GET /simulation/scenarios` | — (7 scenarios + descriptions) |
| `GET /simulation/status` | — (state, rate, counters of the current run) |
| `POST /simulation/start` | `{"mode": "normal"\|"fraud"\|"mixed"\|"high_volume"\|"custom", "tps": (0, 2000], "duration_seconds": (0, 86400] \| null, "max_events"?, "customer_count": 10–500000, "suspicious_pct": 0–100, "avg_amount", "scenarios"?: [..], "seed", "population_seed", "label"?}` → 201 |
| `POST /simulation/pause` · `/resume` · `/stop` | — |
| `POST /simulation/inject` | `{"scenario": "<name>", "customer_id"?, "seed"?, "population_seed"?, "customer_count"?}`. Emits one scenario's events, ending at "now". The local profile returns each event's score and alert id. |
| `GET /simulation/runs` | `limit` (≥ 1, default 20) |
| `POST /simulation/runs/{run_id}/replay` | — → 201 |
| `GET /simulation/configs` · `POST /simulation/configs` | `{"name": 2–80 chars, "config": <start body>}` → 201 |
| `POST /simulation/configs/{config_id}/run` · `DELETE /simulation/configs/{config_id}` | → 201 / 204 |

## 6. Alerts, investigations and customers

Alerts are created automatically for MEDIUM and HIGH transactions.

| Method & path | Role | Parameters / body |
|---|---|---|
| `GET /alerts` | Any | `page`, `size` (1–200), `status` (comma list), `risk_category` (MEDIUM/HIGH), `detection_method`, `customer_id`, `assigned` (`me`\|`unassigned`\|`any`), `q`, `rule`, `since`, `sort` (`created_at`\|`risk_score`\|`updated_at`), `order` |
| `GET /alerts/summary` | Any | counts by status/category |
| `GET /alerts/{alert_uid}` | Any | alert + transaction + prediction (components, rules, explanation) + notes + audit + linked investigation |
| `PATCH /alerts/{alert_uid}/status` | Any* | `{"status": ..., "note"?: ≤ 5000}` |
| `POST /alerts/{alert_uid}/assign` | Any* | `{"user_id": int \| null}` |
| `POST /alerts/{alert_uid}/notes` | Any* | `{"body": 1–5000 chars}` → 201 |
| `GET /alerts/{alert_uid}/audit` | Any | audit rows for this alert |
| `GET /investigations` | Any | `page`, `size` (1–100), `status`, `assigned` (`me`\|`any`), `customer_id` |
| `POST /investigations` | Any | `{"alert_uids": [1–100], "title": 3–200, "priority": LOW\|MEDIUM\|HIGH, "assigned_to_id"?}` → 201. All alerts must belong to one customer (422) and not be linked yet (409). |
| `GET /investigations/{case_uid}` | Any | investigation + alerts + notes |
| `PATCH /investigations/{case_uid}/status` | Any* | `{"status": ..., "final_classification"?, "findings"?}` |
| `POST /investigations/{case_uid}/assign` · `/notes` · `/alerts` | Any* | `{"user_id"}` · `{"body"}` · `{"alert_uid"}` |
| `GET /customers/{customer_id}` | Any | aggregate profile (counts, amounts, devices, cities, categories, risk distribution), last 100 transactions, last 50 alerts; 404 if no transactions |

\* An analyst can act only on items that are unassigned or assigned to them; otherwise 403. Admins can act on any
item.

**State machines** (illegal transitions → `409 "illegal alert transition A -> B"`):

```text
Alert:          NEW → UNDER_REVIEW | DISMISSED
                UNDER_REVIEW → CONFIRMED_SUSPICIOUS | DISMISSED | NEW
                CONFIRMED_SUSPICIOUS | DISMISSED → CLOSED | UNDER_REVIEW
                CLOSED → UNDER_REVIEW            (admin only)
Investigation:  OPEN → IN_PROGRESS | CLOSED
                IN_PROGRESS → RESOLVED | OPEN
                RESOLVED → CLOSED | IN_PROGRESS
                CLOSED → IN_PROGRESS             (admin only)
```

Moving to `RESOLVED` requires `final_classification` ∈ {`CONFIRMED_SUSPICIOUS`, `NOT_SUSPICIOUS`, `INCONCLUSIVE`}
and `findings` of at least 10 characters (otherwise 422).

## 7. Analytics and infrastructure

All endpoints require **Any** role. Optional `minutes` restricts the data to `transactions.processed_at` within the
last N minutes, and responses then include `period {window_minutes, basis, label}`.

| Method & path | Parameters | Returns |
|---|---|---|
| `GET /analytics/overview` | — | processed totals, throughput (60 s), flagged counts, open high-risk alerts, detection rate vs simulator labels, latency p50/p95 over the last 15 min |
| `GET /analytics/timeline` | `minutes` (1–1440, default 30), `bucket_seconds` (5–3600, default 60) | zero-filled volume and flagged counts per bucket |
| `GET /analytics/risk-distribution` | `minutes`? | risk-score histogram |
| `GET /analytics/patterns` | `minutes`? | per simulator scenario: transactions, `labelled_suspicious`, flagged, flag rate, avg risk; rule hits (basis ≤ 20,000 flagged predictions) |
| `GET /analytics/confusion` | `minutes`? | TP/FP/FN/TN of the risk engine **vs simulator labels**, precision, recall, F1, FPR, analyst outcomes |
| `GET /analytics/merchants` | `minutes`?, `category`?, `limit` (≥ 1, clamped to 50) | flag rate by category; top merchants (ids only) |
| `GET /analytics/hourly` | `minutes`? | 24 buckets by UTC hour of **event** time |
| `GET /analytics/velocity` | `minutes` (1–1440, default 15), `limit` (≥ 1, clamped to 50) | most active customers in the window |
| `GET /analytics/geo` | `minutes` (1–10080, default 60), `min_risk` (0–100) | per simulated city centroid: transactions, flagged, HIGH/MEDIUM, avg risk; count of transactions without coordinates |
| `GET /infrastructure/status` | — | PostgreSQL, Redis, Kafka (topics, end offsets, consumer groups and lag), Spark progress per query (incl. checkpoint), persistence-consumer counters, dead letters |

## 8. Models, drift and datasets

| Method & path | Role | Notes |
|---|---|---|
| `GET /models` | Any | registry; `kind` filter (`supervised`/`anomaly`) |
| `GET /models/active` | Any | active models, test metrics, whether the API loaded them, `artifacts_writable` |
| `GET /models/{model_id}` | Any | metrics per split, confusion matrices, hyperparameters, dataset, artifact path |
| `GET /models/compare` | Any | `ids` (comma list) or `experiment` |
| `GET /models/experiments` · `/models/experiments/{experiment_id}` | Any | stored experiment summaries |
| `GET /models/artifacts` | Any | every file in `ml/artifacts` with size, mtime, SHA-256; registry.json vs database consistency |
| `GET /models/live-inference` | Any | `limit` (10–20000, default 5000); measured inference latency per model version |
| `GET /models/training/status` | Any | state of a background training job |
| `POST /models/train` | Admin | `{"quick": bool}` → 202. **409 while `ml/artifacts` is mounted read-only (the default).** |
| `POST /models/sync` | Admin | re-read `registry.json` into the database |
| `POST /models/{model_id}/activate` | Admin | `{"reason": 5–500 chars}`; audited; 409 while read-only |
| `POST /models/rollback` | Admin | `{"kind": "supervised"\|"anomaly", "reason"}`; audited; 409 while read-only |
| `GET /drift/current` | Any | `persist` (default `true`): computes PSI of recent live features/scores vs the training reference and **stores a drift report** |
| `GET /drift/history` · `/drift/reports/{report_id}` | Any | `limit` ≥ 1 |
| `GET /features` | Any | feature names, version `fc-1.0`, descriptions |
| `GET /datasets` | Any | registered datasets (name, rows, SHA-256, provenance) |
| `POST /datasets/upload` | Admin | multipart `kind`, `file` (.csv, ≤ `MAX_UPLOAD_MB`) → 201; 413 / 415 on size or type |

## 9. Risk configuration and settings (optimistic concurrency)

| Method & path | Role | Description |
|---|---|---|
| `GET /risk/rules` | Any | 10 rules: code, name, family, description, enabled, weight, params, `param_bounds`, `last_change` (from audit), `editable` |
| `PATCH /risk/rules/{code}` | Admin | `{"enabled"?: bool, "weight"?: 0–1, "params"?: {name: number}, "expected_updated_at": <rule.updated_at>}` |
| `GET /risk/settings` | Any | `{"settings", "defaults", "updated_at", "last_change", "editable_by", "used_by"}` |
| `PUT /risk/settings` | Admin | full object: `medium_threshold` (1–99), `high_threshold` (2–100), `model_weight` (0–1), `anomaly_weight` (0–1), `anomaly_min_percentile` [0, 1), `block_simulation_enabled` (bool), **+ `expected_updated_at`** |
| `GET /settings` | Any | all settings (`risk`, `drift`, `simulation_defaults`, `preferences`) |
| `GET /settings/{key}` | Any | value, defaults, `updated_at`, last change, used-by, bounds |
| `PUT /settings/{key}` | Admin | `{"value": {partial}, "expected_updated_at": ...}`; `risk` must use `/risk/settings` (404 here). `drift`: `0 < moderate < significant ≤ 10`, `window_rows` 50–100,000, `1 ≤ min_rows ≤ window_rows`. |

### `expected_updated_at` — required on every configuration update

The three write endpoints above use optimistic concurrency, so a concurrent edit cannot be overwritten silently:

1. Read the item (`GET /risk/rules`, `GET /risk/settings` or `GET /settings/{key}`) and keep its `updated_at`.
2. Send that value unchanged as `expected_updated_at` with the update.

The server checks the request in this order:

| Step | Condition | Result |
|---|---|---|
| 1 | role is not ADMIN | **403**, audited as `CONFIG_CHANGE_DENIED` |
| 2 | values invalid (type, bounds, unknown key, non-finite number, cross-field rule) | **422** with the reason, audited as `*_REJECTED` |
| 3 | `expected_updated_at` **missing** | **428 Precondition Required**, audited |
| 4 | `expected_updated_at` differs from the stored `updated_at` | **409 Conflict** ("…changed by someone else since you loaded it; reload and review…"), audited |
| 5 | nothing actually changes | 200 with `changed: false` (not audited as a change) |
| 6 | otherwise | 200, one audit row per changed field or setting (old → new), runtime config republished |

`null` is accepted **only** when the `GET` returned `updated_at: null`, i.e. the item has never been stored. Settings
are seeded at start-up, so in practice always send the timestamp you read. On PostgreSQL the row is locked
(`SELECT … FOR UPDATE`) for the duration of the check. Numbers and booleans are **strict**: `"40"` or `"yes"` are
rejected rather than coerced.

Example (verified against a running API):

```http
GET /api/v1/settings/drift
→ 200 {"key": "drift", "value": {"moderate": 0.1, "significant": 0.25, "window_rows": 2000, "min_rows": 200},
       "updated_at": "2026-11-16T07:29:35.116526Z", ...}

PUT /api/v1/settings/drift   {"value": {"min_rows": 201}}
→ 428 {"detail": "expected_updated_at is required: send the updated_at value of the configuration you loaded (null if it has never been saved), so a concurrent change cannot be overwritten silently"}

PUT /api/v1/settings/drift   {"value": {"min_rows": 201}, "expected_updated_at": "2020-01-01T00:00:00Z"}
→ 409 {"detail": "this setting was changed by someone else since you loaded it; reload and review the current values"}

PUT /api/v1/settings/drift   {"value": {"min_rows": "x"}}
→ 422 {"detail": "min_rows has wrong type"}

PUT /api/v1/settings/drift   {"value": {"min_rows": 201}, "expected_updated_at": "2026-11-16T07:29:35.116526Z"}
→ 200 {"drift": {..., "min_rows": 201}, "changed": true, "updated_at": "2026-11-16T07:30:03.346240Z"}
```

Rule and risk-setting responses also include `changed_fields` / `changed` and `propagation` (`{"redis": "ok"|"error",
"file": "ok"|"error"}`), which says whether the new runtime configuration reached Redis (read by Spark) and the state
file. Changes apply to **new** transactions only; stored scores are never recomputed.

## 10. Reports

| Method & path | Role | Body / parameters |
|---|---|---|
| `GET /reports/types` | Any | 6 types with period basis, allowed filters and `available` for the caller |
| `POST /reports` | Any (system_activity: Admin) | `{"report_type": "fraud_activity"\|"risk_analytics"\|"alert_summary"\|"investigation"\|"model_performance"\|"system_activity", "start": tz-aware ISO, "end": tz-aware ISO, "risk_level"?, "alert_status"?, "investigation_status"?}` → 201 stored report. Naive datetimes, `start ≥ end`, > 366 days, a filter the type cannot apply, or extra fields → 422. |
| `GET /reports/generated` | Any | `page`, `size` (1–100), `report_type`; analysts never see system-activity reports |
| `GET /reports/generated/{uid}` | Any | stored snapshot (metadata, summary, sections, limitations, sources) |
| `GET /reports/generated/{uid}/download` | Any | `format=csv\|pdf`, `section`? → file with a safe name `fraudstream-<type>-<yyyymmdd>-<yyyymmdd>[-section]-<id8>.<ext>`; 409 for a FAILED report |
| `GET /reports/alerts.csv` | Any | `status`? — Investigation Center export (formula-injection guarded) |
| `GET /reports/model-evaluation` | Any | `format=md\|json` |
| `GET /reports/performance` · `/reports/benchmarks` | Any | stored latency/benchmark summaries |

## 11. Audit trail

| Method & path | Role | Parameters |
|---|---|---|
| `GET /audit` | Admin | `page`, `size` (1–200, default 50), `entity_type` (single value or comma list, e.g. `risk_rule,system_setting`), `actor` |

Audit row: `id, created_at, actor, action, entity_type, entity_id, field, old_value, new_value, details`. Actions
include `LOGIN`, `LOGIN_FAILED`, `ALERT_CREATED`, `ALERT_STATUS_CHANGED`, `ALERT_ASSIGNED`, `NOTE_ADDED`,
`INVESTIGATION_OPENED`, `INVESTIGATION_STATUS_CHANGED`, `INVESTIGATION_FINDINGS`, `ALERT_LINKED`, `RULE_UPDATED`,
`RISK_SETTINGS_UPDATED`, `SETTING_UPDATED`, `*_REJECTED`, `CONFIG_CHANGE_DENIED`, `REPORT_GENERATED`,
`REPORT_FAILED`, `REPORTS_PURGED`, `MODEL_ACTIVATED`, `MODEL_DEACTIVATED` and `MODEL_TRAINING_STARTED`.
