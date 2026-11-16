# FraudStream — Development Status

_Last updated: 2026-11-16 (end of Phase 15 — dependency security remediation, release packaging and fresh-extraction
verification; all 15 phases complete)._

This file records the **actual** state of the project. Every "PASS" below was executed on the
development machine; nothing is claimed from code inspection alone.

---

## 1. Environment (inspected, Phase 1)

| Item | Finding |
|---|---|
| OS | Windows 11 Home Single Language 10.0.26200 |
| CPU / RAM | Intel Core 7 240H, 16 logical cores, 15.7 GB RAM (≈3.7 GB free at inspection) |
| Disk | D: 96 GB free, C: 117 GB free |
| Python | 3.12.10 (used, venv `.venv`), 3.14.7 and 3.15.2 also installed |
| Node / npm | v26.13.1 / 11.20.0 |
| Java | **Not installed on the host** → Spark runs in the `fraudstream-spark` Docker image (OpenJDK 17.0.21) |
| Docker | 29.9.2 (Docker Desktop, VM: 16 CPUs, ~8 GB RAM), Compose v5.6.1 |
| Git | 2.56.0 (no repository created, per instructions) |
| Images | apache/kafka:3.9.1, postgres:16-alpine, redis:7-alpine, python:3.12-slim-bookworm, node:22-alpine, nginxinc/nginx-unprivileged:1.27-alpine; built: **fraudstream-spark:local** (3.75 GB as reported by `docker images`), **fraudstream-app:local** (1.97 GB — the xgboost wheel dominates), **fraudstream-frontend:local** (74.6 MB) |

Measured capacity: Kafka (1 GB limit) + PostgreSQL (512 MB) + Redis (256 MB) + Spark (3 GB limit, `local[8]`,
1 GB driver) run together in the Docker VM.

## 2. Execution profiles

| Profile | How it runs | Status |
|---|---|---|
| **A — LOCAL DEVELOPMENT** (FastAPI + SQLite + in-process pipeline + SSE + React) | host processes (uvicorn + Vite) **or** containers: `--profile local` (`api-local` + `frontend-local`, :8081) | **Working** — browser E2E 2/2 on the host-run stack and 2/2 on the containerised local stack (Phase 10). `/health`: `profile=local, stream_processor=in-process`. Uses no Kafka/Spark/Redis/PostgreSQL. |
| **B — FULL DISTRIBUTED** (Kafka + Spark Structured Streaming + PostgreSQL + Redis + persistence consumer + API + dashboard) | **all in containers**: `docker compose --env-file .env --profile distributed up -d --build` (dashboard :8080) | **Working and verified (Phase 10)**: 12/12 container integration tests (incl. crash/outage recovery), browser E2E 2/2, reconciliation PASS. Host-process mode (Phase 9 commands) still works. |
| Fallback: python-kafka processor (non-Spark) | opt-in `--profile python-processor` | Kept as a **separately labelled** development/fallback processor. Never started by the `distributed` profile; it and Spark refuse to run concurrently (Phase 8/9 guard). |

Profiles are selected explicitly; nothing switches between them. The distributed containers set
`FRAUDSTREAM_PROFILE=distributed`, which the settings validator rejects without Kafka, Redis and PostgreSQL, and the API never
falls back to the in-process pipeline: with Kafka down, Simulation Lab requests fail with **HTTP 503** naming Kafka (verified).
The active processor shown by `/health`, `/system/status`, `/infrastructure/status` and the dashboard header is derived from
live heartbeats: "Spark Structured Streaming" only while a fresh (< 30 s), non-terminated, non-failed Spark progress snapshot
exists; "python-kafka processor (non-Spark)" for the fallback; otherwise "NOT RUNNING".

## 3. Completed work

### Phases 1–7
Simulator, schema, feature engineering, datasets, ML training/evaluation/registry/explainability/drift, risk engine,
backend API, frontend (Login, Overview, Live Stream, Investigation Center, Alert & Investigation detail, Simulation
Lab), local E2E.

### Phase 8 — Kafka infrastructure
Compose infra (Kafka KRaft with persistent volume, PostgreSQL, Redis, topic init), 7 topics, idempotent producers,
python-kafka processor, idempotent persistence consumer (commit after DB commit, DB-outage blocking retries,
poison → DLQ), runtime-config watcher, infrastructure status, `scripts/verify_kafka_pipeline.py`.

### Phase 9 — Spark Structured Streaming
| Component | Location | Details |
|---|---|---|
| Spark image | `infrastructure/spark/Dockerfile`, `requirements-spark.txt` | Python 3.12 + OpenJDK 17 + PySpark **4.0.5** + `spark-sql-kafka-0-10_2.13:4.0.5` (jars pre-fetched at build); ML libs pinned identical to the backend (sklearn 1.5.2, xgboost 2.1.3) so artifacts load identically; non-root user; source + artifacts mounted **read-only**. |
| Compose service | `docker-compose.yml` → `spark` (profile `spark`) | `docker compose --env-file .env --profile spark up -d spark`; checkpoints in named volume `spark-checkpoints` at `${SPARK_CHECKPOINT_DIR:-/checkpoints/v2}`; `stop_grace_period: 60s`; Spark UI on 127.0.0.1:4040. |
| Streaming job | `streaming/spark/fraud_stream_job.py` | 6 queries (below), each with its own checkpoint; progress (batch, rows/s, duration breakdown, state rows, rows dropped by watermark, source end offsets, exceptions) → Redis `fraudstream:spark:progress` every 5 s; `terminated` snapshot on shutdown; safe SIGTERM handling. |
| UDF logic | `streaming/spark/logic.py` | Pure Python, host-testable; reuses `parse_event`, the shared feature module, `detect()` (ML + risk engine). |
| Worker daemon | `streaming/spark/daemon.py` (`spark.python.daemon.module`) | Pre-imports the ML stack and loads models before forking Python workers (removed 4–6 s warm-up batches). |

**Queries**

| Query | Path | Implements |
|---|---|---|
| `score` | raw → **Python validation (shared Pydantic schema)** → `groupBy(customer_id).applyInPandasWithState` → normal / suspicious / DLQ | malformed-event handling (invalid → `fraud.dlq`, stage `spark-validation`); per-customer behavioural state in the **checkpointed RocksDB state store**; **event-id dedup** from state; customer-level lateness (> 10 min behind the customer's latest event → DLQ `LATE_EVENT_BEYOND_TOLERANCE`, stage `spark-scoring`); shared features + ML inference + risk engine; output identical in format to the Phase 8 processor (`processor=spark-structured-streaming`) → existing persistence consumer unchanged. |
| `validated-dedup` | raw → **JVM-native validation** → `withWatermark(event_time)` → `dropDuplicatesWithinWatermark(event_id)` → `transactions.validated` | event-time watermark + event-id dedup |
| `window-tumbling_1m`, `window-tumbling_15m` | raw → JVM validation → watermark → tumbling windows → `features.windows` | tumbling windows (velocity, 15-min spend) |
| `window-sliding_5m` (1 m slide), `window-sliding_1h` (15 m slide) | same, sliding | sliding windows (5-min velocity, 1-hour behaviour) |

Window queries use `update` mode; rows later than the watermark (default `WATERMARK_DELAY=10 minutes`) are dropped
by Spark and counted (`rows_dropped_by_watermark` in progress). The JVM-native validator mirrors the Pydantic schema
(incl. rejecting unknown fields); parity is asserted by integration tests.

**Delivery semantics (Spark):** source offsets live in the checkpoint (not a consumer group); Kafka output is
at-least-once (a batch replayed after a crash can be re-emitted); the persistence consumer's idempotent writes absorb
replays → effectively-once records in PostgreSQL. Not end-to-end exactly-once.

### Phase 10 — Full distributed integration and containerisation

**Images**
| Image | File | Contents |
|---|---|---|
| `fraudstream-app:local` | `infrastructure/docker/app.Dockerfile` | python:3.12-slim + `backend/requirements.txt` (identical pins to the host venv and the Spark image's ML libs) + backend, ml (code only), simulator, streaming, benchmarks. Non-root user `app` (uid 10001), tini as PID 1. One image, four roles: API, persistence consumer, `app-init`, fallback processor. **Model artifacts are not baked in** — `ml/artifacts` is bind-mounted, **read-only by default**. |
| `fraudstream-frontend:local` | `frontend/Dockerfile`, `frontend/nginx/default.conf.template` | Multi-stage: `npm ci && npm run build` on node:22-alpine → nginx-unprivileged (non-root, port 8080). nginx serves the SPA (deep links → `index.html`) and reverse-proxies `/api` to the API container on the **same origin** (no CORS, refresh cookie stays first-party). SSE location has buffering off and a 1 h read timeout. Upstream is resolved per request through Docker DNS (`resolver 127.0.0.11`), so nginx needs no restart when the API container is recreated. |
| `fraudstream-spark:local` | unchanged (Phase 9) | now part of the `distributed` profile, with a health check. |

**Compose services (`docker-compose.yml`)**
| Service | Profile | Restart | Health check | Depends on |
|---|---|---|---|---|
| kafka, postgres, redis, kafka-init | (always) | unless-stopped / one-shot | unchanged from Phase 8 | — |
| `spark` | `distributed`, `spark` | unless-stopped | progress file written < 45 s ago **and** every streaming query active without an exception (`PROGRESS_FILE`, already supported by the job) | kafka healthy, kafka-init done, redis healthy |
| `app-init` (one-shot) | `distributed` | no | exit 0 | postgres healthy |
| `api` | `distributed` | unless-stopped | `GET /api/v1/health` = 200 (DB reachable) | infra healthy + kafka-init + app-init done |
| `persistence-consumer` | `distributed` | unless-stopped | heartbeat file < 30 s old (written by the consume loop every ~5 s, **also while it waits out a DB outage**) | infra healthy + kafka-init + app-init done |
| `frontend` | `distributed` | unless-stopped | `GET /healthz` | api healthy |
| `python-processor` | `python-processor` (opt-in) | unless-stopped | — | infra |
| `api-local`, `frontend-local` | `local` | unless-stopped | as api/frontend | — (SQLite in volume `local-data`) |

* **Networking:** everything runs on the private `fraudstream` bridge network (`kafka:29092`, `postgres:5432`, `redis:6379`,
  `api:8000`). Host ports are bound to 127.0.0.1 only. The **API is not published** — the only HTTP entry point is the
  dashboard's nginx (`127.0.0.1:8080`, or 8081 for local); Kafka/PostgreSQL/Redis/Spark UI keep their Phase 8/9 host ports.
* **Persistent volumes:** `postgres-data`, `kafka-data`, `redis-data` (AOF), `spark-checkpoints` (offsets, commits, RocksDB
  state), `app-data` (dataset uploads), `app-state` (derived `runtime_config.json`), `local-data`/`local-state` (local profile).
  Mount points are pre-created in the image and owned by the app user (Docker seeds an empty named volume with that ownership).
* **Secrets:** only in `.env` (gitignored, never copied into an image: `.dockerignore` excludes it) → container environment.
  `scripts/new_env.py --add-missing` appends new keys from `.env.example` with fresh random values and **never changes
  existing values** (unit-tested). Used once in Phase 10 to add `FRAUDSTREAM_ADMIN_PASSWORD` / `FRAUDSTREAM_ANALYST_PASSWORD`
  (22 random chars, satisfy the API password policy) and port/mount settings. `app-init` runs migrations and creates the
  `admin` / `analyst` accounts from those values; existing accounts are never modified.
* **Graceful shutdown:** tini forwards SIGTERM; uvicorn graceful timeout 10 s (stop grace 20 s), consumer 30 s (commits only
  after DB commit), Spark 60 s (flag-based graceful stop from Phase 9).

**Code changes (small, backwards compatible)**
1. `RUNTIME_CONFIG_DIR` setting (default: the artifacts dir, i.e. unchanged host behaviour): containers write the derived
   `runtime_config.json` to the `app-state` volume so `ml/artifacts` can stay read-only. Spark reads the config from Redis.
2. Model train/activate/rollback return **409 with an explicit message** when `ml/artifacts` is read-only, *before* changing
   anything (previously they would have failed half-way with a 500). `ARTIFACTS_MOUNT_MODE=rw` re-enables them.
3. Persistence consumer writes a local heartbeat file (`CONSUMER_HEALTH_FILE`) in addition to the Redis heartbeat.
4. `infrastructure/docker/healthcheck.py` (stdlib-only http / file / spark checks); `infrastructure/docker/app-init.sh`.
5. `.gitattributes` pins LF endings for shell scripts, Dockerfiles and the nginx template.

**Bugs found and fixed in Phase 10**
1. `local-data` / `app-data` volumes were created **root-owned** (mount points missing from the image) → the containerised
   local API could not open its SQLite file and dataset uploads in the distributed API would have failed. Fixed in the
   image; a write probe on `/data/uploads` and `/state` was added to the container tests.
2. Kafka integration fixture claimed to use Redis DB 15, but a db number in `REDIS_URL` (`/0`) overrides redis-py's `db=15`
   argument, so the tests shared heartbeat keys with the live stack's consumer (1 failure while the container stack was
   running). The fixture now forces DB 15 as documented; no assertion was changed.

### Phase 15 — Dependency security remediation, release verification, ZIP packaging

**Status: complete.** Nothing was pushed, published or uploaded. All results below were **executed in Phase 15**
(2026-11-16) unless marked "previous phase". The plan was written before any change; a source tarball, the status
file and the 17-file SHA-256 snapshot were kept in the session scratchpad.

**Dependency upgrades (each advisory read and classified; full table: SECURITY.md §10)**
| Package | Before → after | Advisories fixed | Applicable? |
|---|---|---|---|
| PyJWT | 2.10.1 → **2.15.1** | 17 | **2 reachable**: `crit` header accepted; non-Base64URL signature characters accepted. Both allow token malleability, not forgery, because both need the HMAC secret. The other 15 concern PyJWK/PyJWKClient/JWK/mixed-algorithm paths, which FraudStream does not use. |
| FastAPI / Starlette | 0.115.6 / 0.41.3 → **0.163.1 / 1.10.0** (Starlette pinned explicitly) | 7 | partly (Host/URL reconstruction, form limits on the admin upload) |
| python-multipart | 0.0.19 → **0.0.33** | 6 | partly (DoS on the admin upload) |
| Pillow | 11.0.0 → **13.0.0** (reportlab 4.2.5 kept) | 55 | low (only app-embedded images) |
| pytest (dev) | 8.3.4 → **9.2.0** | 1 | test tooling |
| react-router-dom | 6.28.1 → **6.30.6** | 4 | **yes, one path**: post-login redirect `//host` became protocol-relative; also hardened in the app with `safeRedirect()` |
| vite / postcss / vitest / @playwright/test (dev) | 5.4.11 / 8.4.49 / 2.1.8 / 1.49.1 → **5.4.21 / 8.5.33 / 2.1.9 / 1.65.1** | 22 | dev tooling only |
| Frontend base image | `nginx-unprivileged:1.27-alpine` (Alpine 3.21.3) → **`1.30.6-alpine3.24@sha256:f5116ed4…`** + `apk upgrade --no-cache` (as root, then back to uid 101) | Trivy HIGH/CRITICAL **68 → 0** (0 at any severity) | yes |

Baseline `.github/security/audit-baseline.json`: **123 → 18** entries, a strict subset (verified: 0 added). Remaining:
- pyarrow 18.1.0 (Spark image): the advisory concerns reading Arrow IPC *files*; PySpark uses IPC streams. Deferred to
  the next Spark image rebuild.
- react-router, 2: fixed only in 7.18. The backslash redirect is mitigated by `safeRedirect()`; SSR hydration does
  not apply.
- vite/esbuild, 9; vitest/mocker/tinypool/vite-node, 6: dev tooling. The fix needs Vite ≥ 6.4.4 / Vitest ≥ 4.1.12;
  vitest 3.x still bundles the flagged tinypool.

The Spark image was **not** rebuilt.

**Code changes (no behaviour change except the redirect guard)**
- `backend/requirements.txt`, `requirements-dev.txt`: pins above. 7 backend files: `HTTP_422_UNPROCESSABLE_ENTITY` →
  `HTTP_422_UNPROCESSABLE_CONTENT` (same value 422; the old name is deprecated in Starlette 1.x).
- `frontend/src/pages/Login.tsx`: `safeRedirect()` accepts only `/…` paths without `//`, backslash or control
  characters.
- `frontend/Dockerfile`: new base image + OS update step. `frontend/nginx/default.conf.template`: CRLF → LF, because
  `.gitattributes` requires LF. nginx accepted both; `nginx -t` passes and the image built from the extracted tree
  served the E2E run.
- New tests:
  - `backend/tests/test_phase15_jwt.py` (21): alg none/HS384/HS512, wrong/empty secret, tampering, missing exp/sub,
    expiry, token type, `crit`, Base64URL, missing user, role claim not trusted, malformed headers. **2 fail on PyJWT
    2.10.1; all 21 pass on 2.15.1.**
  - `frontend/src/test/phase15.test.tsx` (14).
- New `scripts/package_release.py`:
  - builds the archive from the gitignore-respecting file list (via a throw-away bare Git directory, so no
    repository is created in the project) plus the synthetic dataset;
  - refuses hygiene violations;
  - one `FraudStream/` root, sorted entries, fixed timestamps, `RELEASE_MANIFEST.sha256`, mode 0755 for `.sh`;
  - writes `FraudStream.zip.sha256`.
- Docs:
  - SECURITY.md §10 rewritten (fixed / remaining, with reasoning);
  - README (versions, security summary, roadmap); ARCHITECTURE (nginx version);
  - new `RELEASE_NOTES.md`;
  - two absolute `D:/fraudstream/...` paths made relative (this file, `docs/phase12/BACKUP_AND_RESTORE.md`).

**Tests executed in Phase 15**
| Suite | Where | Result |
|---|---|---|
| pytest default suite | Windows host, project `.venv` (upgraded) | **224 passed, 2 skipped** (203 + 21 new; the same 2 Windows-only skips) |
| pytest default suite + ruff `E9,F` + ruff `scripts/ci` | Linux `python:3.12-slim-bookworm`, non-root, with fonts, from the **staged release tree** | **226 passed, 0 skipped**; ruff clean |
| pytest default suite | Windows, **extracted ZIP** with its own new venv | **224 passed, 2 skipped** |
| Negative check | PyJWT 2.10.1 temporarily in a scratch venv | `test_phase15_jwt.py`: 2 failed (crit, Base64URL), 19 passed |
| Vitest / typecheck / build | host; and Linux `node:22-bookworm` from the staged tree (`npm ci`) | **78 passed** (64 + 14); pass; pass (JS 853 kB / CSS 23.8 kB), both places |
| Ruff full ruleset | host | 221 style findings: the **same set** as before Phase 15 (not enforced; Phase 14 debt) |
| Compose validation | `check_compose.py` (repo and extracted tree) + `compose config` of both profiles with a `new_env.py` env | 7/7 combinations valid; COPY sources present |
| pip-audit 2.10.1 / npm audit / audit gate | host | pip 87 → 1, npm 36 → 17; gate **PASS, 0 new** |
| Trivy 0.76.2 (pinned digest), OS packages | new images | app (Debian 12.15) **0**; frontend (Alpine 3.24.2) **0** (was 68) |
| Secret scan `scan_secrets.py` | repo (5 env files, 30 values); ZIP `--strict` (7 env files, 43 values) | no values found; no secret-bearing files |
| Repository hygiene | throw-away Git copy of the staged tree (300 tracked files) | PASS |
| gitleaks 8.30.1 (pinned digest) | same copy, `git` and `dir` mode | **no leaks** in either |
| Container suite incl. disruptive | isolated `fraudstream-p15`, fresh volumes, new images | **25 passed** (12 min 25 s) |
| Spark integration | `fraudstream-p15` | **6 passed** |
| Kafka integration | `fraudstream-p15` infrastructure | **12 passed** |
| Playwright E2E distributed (desktop + 390 px) | :18080 after `seed_traffic.py` | **12 passed** |
| Playwright E2E local (desktop + 390 px) | containerised local profile :18081 | **12 passed** |
| Backup → verify → restore | `backup.py` on `fraudstream-p15` → `restore.py` into the new `fraudstream-p15r` | COMPLETE in 2.6 s, 21 checksums, VERIFY OK; **RESTORE PASS**: 0 count mismatches, **16/16 fingerprints**, artifacts/datasets 0 mismatches; non-empty target refused (exit 1). The first attempt failed on a host-port clash (both env files had the default isolated ports), so `.env.p15r` was moved to 2xxxx ports. |
| Doc links | link checker (relative paths + anchors), repo and extracted tree | 128 links, 0 problems |

Not executed:
- any GitHub-hosted run;
- the restore refusal **on the live project** (blocked by the session's safety classifier; verified in Phases 12/14);
- the Spark image rebuild;
- the host-process local profile.

**Release archive and fresh-extraction verification**
- Staged tree: 301 files + `RELEASE_MANIFEST.sha256`.
- Archive checks: `testzip`; single root; no duplicates, case-duplicates, absolute or `..` paths, or symlinks; no
  forbidden files; every required file present; every manifest hash matches. **OK.** The 16 shipped model/dataset
  files are byte-identical to the accepted baseline.
- Release candidate RC2 (identical to the final archive except `DEVELOPMENT_STATUS.md`, `RELEASE_NOTES.md` and the
  test-results paragraph of `README.md`),
  extracted with Windows `Expand-Archive` into a new directory outside the project:
  1. Extracted tree: 302 files; manifest 301/301 OK; `app-init.sh` executable; no `.env` / `.venv` / `node_modules`;
     no reference to the development path.
  2. Documented setup: new venv + `pip install -r backend/requirements-dev.txt`; `scripts/new_env.py` → `.env` with
     random secrets; Compose valid.
  3. Images **built from the extracted tree**; local profile started as the isolated project `fraudstream-x15`, with
     artifacts and dataset mounted from the extracted folder. Healthy in 10 s.
  4. Smoke test, all passed:
     - health (profile local, model loaded); failed login → 401; analyst login; analyst → `/audit` → 403;
     - 8 scenario injections → **22 HIGH alerts** (e.g. score 82.5); 26 transactions;
     - `alert_summary` and `fraud_activity` reports COMPLETED; CSV + PDF downloaded (`%PDF-` checked).
  5. Playwright E2E from the extracted tree: **12 passed**, 24 screenshots.
  6. Stopped with `compose stop` (exit 0 / 143).
- The final archive was verified again in the same way. Its checksum is in `FraudStream.zip.sha256` next to the
  archive (an archive cannot contain its own hash).

**Packaging exclusions (classified, nothing deleted)**
| Item | Class | Why excluded |
|---|---|---|
| `.env`, `.env.isolated`, `.env.restore`, `.env.ci`, `.env.ci-r`, `.env.p15`, `.env.p15r` | private runtime data | secrets |
| `ml/artifacts/runtime_config.json` | derived runtime state | regenerated at start-up (also not in backups) |
| `.venv/`, `frontend/node_modules/`, `frontend/dist/`, `*.tsbuildinfo`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/` | temporary development data | reproducible from the lockfiles / build |
| `test-results/`, `frontend/e2e-results/`, `streaming/tests/spark-it-last.log` | temporary development data | generated test output (curated screenshots are in `docs/screenshots/`) |
| `../FraudStream-backups/`, `../p12-restore/` | private runtime data | outside the repository; contain password hashes |

Deliberately **included** although gitignored: `data/synthetic/` (2 files), which the documented application needs.

**Data integrity (verified at the end)**
- 17/17 model and dataset files SHA-256 identical to the pre-Phase-15 snapshot (= the Phase 13/14 snapshots).
- Live stack: stopped with `compose stop` for memory (approved procedure) and restarted with `compose start` (same
  containers).
  - PostgreSQL counts, 16 table fingerprints, Alembic 0002, risk rules / settings md5 and Spark commits are
    **identical** to the pre-stop snapshot (15,727 tx, 1,556 alerts, 23 investigations, 2,072 audit rows,
    42 reports).
  - **31/31 application partition offsets identical.** Only Kafka's internal `__consumer_offsets` advanced, because
    the consumer group re-joined.
  - Health OK; processor Spark.
- `compose start` also started the live project's two stopped **local-profile** containers; they were stopped again
  within a minute. Their SQLite database: all 22 table counts identical to the pre-Phase-15 backup (Alembic 0002,
  migration no-op, empty WAL); only the file timestamp changed.
- Backups: new additive `20261116T092850Z-fraudstream-pre-phase15` (verified); the 4 earlier backups pass `--verify`;
  107 files in total; nothing deleted.
- Docker: no volume removed. Added (throw-away, stopped): `fraudstream-p15_*` (8), `fraudstream-p15r_postgres-data`,
  `fraudstream-x15_*` (2). Images `fraudstream-app:local` / `fraudstream-frontend:local` rebuilt (the old ones kept
  as `:pre-phase15`); the live containers still run their previous image ids until they are recreated.

**Remaining limitations** (details: §8 and SECURITY.md §10.2 / §11):
- 18 accepted advisories: dev tooling, the react-router 7 migration, and pyarrow with the next Spark rebuild.
- The frontend image's `apk upgrade` step makes it reproducible per build date, not bit-for-bit.
- CI has never run on GitHub.
- Single host, no TLS, no high availability.

### Phase 14 — CI/CD, automated quality gates, security checks, final regression

**Status: complete.** Nothing was pushed or published; **no GitHub Actions run has happened** (no repository
exists). Everything below was executed locally; the distinction is stated per item.

**Owner decisions (asked, 2026-11-16):** LICENSE holder "FraudStream contributors"; approval to stop the live stack
for the isolated regression (as in Phase 12).

**Files created**
| File | Purpose |
|---|---|
| `.github/workflows/ci.yml` | PR / push to `main` / manual. Jobs: `backend` (pinned deps, `ruff --select E9,F`, full ruff on `scripts/ci`, pytest with JUnit + step summary; installs the app image's PDF fonts), `frontend` (`npm ci`, Vitest + JUnit, typecheck, build), `compose` (`check_compose.py`), `security` (hygiene, gitleaks full history, pip-audit + npm audit → baseline gate), `kafka-integration` (Kafka/PostgreSQL/Redis from compose as project `fraudstream-ci`, `pytest -m "integration and not spark"`). `permissions: contents: read`, concurrency cancel, timeouts. |
| `.github/workflows/full-stack.yml` | manual (input `disruptive`) + weekly. Frees disk, regenerates/verifies the dataset, CI env with test-only login limit 100, builds images, distributed up + wait, container suite, Spark IT, seed + E2E distributed, local profile up + seed + E2E local, Trivy OS scan (report only), summaries, artifacts. |
| `.github/dependabot.yml` | weekly GitHub-Actions updates only (app deps deliberately not auto-updated). |
| `.github/security/audit-baseline.json` | 123 reviewed, currently unfixed advisories (87 pip in 6 packages, 36 npm); CI blocks anything else. |
| `scripts/ci/ci_env.py` | throw-away env file from `isolated_env.py` (random secrets, own network, `ci-admin`/`ci-analyst`, `--e2e` → login limit 100 in that file only); `export` → `$GITHUB_ENV` with `::add-mask::` on GitHub only; refuses project `fraudstream`. |
| `scripts/ci/check_compose.py` | `docker compose config` for 7 combinations (default, 4 profiles, isolated override ×2), expected services, build contexts / Dockerfiles / COPY sources, bind sources, 127.0.0.1-only ports, no repo-root / env bind mounts. |
| `scripts/ci/check_repo_hygiene.py` | over `git ls-files -co --exclude-standard`: env files, credentials, DBs, dumps, backups, keys; Argon2 hashes / PEM keys / PGDMP headers inside files; required `.gitignore` rules; placeholder-only `.env.example`. |
| `scripts/ci/audit_gate.py` | pip-audit + npm-audit JSON vs baseline; blocks new, lists resolved; `--write-baseline`. |
| `scripts/ci/prepare_dataset.py` | regenerates the gitignored dataset (seeded) and checks `content_sha256` = the stored experiment's; verify-only if present. |
| `scripts/ci/seed_traffic.py` | 30 s `mixed` simulation + each scenario once through the public API before E2E on a fresh DB; refuses the live ports. |
| `scripts/ci/wait_for_stack.py`, `scripts/ci/junit_summary.py` | readiness wait (only the selected profile's services); JUnit → Markdown step summary. |
| `LICENSE` | MIT, "Copyright (c) 2026 FraudStream contributors" (matches `pyproject.toml` `license = MIT`). |

**Changed files (no runtime behaviour change)**
- Stale references fixed (comments/docstrings only): `ml/training/train.py`, `ml/datasets/public.py` (also the
  non-existent `scripts/import_dataset.py` → the real CLI / upload endpoint), `backend/app/risk_engine/engine.py`,
  `streaming/kafka/client.py`, `streaming/schemas/events.py` (`spark_schema.py` → `fraud_stream_job.py`),
  `frontend/playwright.config.ts` (`e2e_local.ps1` → `run_e2e.py`), `backend/requirements.txt` header comment,
  `.gitignore` comment. (Your own `.env` still has the old first-line comment; not touched.)
- 4 unused imports removed (`app/streaming/processor.py` `header_dict`; 2× `import os` in
  `streaming/tests/test_kafka_integration.py`) so the CI lint gate (`E9,F`) is clean. No assertion touched.
- `.gitignore`: CI outputs (`test-results/`, `e2e-distributed/`, audit JSON, Trivy reports, stack logs).
- One sentence in this file reworded (gitleaks generic-api-key false positive on "simulation/inject account_takeover").
- Docs: README (§5.6 results, new §5.7 CI, security bullet, roadmap, doc map), CONTRIBUTING (§5 lint row and
  expected results, new §11 CI incl. baseline procedure, local reproduction, seeding), SECURITY (new §9 automated
  checks, new §10 open dependency findings; limitations now §11).

**Defects / gaps found while validating (each fixed before the final runs)**
1. **E2E on a fresh database: 3 failed, 2 skipped** (local profile, first run) — Playwright runs specs alphabetically
   and only the last (`workflow.spec.ts`) creates traffic; the Risk/Map tests skipped ("no scored transactions in the
   last 7 days"), Model Intelligence `live-inference`, a report download (180 s timeout) and the 390 px Risk check
   failed waiting for data. Not an application defect; long-lived stacks always had traffic. Fix: `seed_traffic.py`
   before every E2E run (workflow + docs); tests unchanged → **12/12**.
2. My `wait_for_stack.py` waited forever when another profile's containers were stopped (`ps -a` lists them) — now
   filters to the selected profile's services (would not have occurred in the workflow's order; fixed anyway).
3. Ruff: the code base was never clean under its own ruleset (227 findings: 216 style, 4 unused imports, 3 bugbear).
   CI gates `E9,F` (now clean) + the full ruleset on `scripts/ci`; style debt documented. B023 in `processor.py`
   reviewed: false positive (callbacks fire inside the same iteration's `flush()`).
4. `test-results/` at the repo root was not gitignored.

**Tests actually executed in Phase 14 (all local; isolated project `fraudstream-ci`, fresh volumes)**
| Suite | Where | Result |
|---|---|---|
| Backend CI job (ruff E9,F + scripts/ci + pytest) | Linux container `python:3.12-slim-bookworm`, **non-root**, fonts installed, from a clean export of the 286 then-publishable files | **205 passed, 0 skipped** (both Windows skips run and pass on Linux) |
| pytest default suite | Windows host, final code | **203 passed, 2 skipped** (the two Windows-only skips) |
| Frontend CI job (npm ci, Vitest, typecheck, build) | Linux container `node:22-bookworm`, clean export | **64 passed**; typecheck pass; build pass (JS 853.6 kB / CSS 23.8 kB) |
| Compose job | host Docker CLI | 7/7 combinations valid; 3 Dockerfiles' COPY sources present |
| actionlint 1.7.12 (incl. shellcheck) | container | both workflows clean |
| Security job: hygiene / gitleaks (dir mode on the publishable tree) / audit gate | host + container | hygiene pass (291 files); **no leaks** (1 false positive reworded); gate PASS 0 new / 123 known; negative test (one baseline entry removed) → exit 1 |
| Kafka integration | `fraudstream-ci` infra, exported vars | **12 passed** (incl. broker restart) |
| Container suite incl. disruptive | `fraudstream-ci` distributed, fresh DB | **25 passed** (13 min 13 s) |
| Spark integration | `fraudstream-ci` network | **6 passed** |
| Playwright E2E distributed (desktop + 390 px, all pages) | :18080, `ci-analyst`/`ci-admin` | **12 passed** |
| Containerised local profile | :18081, fresh SQLite | `/health` profile=local, in-process; `/system/status` "LOCAL DEVELOPMENT MODE: in-process pipeline (no Kafka/Spark), SQLite"; infrastructure kafka/spark **NOT_USED**, redis NOT_CONFIGURED |
| Playwright E2E local | fresh SQLite | 7 passed, 3 failed, 2 skipped without seeding (finding 1) → **12 passed** after seeding |
| Backup → verify → restore | `backup.py` on `fraudstream-ci` (postgres, artifacts, datasets) to the scratchpad; `restore.py` into new project `fraudstream-ci-r` | COMPLETE 3.8 s, 21 checksums; VERIFY OK; **RESTORE PASS**: 0 count mismatches, **16/16 fingerprints**, files 0 mismatches; refusals verified (non-empty target, live project) |
| Dataset reproducibility | `prepare_dataset.py` into a temp dir | content_sha256 `b57801b3…` = experiment (139,517 rows); `.gz`/meta bytes differ (gzip header, created_at) |
| Trivy 0.76.2 | app + frontend images | app (Debian 12.15) 0 OS HIGH/CRITICAL; frontend (Alpine 3.21.3) **68** (3 critical) — report only |

**Not executed:** any GitHub-hosted run (no repository); gitleaks in `git` mode over a real history (no Git
repository; `dir` mode over exactly the publishable files was run instead); the host-process local profile (not
part of this regression; the containerised local profile was tested); the full-stack workflow as one script (its
steps were run individually in the same order; the local profile ran after stopping the distributed CI stack because
the 7.6 GiB Docker VM cannot hold both — the workflow keeps both on a 16 GB runner); seeding on the distributed path
only exercised the API calls (the stack already held 6,618 transactions, so the wait was satisfied immediately).

**Data integrity (verified at the end)**
- `ml/artifacts` + `data/synthetic`: **17/17 files SHA-256 identical** to the pre-phase snapshot.
- Live PostgreSQL `fraudstream` (read-only queries before stopping and after restarting): **identical** — alembic
  0002; transactions 15,727, predictions 15,727, alerts 1,556, investigations 23, notes 32, audit_logs 2,072,
  users 3, refresh_tokens 307, generated_reports 42, drift_reports 8, dead_letters 0, model_versions 4,
  risk_rules 10, system_settings 4, customers 10,889, merchants 282, simulation_runs 14; md5 of risk_rules
  `3a195f78…`, system_settings `7ead8c21…`, all transaction event ids `77ba24c4…`, audit rows `dfe882b6…`;
  risk settings and drift settings unchanged (min_rows 200).
- Live Kafka: **31/31 partition end offsets identical**. The live stack was restarted with `compose start` (same
  containers and images; nothing recreated); all 7 services healthy, processor Spark.
- Backups `../FraudStream-backups/`: 83 files, sizes and mtimes unchanged; all 4 backups `--verify` OK.
- Docker volumes: none removed; 9 added (`fraudstream-ci_*` ×8, `fraudstream-ci-r_postgres-data`) — throw-away test
  data, left in place (stopped). Remove only if wanted:
  `docker compose -p fraudstream-ci --env-file .env.ci -f docker-compose.yml -f infrastructure/isolated/docker-compose.isolated.yml --profile distributed --profile local down -v`
  (and the same with `-p fraudstream-ci-r --env-file .env.ci-r`). Their env files `.env.ci`, `.env.ci-r` (random
  test secrets, gitignored) are kept for that purpose. The Phase 12 projects `fraudstream-p12` / `-p12r` untouched.
- Images `fraudstream-app/frontend/spark:local` were rebuilt from the current code (comment and unused-import changes
  only); the live containers still run the previous image ids until they are next recreated.
- `frontend/e2e-results/screenshots/` now holds the Phase 14 run's screenshots (regenerated by every E2E run;
  gitignored); the curated copies in `docs/screenshots/` are unchanged.

**Known CI limitations**
- Not yet run on GitHub; first real run may surface runner differences (disk, timing). Actions are SHA-pinned to
  v7.x releases resolved on 2026-11-16.
- 123 known dependency advisories are accepted in the baseline pending upgrades (SECURITY.md §10); PyJWT first.
- Trivy is report-only; frontend base image has 68 fixable HIGH/CRITICAL OS findings.
- 216 ruff style findings not enforced.
- Full-stack workflow ~60–90 min and ~7 GB images → manual/weekly, not per PR. Spark integration only there.

### Phase 13 — Documentation, architecture communication, onboarding, demo material

**Status: complete.** Documentation only: no application feature, no model, risk configuration, database or volume
was changed. Every technical statement was taken from the code, `docker-compose.yml`, the dependency files, the stored
experiment and the Phase 12 reports, and the key behaviours were re-checked by running them (see *Validation*).

**Created**
| File | Contents |
|---|---|
| `README.md` (root, new) | introduction, problem, users, limitations banner; feature table; versioned tech stack (from the pins); system diagram; PowerShell setup for both profiles (env, DB initialisation via `app-init`, credentials from `.env`, tests, safe stop and the `down -v` warning); demo summary; Phase 12 performance with machine and caveats; data-integrity and research limitations; security/reliability summary; documentation map; roadmap (remaining work only) |
| `docs/DIAGRAMS.md` | 5 Mermaid diagrams: A system architecture (Spark drawn as ONE container, `local[8]`), B transaction lifecycle (sequence: generation → Kafka → validation → dedup → features → ML → risk → persistence → alert → SSE → analyst → audit), C ML lifecycle, D failure recovery (Kafka, Spark, PostgreSQL, Redis, full restart), E backup → verification → isolated restore → DB/artifact verification → startup |
| `docs/ARCHITECTURE.md` | components and responsibilities, data/config/status flows, protocols, storage per store (21 tables, volumes, mounts), delivery guarantees, design decisions with their measured reasons, architectural limitations |
| `docs/API_DOCUMENTATION.md` | conventions (auth, roles, rate limits, error format, status codes, shared bounds) and every route group with role, parameters (from the generated OpenAPI schema) and responses; alert/investigation state machines; the mandatory `expected_updated_at` sequence 403 → 422 → 428 → 409 → 200 with a verified example |
| `docs/DATA_AND_ML.md` | simulator and 7 scenarios, dataset spec and schema, 29 features, leakage controls, temporal + customer-holdout split with sizes, model comparison from the stored experiment (all 3 splits), threshold selection, risk-engine formula and 10 rules, inference architecture, drift (PSI) and research limitations; synthetic vs real-world stated throughout |
| `docs/STREAMING_ARCHITECTURE.md` | Kafka settings and 7 topics, producer/consumer configs, Spark settings and 6 queries, checkpoints, progress/health, persistence consumer algorithm, dedup layers, DLQ stages, delivery semantics, recovery, Phase 12 throughput, backpressure limitations |
| `SECURITY.md` (root) | authentication, RBAC matrix, rate limiting, validation, optimistic concurrency, audit, containers, secret handling, known limitations (from `docs/phase12/SECURITY_REVIEW.md`) |
| `docs/OPERATIONS.md` | startup/shutdown, health checks, logs, failure recovery table, backup and restore (Phase 12 verified procedure + the account-name and network-name lessons), migrations, maintenance (report retention dry run), troubleshooting |
| `CONTRIBUTING.md` (root) | repository layout, prerequisites, local and distributed dev setup, isolated stack, every test suite with what it writes to, integration env vars, adding endpoints/pages, artifact/dataset handling, **data-protection rules** (never `down -v`, never disruptive tests on `fraudstream`, never edit `ml/artifacts`, never delete checkpoints/topics), coding and testing expectations |
| `docs/DEMO_GUIDE.md` | 12-step, 10–15 min script with expected observations, screenshots, commands and a failure-recovery table; reverts its own drift change; local-profile fallback |
| `docs/PROJECT_OVERVIEW.md` | one-page interview summary: problem, architecture, six technical highlights, key measured numbers, honest limitations, likely questions |
| `docs/screenshots/` | 16 desktop + 7 mobile (390 px) screenshots, **unmodified copies** of the final Phase 12 E2E screenshots (distributed profile, isolated stack, 2026-11-16) + `README.md` with provenance and caveats (benchmark data volumes, test account names). Originals left in `frontend/e2e-results/screenshots/`. Older Phase 7/10 screenshots not used (older UI). |

**Updated**
- `.env.example` line 1 only: the comment pointed to a non-existent `scripts/new_env.ps1`; now `python scripts/new_env.py`
  (no key or value changed; `new_env.py` ignores comment lines).
- `DEVELOPMENT_STATUS.md` (this section, §6, §7, §9, §11). Backup of the previous version:
  `DEVELOPMENT_STATUS.before-13.md` in the session scratchpad (earlier backups kept as before).
- Existing `docs/phase12/*` and `benchmarks/BENCHMARKS.md` were **kept unchanged** and are linked as the detailed
  evidence (no duplicate tables beyond summaries).

**Validation (executed in Phase 13)**
- Smoke test of the README's **host-run local profile** commands on a throwaway SQLite DB and runtime-config dir in the
  scratchpad (no project data touched): `app.cli migrate` (0001 → 0002), `create-user`, uvicorn start, `/health` ok
  (`profile=local`, model loaded), login, 6 authenticated pages 200, an injected account-takeover scenario → HIGH alerts,
  configuration concurrency exactly as documented: no version **428**, stale **409**, wrong type **422**, current
  version **200**, all four audited. Server stopped afterwards.
- `python -m pytest`: **203 passed, 2 skipped** (same as end of Phase 12).
- `docker compose --env-file .env.example --profile distributed config --services`: valid (9 services).
- Generated OpenAPI schema: every route named in `API_DOCUMENTATION.md` exists (the only non-matches are the concrete
  example `/settings/drift` of `/settings/{key}` and nginx's `/healthz`); all query bounds copied from it.
- Link check (scratchpad script): all relative links, image paths and `#anchors` in the 12 documents resolve; every
  backticked repository path exists (except `data/fraudstream.db`, created on first host run — stated as such).
- Secret check: none of the 15 secret values from `.env`, `.env.isolated`, `.env.restore` appears in any document; no
  Argon2 hash; no scratchpad/internal path.
- Screenshots: all 23 referenced files exist; content inspected (no secrets; only test account names).
- Mermaid: all **6** blocks (5 in `docs/DIAGRAMS.md` + the system diagram in `README.md`) rendered to SVG with
  `@mermaid-js/mermaid-cli` 11.4.2 (installed in the scratchpad, not the project) using the installed Chrome:
  **6 OK, 0 failed**, no "Syntax error" output.
- Artifacts/datasets: SHA-256 of all **17** files in `ml/artifacts` + `data/synthetic` before vs after Phase 13:
  **identical** (incl. the derived `runtime_config.json`; the smoke test wrote its runtime config to the scratchpad).
- Not re-executed in Phase 13: the distributed-profile commands — Docker Desktop was not running and starting it would
  have restarted the live stack. The documented commands are the ones executed in Phases 10–12; the compose file
  validates.

**Remaining documentation limitations**
- Code comments still reference documents that never existed: `docs/DATASETS.md` (`ml/training/train.py`,
  `.gitignore`), `docs/RISK_SCORING.md` (`risk_engine/engine.py`), `docs/DEPENDENCIES.md` (`backend/requirements.txt`),
  `spark_schema.py` (`streaming/schemas/events.py`; the JVM schema lives in `fraud_stream_job.py`),
  `scripts/e2e_local.ps1` (`frontend/playwright.config.ts`). Their content is now in `docs/DATA_AND_ML.md`,
  `README.md` §3 and `CONTRIBUTING.md` §5. Left unchanged to avoid touching code/config in a documentation phase.
  (`ARCHITECTURE.md §Delivery guarantees`, referenced by `streaming/kafka/client.py`, now exists as `docs/ARCHITECTURE.md` §6.)
- Screenshots come from the isolated benchmark stack (large volumes, alert-storm share, outage-inflated latency tiles);
  a fresh demo looks quieter. Labelled in `docs/screenshots/README.md`.
- No LICENSE file (pyproject declares MIT) and no CI workflow — listed in the README roadmap.
- API documentation is hand-written from the OpenAPI schema; the schema itself has no response models (handlers
  return dicts), so `/api/docs` shows parameters but not response shapes.

### Phase 12 — Performance, reliability, backup & recovery, security hardening

**Status: complete.** Plan: `docs/phase12/PLAN.md`. Reports: `docs/phase12/RELIABILITY_REPORT.md` (all measurements),
`docs/phase12/BACKUP_AND_RESTORE.md`, `docs/phase12/SECURITY_REVIEW.md`, `benchmarks/BENCHMARKS.md` (methodology). Raw
results: `benchmarks/results/*.json`. No new dashboard module, no architectural change, Phase 13 not started.

**Isolation.** All benchmarks, failure injection and restore tests ran on a separate compose project **`fraudstream-p12`**
(`infrastructure/isolated/docker-compose.isolated.yml` renames the network — the base file pins `fraudstream`, so a second
project would have joined the LIVE network; `scripts/isolated_env.py` → `.env.isolated` with fresh random secrets, ports
19092/15433/16380/14040/18080/18081, accounts `p12admin`/`p12analyst`). It was built by **restoring a backup of the live
stack** (DB, local DB, files, and Kafka + Spark volumes taken while live was stopped). The Docker VM (7.6 GiB) cannot hold
two stacks, so — with the owner's approval — the **live stack was stopped** (`compose stop`, nothing removed) for the
duration and restarted at the end. A third project `fraudstream-p12r` (`.env.restore`) proved the final restore cycle.

**Benchmarks (measured; single machine, not production capacity)** — `benchmarks/stream_bench.py`, `api_bench.py`,
`report_bench.py`, `persist_microbench.py`, `common.py`:
- Kafka → Spark → Kafka → PostgreSQL, separate stage latencies on the Docker VM clock (measuring process runs inside the
  network): 10 / 25 / 50 / 100 ev/s for 60 s keep up, end-to-end p50/p95 **2.5/3.4, 2.6/3.5, 2.7/3.7, 3.8/5.6 s**
  (Spark ≈ 2.2–2.7 s of it incl. the 2 s trigger); 150 ev/s falls behind (115 ev/s persisted, p95 24 s). Sustained 600 s at
  112.6 ev/s offered: **105.5 ev/s persisted**, backlog grows (p95 57 s) → **sustained capacity ≈ 100–105 ev/s**. Burst of
  5,000: Spark one 14.7 s batch (≈ 340 ev/s), persistence 217 ev/s. **Every run: 0 lost, 0 duplicate rows, 0 dead letters.**
  A first sustained run used a fixed 2,000-customer population → 40 % of events flagged (velocity rules): an alert storm
  created by the benchmark, kept as a labelled stress point; the population now scales with the rate.
- HTTP ingestion (one API process): saturates at **≈ 65 accepted ev/s** (HTTP p50 243 ms there); 0 errors.
- Bottlenecks: persistence consumer first (DB work alone 259 ev/s per-event tx / 353 batched — 1.36×, not changed: Spark is
  close behind and the change would touch the exactly-once path), Kafka fsync-per-message under load (ack p50 19 → 147 ms;
  a durability trade-off left to the owner), Spark at its 3 GiB limit (no OOM).
- Reports: all six types ≤ 2.3 s to generate, CSV ≤ 0.4 s (1.9 MB), PDF ≤ 2.5 s on ~190k transactions / 45k alerts; API
  memory peak 385 MiB; other requests slowed (probe p50 16 → 111 ms with 4 concurrent large reports, max 482 ms) but never
  blocked → **no job queue** (smallest future step documented: `ProcessPoolExecutor` + PENDING row + polling).

**Defects found and fixed in Phase 12 (each with a test, before/after measured)**
1. **HTTP 500 under concurrent analytics** (up to 99 % of requests at concurrency 8–32): PostgreSQL parallel hash joins put
   their shared hash tables in `/dev/shm` (Docker default 64 MB) → `could not resize shared memory segment`. 1 GiB of memory
   alone did not fix it. Fix: `shm_size: 256m` + `postgres -c enable_parallel_hash=off` (memory limit unchanged) →
   **0 errors on all 29 benchmarked endpoints at all levels**; some single-request latencies rose (e.g. merchants p50
   248 → 474 ms). Latent on the live stack (only appears with ~190k rows).
2. **Redis outage degraded everything:** each Redis call failed after ~3.9 s (stopped container's name no longer resolves),
   so every API request took +4 s (`/system/status` 12 s, `/health` 4.1 s of its 6 s check timeout) and persistence fell to
   ~0.25 ev/s. Fix: `app/services/redis_guard.py` circuit breaker (fail fast, background PING closes it) on every Redis
   client (API, broker, consumer, fallback processor, Spark driver/workers) → requests at baseline after one tripping call,
   200 events persisted during the outage in 13.6–20.7 s. Rate limiter now reports `redis` again after recovery.
3. **Redis outage silently changed Spark's scoring rules** (watcher fell back to a stale `runtime_config.json` or to the
   built-in defaults) → keeps the last Redis configuration (`redis (last known; Redis unavailable)`).
4. **False "NOT RUNNING"** during a Redis outage → `UNKNOWN (Redis unavailable: …)`, shown as a warning in the header.
5. **Login rate-limit bypass via `X-Forwarded-For`** (nginx appended to a client value; uvicorn `--forwarded-allow-ips *`
   takes the left-most) — reproduced: 15/15 attempts processed, forged IPs in the audit trail → nginx sets
   `X-Forwarded-For $remote_addr` → limit enforced (10 × 401, 5 × 429).
6. **Spark container could read `.env`** (whole repository mounted) → only `backend/ ml/ streaming/ simulator/` + health check.
   (Same narrowing for the Spark integration-test container.)
7. **Optimistic concurrency bypassable** → `expected_updated_at` mandatory on rule/risk/setting updates: 422 for invalid
   values first, then **428** if missing (audited), 409 if stale; `null` only for a never-saved setting. 9 requests in older
   backend tests now send the version (no assertion changed); the dashboard always sent it.
8. **CSV formula injection in `/reports/alerts.csv`** (free-text `customer_id` from ingestion) → same guard as 11C exports.
9. **500 for out-of-range integers** on 11 GET routes; `limit=-1` = unlimited on SQLite → shared bounds `app/api/params.py`.
10. API rate-limit bucket per arbitrary bearer string → invalid tokens share the client-address bucket.
11. Spark UI kill actions disabled (`spark.ui.killEnabled=false`).

**Backup & restore** — `scripts/backup.py` (snapshot-consistent `pg_dump` + per-table counts and content fingerprints in the
same snapshot, SQLite online backup, files + SHA-256, volume archives, `.partial`/`.FAILED`, never overwrites, no `.env`,
default destination `../FraudStream-backups` outside the repo), `scripts/restore.py` (empty targets only, refuses the live
project, single-transaction `pg_restore`, compares with the manifest), `scripts/verify_restore.py` (running stack vs
manifest, 10 checks). Proven twice: live → isolated (all counts, 16 table fingerprints, 31 Kafka offsets, Spark commits
identical; healthy in 48 s; 10/10) and isolated (197k tx, all 9 components) → new project (healthy in 43 s; 10/10;
workflow tests pass). Lesson: a restore env must not reuse an existing username with a new password (app-init never
changes existing accounts).

**Report retention** — `python -m app.cli prune-reports --older-than-days N [--keep-latest K] [--type T] [--apply]`
(`app/services/report_retention.py`): dry run by default, newest K per type always kept, deletes only `generated_reports`
rows by id in one transaction with one `REPORTS_PURGED` audit row listing every deleted report. Tested on a throwaway
SQLite DB and on the isolated PostgreSQL (3 back-dated copies deleted, nothing else changed). **Not run on the live DB; no
automatic schedule.**

**PDF Unicode** — per-character fallback chain after DejaVu: **Droid Sans Fallback** (Apache-2.0; 20,902 CJK ideographs,
kana — but only 3 of 11,172 Hangul syllables, found by rendering) + **Nanum Gothic** (SIL OFL 1.1, Hangul); both TrueType,
subset-embedded (Noto CJK is CFF, which reportlab cannot load). Verified in the app image: rendered with poppler —
Chinese, Japanese, Korean, Greek, Cyrillic, €, é all visible; `pdftotext` returns the text; `pdffonts` shows the embedded
subsets. Fonts added to `app.Dockerfile` (`fonts-droid-fallback fonts-nanum`). CSV unchanged (always Unicode).

**Other tooling:** `scripts/scan_secrets.py` (secret values / secret-bearing files in a tree or ZIP; `--strict` for
packages), `scripts/probe_login_rate_limit.py`, `scripts/run_e2e.py` (E2E with accounts from an env file),
`scripts/isolated_env.py`. Container tests are target-configurable (`FS_COMPOSE_PROJECT`, `FS_ENV_FILE`,
`FS_COMPOSE_FILES`) and **disruptive tests refuse the live project** unless `FS_ALLOW_DISRUPTIVE_ON_LIVE=1`; Kafka/Spark
integration tests take `SPARK_IT_NETWORK` / `SPARK_IT_COMPOSE_PROJECT` (defaults unchanged).

**Recovery (isolated, measured; two full runs)** — Kafka stopped 20.5 s during 25 ev/s production: every acknowledged
event persisted exactly once (run 1: 194/1,750 sends reported `Message timed out` to the producer and were not persisted;
run 2: 1,750/1,750), lag 0 within 49–56 s; Spark backlog of 3,000: drained exactly once in 53–78 s; Redis: see defect 2;
full stack stop + `up -d`: healthy 22–45 s, first new event persisted and on the live feed 41–77 s, rows/checkpoints/
artifacts preserved; plus the Phase 10 crash/outage tests (consumer, Spark crash/stop, Kafka, PostgreSQL, API recreate).

### Phase 11C — Reports and Settings

**Status: complete.** Two new pages (`/reports`, `/settings`, both in the navigation). Every displayed value comes from the
API; every configuration action is validated, permission-checked and audited **by the backend**.

*Reports* (`/reports`) — backend `app/services/reports.py` + `app/api/routes/reports.py`:
- Six report types, all computed by SQL over persisted data for an explicit UTC period `[start, end)` (≤ 366 days):
  fraud activity (`transactions.processed_at`; filter: risk level), risk analytics (score bins, categories, methods, rule
  triggers, confusion vs simulator labels, hourly), alert summary (`alerts.created_at`; filters: risk level, alert status;
  status/risk/method/rule/assignee breakdowns + alert list), investigation (`investigations.created_at`; filter: status;
  unresolved / resolved / closed, classifications, median time to close, case list with findings), model performance (registry
  offline metrics, live inference latency and live labelled precision/recall in the period, drift checks in the period),
  system activity (audit trail; **admin only**). Each report has id, generation time, generator, period + basis, applied
  filters, summary metrics, sections, limitations and data sources. Amounts are summed per currency. 0/0 rates are `null`
  ("n/a"), never 0.
- **Filters are server-side only**; a filter the type cannot apply (e.g. risk level on risk analytics) is rejected with 422
  instead of being ignored. Naive datetimes, start ≥ end and > 366 days → 422.
- **Persistence:** each generated report is stored as a snapshot (`generated_reports`, migration `0002`), so downloads
  reproduce what was generated (tested: data added later does not change a stored report). Failures are stored as `FAILED`
  (error class only, no internal details) and cannot be downloaded (409). Generation is audited (`REPORT_GENERATED` /
  `REPORT_FAILED`). History: paginated, server-side type filter; analysts never see system-activity reports (list, view,
  download all enforce it).
- **Exports:** CSV (UTF-8 with BOM, Python `csv` quoting for commas/quotes/newlines, Unicode preserved, formula-injection guard:
  strings starting with `= + - @ TAB CR` are prefixed with `'`; numbers stay numbers) as a multi-section file or one section
  as a clean table; **PDF** via **reportlab 4.2.5** (new dependency; pure-Python layout, + Pillow 11.0.0 / chardet 5.2.0)
  with an **embedded TrueType font** (DejaVu Sans in the image — `fonts-dejavu-core` added to `app.Dockerfile`; reportlab's
  bundled Vera on hosts), landscape A4, metadata block, summary, tables with repeated headers, page footer. Large reports:
  detail tables store ≤ 10,000 rows (`total_rows` keeps the true count, truncation stated), the PDF prints ≤ 500 rows per
  table and ≤ 400 characters per cell (stated in the PDF), the browser preview shows ≤ 100 rows; a 5,000-row PDF with a
  20,000-character cell renders (test). Safe file names `fraudstream-<type>-<yyyymmdd>-<yyyymmdd>[-section]-<id8>.<ext>`.
- Frontend: generator (type, presets or custom UTC dates with an exact `start → end (end exclusive)` preview, per-type
  filters), preview (metadata, summary tiles, section tables with per-section CSV, limitations), history with View/CSV/PDF,
  deep link `?report=<id>`. The shared `download()` helper now uses the server's file name (sanitised), detects empty bodies
  and network/HTTP errors, attaches the link to the DOM and revokes the blob URL later (it revoked immediately before).

*Settings* (`/settings`, tabs `rules | scoring | drift | account | system | audit`):
- **Risk rules** (`/risk/rules`): name, code, family, description, enabled, weight, parameters, last change (from the audit
  trail) — plus an explanation of how rules combine. Admins edit inline → client validation mirroring the server bounds →
  confirmation dialog listing exactly `field: old → new` → PATCH with only the changed fields and `expected_updated_at`.
- **Risk scoring** (`/risk/settings`): thresholds, weights, anomaly percentile, BLOCK_SIMULATION flag, defaults, used-by,
  last change; same review/confirm flow; after saving the page shows the active configuration returned by the server and
  whether the runtime config reached Redis (read by Spark) and the file.
- **Drift monitoring** (`/settings/drift`): PSI thresholds, window, minimum rows (only affects drift checks).
- **Account & access:** `/auth/me` (username, role, created, last sign-in), access-token expiry (only the `exp` claim is read;
  the token is never shown), role-permission matrix matching the backend, admins: read-only account list (`/users`). No
  user-management UI and no fake preferences (the backend stores none per account).
- **System** (read-only, labelled): profile, environment, version, start time, processor, broker, rate limiter, runtime-config
  publish status (ok/error only), PostgreSQL/SQLite, Redis, Kafka, Spark, consumer health, active models, artifact mode,
  registered datasets. No environment variables are displayed. Settings stored but read by nothing (`preferences`,
  `simulation_defaults`) are named as such and not offered for editing.
- **Settings audit trail** (admin): `/audit?entity_type=risk_rule,system_setting`, incl. rejected and denied attempts with reasons.
- Analysts see everything read-only with an explanation; the API rejects their changes regardless of the UI.
- Accessible confirmation dialog (`ConfirmDialog`): `role=dialog`, labelled, focus trapped, initial focus on **Cancel**
  (a stray Enter cannot save a risk change), Escape/backdrop cancel, focus restored, width capped to the viewport.

**Backend changes (backward compatible unless noted)**
- New: `POST /reports`, `GET /reports/types`, `GET /reports/generated[?page,size,report_type]`, `GET /reports/generated/{id}`,
  `GET /reports/generated/{id}/download?format=csv|pdf[&section=]`, `GET /settings/{key}` (value, defaults, updated_at,
  last change, used-by, bounds). Existing report endpoints unchanged.
- `/risk/rules` items add `last_change`, `editable`, `param_bounds`; `/risk/settings` adds `updated_at`, `last_change`,
  `editable_by`, `used_by`; PUT/PATCH responses add `changed`/`changed_fields` and `propagation`.
- `/audit?entity_type=` accepts a comma-separated list (a single value behaves as before).
- **Configuration-change hardening** (`PATCH /risk/rules/{code}`, `PUT /risk/settings`, `PUT /settings/{key}`):
  optimistic concurrency via optional `expected_updated_at` (409 if changed meanwhile) + `SELECT … FOR UPDATE` on PostgreSQL;
  per-parameter rule bounds (`count`, `min_history`: whole 1–1000; `ratio` 1–1000; `zscore` (0,100]; `absolute` (0,1e9];
  `kmh` (0,20000]) and finite numbers only (previously only "non-negative": 0 or ∞ were accepted); **strict types** (`"40"` or
  `"yes"` are now rejected instead of coerced — a deliberate tightening); drift bounds (`window_rows` 50–100,000, `min_rows`
  1–window, `0 < moderate < significant ≤ 10`; previously `window_rows=-5` was accepted, which SQLite treats as *no limit*,
  and `true` passed as a number); only fields that actually change are audited (no-op saves are not changes); the previous
  configuration is kept on any failure (single transaction). No default value was changed.
- **Audit of failures:** `RULE_UPDATE_REJECTED`, `RISK_SETTINGS_REJECTED`, `SETTING_UPDATE_REJECTED` (reason, attempted
  payload ≤ 1,000 chars, status) and `CONFIG_CHANGE_DENIED` (actor, role, path) for analysts. 403 message unchanged.

**Database migration `0002_generated_reports`** — additive only: creates table `generated_reports` (+ 2 indexes); no
existing table/column/row touched; downgrade drops only that table (tested on a populated 0001 SQLite DB: rows intact after
upgrade and after downgrade). Applied to PostgreSQL `fraudstream` by `app-init` and to the local SQLite volume by `api-local`
at start-up. Backups taken before applying: `pg_dump -Fc` of `fraudstream` (3.9 MB, verified with `pg_restore --list`, 21
table-data entries) and a copy of `fraudstream-local.db`, both in the session scratchpad (they contain password hashes).

**Bugs found and fixed in Phase 11C**
1. Drift settings accepted `window_rows=-5` (SQLite `LIMIT -5` = unlimited) and booleans as numbers; rule params accepted 0
   and infinity; config endpoints coerced `"yes"`/`"40"` (Pydantic lax mode). Fixed as above (tests).
2. `download()` revoked the object URL right after `click()` (can cancel downloads), never attached the link, ignored the
   server file name and reported empty bodies as success.
3. Report preview cap was bound at import time (`preview(rows=PREVIEW_ROWS)` default) — found by a test; now read at call time.
4. Settings save message vanished in the real browser: the forms are keyed by `updated_at` (to reload server values), so a
   successful save remounted the form and dropped its message. Predicted from the code, then confirmed on the (stale) deployed
   build with a measurement-only browser diagnostic (no navigation, no errors; PUT 200, GET 200, message absent). Fix: the
   message lives in the tab, outside the keyed form; two regression unit tests (stateful backend, two consecutive saves).
5. E2E harness: `toBeDisabled()` on an `<option>` hung until the test timeout in Chrome; now asserts the `disabled` property.

### Phase 11B — Risk Analytics and Fraud Intelligence Map

**Status: complete.** Both pages use only backend data; every filter changes what the server computes, except the map's
country selector, which is labelled "view filter" and narrows the points already returned. Filters live in the URL (survive
reload, deep-linkable); queries are keyed by their filter values, so a slow reply for an old filter cannot overwrite a newer
one (unit-tested with a delayed response); previous results stay visible, dimmed, with "updating…" while a new filter loads.

*Risk Analytics* (`/risk`) — endpoints: `/analytics/patterns`, `/analytics/confusion`, `/analytics/merchants`, `/analytics/hourly`,
`/analytics/velocity`, `/alerts?size=1&since=` (alert count), `/risk/settings` (thresholds for badges).
- Filters: period (1 h … 30 d, all time; server `minutes`), merchant category (server `category`), velocity window (5 min … 24 h,
  server; independent and stated, the API caps velocity at 24 h), refresh.
- Summary: transactions analysed, flagged MEDIUM/HIGH (+ share), alerts raised in the period, analyst-confirmed/dismissed,
  most-flagged *injected* pattern, most-triggered rule, period label. Unavailable values show "n/a" with a reason, never 0.
- Patterns: per simulator scenario — transactions, **labelled suspicious** (simulator truth), **flagged** (risk engine), flag rate,
  avg risk; "injected"/"benign" decided by the label count (not the name — `impossible_travel_anchor` is labelled benign);
  flagged benign rows highlighted as false alerts; explicit legend separating labelled / flagged / analyst-confirmed; flag-rate
  chart; rule-trigger chart with its basis (flagged predictions examined, cap 20,000).
- Confusion matrix (risk engine vs simulator labels): TP/FP/FN/TN, precision, recall, F1 (computed from P and R, labelled),
  false-positive rate, analyst outcomes, plain-language metric definitions, SYNTHETIC-labels warning.
- Merchants: flag rate by category (chart + table with volume and avg risk); top merchants ranked by flagged count (rank, id,
  category, tx, flagged, rate, avg/max risk badge). Merchant names are not in the API (ids only — stated).
- Hourly: 24 stacked bars (LOW vs MEDIUM/HIGH) by hour of event time (UTC); zero hours are real zeros (stated); peak flag-rate hour.
- Velocity: top 15 customers by count in the window, tx/min, amount, max-risk badge, link to their alerts (all statuses).

*Fraud Intelligence Map* (`/map`) — endpoint `/analytics/geo` (+ `/risk/settings`).
- Filters: period (≤ 7 d — the endpoint maximum), risk level (all / MEDIUM+HIGH / HIGH only → server `min_risk` from the configured
  thresholds), country (view filter), refresh.
- Dependency-free SVG latitude/longitude plot (graticule, no basemap, no external tiles): bubble size = transactions, colour =
  flagged share on a single-hue scale from 0 % to the **highest share shown** (stated in the legend); labels on the 8 most-flagged
  cities; hover tooltip and click selection pick the **nearest city centre** (overlapping bubbles stay reachable); selected-city
  panel; city ranking table; country aggregation table.
- Integrity: permanent disclaimer (backend label + "public city centroid, not a customer/merchant address or a confirmed fraud
  location; flagged = detection, not confirmed fraud; no basemap"); "location type: simulated city centroid"; transactions
  without coordinates are counted and never placed; unavailable/empty states explain why. At most 38 points (the simulator's
  cities), so no clustering is needed.

**Backend changes (backward compatible; omitted parameters = previous behaviour)**
- Optional `minutes` (1–43,200; on `transactions.processed_at`, as timeline/velocity/geo) on `/analytics/patterns`, `/confusion`,
  `/merchants`, `/hourly`; responses carry `period` {window_minutes, basis, label}.
- `/analytics/merchants`: optional `category`; top merchants also return `flag_rate`, `avg_risk`, `max_risk`; deterministic ranking.
- `/analytics/patterns`: per-scenario `labelled_suspicious` (exact count from `sim_is_suspicious`); `rule_hits_basis`; rule hits
  restricted to the period.
- `/analytics/confusion`: analyst outcomes restricted to alerts of transactions in the period.

**Bugs found and fixed in Phase 11B**
1. `limit` on `/analytics/merchants` and `/analytics/velocity` had no lower bound: `limit=0`/negative reached SQL, and on SQLite
   `LIMIT -1` means *unlimited*. Now `ge=1` (values > 50 are still clamped as before).
2. Top-merchant and velocity rankings were non-deterministic for ties (order changed between refreshes) — tie-breakers added.
3. Map hover: a smaller city's oversized hit circle covered a larger neighbour's centre (Toronto over Chicago; found by E2E —
   Playwright reported "intercepts pointer events"), so that city could not be hovered at its centre. Replaced by nearest-centre
   hit-testing; E2E now hovers **every** city at its centre.
4. Phone overflow on `/risk` (local profile only, data-dependent): a hidden but positioned Recharts tooltip containing a long rule
   name (`HIGH_RISK_MERCHANT_NEW`) sat 5 px beyond the viewport (395 vs 390). Diagnosed with the 11A read-only DOM measurement
   (scratchpad script, inspected before running, deleted after). Fix: tooltips are capped to their chart and wrap long labels;
   bar-chart wrappers clip. Long names in summary tiles now wrap instead of being cut.
5. Map colour scale ran 0–100 % while every city was at 10–15 %, so all bubbles looked identical; now 0 → highest observed share.
6. E2E harness: each cross-check logged in again; the API's login limit (10/min per client, a security control — unchanged)
   was exhausted, the login answered 429 and the next GET went out with `Bearer undefined` (a misleading 401). Reproduced with a
   status-only script (all 11 logins → 429, no token). Fix: `frontend/e2e/api.ts` logs in once per worker and fails loudly on a
   rejected login.

### Phase 11A — Streaming Infrastructure and Model Intelligence pages

**Frontend: two new pages** (in the navigation; existing dark design system; desktop and phone layouts)

*Streaming Infrastructure* (`/streaming`), all values from `/infrastructure/status`, `/analytics/overview`, `/analytics/timeline`:
- Health of PostgreSQL, Redis, Kafka, Spark, persistence consumer, fallback processor (state always in text, never colour alone);
  active processor from live heartbeats.
- Tiles: Spark score-query input / processed rows/s, persisted throughput, end-to-end p50/p95 (15 min), Spark lag vs the raw
  topic, persistence consumer-group lag.
- Charts: persisted volume (PostgreSQL, 30 min); Spark rows/s **sampled by the page while it is open** (labelled; not stored).
- Spark query table: state, batch, rows, rates, trigger time, state rows, late rows dropped, lag, **checkpoint** (last committed
  batch + age). Filter (scoring / dedup / windows / inactive-or-failed); expandable rows with duration breakdown, watermark,
  run id, checkpoint location, last committed / planned batch, uncommitted batches, last commit time, state store.
- Kafka topics (partitions, end offset, retained, msgs/s — n/a until a second check), consumer groups with per-topic lag
  (inactive fallback group explained, not counted as lag), persistence-consumer counters, DLQ (`fraud.dlq` retained + database
  dead letters + recent reasons).
- Auto-refresh 5/10/30 s or paused, manual refresh, stale-data banner if a refresh fails, loading/error/empty states.
- Local profile: explicit note that Kafka/Spark are not used, in-process pipeline counters instead.

*Model Intelligence* (`/models`, tabs deep-linkable with `?tab=registry|compare|drift|artifacts`):
- Active supervised/anomaly models: test PR-AUC, precision, recall, F1, offline p50 latency, whether the API actually loaded
  them; artifact mode banner (READ-ONLY by default).
- **Registry:** all versions, kind filter, test metrics, offline p95 and **live** p95 from persisted predictions; detail panel
  (metrics per split, confusion matrix, hyperparameters, dataset, artifact path, live prediction count); admin-only
  Activate / Roll back with an audited reason — **disabled with an explanation while artifacts are read-only**; live
  inference-latency table.
- **Comparison:** stored experiment results; experiment and split selectors (temporal test / unseen customers); per-model
  toggles; grouped bar chart + table (best value highlighted, recommended model marked, selection rule stated).
- **Drift:** "Run drift check now" (stores a report — existing endpoint behaviour), history table + PSI trend chart, per-feature
  PSI chart with thresholds and status filter, labelled live precision/recall vs test (simulator ground truth),
  insufficient-data state.
- **Experiments & artifacts:** experiment history + detail (dataset, SHA-256, split sizes/positives, selection rule), registered
  datasets, every file in `ml/artifacts` (size, mtime, SHA-256, filter), registry.json-vs-database consistency check.
- Charts: one y-axis each; model colours fixed per model name (validated categorical palette, adjacent CVD ΔE ≥ 8.4 on the
  dark surface); drift status colours always paired with text.

**Backend (read-only additions, no schema change)**
- `GET /models/artifacts` — files actually present (size, mtime, SHA-256 cached by size+mtime), writability, registry.json vs DB active.
- `GET /models/live-inference?limit=` — measured `Prediction.inference_ms` per model version (samples, mean, p50/p95/p99, max).
- `GET /models/experiments/{id}` — stored experiment summary + its model versions.
- `GET /drift/reports/{id}` — a stored drift report in full; `/drift/history` also returns `score_psi`.
- `GET /models/active` includes `artifacts_writable`.
- Spark progress snapshot includes `checkpoint_root` and a per-query `checkpoint` block — `streaming/spark/checkpoint.py`
  (stdlib, read-only listing of `offsets/` and `commits/`).

**Bugs found and fixed in Phase 11A**
1. **SSE live feed ended after 15 s without traffic** — root cause of the "unexplained" Phase 10 SSE test failure. The endpoint
   wrapped the broker generator's `__anext__` in `asyncio.wait_for(…, 15)`; the timeout *cancelled* the generator, which closed
   the Redis subscription, and the next iteration ended the stream. Reproduced through nginx on the idle stack: stream ended at
   15.1 s, right after the first keep-alive. Effects: the Live Stream page reconnected every ~17 s when idle and lost messages
   published during the gap; the workflow test failed whenever delivery took > 15 s (e.g. a slow first Spark batch).
   Fix: brokers yield an idle tick every ≤ 5 s instead of being cancelled; keep-alive every 15 s; `connected` is sent only
   **after** the subscription is active (fixes a second race where a message published right after `connected` could be lost).
   Verified: the same probe stayed open 60 s (keep-alives at 15/30/45/60 s); 3 unit regression tests + 1 container test.
2. **Spark reported healthy while still starting after `docker compose restart`**: `/tmp/spark-progress.json` from the previous
   run survives a container restart, so the health check passed 7 s after restart. The job now deletes the file at start-up
   and on shutdown (observed: health stays `starting` until the new job reports; 120 s start period unchanged).
3. **Phone layout overflow** (found by the new E2E test): `Unavailable` ("n/a") renders screen-reader text with Tailwind
   `sr-only` (`position: absolute`); inside a table cell its containing block was the page, not the table's `overflow-x-auto`
   wrapper, so it was not clipped and widened `/models` to 1053 px at a 390 px viewport. Identified with a read-only DOM
   measurement (3 `sr-only` spans at x = 1052 px, `offsetParent` = BODY). Fix: the `n/a` span is `relative`.

**nginx API address refresh after recreation — now actually exercised.** New disruptive test: the API's old IP is held by a
placeholder container while the API is recreated, forcing a new address (172.28.0.8 → 172.28.0.9 in the suite; 172.28.0.5 →
172.28.0.9 manually); the frontend container is not restarted; requests through nginx return 200 from the new container.
(`docker compose up --force-recreate` alone again kept the same IP.)

## 4. Trained models (unchanged)
Experiment `v20261114T124627Z` in `ml/artifacts/` (14 files, unchanged). Active: xgboost (test PR-AUC 0.9996,
precision 0.994, recall 0.993 on the synthetic temporal test split — optimistic, synthetic, not real-world).

## 5. Tests actually executed

### Latest runs

Phase 15 (2026-11-16): see the table in §3 Phase 15 (pytest 224 + 2 skipped on Windows / 226 on Linux, Vitest 78, container 25, Kafka 12, Spark 6, E2E 12 + 12, restore PASS, extracted-archive smoke + E2E 12).

### End of Phase 12 runs (historical, 2026-11-15/16)
| Suite | Command / target | Result |
|---|---|---|
| Default Python suites | `python -m pytest` | **203 passed, 2 skipped** (183 + 20 new: `test_phase12_security.py` 13, `test_report_retention.py` 4, `test_redis_guard.py` 2, processor detection 1; + 1 Kafka unit). Skips: Windows read-only check (as before); CJK PDF test on a host without a CJK font (passes with `FRAUDSTREAM_PDF_FALLBACK_FONT`; verified in the app image) |
| Frontend unit / typecheck / build | `npx vitest run`; `tsc -b --noEmit`; `npm run build` | **64 passed** (61 + 3 new `phase12.test.tsx`); pass; pass (JS 854 kB / CSS 23.8 kB). Two cold first runs while the host was busy had 1–2 timing failures (incl. one existing 11C test); 4 consecutive runs then passed — the new test waits up to 5 s for the badge |
| Container integration, full incl. disruptive | `FS_COMPOSE_PROJECT=fraudstream-p12 … pytest -m containers infrastructure/tests` (**isolated**) | **25 passed** in 13 min 25 s (20 + 5 new). First run: 23 passed, 2 failed — Redis outage (real defect 2, fixed) and the artifact test (compared with the repo dir while the isolated stack mounts the restored copy; now compares with the mounted dir, assertion unchanged) |
| Container tests on the restored project | `FS_COMPOSE_PROJECT=fraudstream-p12r …` | 3 passed (health, status, full workflow) |
| Container tests on the **live** stack (read-only subset) | `-k "services_healthy or isolation_secrets or spark_container_cannot or dashboard_status or every_dashboard_page or streaming_page"` | **6 passed**; a disruptive test is refused on live (skip, as designed) |
| Kafka integration | isolated infra (19092/15433/16380) | **12 passed** (first attempt 11 + 1 skipped: Kafka looked up in the live project — now configurable) |
| Spark integration | isolated network `fraudstream-p12` | **6 passed** |
| Browser E2E — distributed (isolated, :18080) | `python scripts/run_e2e.py --env-file .env.isolated --profile distributed` | **12 passed**. Two earlier runs: 11 passed, 1 failed — the suite (1 worker) makes 10 logins in its first minute and the 11th hits the 10/min login limit (screenshot: "Too many attempts"); passed alone; the clean run used `LOGIN_RATE_LIMIT_PER_MINUTE=100` on the isolated stack (as the backend tests use 1000), restored to 10 afterwards |
| Browser E2E — local profile (isolated, :18081, restored SQLite) | `--profile local` | **12 passed** (incl. every page + dialogs at 390 px; screenshots `frontend/e2e-results/screenshots/*-mobile.png`) |
| Restore verification | `scripts/verify_restore.py` | live→isolated 10/10; final cycle 10/10; **live after restart vs its pre-Phase-12 fingerprints 10/10** |

### Phase 11C runs (historical)
| Suite | Command | Result |
|---|---|---|
| Default Python suites | `python -m pytest` | **183 passed, 1 skipped** (161 from 11B + 22 new; same Windows-only skip) — run twice |
| Frontend unit tests | `npx vitest run` | **61 passed** (39 existing + 22 new) |
| Frontend typecheck | `tsc -b --noEmit` | pass |
| Production build | `npm run build` | pass — one JS chunk 853.6 kB (241.2 kB gzip), CSS 23.8 kB; no new frontend dependency |
| Container integration, non-disruptive | `pytest -m "containers and not disruptive" infrastructure/tests` | **11 passed** (9 + 2 new) |
| Container integration, full incl. disruptive | `pytest -m containers infrastructure/tests` | **20 passed** in 6 min 39 s (17 + 3 new) |
| Kafka integration | `pytest -m "integration and not spark" streaming/tests` | **12 passed** |
| Spark integration | `pytest -m "integration and spark" streaming/tests/test_spark_integration.py` | **6 passed** |
| Browser E2E — distributed, containers (:8080) | as in 11A + `E2E_ADMIN_USERNAME/E2E_ADMIN_PASSWORD` (admin account from `.env`) | **12 passed, 0 skipped** (8 existing + 4 new) |
| Browser E2E — local profile, containers (:8081) | same, `E2E_BASE_URL=http://127.0.0.1:8081 E2E_PROFILE=local` | **12 passed, 0 skipped** |

New tests: `backend/tests/test_reports.py` (14: every type = direct DB queries, period bounds incl. end-exclusive, server
filters + rejected unsupported filters, admin-only system activity everywhere, CSV escaping/Unicode/formula guard, PDF with
embedded TrueType font for every type, empty period, caps/truncation, 5,000-row PDF, failed generation, snapshot history +
pagination + download errors, migration on a populated 0001 DB incl. downgrade), `backend/tests/test_config_changes.py` (8:
bounds/finite/strict types, only-changed audit, 409 on stale edits, risk-settings validation keeps previous config and engine,
persistence = a freshly built engine from the DB sees the change, analyst denials audited, drift bounds, multi-type audit
query), `frontend/src/test/phase11c.test.tsx` (22), `frontend/e2e/reports-settings.spec.ts` (4: every available report type
generated in the browser and checked against the stored report and independent endpoints, real CSV/PDF/section downloads
inspected, server filter in the request and in every row, empty period, reload by id, history filter; analyst read-only +
API 403; admin client and server validation, cancelled rule edit changes nothing, confirmed drift change shown/audited/
restored; both pages, all six settings tabs and an open dialog at 390 px), container tests
`test_reports_match_postgresql_and_export_through_nginx` (PostgreSQL dialect: values = direct SQL, breakdowns add up, CSV/PDF
through nginx, DejaVu embedded, snapshot row in `generated_reports`, Alembic 0002),
`test_analyst_cannot_change_configuration_and_the_attempt_is_audited`, and disruptive
`test_settings_and_reports_persist_across_api_restart` (drift change + report survive `restart api`; stale version → 409;
Redis runtime config = DB rules/settings after restart; value restored). Existing tests: only *added* routes/endpoints to the
container route test's lists; test support gained an optional `headers` in the fetch mock and a per-user token cache in
`e2e/api.ts`. No assertion was weakened.
Failures seen on the way: report cap test (only 2 alerts in an isolated run → the test now creates its own data; also found
bug 3); config test (`"yes"` accepted → bug 1); first distributed E2E run 2 failed (option hang → bug 5; vanished message →
bug 4, the deployed frontend image predated the fix); one image build failed on a type error in a new unit test (vitest does
not typecheck; fixed, then typecheck + build were run before every later image build). Screenshots:
`frontend/e2e-results/screenshots/{distributed,local}-17…24-*.png` (reports, settings rules/system/audit, rule confirmation,
reports/settings/dialog at 390 px).
Not run in 11C: host-process local profile (containerised local profile used, as in 11A/11B).

### Phase 11B runs (historical)
| Suite | Command | Result |
|---|---|---|
| Default Python suites | `python -m pytest` | **161 passed, 1 skipped** (159 from 11A + 2 new; same Windows-only skip as 11A) |
| Frontend unit tests | `npx vitest run` | **39 passed** (28 existing + 11 new) |
| Frontend typecheck | `tsc -b --noEmit` | pass |
| Production build | `npm run build` | pass — one JS chunk 800.8 kB (227.2 kB gzip), CSS 22.5 kB; no new dependency |
| Container integration, non-disruptive | `pytest -m "containers and not disruptive" infrastructure/tests` | **9 passed** |
| Container integration, full incl. disruptive | `pytest -m containers infrastructure/tests` | **17 passed** in 6 min 26 s (16 from 11A + 1 new). nginx re-resolution again forced a new IP (172.28.0.6 → 172.28.0.9) |
| Browser E2E — distributed, containers (:8080) | as in 11A | **8 passed, 0 skipped** (5 existing + 3 new) on the final build |
| Browser E2E — local profile, containers (:8081) | as in 11A | **8 passed, 0 skipped** on the final build |

Failures seen on the way (all diagnosed before fixing; no assertion weakened): distributed E2E run 1 — 2 failed (E2E login
rate limit → bug 6; map hover → bug 3); run 2 — 1 failed (my assertion picked the first hourly bar, hour 00, which has no
transactions and is drawn empty; now asserts a drawn bar); local run 1 — 1 failed (390 px overflow → bug 4). Two unit-test
expectations of mine were corrected to the right values (F1 = 1754/2107 = 0.832, not 0.833; the map draws one circle per city
after the hit-layer change). Test-support addition: optional `delay` in the fetch mock (`src/test/utils.tsx`), used to test
stale-response protection.
New tests: `backend/tests/test_risk_analytics.py` (window counts = direct DB counts; category filter; deterministic ranking;
limit bounds; `labelled_suspicious` = DB truth), `frontend/src/test/phase11b.test.tsx` (rendering with real-shaped data,
server-side filters + URL, stale responses, empty, API failure, missing fields, map tooltip/selection/legend, risk/period
server filters, country view filter without a request, unavailable geography, error), `frontend/e2e/risk.spec.ts` (values
cross-checked against the API, server filters, reload keeps filters, every city hoverable, **all 9 dashboard pages at 390 px**),
container test `test_risk_analytics_windows_match_postgresql` (windowed aggregates = direct PostgreSQL SQL; geo points =
simulator centroids). Page screenshots: `frontend/e2e-results/screenshots/{distributed,local}-13…16-*.png`.
Not run in 11B: Kafka (12) and Spark (6) integration suites — no streaming code changed in 11B (both passed at the end of 11A);
host-process local profile.

### Phase 11A runs (historical)
| Suite | Command | Result |
|---|---|---|
| Default Python suites | `python -m pytest` | **159 passed, 1 skipped** (150 from Phase 10 + 10 new). The skip is the chmod-based read-only test, which cannot work on Windows; read-only is verified by the container test instead |
| Frontend unit + typecheck | `npx vitest run`; `tsc -b --noEmit` | **28 passed** (14 existing + 14 new); pass |
| Container integration, non-disruptive | `pytest -m "containers and not disruptive" infrastructure/tests` | **8 passed** (5 existing + 3 new) |
| Container integration, full incl. disruptive | `pytest -m containers infrastructure/tests` | **16 passed** in 6 min 17 s (12 existing + 4 new) |
| Kafka integration | `pytest -m "integration and not spark" streaming/tests` | **12 passed** |
| Spark integration | `pytest -m "integration and spark" streaming/tests/test_spark_integration.py` | **6 passed** |
| Browser E2E — distributed, containers (:8080) | `E2E_BASE_URL=http://127.0.0.1:8080 E2E_PROFILE=distributed E2E_ARTIFACTS=read-only E2E_EXPECT_PROCESSOR="Spark Structured Streaming" npx playwright test` | **5 passed** (2 existing + 3 new). First run: 4 passed, **1 failed** (phone-width overflow — bug 3), passed after the fix |
| Browser E2E — local profile, containers (:8081) | `E2E_BASE_URL=http://127.0.0.1:8081 E2E_PROFILE=local E2E_ARTIFACTS=read-only npx playwright test` | **5 passed** |

New tests: `backend/tests/test_model_intelligence.py`, `backend/tests/test_sse_stream.py`, `streaming/tests/test_spark_checkpoint.py`,
`frontend/src/test/phase11.test.tsx`, `frontend/e2e/intelligence.spec.ts` (browser values cross-checked against the API; filters,
row expansion, refresh, tabs, comparison toggles, drift check, artifact filter; no horizontal scroll at 390 px), and container
tests `test_model_intelligence_reports_actual_artifacts_read_only` (served SHA-256 = host files; activation → 409; nothing
changed), `test_streaming_page_reports_spark_checkpoints`, `test_sse_live_feed_stays_open_through_idle_periods_via_nginx`,
`test_nginx_follows_recreated_api_to_a_different_ip`.
Test support: `frontend/src/test/setup.ts` adds a no-op `ResizeObserver` (jsdom lacks it; Recharts needs it to mount). One new
test expectation was corrected (I expected "14.2 ms"; the existing formatter shows "14 ms" for ≥ 10 ms). No existing assertion
was changed or weakened. Not run in 11A: the host-process local profile (Phase 7 path); the containerised local profile was used.

### Phase 10 runs (historical)

| Suite | Command | Result |
|---|---|---|
| Default Python suites (simulator, features, ML, fast path, backend, Kafka units, Spark logic units, processor detection, **container support units**) | `python -m pytest` | **150 passed**, 0 failed (146 from Phase 9 + 4 new) |
| Backend suite stability | `pytest backend/tests` ×3 consecutive | **63/63 each run** |
| Kafka integration (real Kafka/PostgreSQL/Redis, run while the container stack was up) | `pytest -m "integration and not spark" streaming/tests` | **12 passed** (after the fixture DB-15 fix; 11/12 before — see bug 2) |
| Spark integration (real Kafka + Spark container) | `pytest -m "integration and spark" streaming/tests/test_spark_integration.py` | **6 passed** |
| **Container integration (new)** — non-disruptive | `pytest -m "containers and not disruptive" infrastructure/tests` | **5 passed**; 1 failure on the very first run (SSE assertion, cause not identified — see §8), then 5 consecutive green runs |
| **Container integration (new)** — full, incl. disruptive | `pytest -m containers infrastructure/tests` | **12 passed** (two full runs: 6 min 38 s and 5 min 29 s) |
| Frontend unit / typecheck | `npx vitest run`; `tsc -b --noEmit` | **14 passed**; pass |
| Browser E2E — **distributed, fully containerised** (:8080, generated analyst account) | `E2E_BASE_URL=http://127.0.0.1:8080 E2E_PROFILE=distributed E2E_EXPECT_PROCESSOR="Spark Structured Streaming" npx playwright test` | **2 passed** |
| Browser E2E — local, **containerised** (:8081) | `E2E_BASE_URL=http://127.0.0.1:8081 E2E_PROFILE=local npx playwright test` | **2 passed** |
| Browser E2E — local, host processes (Phase 7 path, throwaway SQLite DB) | `E2E_PROFILE=local npx playwright test` | **2 passed** |
| Reconciliation through the containers | `scripts/verify_kafka_pipeline.py --api http://127.0.0.1:8080/api/v1` | **PASS ×2** (run 2: 927 sent / 927 persisted / 927 SSE, 83 alerts, Spark lag 0, persistence lag 0, DLQ 0) |

**Container integration tests** (`infrastructure/tests/test_container_stack.py`, against the running stack only through
nginx :8080, Kafka :9092 and PostgreSQL :5433; nothing mocked):
1. Every service healthy, `kafka-init`/`app-init` exited 0, `unless-stopped` on every long-running service.
2. API not published on the host; no secret in any image's environment; API/consumer/Spark/frontend run as non-root;
   artifacts mounted read-only; uploads/state volumes writable; DB/Kafka/Redis/Spark checkpoints on named volumes.
3. Dashboard status = reality: `/health`, `/system/status`, `/infrastructure/status` report distributed, PostgreSQL, Redis,
   Kafka UP (1 broker, all topics), Spark RUNNING with master `local[…]`, consumer UP and its group active, fallback
   processor NOT running.
4. Every SPA route (incl. deep links) served; every GET endpoint the pages call returns backend JSON (FastAPI request id
   present) through the proxy; CSV export works; unauthenticated → 401 from the backend.
5. **Transaction → investigation workflow:** Simulation Lab inject (API container → Kafka) → Spark → Kafka → consumer →
   PostgreSQL exactly once → **SSE live feed through nginx** → alert → under review + note → investigation opened →
   in progress → resolved (confirmed suspicious, findings) → closed → audit trail; the alert's status follows the decision.
6. API `restart` and `--force-recreate`: healthy again, JWT still valid, nginx keeps proxying.
7. **Consumer crash** (SIGKILL of the main process — `docker kill` would count as a manual stop, which restart policies
   ignore): restarted by the policy; 300 events produced around the crash persisted **exactly once**.
8. **Spark crash** (SIGKILL): restarted by the policy, recovered from its checkpoint; 200 events produced during the
   outage persisted exactly once; status comes from the **new** Spark application id; never reported a fallback.
9. **Spark stopped** (graceful, held > 25 s): dashboard reports NOT RUNNING within seconds (terminated snapshot) and stays
   so; events produced meanwhile are **not** processed by anything else; after start they are persisted exactly once.
10. **Kafka outage** (broker stopped ≥ 20 s): Simulation Lab → 503 naming Kafka (no local fallback), infrastructure
    status Kafka DOWN, profile still distributed; after restart Spark/consumer/API healthy and new events exactly once.
11. **PostgreSQL outage:** API `/health` → 503 `database=down`; Spark keeps scoring; the consumer blocks and retries
    (`db_retries` rises), stays running and healthy (no crash loop, restart count unchanged); after restart all 200
    events produced during the outage persisted exactly once.
12. **Full stack stop/start:** transaction/alert/user counts identical after `stop` + `up -d`; login works; Spark
    resumes from the same checkpoint (score commits advance); new events exactly once.

### End-to-end demonstrations (production topics, database `fraudstream_phase8_verify`)
| Run | Sent | Persisted | SSE live | Alerts | Spark lag / persistence lag | DLQ | Result |
|---|---|---|---|---|---|---|---|
| Verification script (Spark) | 940 | 940 | 940 | 90 | 0 / 0 | 0 | **PASS** |
| **Spark crash recovery** (SIGKILL at t=20 s, ~80 s outage+recovery) | 1,989 | 1,989 tx / 1,989 predictions; 0 duplicate event ids / alerts / audit rows | — | 250 | 0 / 0 | 0 | **PASS** |
| Dashboard E2E (Playwright, distributed) | — | — | live table populated | investigation opened, resolved, closed, audited | — | — | **PASS** |

During the outage `/health`, `/system/status` and the dashboard reported **"NOT RUNNING"**; after restart,
"Spark Structured Streaming". Spark score commits advanced 46 → 57.

**Measured latency (20 TPS, this machine):**
| Path | Before tuning | After tuning |
|---|---|---|
| Kafka → Spark → Kafka (scored output) | p50 6.7 s / p95 9.4 s | **p50 1.82 s / p95 2.87 s / max 3.86 s** |
| Kafka → Spark → Kafka → PostgreSQL (full) | p50 ~9 s / p95 ~14 s | **p50 2.75 s / p95 3.87 s / max 4.6 s** |
Tuning (each step measured): Python validation moved to a single query; JVM-native validation elsewhere;
shuffle partitions 6 → 2; `local[4]` → `local[8]`; custom pre-loading worker daemon. Detection quality on the Spark
run: 93.5 % of injected suspicious events flagged, 0.47 % false flags (same shared logic as Phase 8).

Phase 9 runs above used database `fraudstream_phase8_verify`; Phase 10 runs used `fraudstream` (see §6).

### Bugs found and fixed in Phase 9
1. **`inject()` emitted future-dated events** (scenarios span up to ~9 min): in the distributed profile one Simulation
   Lab click would advance event-time watermarks and make genuine real-time events "late". Injection now ends at
   "now" (`align="end"`); `align="start"` kept for tests. Regression tests added.
2. **Kafka advertised `localhost:9092`**: host clients connected to `::1` first and stalled ~2 s per connection.
   Now advertises `127.0.0.1`; Kafka recreated with its volume — all data retained (offsets verified).
3. **Timeline aggregation on SQLite never bucketed** (SQLAlchemy 2 `/` is true division) — fixed with `//`; the
   chart now zero-fills idle buckets instead of drawing curves across gaps. Test asserts minute-aligned buckets and
   exact totals.
4. Spark SIGTERM handler made re-entrant Py4J calls → now flag-based graceful shutdown (exit 0 verified).
5. PySpark 4 returns source offsets as a Python-repr string → parsed with `ast.literal_eval` (Spark lag now reported).
6. Infrastructure status now reports each consumer group's state/members; inactive groups (e.g. the fallback
   processor's group while Spark runs) are no longer treated as lagging.
7. Overview latency now windowed to the last 15 minutes and labelled; model metric tiles no longer truncate.
8. Flaky tests fixed without weakening assertions: investigation helper now selects the scenario customer's alerts;
   rate-limit test avoids straddling a fixed-window boundary; Spark tests use per-test customer namespaces and a
   long-lived Kafka collector (the old one-shot reader could stop early).

## 6. Preserved data / artifacts
- `data/synthetic/synthetic-fraudstream-v1.0.jsonl.gz` and `.meta.json` — SHA-256 verified unchanged at end of Phase 10.
- `ml/artifacts/` — all **14 original files byte-identical** (SHA-256 snapshot before vs after Phase 10). One derived file
  was added: `ml/artifacts/runtime_config.json` (gitignored runtime output), written by the **host-run** local API used for
  the Phase 7-style E2E (host processes default to writing it there; containers write it to the `app-state` volume). Not
  deleted — remove it only if you want to.
- Databases: `fraudstream_phase8_verify` (8,822 tx) untouched. `fraudstream` kept and **used by the container stack**: it grew
  from 3,088 to 9,363 synthetic transactions (448 → 1,005 alerts, 9 investigations) — container-test traffic uses customer
  ids prefixed `CT<hex>-`; Simulation Lab / reconciliation / E2E traffic is ordinary synthetic traffic. Nothing deleted.
  The `postgres` container was recreated once by Compose on the first Phase 10 `up` (config hash change); data is in the
  named volume and the counts were verified identical immediately after.
- Spark checkpoints `/checkpoints/v2` reused (score commit 57 resumed at the first container start); v1 dirs kept.
- `.env` backed up before `--add-missing` (in the session scratchpad); existing secrets unchanged.
- Nothing was deleted in Phase 10. Containers of the local profile were **stopped**, not removed.
- **Phase 11A:** `ml/artifacts` (15 files) + `data/synthetic` (2 files): SHA-256 snapshot before vs after — **all 17
  byte-identical**. Database `fraudstream`: 11,054 transactions, 1,166 alerts, 13 investigations, **2 drift reports** (stored by
  the drift-check button during E2E — monitoring results, existing endpoint behaviour), 0 dead letters;
  `fraudstream_phase8_verify` untouched. Images `fraudstream-app:local` / `fraudstream-frontend:local` rebuilt; `api`,
  `persistence-consumer`, `frontend` recreated (named volumes untouched); Spark restarted gracefully and resumed from
  `/checkpoints/v2`. Local-profile containers started for E2E and **stopped** again. IP-placeholder containers removed (verified
  none left). Status file backed up to the session scratchpad before editing. Nothing else deleted.
- **Phase 11B:** the same 17 artifact/dataset files — SHA-256 before vs after: **all byte-identical** (also identical to the 11A
  start snapshot). Database `fraudstream`: 14,230 transactions, 1,412 alerts, 20 investigations, 7 drift reports, 0 dead letters
  (growth = E2E/container-test traffic and drift checks; nothing deleted or reset). Images rebuilt; `api`, `persistence-consumer`,
  `frontend` recreated (volumes untouched). Local-profile containers started for E2E and **stopped** again. Status file backed up
  as `DEVELOPMENT_STATUS.before-11B.md` in the session scratchpad (the 11A backup is kept too).
- **Phase 13:** documentation only. The 17 artifact/dataset files: SHA-256 snapshot taken before any change
  (`pre-phase13-sha256.txt` in the session scratchpad) and compared at the end: **all 17 byte-identical**. No model trained or activated; risk configuration untouched. **No database record of any project database
  was read for writing, modified or deleted** — Docker Desktop was not running during the phase, so the live
  PostgreSQL, Kafka, Spark and local-profile volumes were not even accessed; the smoke test used a throwaway SQLite file
  in the scratchpad. The default pytest run used its own temporary databases/directories as in every phase. Backups
  (`../FraudStream-backups/` (next to the repository)), restore copies, isolated projects and screenshots untouched (screenshots
  were **copied**, not moved). Nothing deleted.
- **Phase 12:** the 17 artifact/dataset files — SHA-256 before vs after: **all byte-identical**. No model trained or
  activated. Live database `fraudstream`: **unchanged** — 16 tables' content fingerprints (rows with id ≤ the pre-phase max)
  identical to the pre-Phase-12 backup, counts 15,727 tx / 1,556 alerts / 23 investigations / 42 reports / 3 users; audit
  2,069 → 2,070 (one verification login). Live Kafka: all 31 topic-partition end offsets identical; Spark resumed at commit
  219/311. Live risk configuration untouched. The live stack was **stopped** (approved) during the phase and restarted with
  the Phase 12 images: `api`, `persistence-consumer`, `frontend`, `app-init`, `spark` (mounts) and `postgres` (shm/command)
  were recreated on their existing volumes. Backups (outside the repo, sensitive): `../FraudStream-backups/` (next to the repository)
  (`…-pre-phase12`, `…-live-streaming-stopped`, `…-live-fingerprinted`, `…-p12-final-cycle`); restored copies and evidence:
  `../p12-restore/`. Isolated projects `fraudstream-p12` and `fraudstream-p12r` are **stopped, not removed**
  (their volumes hold ~2 GB of benchmark data; remove with `docker compose -p <project> … down -v` only if you want to —
  not done without approval). Two failed partial backups of my own fingerprint bug (`*.FAILED`) were deleted. Status file
  backed up as `DEVELOPMENT_STATUS.before-12.md` in the session scratchpad (with a source tarball and the checksums).
- **Phase 11C:** the same 17 artifact/dataset files — SHA-256 before vs after: **all byte-identical**. No model trained or
  activated. Database `fraudstream` before → after: transactions 14,230 → 15,727, alerts 1,412 → 1,556, investigations
  20 → 23, audit rows 1,788 → 2,069, drift reports 7 → 8, users 3 → 3, dead letters 0, `generated_reports` 0 → 42 (growth =
  E2E/container/integration traffic, generated reports, one drift check; nothing deleted or reset). **Risk settings and all 10
  risk rules are unchanged** on the live database (the E2E only cancels a rule edit; scoring changes were tested on the test
  DBs). Drift `min_rows` was changed and **restored to 200** by the tests (each step audited). Local SQLite DB: 1,436 → 1,670
  transactions, 97 → 117 alerts (E2E traffic), migrated to 0002. Backups before migrating: `fraudstream-before-11C.dump`
  (pg_dump) and `fraudstream-local-before-11C.db` in the session scratchpad. Images `fraudstream-app:local` (reportlab +
  DejaVu) and `fraudstream-frontend:local` rebuilt; `app-init` re-run (migration), `api`, `persistence-consumer`, `frontend`
  recreated (volumes untouched); Spark/Kafka checkpoints and topics untouched (Kafka/Spark restarted only by the disruptive
  and integration suites, as before). Local-profile containers started for E2E and **stopped** again. One measurement-only
  browser diagnostic was created in the scratchpad, inspected, run (its only effect: the audited drift restore 201 → 200)
  and deleted. Status file backed up as `DEVELOPMENT_STATUS.before-11C.md` in the session scratchpad.

## 7. Incomplete work (honest list)
- All planned dashboard pages are built (Reports and Settings completed in 11C) and verified against the real backend on both
  profiles and at 390 px.
- No user-management UI (accounts are managed through the `/users` API; Settings shows them read-only to admins). No
  per-account preferences exist in the backend, so none are offered. `preferences` and `simulation_defaults` settings are
  stored but read by no component (the Simulation Lab has its own defaults) and are not editable in the UI.
- Model Intelligence has no training trigger in the UI (endpoints exist; artifacts are read-only by default).
- Spark runs in **local mode** (one JVM, `local[8]`) inside one container; there is no standalone/YARN/Kubernetes cluster and
  no multi-node deployment.
- Documentation done in Phase 13 (README, ARCHITECTURE, API, DATA_AND_ML — covers datasets and risk scoring —,
  STREAMING, SECURITY, OPERATIONS, CONTRIBUTING — covers testing —, DEMO_GUIDE, PROJECT_OVERVIEW, DIAGRAMS).
  LICENSE and CI were added in Phase 14; ZIP packaging and extraction verification in Phase 15. Still missing:
  PowerShell helper scripts, Locust.
- MLflow / `shap` package not used; public datasets not downloaded (Kaggle login required).

## 8. Known limitations
- Kafka output from Spark is at-least-once; correctness relies on idempotent persistence (verified).
- Shuffle-partition count is fixed by a query's checkpoint: changing it requires a new `SPARK_CHECKPOINT_DIR`
  (old checkpoints are kept, never deleted automatically).
- `HDFSBackedStateStore` measured slightly faster but keeps all state in JVM memory; RocksDB kept for bounded memory.
- Single Kafka broker (RF=1): broker restarts verified; no tolerance to disk loss.
- A new Spark checkpoint with `SPARK_STARTING_OFFSETS=latest` (used for the demo to avoid replaying old demo traffic
  into the clean DB) skips records produced before the first start; the default is `earliest`.
- Playwright's bundled Chromium download stalled; E2E uses installed Chrome via `E2E_CHROMIUM_PATH`.
- Local profile is single-process (~49 events/s); use `127.0.0.1` rather than `localhost` on Windows.
- **Single host, single Kafka broker (RF=1), single PostgreSQL, single Redis**; Docker restart policies give process-level
  recovery only. No HA, no replication; backups are manual (Phase 12 scripts, local directories); TLS not configured (all traffic is on a private bridge network
  and 127.0.0.1); `SECURE_COOKIES=false` because the dashboard is plain HTTP on localhost.
- **Processor-status detection lag:** a Spark *crash* is restarted by Docker within seconds and the new job reports again
  before the 30 s freshness window expires, so a short crash may never appear as NOT RUNNING on the dashboard (observed in
  every Phase 10 crash run). A graceful stop or an outage longer than ~30 s is shown (verified). The window was kept at 30 s
  to avoid false alarms under load.
- Spark uses **2.64 GiB of its 3 GiB** container limit at steady state (measured with `docker stats`); larger state or more
  partitions may need a higher limit. Docker VM total is ~8 GB.
- (Resolved in 11A) nginx re-resolution of a changed API IP is now exercised by a test that forces a new IP.
- (Resolved in 11A) The Phase 10 SSE failure is root-caused and fixed (stream ended after 15 s idle).
- Kafka "msgs/s" per topic is the change in end offsets between the API's two most recent status checks (per API process);
  n/a until a second check. The Spark rows/s chart is sampled in the browser while the page is open (not stored). Spark's
  progress `batch_id` can be one ahead of the last committed batch on idle triggers (shown as Spark reports it).
- Drift on the current live data is SIGNIFICANT (e.g. `hour_of_day` PSI 8.29, model-score PSI 0.75): simulator live traffic
  (event times ≈ now, short customer histories) differs from the 24 h training dataset. Shown as measured; labelled live
  precision/recall 0.964/0.924 vs test 0.994/0.993 (simulator ground truth).
- Risk Analytics (11B): periods filter on processing time, the alert count on alert creation time (no `minutes` on `/alerts`);
  hourly uses the hour of *event* time (UTC) — simulated traffic is stamped with generation time, so hours show when the simulator
  ran. Velocity is limited to 24 h by its endpoint; the map to 7 days. Merchant names are not returned by the analytics API.
  Rule hits are counted over at most 20,000 flagged predictions. All labels are synthetic simulator ground truth.
- Fraud Intelligence Map (11B): no basemap (lat/lon plot only); points are the simulator's 38 public city centroids; dense
  regions (Europe) overlap visually (nearest-centre hover + tables keep every city reachable); the map is small on phones.
- Frontend bundle is one 854 kB chunk (241 kB gzip), mostly React + Recharts (needed on the landing page); not code-split.
- Reports (11C; Phase 12 measured ≤ 2.3 s generation / 2.5 s PDF on ~190k tx — see the reliability report): generated synchronously in the request; earlier note: generation time was not benchmarked (the E2E generated all five
  analyst report types over 7 days, ~15k transactions / ~1.4k alerts, plus downloads, in 6–10 s per test). No background job
  queue, so very large periods on much larger databases would make the request slow.
  Detail tables are capped at 10,000 stored rows, PDF at 500 rows/table and 400 characters/cell (all stated in the outputs).
  Rule-trigger and latency figures examine at most 20,000 / 100,000 rows (stated). Stored reports are snapshots and are never
  deleted automatically (no retention policy yet). PDF glyph coverage = DejaVu Sans (Latin, Greek, Cyrillic, symbols); CJK
  text is kept in the CSV but renders as missing glyphs in the PDF. Periods are UTC calendar dates / rolling windows.
- Settings (11C): concurrency protection is optimistic (`expected_updated_at`); since Phase 12 it is **mandatory** (428 without it). Schema-level rejections of a body that is not a JSON object are answered 422 by FastAPI before the
  handler and are therefore not audited. The audit trail shows `LOGIN_FAILED` entity ids = the attempted username as typed.
  Risk changes apply to new transactions only; stored scores/categories are never recomputed.
- Latency tiles cover the last 15 minutes; right after the disruptive tests they include events delayed by the simulated
  outages (observed p95 51.9 s) — correct data, not steady-state latency.
- Model training/activation from the API is disabled while `ml/artifacts` is mounted read-only (default); the API answers 409.
- `fraudstream-app:local` is 1.97 GB (xgboost wheel); `fraudstream-spark:local` 3.75 GB.
- A leftover Vite dev server from Phase 9 (node, port 5173) was still running on the host; it was reused for the host E2E and
  left running.

## 9. Infrastructure state at end of Phase 15
Docker Desktop **running**. Live stack `fraudstream` running and healthy (all 7 services, restarted with `compose
start`, same containers and previous image ids; processor Spark; data verified identical, §3 Phase 15). Its two
local-profile containers are stopped again. Stopped, not removed: `fraudstream-p15` (distributed + local),
`fraudstream-p15r` (postgres), `fraudstream-x15` (local profile of the extracted archive, in
`../extract-test-rc2/`), and the earlier `fraudstream-ci`, `fraudstream-ci-r`, `fraudstream-p12`, `fraudstream-p12r`.
Release output (outside the repository): `../FraudStream-release/` (final `FraudStream.zip`, `.sha256`, staging copy,
extraction screenshots); release candidates `../FraudStream-release-rc/`, `../FraudStream-release-rc2/`.

### State at end of Phase 14 (historical)
Docker Desktop **running**. Live stack `fraudstream` running and healthy (all 7 services, restarted with `compose
start` after the regression — same containers, previous image ids; processor Spark; data verified identical, §3
Phase 14). Stopped, not removed: `fraudstream-ci` (distributed + local profile containers), `fraudstream-ci-r`
(postgres only), `fraudstream-p12`, `fraudstream-p12r`, the live project's local-profile containers.

### State at end of Phase 13 (historical)
Docker Desktop was **not running** for the whole of Phase 13 (found stopped at the start; not started, so the live
stack was neither restarted nor modified). Container and volume state is therefore exactly as at the end of Phase 12
(below); the live stack comes back with `docker compose --env-file .env --profile distributed up -d` once Docker
Desktop is started. Nothing is running on the host from Phase 13 (the smoke-test API was stopped).

### State at end of Phase 12 (historical)
Running: the live distributed stack (all 7 services healthy, Phase 12 images; Alembic `0002`). Stopped, not removed:
local-profile containers of the live project; the isolated projects `fraudstream-p12` and `fraudstream-p12r`.

### State at end of Phase 11C (historical)
Running (left up): the full distributed stack — `kafka`, `postgres`, `redis`, `spark`, `api`, `persistence-consumer`,
`frontend` (all healthy, Phase 11C images; one-shots `kafka-init`, `app-init` exited 0; Alembic head `0002`). Local-profile containers `api-local`,
`frontend-local` stopped (not removed). Stop everything with:
`docker compose --env-file .env --profile distributed stop` (volumes and data are kept; `down -v` would delete them — don't).

## 10. How to run

**Full distributed platform, all containers (recommended):**
```powershell
.\.venv\Scripts\python scripts\new_env.py               # first time: .env with random secrets
.\.venv\Scripts\python scripts\new_env.py --add-missing # existing .env: append new keys only
docker compose --env-file .env --profile distributed up -d --build
docker compose --env-file .env --profile distributed ps    # wait until all are (healthy)
# dashboard: http://127.0.0.1:8080  — accounts: FRAUDSTREAM_ADMIN_USERNAME / FRAUDSTREAM_ANALYST_USERNAME (+ passwords) in .env
.\.venv\Scripts\python -m pytest -m containers infrastructure/tests -v   # container tests (disruptive ones restart services)
```
**Containerised local profile:** `docker compose --env-file .env --profile local up -d --build api-local frontend-local`
→ http://127.0.0.1:8081 (SQLite in volume `local-data`; no Kafka/Spark).

**Host processes (Phase 9 style)** still work: start only `kafka postgres redis kafka-init` (+ `--profile spark up -d spark`),
then run the persistence consumer, uvicorn and Vite on the host as before (`DATABASE_URL`/`REDIS_URL` with 127.0.0.1 host
ports). Don't run the host consumer/API at the same time as the containerised ones against the same database.

Kafka/Spark integration tests need the infra up plus `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092`,
`IT_POSTGRES_ADMIN_URL=…@127.0.0.1:5433/postgres`, `REDIS_URL=redis://:<pw>@127.0.0.1:6380/0` (tests force DB 15) and
`SPARK_IT_REDIS_URL=redis://:<pw>@redis:6379/15`.

## 11. Exact next steps
1. **Phases 1–15: done.** The release archive is `../FraudStream-release/FraudStream.zip` (checksum in
   `FraudStream.zip.sha256`). Rebuild it with `python scripts/package_release.py --out <dir outside the repo>`.
2. Owner: publish the repository / archive (not done by the assistant); the first real GitHub Actions run follows.
3. Owner decisions still open: frontend toolchain refresh (Vite ≥ 6.4.4, Vitest ≥ 4.1.12), React Router 7, PyArrow
   ≥ 23.0.1 with the next Spark image rebuild; Kafka fsync-per-message; Spark memory limit; batched persistence
   transactions (1.36×); removal of the throw-away `fraudstream-ci*`, `fraudstream-p12*`, `fraudstream-p15*` and
   `fraudstream-x15` volumes (`docker compose -p <project> ... down -v`, only if wanted); recreating the live
   containers on the Phase 15 images (`docker compose --env-file .env --profile distributed up -d` after a backup).

### How to resume
Read this file; `python -m pytest` (224 passed, 2 skipped); `cd frontend; npx vitest run` (78 passed); with the stack up,
the container (25 — disruptive ones on the isolated stack only), Kafka (12) and Spark (6) integration suites and Playwright E2E (12 per profile, needs
`E2E_USERNAME/E2E_PASSWORD` and `E2E_ADMIN_USERNAME/E2E_ADMIN_PASSWORD`; wait ≥ 60 s between two
profile runs from the same machine — login rate limit). Models are trained; do not retrain unless needed.
