# Contributing to FraudStream

This guide gets a new developer from a fresh clone to a running, tested change, **without damaging the datasets,
databases, Kafka log, Spark checkpoints or model artifacts** that the project depends on. Read §9 before running
anything that writes data.

## 1. Repository layout

```text
backend/                 FastAPI application (package `app`), Alembic migrations, backend tests
  app/api/routes/        HTTP endpoints (alerts, analytics, auth, models, reports, simulation, transactions)
  app/api/params.py      shared query/path bounds (page, ids, minutes)
  app/core/              settings (pydantic-settings), logging, runtime (loaded models, broker, limiter)
  app/database/          engine/session, Alembic upgrade helper
  app/models/entities.py SQLAlchemy models (21 tables)
  app/schemas/api.py     request models + response serialisers
  app/risk_engine/       rules + noisy-OR scoring (no web/DB imports: also used by Spark)
  app/investigations/    alert/investigation state machines
  app/security/          Argon2id, JWT, refresh rotation, RBAC, rate limiter
  app/services/          persistence, reports, audit, runtime config, broker, Redis circuit breaker, ...
  app/streaming/         persistence consumer, fallback processor, runtime-config watcher
  app/cli.py             migrate | create-user | dev-bootstrap | prune-reports
  migrations/versions/   0001_initial_schema, 0002_generated_reports
  tests/                 backend tests (SQLite, TestClient)
frontend/                React + TypeScript + Vite dashboard
  src/pages/             one file per page (Overview, LiveStream, Investigations, Details, SimulationLab,
                         Streaming, Models, RiskAnalytics, FraudMap, Reports, Settings, Login)
  src/services/api.ts    typed API client; src/types/api.ts response types
  src/test/              Vitest + Testing Library; e2e/  Playwright specs (real stack, nothing mocked)
  nginx/                 nginx template (SPA + /api reverse proxy)
ml/                      datasets, features, preprocessing (splits), training, evaluation, inference,
                         explainability, drift; artifacts/ = trained models (do not modify, §8)
simulator/               transaction generator + 38-city geography
streaming/               shared event schema, Kafka client/topics, Spark job + logic + worker daemon, tests
infrastructure/          Dockerfiles, app-init, health checks, Kafka topic script, isolated-stack override,
                         container integration tests
scripts/                 env generation, backup/restore/verify, E2E runner, secret scan, probes
benchmarks/              Phase 12 benchmark scripts, results/*.json
data/synthetic/          training dataset (gitignored; keep it, §8)
docs/                    documentation (start at the README's documentation map)
```

## 2. Development prerequisites

- Python **3.12** and a virtual environment in `.venv`
- Node.js **22+** and npm
- Docker Desktop with Compose v2. Allow **≥ 8 GB** for the Docker VM if you run the distributed profile; the Spark
  container alone has a 3 GiB limit.
- Windows: use `127.0.0.1`, not `localhost`, in URLs. `localhost` resolves to `::1` first and stalls clients.

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r backend\requirements-dev.txt
cd frontend; npm ci; cd ..
.\.venv\Scripts\python scripts\new_env.py        # .env with random secrets (never commit it)
```

## 3. Local development setup (fast loop, no Kafka/Spark)

```powershell
# API (terminal 1)
$env:PYTHONPATH = "$PWD;$PWD\backend"
.\.venv\Scripts\python -m app.cli migrate
.\.venv\Scripts\python -m app.cli dev-bootstrap     # admin + analyst with random passwords -> .dev-credentials.txt
.\.venv\Scripts\python -m uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8000 --reload

# Frontend (terminal 2) - Vite on http://127.0.0.1:5173, proxies /api to :8000
cd frontend; npm run dev
```

The `local` profile (from `.env`) uses SQLite at `data\fraudstream.db` and the in-process pipeline. To keep your
experiments away from any database you care about, point `DATABASE_URL` at a throwaway file. Set
`RUNTIME_CONFIG_DIR` to a scratch directory if you do not want `ml\artifacts\runtime_config.json` rewritten (it is a
derived, gitignored file, but it sits next to the models):

```powershell
$env:DATABASE_URL = "sqlite:///C:/temp/fraudstream-dev.db"
$env:RUNTIME_CONFIG_DIR = "C:\temp\fraudstream-state"
```

## 4. Distributed development setup

```powershell
docker compose --env-file .env --profile distributed up -d --build
docker compose --env-file .env --profile distributed ps
```

- After changing **backend / ml / streaming / simulator code**, rebuild the app image:
  `docker compose --env-file .env --profile distributed up -d --build api persistence-consumer`.
  Spark mounts the source read-only, so `docker compose --env-file .env --profile distributed restart spark` picks up
  changes. Rebuild Spark only if `infrastructure/spark/*` changed.
- After changing the **frontend**: `... up -d --build frontend`. Run `npm run typecheck` first, because Vitest does
  not type-check and a type error breaks the image build.
- Running the API or consumer on the host against the containerised infrastructure is possible: start only
  `kafka postgres redis kafka-init` and use `127.0.0.1` ports 9092 / 5433 / 6380. **Never** run a host consumer or API
  at the same time as the containerised ones against the same database.

For anything disruptive (benchmarks, failure tests, restore experiments) use an **isolated stack**, a separate compose
project with its own network, volumes, ports and secrets:

```powershell
.\.venv\Scripts\python scripts\isolated_env.py        # .env.isolated (ports 18080, 19092, 15433, 16380, 14040)
docker compose -p fraudstream-p12 --env-file .env.isolated -f docker-compose.yml `
    -f infrastructure/isolated/docker-compose.isolated.yml --profile distributed up -d
```

The Docker VM on the reference machine cannot hold two full stacks, so stop the main one first (`stop`, not `down -v`).

## 5. Testing

| Suite | Command | Needs | Writes to |
|---|---|---|---|
| Python unit + backend + ML + streaming logic | `.\.venv\Scripts\python -m pytest` | nothing | temp SQLite / temp dirs only |
| Lint (CI gate) | `.\.venv\Scripts\python -m ruff check . --select E9,F` | — | — |
| Frontend unit | `cd frontend; npx vitest run` | — | — |
| Frontend types / build | `npm run typecheck`; `npm run build` | — | `frontend/dist` |
| Kafka integration | see below | infra containers | **own** `it-<random>.` topics + temp DB `fraudstream_it_<random>` (dropped afterwards); Redis DB 15 |
| Spark integration | see below | infra + Spark image | own topics; runs a Spark container on the given network |
| Container integration (non-disruptive) | `pytest -m "containers and not disruptive" infrastructure/tests` | running distributed stack | test traffic (customer ids `CT<hex>-`) |
| Container integration (**disruptive**) | `pytest -m containers infrastructure/tests` with `FS_COMPOSE_PROJECT` set | **isolated** stack | stops, kills and restarts containers |
| Browser E2E | `python scripts\run_e2e.py --env-file <env> --profile distributed\|local` | running stack, Chrome | alerts, investigations, reports, audited setting changes (restored afterwards) |

Expected results (end of Phase 14): pytest 203 passed / 2 skipped on Windows, **205 passed / 0 skipped on Linux**
(the read-only-directory test and the CJK PDF font test only run there); Vitest 64; container 25; Kafka 12; Spark 6;
E2E 12 per profile. The same suites run in CI, see §11.

**Integration environment variables** (values come from your env file; never commit them):

```powershell
# Kafka integration (infra up)
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
$env:IT_POSTGRES_ADMIN_URL   = "postgresql+psycopg://fraudstream:<POSTGRES_PASSWORD>@127.0.0.1:5433/postgres"
$env:REDIS_URL               = "redis://:<REDIS_PASSWORD>@127.0.0.1:6380/0"      # tests force DB 15
.\.venv\Scripts\python -m pytest -m "integration and not spark" streaming/tests

# Spark integration (additionally)
$env:SPARK_IT_REDIS_URL = "redis://:<REDIS_PASSWORD>@redis:6379/15"
$env:SPARK_IT_NETWORK = "fraudstream"; $env:SPARK_IT_COMPOSE_PROJECT = "fraudstream"   # or the isolated names
.\.venv\Scripts\python -m pytest -m "integration and spark" streaming/tests/test_spark_integration.py

# Container suite against the isolated stack
$env:FS_COMPOSE_PROJECT = "fraudstream-p12"; $env:FS_ENV_FILE = ".env.isolated"
$env:FS_COMPOSE_FILES = "docker-compose.yml,infrastructure/isolated/docker-compose.isolated.yml"
.\.venv\Scripts\python -m pytest -m containers infrastructure/tests -v
```

Disruptive tests **refuse the live project** `fraudstream` unless `FS_ALLOW_DISRUPTIVE_ON_LIVE=1`. Do not set it.

E2E notes: the suite logs in several times, and the login limit is 10/min. Wait ≥ 60 s between two runs from the same
machine, or raise `LOGIN_RATE_LIMIT_PER_MINUTE` **on an isolated stack only**. If Playwright's Chromium download is
unavailable, `run_e2e.py` uses the installed Chrome (`E2E_CHROMIUM_PATH`).

## 6. Adding an API endpoint

1. Add the handler to the matching router in `backend/app/api/routes/`. All routers are included under `/api/v1` in
   `app/main.py`; a new router file must be added to the `include_router` loop.
2. **Authentication:** take `user: CurrentUser` (any role) or `admin: AdminUser`. For configuration endpoints use
   `ConfigAdmin`, which audits denials. Only `/health` and the auth endpoints are public.
3. **Validation:** request bodies are Pydantic models in `app/schemas/api.py`. Use
   `model_config = ConfigDict(extra="forbid")` and explicit bounds. Use `Page` and `IdPath` from `app/api/params.py`
   and give every `limit` / `minutes` query a `ge` and `le`. A test probes every GET route with hostile values and
   fails on any 5xx.
4. **State changes** write an audit row with `app.services.audit.record(...)` in the same transaction.
   Configuration-like resources must require `expected_updated_at` (see `_check_version` in `routes/models.py`).
5. Return plain dicts built by the serialisers in `app/schemas/api.py`. Never return password or token hashes, file
   system paths or environment values.
6. Tests: add to `backend/tests/` (TestClient on SQLite). If the endpoint is called by a page, add it to the container
   test's route list in `infrastructure/tests/test_container_stack.py` so it is checked through nginx on PostgreSQL.
7. Document it in [docs/API_DOCUMENTATION.md](docs/API_DOCUMENTATION.md).

## 7. Adding a frontend page

1. Create `frontend/src/pages/<Name>.tsx`, add a `<Route>` in `src/App.tsx` and a navigation entry in
   `src/layouts/AppLayout.tsx`.
2. Fetch through `src/services/api.ts` with TanStack Query. Add response types to `src/types/api.ts`. Key queries by
   their filter values so a slow, stale response cannot overwrite a newer one.
3. Display only values from the API. Unavailable values render as "n/a" with a reason (the `Unavailable` component),
   never as 0. Synthetic data and simulated locations must be labelled as such.
4. Reuse `src/components/ui.tsx` and `src/charts/Charts.tsx`. State must be conveyed in text, never by colour alone.
5. Check the layout at **390 px** width: no horizontal page scroll (an E2E test asserts this for every page).
6. Tests: `src/test/*.test.tsx` (Vitest, mocked fetch via `src/test/utils.tsx`) and, for real-stack behaviour,
   a Playwright spec in `frontend/e2e/` that cross-checks displayed values against the API.
7. Run `npm run typecheck` and `npm run build` before building the image.

## 8. Model artifacts and datasets

- `ml/artifacts/` holds the trained models of experiment `v20261114T124627Z` (14 files: 4 models × 3 files,
  `registry.json`, the experiment JSON) plus a derived `runtime_config.json`. **Retraining does not reproduce these
  files byte-for-byte**, so treat them as irreplaceable. Phases 10–12 verified their SHA-256 before and after every
  phase.
- `data/synthetic/` holds the training dataset (`.jsonl.gz` + `.meta.json` with its SHA-256). It is gitignored.
- Containers mount both **read-only** by default. Training, activation and rollback endpoints return 409 until
  `ARTIFACTS_MOUNT_MODE=rw` is set deliberately.
- Training: `python -m ml.training.train` writes a **new** version directory and updates `registry.json`. Back up
  `ml/artifacts` first (`scripts\backup.py --components artifacts,datasets`). `--quick` trains on a small dataset for
  tests only; its results are not comparable.
- Feature changes must bump `FEATURE_CONFIG_VERSION` in `ml/features/definitions.py`. ML library versions must stay
  identical in `backend/requirements.txt` and `infrastructure/spark/requirements-spark.txt`.

## 9. Protecting existing data — rules for every contributor

| Never | Instead |
|---|---|
| `docker compose down -v`, `docker volume rm`, `docker system prune --volumes` | `stop` / `down` (volumes kept); take a backup first if in doubt |
| delete or edit files in `ml/artifacts/` or `data/synthetic/` | train into a new version; back up first |
| run disruptive tests, benchmarks or restores against project `fraudstream` | use an isolated compose project (§4) |
| run `restore.py --allow-live-project` | restore into an empty, separate project |
| delete a Spark checkpoint directory to "fix" Spark | set a new `SPARK_CHECKPOINT_DIR`; old directories are kept |
| delete Kafka topics of the main stack | integration tests create and delete only their own `it-<random>.` topics |
| reset or truncate the PostgreSQL database | use a throwaway database (the Kafka tests create `fraudstream_it_<random>`) |
| run `prune-reports --apply` without a dry run first | run it without `--apply`, read the list, back up |
| commit `.env`, `.env.*`, `.dev-credentials.txt`, backups or dumps | they are gitignored; run `python scripts\scan_secrets.py` before sharing anything |

Backups go to `..\FraudStream-backups` (outside the repo) and contain password hashes. Never move them into the
repository.

## 10. Coding and testing expectations

- **Python:** 3.12, type hints, `ruff` (line length 125; rules E, F, W, I, B, UP). CI enforces the correctness rules
  (`E9`, `F`) on the whole tree and the full ruleset on `scripts/ci`. The older code still has 216 style findings
  (long lines, `timezone.utc` → `UTC`, import order) that are not enforced yet; do not add new ones. Keep `app/risk_engine` and
  `ml/features` free of web and database imports, because Spark imports them.
- **TypeScript:** strict type check must pass. No new runtime dependency without a reason; the bundle is already a
  single 854 kB chunk.
- **Every bug fix comes with a test** that fails without the fix. Do not weaken or delete an existing assertion to
  make a change pass. If an assertion is wrong, explain why in the change.
- **Report honestly.** Record which suites you ran and their exact results, including failures and skips. If you
  could not run a suite, say so instead of claiming it passes.
- **Schema changes** go through a new Alembic revision that is additive or has a tested downgrade. Test it on a copy
  of a populated database, and back up before applying it to a real one.
- **Documentation:** update the relevant file in `docs/` and the engineering log in `DEVELOPMENT_STATUS.md`.

## 11. Continuous integration

Two GitHub Actions workflows live in `.github/workflows/`. Neither deploys anything or uses repository secrets. Every
credential is generated randomly on the runner by `scripts/ci/ci_env.py` and masked in the logs. Third-party actions
are pinned to commit SHAs, and Dependabot (`.github/dependabot.yml`) proposes updates for them.

### `ci.yml` — on every pull request, every push to `main`, and on demand

| Job | What runs | Blocks the PR when |
|---|---|---|
| **backend** | `pip install -r backend/requirements-dev.txt` (pinned); `ruff check . --select E9,F` (syntax errors, undefined names, unused imports); full ruleset on `scripts/ci`; `pytest` (the default suite: simulator, ML, backend API incl. migrations, auth/RBAC, settings concurrency, reports; streaming logic units). The app image's PDF fonts are installed so the CJK PDF test runs. | any lint error or failing test |
| **frontend** | `npm ci` (lockfile, never rewritten); Vitest; `npm run typecheck`; `npm run build` | any failure |
| **compose** | `scripts/ci/check_compose.py`: `docker compose config` for the default services, every profile (`distributed`, `local`, `spark`, `python-processor`) and the isolated override; the expected services per profile; build contexts, Dockerfiles and their `COPY` sources; bind-mount sources; every host port bound to 127.0.0.1; no bind mount of the repository root or an env file. Nothing is started. | invalid config or missing file |
| **security** | `scripts/ci/check_repo_hygiene.py` (no env files, databases, dumps, backups, Argon2 hashes, private keys; required `.gitignore` rules; placeholder-only `.env.example`); **gitleaks** 8.30.1 over the full history (image pinned by digest, findings redacted); **pip-audit** 2.10.1 over all three requirement files and **npm audit** over `package-lock.json`, gated by `scripts/ci/audit_gate.py` | a secret, a forbidden file, or a dependency advisory that is **not** in `.github/security/audit-baseline.json` |
| **kafka-integration** (after backend) | Kafka, PostgreSQL and Redis from `docker-compose.yml` as the throw-away project `fraudstream-ci`; `pytest -m "integration and not spark" streaming/tests` (own topics, temporary database, broker restart) | any failing test |

### `full-stack.yml` — manual (`workflow_dispatch`) and weekly (Mondays)

Builds all three images and starts the complete distributed profile as `fraudstream-ci`. It regenerates the
gitignored dataset with `scripts/ci/prepare_dataset.py`, which checks its content hash against the stored experiment.
It then runs the container suite (optionally without the disruptive tests), the Spark integration suite, Playwright
E2E on the distributed profile, the containerised **local** profile with its own E2E run, and a Trivy scan of the
OS packages in the app and frontend images (report only). Screenshots, JUnit files and, on failure, service logs are
uploaded as artifacts. It runs separately from `ci.yml` because it needs ~7 GB of images and 60–90 minutes.

**Login rate limit in E2E.** The Playwright suite logs in more than 10 times per minute. The generated CI env file
sets `LOGIN_RATE_LIMIT_PER_MINUTE=100` for that throw-away stack only. `docker-compose.yml` and `.env.example`
keep the production default of 10/min, and the backend tests that check the limit are unchanged.

### Dependency-advisory baseline

The pinned dependencies date from 2024 and have published advisories. Upgrading them is a deliberate decision with
retesting, not something CI does on its own. `.github/security/audit-baseline.json` therefore records every
**currently known** finding (package, version, advisory id). CI fails on anything new: a new advisory, or a
dependency change that brings one in. When you upgrade a package, regenerate the baseline in the same change so that
resolved entries disappear:

```powershell
.\.venv\Scripts\python -m pip install pip-audit==2.10.1
.\.venv\Scripts\python -m pip_audit -r backend/requirements.txt -r backend/requirements-dev.txt -r infrastructure/spark/requirements-spark.txt -f json -o pip-audit.json
cd frontend; npm audit --json > ..\npm-audit.json; cd ..
.\.venv\Scripts\python scripts/ci/audit_gate.py --pip-json pip-audit.json --npm-json npm-audit.json --baseline .github/security/audit-baseline.json            # check
.\.venv\Scripts\python scripts/ci/audit_gate.py --pip-json pip-audit.json --npm-json npm-audit.json --baseline .github/security/audit-baseline.json --write-baseline   # after a reviewed upgrade
```

Never add an entry to silence a new finding without recording why in `SECURITY.md`.

### Reproducing the CI jobs locally

```powershell
# backend + frontend + compose (no Docker stack needed except for compose's CLI)
.\.venv\Scripts\python -m ruff check . --select E9,F
.\.venv\Scripts\python -m pytest
cd frontend; npm ci; npx vitest run; npm run typecheck; npm run build; cd ..
.\.venv\Scripts\python scripts/ci/check_compose.py --env-file .env.example

# kafka-integration / full-stack on a throw-away project (stop the main stack first if memory is short)
.\.venv\Scripts\python scripts/ci/ci_env.py create --out .env.ci --project fraudstream-ci --e2e
$C = "-p fraudstream-ci --env-file .env.ci -f docker-compose.yml -f infrastructure/isolated/docker-compose.isolated.yml".Split()
docker compose @C --profile distributed up -d --build
.\.venv\Scripts\python scripts/ci/wait_for_stack.py --project fraudstream-ci --env-file .env.ci --profile distributed
$env:GITHUB_ENV = "$env:TEMP\fraudstream-ci.env"; .\.venv\Scripts\python scripts/ci/ci_env.py export --env-file .env.ci --github-env
Get-Content $env:GITHUB_ENV | ForEach-Object { $k, $v = $_ -split '=', 2; Set-Item "env:$k" $v }
.\.venv\Scripts\python -m pytest -m "integration and not spark" streaming/tests
.\.venv\Scripts\python -m pytest -m containers infrastructure/tests -v
.\.venv\Scripts\python -m pytest -m "integration and spark" streaming/tests/test_spark_integration.py
.\.venv\Scripts\python scripts/ci/seed_traffic.py --env-file .env.ci --profile distributed   # fresh DB: E2E needs recent traffic
.\.venv\Scripts\python scripts/run_e2e.py --env-file .env.ci --profile distributed
```

On a **fresh** database, always run `seed_traffic.py` before the E2E suite. Playwright runs the specs alphabetically,
and only the last one (`workflow.spec.ts`) creates traffic. Without seeding, the Risk Analytics and map tests skip
("no scored transactions in the last 7 days") and the Model Intelligence, report-download and 390 px tests fail
waiting for data. The script only starts a 30 s synthetic simulation and injects each scenario once.

`.env.ci` and the exported file contain throw-away secrets. Both are gitignored; delete them when you are done.
`fraudstream-ci` is a separate project with its own volumes. Stop it with `docker compose @C stop`. Its volumes
contain only test data; remove them with `docker compose @C down -v` only if you are sure the `-p fraudstream-ci`
is in the command.

**Not the same as a GitHub run.** Locally, Docker Desktop runs Linux containers, but the host tools run on Windows.
The Linux behaviour of the backend and frontend jobs can be reproduced exactly by running their steps inside
`python:3.12-slim-bookworm` / `node:22-bookworm` containers, which is how they were validated before publication.
