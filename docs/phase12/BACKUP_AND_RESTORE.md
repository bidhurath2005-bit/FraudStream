# FraudStream — Backup and restore (Phase 12)

Scripts: `scripts/backup.py`, `scripts/restore.py`, `scripts/verify_restore.py` (shared helpers in `scripts/_ops.py`).
Python standard library + `psycopg` (already in the venv) + the Docker CLI. Works on Windows and Linux.

> **A backup is sensitive.** The database dump contains password hashes, refresh-token hashes and the full audit
> trail. The default destination is `../FraudStream-backups` — **outside** the repository — so it can never end up in
> Git or in the project ZIP. `.env` (secrets) is **never** copied into a backup.

## 1. What is backed up, and why

| Component | Source | Method | Classification |
|---|---|---|---|
| `postgres` | PostgreSQL `fraudstream` (volume `postgres-data`) | `pg_dump -Fc` inside an **exported snapshot**; per-table row counts **and content fingerprints** taken in the same snapshot | **Required**; **sensitive** |
| `sqlite` | local-profile DB in volume `local-data` | SQLite online-backup API (inside `api-local` if running, else on a copy of the stopped volume) + `PRAGMA integrity_check` | Required for the local profile; sensitive |
| `artifacts` | `ml/artifacts` (models, registry, experiment) | file copy + SHA-256 per file (source and copy compared) | **Required** (retraining gives different models) |
| `datasets` | `data/synthetic` | file copy + SHA-256 | Required for reproducibility |
| `uploads` | volume `app-data` (uploaded datasets) | `tar.gz` from a read-only mount | Optional (re-uploadable) |
| `state` | volume `app-state` (`runtime_config.json`) | `tar.gz` | **Re-creatable** (regenerated from the DB at API start) |
| `redis` | volume `redis-data` (AOF) | `tar.gz` | **Re-creatable** (config re-published at API start; heartbeats and rate-limit counters are transient) |
| `kafka` | volume `kafka-data` | `tar.gz`; **consistent only while Kafka is stopped** (refused while running unless `--allow-running-streaming`, recorded as crash-consistent) | Optional (in-flight, unprocessed events) |
| `spark` | volume `spark-checkpoints` | `tar.gz`; consistent only while Spark is stopped; back up **together with kafka** (checkpoint offsets refer to the Kafka log) | Optional (without it Spark starts a new checkpoint) |
| config | `docker-compose.yml`, `.env.example`, `create_topics.sh` + the list of required env **key names** | copy | Reference only |
| **not included** | `.env` | — | **Sensitive, re-creatable**: `python scripts/new_env.py` makes new random secrets. Accounts keep working (password hashes are in the DB); only issued tokens become invalid. Keep a copy in your own secret store if you want the same secrets. |

Default components: `postgres,sqlite,artifacts,datasets,uploads`. `--components all` adds the rest.

## 2. Guarantees of `backup.py`

- Timestamped id `<UTC yyyymmddThhmmssZ>-<project>[-label]`; written to `<id>.partial/`, renamed to `<id>/` **only after**
  every component succeeded, `manifest.json` (status `COMPLETE`) and `SHA256SUMS` were written and re-verified.
- Any failure → renamed to `<id>.FAILED/` with `FAILED.txt`, exit code 1, never used by `restore.py` (it verifies first).
  Observed twice during development (schema assumptions in the fingerprint query) — both runs ended as `.FAILED` with
  a clear message, nothing was presented as a backup.
- Never overwrites: an existing `<id>`, `.partial` or `.FAILED` directory aborts the run.
- PostgreSQL integrity: the archive is listed with `pg_restore --list` in a throwaway container; every table counted
  in the snapshot must have a `TABLE DATA` entry.
- `--verify <dir>`: every file listed in `SHA256SUMS` exists with that hash, no unlisted file exists, the manifest is
  `COMPLETE` and its per-file hashes agree.

## 3. Commands

```powershell
# live stack (online; PostgreSQL is dumped consistently while the stack runs)
.\.venv\Scripts\python scripts\backup.py --label nightly
# consistent streaming state too: stop kafka + spark first (or the whole stack)
docker compose --env-file .env --profile distributed stop spark kafka
.\.venv\Scripts\python scripts\backup.py --components kafka,spark,redis,state --label streaming
docker compose --env-file .env --profile distributed start kafka spark
# verify an existing backup
.\.venv\Scripts\python scripts\backup.py --verify ..\FraudStream-backups\<id>
```

## 4. Restore (isolated project — the tested path)

`restore.py` refuses the live project `fraudstream` (use `--allow-live-project` only for disaster recovery onto
**fresh, empty** volumes), refuses a non-empty target database / volume / directory, verifies the backup first, and
restores PostgreSQL with `pg_restore --single-transaction --exit-on-error` (all or nothing). It then compares the
restored row counts and content fingerprints with the manifest.

Expected sequence (required services in brackets):

```powershell
python scripts\isolated_env.py                                     # .env.isolated: fresh secrets, other ports
$I = "-p fraudstream-p12 --env-file .env.isolated -f docker-compose.yml -f infrastructure/isolated/docker-compose.isolated.yml"
# 1. database + local DB + files (starts only [postgres]; restored files go to an EMPTY directory)
python scripts\restore.py --backup ..\FraudStream-backups\<id> $I --components postgres,sqlite,artifacts,datasets,uploads `
    --files-to ..\p12-restore\files --report restore.json
# 2. optional streaming state (kafka + spark must not be running) from a backup taken with them stopped
python scripts\restore.py --backup ..\FraudStream-backups\<streaming-id> $I --components kafka,spark
# 3. point the isolated stack at the restored artifacts/datasets (read-only mounts) in .env.isolated:
#      ARTIFACTS_HOST_DIR=../p12-restore/files/artifacts
#      DATASETS_HOST_DIR=../p12-restore/files/datasets
# 4. start everything [kafka, postgres, redis, kafka-init, spark, app-init, api, persistence-consumer, frontend]
docker compose $I.Split() --profile distributed up -d
# 5. verify the RUNNING application against the backup
python scripts\verify_restore.py --backup ..\FraudStream-backups\<id> $I --report verify.json
```

Required environment variables: the keys listed in the manifest (`required_env_keys`, from `.env.example`); the
restore target needs at least `POSTGRES_PASSWORD`, `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_HOST_PORT`,
`REDIS_PASSWORD`, `JWT_SECRET` and login accounts for `app-init`. Kafka: `KAFKA_CLUSTER_ID` must equal the one the
restored `kafka-data` was created with (default `fsk1-lOcAlClusTer0001A`; not a secret).

## 5. Restoration evidence (measured 2026-11-15)

Source: the **live** stack. Online backup `20261115T152746Z-fraudstream-pre-phase12` (9.6 s: PostgreSQL 5.97 MB dump
in 3.2 s, SQLite 5.54 MB, 14 artifact files 6.88 MB, 2 dataset files 12.12 MB), streaming backup
`20261115T153044Z-fraudstream-live-streaming-stopped` taken with the live stack stopped (Kafka 20.0 MB / 503 files,
Spark checkpoints 27.0 MB / 11,968 files, Redis, state; 9.9 s), and `20261115T153455Z-fraudstream-live-fingerprinted`
(PostgreSQL + artifacts + datasets with content fingerprints; live DB unchanged since the first backup — identical
counts). Target: isolated project `fraudstream-p12` (own network, volumes, ports, secrets).

| Check | Result |
|---|---|
| Backup verification before restore (23 + 8 checksums) | PASS |
| PostgreSQL restore (single transaction) | PASS, 1.4 s `pg_restore`; **22/22 tables: row counts identical** (transactions 15,727, alerts 1,556, investigations 23, generated_reports 42, audit_logs 2,069, …); Alembic `0002` |
| SQLite restore | PASS — `integrity_check ok`, counts identical (1,670 transactions, 117 alerts, 6 investigations, 9 reports, 264 audit rows), Alembic `0002` |
| Artifacts / datasets restore | PASS — 14 + 2 files, SHA-256 identical to the manifest |
| Kafka / Spark volume restore | PASS — 503 / 11,968 files; **all 31 topic-partition end offsets identical** to the live stack; persistence-consumer lag 0; Spark resumed at the same committed batch (score 219, validated-dedup 311) — no replay, no duplicate rows (15,727 transactions = 15,727 distinct event ids) |
| Application start on the restored data | all 7 services healthy **48 s** after `up -d` |
| `verify_restore.py` on the running stack | **10/10 PASS**: content fingerprints of 16 tables (rows with id ≤ backup max) identical; Alembic; `/health` (DB up, model loaded, distributed); login; SHA-256 of all 14 artifacts **as served by the API** = manifest; active model loaded; dataset SHA-256 inside the API container = manifest; 15 dashboard endpoints 200 through nginx; 11 SPA routes; Spark reported as the active processor |
| Refusals | restore into the non-empty DB → refused; SQLite file exists → refused; live project → refused |

A second, end-to-end cycle with the final tooling (backup of the isolated stack with every component → restore into
a fresh project → verify) is recorded in §6.

## 6. Final restore cycle

Source: the isolated stack after all benchmarks and failure tests (197,281 transactions), Spark and Kafka **stopped**
for a consistent streaming snapshot. Target: a third, brand-new project `fraudstream-p12r` (own network, volumes and
secrets from `isolated_env.py --out .env.restore`).

| Step | Result |
|---|---|
| `backup.py --components all` | **COMPLETE in 65 s**: PostgreSQL 84.9 MB, SQLite 6.3 MB, artifacts 14 files, datasets 2, uploads, state, Redis, Kafka 171.6 MB / 548 files, Spark checkpoints 64.9 MB / 12,804 files; 27 checksums verified |
| `restore.py` (all 9 components) | **PASS in 73 s**: 0 count mismatches, **16/16 table fingerprints identical**, SQLite integrity ok, every file checksum identical, volume file counts identical |
| `up -d` on the restored data | all 7 services healthy after **43 s** |
| Streaming state | **31/31 topic-partition end offsets identical**; Spark resumed at the same committed batches (score 646, validated-dedup 1076); 197,281 transactions = 197,281 distinct event ids (no replay) |
| `verify_restore.py` | first run 3/4 — **admin login 401**, see below; after giving the restore env new account names: **10/10 PASS** |
| New work on the restored stack | container tests: services healthy, status = reality, full transaction → alert → investigation workflow: **3 passed** |

**Lesson recorded:** the restored database already contains the source's accounts with the source's password hashes,
and `app-init` never modifies an existing account. A restore env that reuses an existing username with a newly
generated password therefore cannot log in (correct, safe behaviour). Either keep the source credentials in your secret
store, or give the restore env **new** account names (`FRAUDSTREAM_ADMIN_USERNAME`, `FRAUDSTREAM_ANALYST_USERNAME`) so
`app-init` creates them, then reset other passwords through the API.

The **live** stack was also checked against its pre-Phase-12 fingerprinted backup after its restart on the Phase 12
images: `verify_restore.py` **10/10**, all 31 Kafka offsets identical to before, Spark resumed at commit 219/311.

## 7. Known limitations

- Single-host tooling: backups are directories on the same machine (copy them elsewhere yourself); no encryption at
  rest, no retention/rotation of backups, no scheduling.
- Kafka/Spark state is only consistent when those services are stopped; an online backup covers PostgreSQL (the
  system of record) only. Losing in-flight Kafka events is the accepted gap of an online backup.
- `restore.py` restores into empty targets only; it does not merge or roll back a live database in place.
- Restoring PostgreSQL into a different major version than 16 is untested.
- The restored stack creates its own login accounts (`app-init`, from its env file) only for usernames that do not
  exist yet; restored accounts keep their original password hashes (see the lesson in §6).
