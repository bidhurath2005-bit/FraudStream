# FraudStream — Operations

Runbook for the containerised stack on one Docker host. Commands are **Windows PowerShell** from the repository root.
Every procedure here was exercised in Phases 10–12. The backup and restore steps are the ones verified in
[phase12/BACKUP_AND_RESTORE.md](phase12/BACKUP_AND_RESTORE.md).

Shorthand used below (define it in your session):

```powershell
function dc { docker compose --env-file .env --profile distributed @args }
```

## 1. Startup

```powershell
dc up -d --build          # first start or after code changes (builds the app, frontend and Spark images)
dc up -d                  # later starts
dc ps                     # wait until every long-running service is (healthy)
```

Start order is enforced by `depends_on` health conditions:
kafka, postgres, redis → kafka-init (topics) and app-init (migrations + accounts) → spark, api, persistence-consumer →
frontend.

| Service | Becomes healthy when | Typical time |
|---|---|---|
| postgres / redis | `pg_isready` / `PING` | seconds |
| kafka | broker API answers | 10–20 s |
| api | `GET /api/v1/health` = 200 (database reachable) | 40 s start period |
| persistence-consumer | heartbeat file < 30 s old | 30 s start period |
| spark | progress file < 45 s old **and** all 6 queries active | up to 120 s start period |
| frontend | `GET /healthz` | after api |

Measured full-stack start after a stop: **all healthy in 22–45 s**; first new event persisted and visible on the live
feed after 41–77 s.

Dashboard: http://127.0.0.1:8080. Local profile:
`docker compose --env-file .env --profile local up -d api-local frontend-local` → http://127.0.0.1:8081.

## 2. Shutdown

```powershell
dc stop                   # graceful: keeps containers and ALL volumes
dc down                   # removes containers and the network; volumes are KEPT
```

Grace periods: API 20 s (uvicorn graceful timeout 10 s), consumer 30 s (commits only after the database commit),
Spark 60 s (finishes the current batch, writes a `terminated` snapshot, exits 0).

> **Never run `docker compose down -v`** unless you intend to delete the database, the Kafka log, the Spark
> checkpoints and the uploads. Take a backup first (§6).

Stop a single service: `dc stop spark` (the dashboard shows the processor as NOT RUNNING within seconds; events wait
in Kafka and are processed exactly once after `dc start spark`).

## 3. Health verification

```powershell
dc ps                                                              # health column of every service
Invoke-RestMethod http://127.0.0.1:8080/api/v1/health              # public: status, database, model_loaded, profile, stream_processor
```

Expected `/health` in the distributed profile: `status=ok`, `database=up`, `model_loaded=true`,
`profile=distributed`, `stream_processor="Spark Structured Streaming"`.

Authenticated detail (as any user): the **Streaming Infrastructure** page, or `GET /api/v1/infrastructure/status` and
`GET /api/v1/system/status`. These show PostgreSQL, Redis, Kafka (topics, offsets, consumer-group lag), every Spark
query with its last committed checkpoint batch, consumer counters and dead letters.

End-to-end reconciliation, which sends events and checks that each one is persisted and delivered exactly once:

```powershell
$env:FS_PW = "<analyst password from .env>"
.\.venv\Scripts\python scripts\verify_kafka_pipeline.py --api http://127.0.0.1:8080/api/v1 --username analyst --password-env FS_PW
```

It writes synthetic transactions into the database.

## 4. Logs

```powershell
dc logs -f api                       # follow one service
dc logs --since 10m spark persistence-consumer
dc logs --tail 200                   # everything
```

The API logs structured JSON lines (request id, path, status, latency for errors). Every response carries an
`X-Request-ID` header to correlate with the logs. Log rotation is set in `docker-compose.yml` (`json-file`,
10 MB × 3 per container). The Spark UI is at http://127.0.0.1:4040 (localhost only; kill actions disabled).

## 5. Failure recovery

All automatic behaviours below are covered by `infrastructure/tests/test_container_stack.py` (disruptive tests) or
the Phase 12 failure tests. Diagram: [DIAGRAMS.md §D](DIAGRAMS.md#d-failure-recovery).

| Failure | What you see | What happens / what to do |
|---|---|---|
| **Kafka down** | Kafka DOWN on Streaming Infrastructure; Simulation Lab and `/transactions/ingest` → **503** naming Kafka | No fallback, by design. `dc start kafka`. Spark and the consumer resume; every acknowledged event is persisted exactly once (lag 0 after ~50 s). Events whose send timed out (> 30 s) were reported failed to the producer and must be re-sent. |
| **Spark crashed** | possibly nothing: a crash shorter than ~30 s may never show as NOT RUNNING | `restart: unless-stopped` restarts it from the checkpoint. Check `dc logs spark` and the restart count in `docker inspect`. |
| **Spark stopped / unhealthy** | processor NOT RUNNING; Spark lag grows | `dc start spark`. The backlog is drained from the checkpoint (3,000 events in 53–78 s measured). |
| **Persistence consumer crashed** | consumer DOWN, persistence lag grows | restarted by policy; offsets were never committed ahead of the database, so nothing is lost or duplicated. |
| **PostgreSQL down** | `/health` → 503 `database=down`; dashboard API errors | Spark keeps scoring into Kafka. The consumer blocks and retries without committing offsets and stays healthy. `dc start postgres`, and the backlog is persisted exactly once. |
| **Redis down** | Redis DOWN; processor **UNKNOWN (Redis unavailable)**; live feed stalls | Circuit breaker keeps the API fast; persistence continues; login uses the in-process limiter; Spark keeps its last Redis risk config. `dc start redis`: the feed recovers in ~16–21 s. |
| **API crash / recreate** | 502 from nginx briefly | restarted by policy. nginx re-resolves the new container address without a restart (tested with a forced IP change). Access tokens stay valid (same `JWT_SECRET`). |
| **Full stack restart** (host reboot, Docker restart) | — | `dc up -d`. Named volumes keep everything; Spark resumes at the same commit. |

## 6. Backup and restore

Scripts: `scripts/backup.py`, `scripts/restore.py`, `scripts/verify_restore.py` (Python stdlib + psycopg + Docker
CLI). Details, guarantees and evidence: [phase12/BACKUP_AND_RESTORE.md](phase12/BACKUP_AND_RESTORE.md).

> A backup contains password and refresh-token hashes and the full audit trail. **Treat it as sensitive.** The default
> destination `..\FraudStream-backups` is outside the repository. `.env` is never included; keep your secrets in your
> own secret store.

### Back up

```powershell
# online, while the stack runs: PostgreSQL (snapshot-consistent), local SQLite, artifacts, datasets, uploads
.\.venv\Scripts\python scripts\backup.py --label nightly

# consistent streaming state as well: stop Kafka and Spark first
dc stop spark kafka
.\.venv\Scripts\python scripts\backup.py --components kafka,spark,redis,state --label streaming
dc start kafka spark

# verify an existing backup (checksums, manifest)
.\.venv\Scripts\python scripts\backup.py --verify ..\FraudStream-backups\<backup-id>
```

A failed run ends as `<id>.FAILED\` with exit code 1 and can never be restored. The script never overwrites an
existing backup.

### Restore (into a separate, empty compose project — the tested path)

```powershell
.\.venv\Scripts\python scripts\isolated_env.py --out .env.restore       # fresh secrets + host ports 18080/19092/15433/...
# then edit .env.restore by hand (as done for the Phase 12 restore project):
#   FRAUDSTREAM_NETWORK=fraudstream-restore        (the generator always writes fraudstream-p12)
#   FRAUDSTREAM_ADMIN_USERNAME / FRAUDSTREAM_ANALYST_USERNAME = names that do NOT exist in the backup (see note below)
$I = "-p fraudstream-restore --env-file .env.restore -f docker-compose.yml -f infrastructure/isolated/docker-compose.isolated.yml"

# 1. database, local DB and files (starts only postgres; files go to an EMPTY directory)
.\.venv\Scripts\python scripts\restore.py --backup ..\FraudStream-backups\<id> $I.Split() `
    --components postgres,sqlite,artifacts,datasets,uploads --files-to D:\restore\files --report restore.json
# 2. optional: streaming state from a backup taken with Kafka/Spark stopped
.\.venv\Scripts\python scripts\restore.py --backup ..\FraudStream-backups\<streaming-id> $I.Split() --components kafka,spark
# 3. in .env.restore point the read-only mounts at the restored files:
#      ARTIFACTS_HOST_DIR=D:/restore/files/artifacts
#      DATASETS_HOST_DIR=D:/restore/files/datasets
# 4. start the application on the restored data
docker compose $I.Split() --profile distributed up -d
# 5. verify the RUNNING application against the backup (10 checks)
.\.venv\Scripts\python scripts\verify_restore.py --backup ..\FraudStream-backups\<id> $I.Split() --report verify.json
```

- `restore.py` refuses the live project `fraudstream` (unless `--allow-live-project`, intended only for disaster
  recovery onto **fresh, empty** volumes). It also refuses any non-empty target, verifies the backup first, and
  restores PostgreSQL in a single transaction.
- The isolated override renames the Docker network. Without it, a second project would join the live `fraudstream`
  network.
- **Accounts after a restore:** the restored database already contains the source's accounts with their original
  password hashes, and `app-init` never changes an existing account. Reusing a username with a newly generated
  password therefore gives 401. Use new account names in the restore env, or the original passwords.
- Kafka: `KAFKA_CLUSTER_ID` must equal the one the restored `kafka-data` was created with (the default is the same in
  both env files).
- The Docker VM on the development machine (7.6 GiB) cannot run two full stacks at once. Stop the live stack (`dc
  stop`) before starting a restored copy.

Measured: online backup 9.6 s; restore healthy 43–48 s after `up -d`; `verify_restore.py` 10/10.

## 7. Database migrations

- Alembic, in `backend/migrations/versions/` (`0001_initial_schema`, `0002_generated_reports`).
- **Distributed:** the one-shot `app-init` container runs `python -m app.cli migrate` on every `up`. To re-run it after
  pulling a new migration: `dc up -d --build app-init api persistence-consumer`, or
  `dc run --rm app-init`.
- **Local containers:** `api-local` migrates its SQLite database at start-up.
- **Host processes:** `.\.venv\Scripts\python -m app.cli migrate` (with `PYTHONPATH` set as in the README).
- **Before applying a migration to a database you care about, take a backup (§6).** Migrations must be additive or
  carry a tested downgrade. `0002` was tested on a populated `0001` database, upgrade and downgrade.

## 8. Routine maintenance

| Task | Command | Notes |
|---|---|---|
| Report retention | `dc exec api python -m app.cli prune-reports --older-than-days 90` | **dry run** by default; add `--apply` to delete. The newest 20 per type are always kept (`--keep-latest`); writes one `REPORTS_PURGED` audit row. Not scheduled. |
| Rotate application logs | automatic (10 MB × 3 per container) | |
| Disk growth | `docker system df -v` | estimated ≈ 6.7 MB (Kafka) + 5.7 MB (PostgreSQL) + 3.0 MB (checkpoints) per 1,000 events. Kafka retention bounds its share; PostgreSQL has **no** retention policy. |
| Add an account | `dc exec api python -m app.cli create-user --username <name> --role ANALYST --password-env <VAR>` (VAR set in the container env) or `POST /api/v1/users` as admin | existing accounts are never modified by `app-init` |
| Enable model activation/training | set `ARTIFACTS_MOUNT_MODE=rw` in `.env`, `dc up -d api` | deliberate. It allows the API to write to `ml/artifacts`. Back the artifacts up first. |

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `api` exits with `distributed profile requires [...]` | missing `KAFKA_BOOTSTRAP_SERVERS` / `REDIS_URL` / PostgreSQL URL | use the compose file unchanged; check `.env` has `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `JWT_SECRET` |
| `set POSTGRES_PASSWORD in .env` on `up` | `.env` missing or incomplete | `python scripts\new_env.py` (new) or `--add-missing` (existing) |
| Spark `starting` for up to 2 minutes | JVM start, Kafka connector, model loading | normal within the 120 s start period |
| Spark restarts repeatedly / exit 137 | Docker VM memory too small | give Docker ≥ 8 GB; Spark alone has a 3 GiB limit |
| Processor `NOT RUNNING` | Spark stopped or unhealthy | `dc ps`, `dc logs spark`, `dc start spark` |
| Processor `UNKNOWN (Redis unavailable)` | Redis down | `dc start redis` |
| Simulation Lab → 503 "Kafka unavailable" | Kafka down | `dc start kafka` (no fallback by design) |
| Login → 429 "too many login attempts" | 10/min login limit | wait 60 s. Automated E2E runs can hit this; wait ≥ 60 s between two runs. |
| Login → 401 after a restore | reused username with a new password | see §6 "Accounts after a restore" |
| Model activate/train → 409 | `ml/artifacts` is read-only (default) | intended; see §8 |
| Settings save → 409 | someone changed the item since you loaded it | reload and review. 428 means the client sent no `expected_updated_at`. |
| Host tools stall ~2 s per connection | `localhost` resolves to `::1` first on Windows | use `127.0.0.1` |
| Port 8080 / 9092 / 5433 / 6380 / 4040 already in use | another process or stack | change `FRONTEND_HOST_PORT`, `KAFKA_HOST_PORT`, … in `.env` |
| Need different Spark shuffle partitions | partition count is fixed by the checkpoint | set a new `SPARK_CHECKPOINT_DIR` (e.g. `/checkpoints/v3`); old checkpoints are kept. A new checkpoint replays the retained topic unless `SPARK_STARTING_OFFSETS=latest`. |
| Latency tiles show tens of seconds | window includes events delayed by an outage or benchmark | the tiles cover the last 15 minutes; values return to ~3 s at normal load |
