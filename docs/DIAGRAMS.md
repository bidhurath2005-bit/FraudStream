# FraudStream — Architecture diagrams

All diagrams are [Mermaid](https://mermaid.js.org/) and render on GitHub. Each one shows only components and
connections that exist in the code and in `docker-compose.yml`. Where a diagram simplifies something, the text
below it says what was left out.

- [A. System architecture (distributed profile)](#a-system-architecture-distributed-profile)
- [B. Transaction lifecycle](#b-transaction-lifecycle)
- [C. ML lifecycle](#c-ml-lifecycle)
- [D. Failure recovery](#d-failure-recovery)
- [E. Backup and restore](#e-backup-and-restore)

---

## A. System architecture (distributed profile)

```mermaid
flowchart LR
    user(["Analyst / admin<br/>browser"])

    subgraph host["Single Docker host · private bridge network 'fraudstream' · host ports bound to 127.0.0.1"]
        direction LR
        nginx["frontend (nginx)<br/>React SPA + reverse proxy<br/>127.0.0.1:8080"]

        subgraph apibox["api container (FastAPI, 1 uvicorn worker)"]
            api["REST API /api/v1<br/>JWT auth · RBAC · audit"]
            sse["SSE live feed<br/>/api/v1/stream/transactions"]
        end

        kafka[("Kafka 3.9.1<br/>single broker, KRaft<br/>7 topics")]

        subgraph sparkbox["spark container — ONE JVM, local[8] (not a cluster)"]
            spark["Spark Structured Streaming 4.0.5<br/>6 queries"]
            infer["Feature engineering +<br/>ML inference (XGBoost, Isolation Forest)"]
            risk["Risk engine<br/>(rules + noisy-OR)"]
            spark --> infer --> risk
        end

        consumer["persistence-consumer<br/>(idempotent writer)"]
        pg[("PostgreSQL 16<br/>system of record")]
        redis[("Redis 7<br/>pub/sub · heartbeats ·<br/>rate limits · runtime config")]
        ckpt[("volume spark-checkpoints<br/>offsets · commits · RocksDB state")]
        artifacts[["ml/artifacts<br/>(bind mount, read-only)"]]
        init["app-init (one-shot)<br/>Alembic migrations + accounts"]
    end

    user -- "HTTP :8080" --> nginx
    nginx -- "proxy /api/*" --> api
    nginx -- "proxy /api/v1/stream/* (unbuffered)" --> sse
    api -- "produce transactions.raw<br/>(Simulation Lab, /transactions/ingest),<br/>read topic offsets for status" --> kafka
    api -- "SQL (SQLAlchemy / psycopg)" --> pg
    api -- "publish runtime config,<br/>read heartbeats, rate limits" --> redis
    redis -- "pub/sub fraudstream:live" --> sse
    kafka -- "transactions.raw" --> spark
    risk -- "transactions.normal / .suspicious /<br/>.validated · features.windows · fraud.dlq" --> kafka
    spark -- "progress snapshot every 5 s" --> redis
    redis -- "runtime risk config" --> spark
    spark <--> ckpt
    artifacts -. "loaded at start" .-> infer
    artifacts -. "loaded at start" .-> api
    kafka -- "transactions.normal / .suspicious" --> consumer
    consumer -- "transaction + prediction + alert<br/>(one DB transaction)" --> pg
    consumer -- "fraud.alerts · fraud.dlq (poison only)" --> kafka
    consumer -- "publish live message + heartbeat" --> redis
    init -- "migrate" --> pg
```

Notes
- The API container is **not published** on the host; nginx is the only HTTP entry point.
- The **local profile** (`--profile local`) replaces everything right of nginx with one `api-local` container that
  runs the same feature/ML/risk code **in-process** on SQLite (no Kafka, Spark, Redis or PostgreSQL).
- Not drawn: the optional `python-processor` fallback (profile `python-processor`, never started by `distributed`,
  refuses to run while Spark reports), the one-shot `kafka-init` topic creator, and the Spark UI (127.0.0.1:4040).

---

## B. Transaction lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Gen as Simulator<br/>(Simulation Lab in the API)
    participant K as Kafka
    participant S as Spark "score" query
    participant C as Persistence consumer
    participant DB as PostgreSQL
    participant R as Redis
    participant UI as Dashboard (SSE)
    actor A as Analyst

    Gen->>K: produce event → transactions.raw (key = customer_id, acks=all, idempotent)
    K->>S: micro-batch every 2 s (offsets tracked in the checkpoint)
    S->>S: validate with the shared Pydantic schema
    alt invalid or > 10 min behind the customer's latest event
        S->>K: fraud.dlq (reason header)
    else valid
        S->>S: drop duplicate event_id (per-customer state in RocksDB)
        S->>S: behavioural features from per-customer state
        S->>S: ML inference (XGBoost + Isolation Forest)
        S->>S: risk engine → score 0–100, LOW / MEDIUM / HIGH
        S->>K: transactions.normal (LOW) or transactions.suspicious (MEDIUM/HIGH)
    end
    K->>C: consume (manual offset commit)
    C->>DB: one transaction: transaction + prediction (+ alert if MEDIUM/HIGH, + ALERT_CREATED audit row)
    Note over C,DB: UNIQUE(event_id) etc. — a re-delivered record is detected as a duplicate
    C->>K: fraud.alerts (after the DB commit)
    C->>K: commit offsets (only after the DB commit)
    C->>R: publish to fraudstream:live
    R->>UI: SSE event via API + nginx (Live Stream, Overview)
    A->>UI: open alert → set status / add note
    UI->>DB: PATCH /alerts/{alert_uid}/status, POST /investigations …
    DB->>DB: audit_logs row per action (ALERT_STATUS_CHANGED, INVESTIGATION_OPENED, …)
```

Notes
- In parallel, the `validated-dedup` query writes schema-valid, watermark-deduplicated events to
  `transactions.validated`, and four window queries write per-customer aggregates to `features.windows`
  (monitoring outputs; the persistence consumer does not read them).
- `POST /transactions/ingest` takes the same path as the simulator from step 1.

---

## C. ML lifecycle

```mermaid
flowchart TD
    gen["Synthetic dataset<br/>simulator.generator (seed 2026)<br/>139,517 events, 5.06 % labelled suspicious"]
    feat["Feature engineering (ml/features)<br/>25 numeric + 4 categorical, fc-1.0<br/>labels / ids stripped before features"]
    split{"Temporal split by event_time<br/>60 / 20 / 20 %<br/>+ 15 % customers held out"}
    train["Train split<br/>72,034 rows"]
    val["Validation split<br/>24,079 rows"]
    test["Temporal test split<br/>24,155 rows"]
    unseen["Unseen-customer test<br/>3,749 rows"]
    fit["Fit 4 models<br/>logistic regression (baseline), random forest,<br/>XGBoost, Isolation Forest"]
    thr["Threshold per model<br/>max F1 on VALIDATION"]
    select["Recommended model<br/>highest validation PR-AUC"]
    eval["Report-only evaluation<br/>test + unseen-customer splits"]
    art[["Versioned artifacts<br/>ml/artifacts/models/MODEL/VERSION/<br/>model.joblib · metadata.json · reference_profile.json"]]
    reg[["registry.json + experiment JSON<br/>synced into PostgreSQL (model_versions)"]]
    infer["Inference<br/>Spark workers (distributed) / API in-process (local)<br/>shared scorer + risk engine"]
    drift["Drift monitoring<br/>PSI of live features & scores vs reference_profile<br/>(on demand: GET /drift/current)"]

    gen --> feat --> split
    split --> train & val & test & unseen
    train --> fit --> thr
    val --> thr --> select
    test --> eval
    unseen --> eval
    select --> art
    eval --> art
    art --> reg --> infer --> drift
    art -. "reference profile" .-> drift
```

The test splits are never used to choose a model or a threshold. Details and limitations:
[DATA_AND_ML.md](DATA_AND_ML.md).

---

## D. Failure recovery

Behaviour verified by the container test suite (`infrastructure/tests/test_container_stack.py`) and the Phase 12
failure tests on an isolated stack. Times are measured on one development machine.

```mermaid
flowchart TD
    subgraph kafkaF["Kafka broker stopped"]
        k1["Simulation Lab / ingest → HTTP 503 naming Kafka<br/>(no fallback to in-process scoring)"]
        k2["Producers buffer. Records not acknowledged<br/>within 30 s are reported failed, never persisted"]
        k3["Broker restarts → Spark + consumer resume,<br/>every acknowledged event persisted exactly once<br/>(lag 0 after 49–56 s)"]
        k1 --> k2 --> k3
    end
    subgraph sparkF["Spark crash or stop"]
        s1["restart: unless-stopped (crash)<br/>or manual start"]
        s2["Resume from checkpoint<br/>(offsets + RocksDB state)"]
        s3["Backlog of 3,000 drained in 53–78 s,<br/>no duplicates. Graceful stop shows NOT RUNNING"]
        s1 --> s2 --> s3
    end
    subgraph pgF["PostgreSQL stopped"]
        p1["API /health → 503 database=down"]
        p2["Consumer blocks + retries with backoff,<br/>offsets NOT committed, stays healthy"]
        p3["DB back → all events produced<br/>during the outage persisted once"]
        p1 --> p2 --> p3
    end
    subgraph redisF["Redis stopped"]
        r1["Circuit breaker fails fast<br/>(no +4 s per request)"]
        r2["Persistence continues. Live feed and<br/>processor status UNKNOWN, login uses<br/>in-process rate limiter"]
        r3["Spark keeps last Redis risk config.<br/>Redis back → feed recovers in 16–21 s"]
        r1 --> r2 --> r3
    end
    subgraph fullF["Full stack stop + up -d"]
        f1["Named volumes keep DB, Kafka log,<br/>checkpoints. Artifacts are a bind mount"]
        f2["All 7 services healthy in 22–45 s"]
        f3["First new event persisted and on<br/>the live feed after 41–77 s"]
        f1 --> f2 --> f3
    end
```

A Spark crash shorter than the 30 s heartbeat window may never appear as NOT RUNNING on the dashboard because Docker
restarts the job before the status goes stale. Procedures: [OPERATIONS.md](OPERATIONS.md#5-failure-recovery).

---

## E. Backup and restore

```mermaid
flowchart TD
    b0(["scripts/backup.py"]) --> b1["Write to BACKUP_ID.partial/ outside the repo<br/>(default ../FraudStream-backups)"]
    b1 --> b2["pg_dump -Fc inside an exported snapshot<br/>+ row counts and content fingerprints<br/>from the same snapshot"]
    b1 --> b3["SQLite online backup + integrity_check<br/>artifacts + datasets with SHA-256<br/>optional volume archives (Kafka/Spark only when stopped)"]
    b2 & b3 --> b4{"Integrity verification<br/>pg_restore --list · SHA256SUMS · manifest"}
    b4 -- "any failure" --> bf["rename to BACKUP_ID.FAILED/<br/>exit 1, never restorable"]
    b4 -- "ok" --> b5["rename to BACKUP_ID/ · status COMPLETE"]

    b5 --> r0(["scripts/restore.py<br/>(target: a separate compose project)"])
    r0 --> r1{"Refuse if target is the live project<br/>or any target is non-empty"}
    r1 -- "refused" --> rx["stop, nothing changed"]
    r1 -- "ok" --> r2["Verify backup checksums first"]
    r2 --> r3["pg_restore --single-transaction --exit-on-error<br/>→ compare counts + fingerprints with manifest"]
    r2 --> r4["Restore files to an EMPTY directory<br/>→ compare SHA-256 (artifact verification)"]
    r3 & r4 --> r5["docker compose -p PROJECT --profile distributed up -d<br/>(application startup)"]
    r5 --> r6(["scripts/verify_restore.py<br/>10 checks against the RUNNING stack:<br/>fingerprints, Alembic, /health, login,<br/>served artifact SHA-256, routes, processor"])
```

Measured: live → isolated restore healthy in 48 s, final cycle healthy in 43 s, `verify_restore.py` 10/10 both times.
Full procedure: [OPERATIONS.md](OPERATIONS.md#6-backup-and-restore) and
[phase12/BACKUP_AND_RESTORE.md](phase12/BACKUP_AND_RESTORE.md).
