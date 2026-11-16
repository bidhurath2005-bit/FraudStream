# FraudStream — Architecture

This document describes the system **as implemented**. Diagrams are in [DIAGRAMS.md](DIAGRAMS.md). Streaming details
are in [STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md), and the ML and risk-scoring details are in
[DATA_AND_ML.md](DATA_AND_ML.md).

## 1. System context

FraudStream runs on **one Docker host**. All containers share the private bridge network `fraudstream`. Host ports are
bound to `127.0.0.1` only, and the only HTTP entry point is the dashboard's nginx. Two execution profiles share the
same code:

| Profile | Started with | Processing path | Storage |
|---|---|---|---|
| **distributed** | `docker compose --env-file .env --profile distributed up -d` | API → Kafka → Spark → Kafka → persistence consumer → PostgreSQL | PostgreSQL, Redis, Kafka log, Spark checkpoints |
| **local** | `--profile local` (containers) or uvicorn + Vite on the host | API scores each event **in-process** with the same feature, ML and risk code | SQLite |

The profile is fixed by `FRAUDSTREAM_PROFILE`. The settings validator refuses `distributed` without Kafka, Redis and a
PostgreSQL URL. **There is no automatic fallback** between profiles or processors.

The component diagram is in [DIAGRAMS.md §A](DIAGRAMS.md#a-system-architecture-distributed-profile).

## 2. Components and responsibilities

| Component | Code / image | Responsibility |
|---|---|---|
| **frontend** | `frontend/` → `fraudstream-frontend:local` (nginx-unprivileged 1.30.6 on Alpine 3.24, non-root, port 8080) | Serves the React SPA (deep links → `index.html`); reverse-proxies `/api/` to the API on the **same origin** (no CORS, first-party refresh cookie); unbuffered SSE location with a 1 h read timeout; sets `X-Forwarded-For $remote_addr`; resolves the API through Docker DNS per request, so a recreated API container is followed without restarting nginx. |
| **api** | `backend/app` → `fraudstream-app:local` (uvicorn, 1 worker, uid 10001, tini) | REST API under `/api/v1`: authentication, RBAC, alerts and investigations, analytics, reports, settings, audit trail, model registry, drift checks, Simulation Lab. In distributed mode it produces raw events to Kafka and reads Spark/consumer heartbeats from Redis. It also loads the active models (health reporting, drift, and the local pipeline). |
| **persistence-consumer** | `backend/app/streaming/persistence_consumer.py` (same image) | Consumes `transactions.normal` and `transactions.suspicious`; writes transaction + prediction + alert + audit row in **one database transaction**; then emits `fraud.alerts`, commits Kafka offsets and publishes the live-feed message. Poison messages go to `fraud.dlq`. |
| **spark** | `streaming/spark/` → `fraudstream-spark:local` (Python 3.12 + OpenJDK 17 + PySpark 4.0.5) | Spark Structured Streaming in **local mode** (`local[8]`, one JVM, one container — not a cluster). Six queries: validation, dedup, stateful features, ML inference and risk scoring, plus window aggregates. Publishes progress to Redis every 5 s. |
| **kafka** | `apache/kafka:3.9.1`, KRaft, single broker | Durable event log: 7 topics, replication factor 1, fsync per message. |
| **postgres** | `postgres:16-alpine` | System of record for every business entity (§5). `shm_size: 256m`, `enable_parallel_hash=off` (Phase 12 fix). |
| **redis** | `redis:7-alpine`, AOF, password | Live-feed pub/sub, heartbeats, Spark progress, rate-limit counters, published runtime risk configuration. Holds no data that cannot be re-created. |
| **app-init** | one-shot, same image | Alembic migrations + creation of the admin/analyst accounts from `.env` (existing accounts are never modified). |
| **kafka-init** | one-shot, Kafka image | Creates the 7 topics idempotently (`infrastructure/kafka/create_topics.sh`). |
| **python-processor** | opt-in profile `python-processor` | Development fallback processor (non-Spark) using the same shared logic. Never started by `distributed`; it and Spark refuse to run at the same time. |
| **api-local / frontend-local** | profile `local` | API with the in-process pipeline on SQLite in volume `local-data`; dashboard on :8081. |

Shared libraries (imported by API, consumer, Spark workers and tests):

| Package | Purpose |
|---|---|
| `streaming/schemas/events.py` | Canonical transaction event schema (Pydantic, versioned `1.0`), shared by simulator, API, Kafka and Spark |
| `simulator/` | Deterministic transaction generator and the 38-city geography |
| `ml/features/` | Feature definitions (`fc-1.0`) and per-customer stateful feature computation |
| `ml/inference/` | Model loading, scoring and the fast transform path |
| `backend/app/risk_engine/` | Rules, risk settings, noisy-OR combination, `detect()`. Free of web and database imports so Spark can use it. |
| `ml/drift/` | PSI reference profiles and comparison |

## 3. Data flows

### 3.1 Event path (distributed)

1. **Generation.** The Simulation Lab (inside the API process) or `POST /transactions/ingest` produces an event to
   `transactions.raw`, keyed by `customer_id`, with `acks=all` and idempotence. Ingest requests wait for the broker
   acknowledgement before answering 202.
2. **Scoring.** Spark's `score` query reads `transactions.raw` in 2 s micro-batches. It validates each event with
   the shared Pydantic schema (invalid → `fraud.dlq`), groups by customer
   (`applyInPandasWithState`), drops duplicate event ids and late events (> 10 min behind that customer's latest
   event) from per-customer state, computes features, runs both models and the risk engine, and writes the scored
   record to `transactions.normal` (LOW) or `transactions.suspicious` (MEDIUM/HIGH).
3. **Persistence.** The persistence consumer writes the rows in one transaction and only then emits `fraud.alerts`,
   commits offsets and publishes to Redis `fraudstream:live`.
4. **Delivery to the dashboard.** The API subscribes to `fraudstream:live` and streams Server-Sent Events through
   nginx to the Live Stream and Overview pages.
5. **Investigation.** Analysts change alert status, add notes and open investigations through REST calls. Every
   change writes an audit row in the same transaction.

The sequence is drawn in [DIAGRAMS.md §B](DIAGRAMS.md#b-transaction-lifecycle).

### 3.2 Configuration flow

Risk rules and settings live in PostgreSQL (`risk_rules`, `system_settings`). After a successful admin change the API
rebuilds its in-memory engine and **publishes the runtime configuration** to Redis (`fraudstream:runtime_config`) and
to `runtime_config.json` in `RUNTIME_CONFIG_DIR` (the `app-state` volume in containers). Spark and the fallback
processor run a watcher that reloads from Redis. During a Redis outage the watcher keeps the **last configuration it
loaded from Redis**; it does not fall back to a stale file or to defaults.

### 3.3 Status flow

Spark writes a progress snapshot (`fraudstream:spark:progress`), and the consumer and fallback processor write
heartbeats (`fraudstream:consumer:heartbeat`, `fraudstream:processor:heartbeat`). The API derives the active processor
from these: "Spark Structured Streaming" only while a fresh (< 30 s), non-terminated, non-failed Spark snapshot
exists; "NOT RUNNING" otherwise; and `UNKNOWN (Redis unavailable: …)` while Redis is down. Kafka offsets and lag are
read with the Kafka admin client.

## 4. Communication protocols

| From → To | Protocol | Notes |
|---|---|---|
| Browser → nginx | HTTP/1.1, plain (127.0.0.1:8080) | no TLS (see §8) |
| nginx → API | HTTP/1.1 proxy on the private network (`api:8000`) | SSE location unbuffered |
| API → browser (live feed) | Server-Sent Events over the proxied connection | keep-alive comment every 15 s; `connected` sent after the Redis subscription is active |
| API / Spark / consumer → Kafka | Kafka protocol (librdkafka via confluent-kafka; Spark's Kafka connector), PLAINTEXT, `kafka:29092` | host clients use `127.0.0.1:9092` |
| API / consumer → PostgreSQL | PostgreSQL wire protocol via psycopg 3 / SQLAlchemy 2 | |
| All services → Redis | RESP with password | every client is wrapped in a circuit breaker (`app/services/redis_guard.py`) |
| Spark driver → Python workers | PySpark Arrow batches (`applyInPandasWithState`) | custom worker daemon pre-loads the models |

## 5. Storage responsibilities

| Store | Holds | Durability |
|---|---|---|
| **PostgreSQL** (`postgres-data`) | 21 tables: `users`, `roles`, `refresh_tokens` (SHA-256 only), `customers`, `merchants`, `transactions`, `predictions`, `alerts`, `investigations`, `investigation_notes`, `audit_logs`, `risk_rules`, `system_settings`, `datasets`, `model_experiments`, `model_versions`, `drift_reports`, `simulation_configs`, `simulation_runs`, `dead_letters`, `generated_reports` | system of record; back it up |
| **Kafka** (`kafka-data`) | 7 topics with time-based retention (1–30 days) | buffer and replay log; consistent backup only when stopped |
| **Spark checkpoints** (`spark-checkpoints`, `/checkpoints/v2`) | source offsets, commit log, RocksDB state store (per-customer behaviour, seen event ids) | needed for exactly-once recovery of Spark state; pair it with the Kafka volume |
| **Redis** (`redis-data`, AOF) | pub/sub, heartbeats, progress, rate limits, published runtime config | re-creatable: the API re-publishes the config at start-up |
| **`ml/artifacts`** (bind mount, read-only) | model files, metadata, reference profiles, `registry.json`, experiment JSON | **not re-creatable byte-for-byte**; retraining produces different models |
| **`data/synthetic`** (bind mount, read-only) | training dataset (`.jsonl.gz` + meta with SHA-256) | regenerable deterministically, but keep the original |
| `app-data`, `app-state` | dataset uploads; derived `runtime_config.json` | uploads optional; state re-creatable |
| SQLite (`local-data` or `data/fraudstream.db`) | the local profile's database (same schema) | |

Database schema changes go through Alembic (`backend/migrations/versions/0001_initial_schema.py`,
`0002_generated_reports.py`).

## 6. Delivery guarantees

- **Producers:** `acks=all`, `enable.idempotence=true`, `message.timeout.ms=30000`. A record the producer reports as
  acknowledged is on the broker's disk (fsync per message). A record not acknowledged within 30 s is reported to the
  producer as failed and is never processed. `POST /transactions/ingest` waits up to 10 s for the acknowledgement and
  answers 503 if it does not arrive.
- **Spark:** source offsets live in the checkpoint (not a consumer group). Kafka output is **at-least-once**: a batch
  replayed after a crash can be emitted again.
- **Persistence consumer:** manual offset commit, only **after** the database transaction commits. Duplicates are
  rejected by `UNIQUE(event_id)`, `UNIQUE(prediction.transaction_id)` and `UNIQUE(alert.transaction_id)`, and by an
  event-id check before insert. Transient database errors are retried with capped backoff without advancing the
  offset. Only messages that can never succeed are dead-lettered.
- **Result:** effectively-once rows in PostgreSQL (verified: 0 duplicate rows in every crash, outage and benchmark
  run). This is **not end-to-end exactly-once**; Kafka transactions are not used.

## 7. Design decisions

| Decision | Reason |
|---|---|
| One shared feature, ML and risk library for Spark, the local pipeline and the fallback processor | One definition of each feature; integration tests assert identical output |
| Python validation and stateful scoring in **one** Spark query; JVM-native validation in the others | Measured: moving Python validation to a single query and tuning (`local[8]`, 2 shuffle partitions, pre-loading worker daemon) brought Kafka → Spark → Kafka p50 from 6.7 s to 1.8 s at 20 TPS |
| RocksDB state store | Bounded JVM memory. The HDFS-backed store measured slightly faster but keeps all state on the heap. |
| Persistence in a separate consumer instead of inside Spark | Database writes, alert notifications and offset commits stay in one place with explicit ordering; the consumer can block during a database outage without failing Spark |
| PostgreSQL as system of record, Redis only for re-creatable state | Backups need only PostgreSQL (plus artifacts) to recover business data |
| API not published; nginx as the single origin | No CORS; refresh cookie stays first-party; one place to set forwarding headers |
| Model artifacts mounted read-only by default | Prevents accidental retraining or activation in a running stack; model endpoints answer 409 with an explanation (`ARTIFACTS_MOUNT_MODE=rw` re-enables them) |
| Optimistic concurrency (mandatory `expected_updated_at`) on configuration | Two admins cannot silently overwrite each other's risk changes |
| Synchronous report generation (no job queue) | Measured ≤ 2.3 s (+ ≤ 2.5 s PDF) on ~190k transactions; other requests slowed but were never blocked |
| Kafka `log.flush.interval.messages=1` | Durability on a single broker. It is the second bottleneck under sustained load; changing it is left to the owner. |
| Explicit profiles, no automatic fallback | The dashboard must never claim "Spark" while something else is processing |

## 8. Architectural limitations

- **Single host, no high availability.** One Kafka broker (RF=1), one PostgreSQL, one Redis, Spark in local mode in a
  single container. Docker restart policies give process-level recovery only, with no tolerance to disk loss.
- **Spark is not distributed.** `local[8]` threads in one JVM with a 3 GiB limit (2.6–3.0 GiB used under load).
  Moving to a cluster would need a cluster manager, shared checkpoint storage and a matching Python environment on
  each executor.
- **Throughput.** ≈ 100–105 events/s sustained on the development machine; one API process accepts ≈ 65 ingest
  requests/s. Beyond that the backlog grows. There is no autoscaling and no backpressure to producers beyond Kafka
  buffering.
- **No TLS, no Kafka authentication.** Acceptable only because every port is on 127.0.0.1 and a private network.
- **Changing Spark shuffle partitions** requires a new checkpoint directory (`SPARK_CHECKPOINT_DIR`). Old checkpoints
  are kept, never deleted automatically.
- **Status detection lag.** A Spark crash shorter than the 30 s heartbeat window may never show as NOT RUNNING.
- **No retention policy** for transactions, predictions or audit rows. PostgreSQL grows by ≈ 5.7 MB per 1,000 events
  (estimated from the sustained benchmark).
- **Frontend** ships as one 854 kB JavaScript chunk (241 kB gzip), not code-split.
