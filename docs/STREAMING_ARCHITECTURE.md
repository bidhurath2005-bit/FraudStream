# FraudStream — Streaming architecture

Scope: the distributed profile's event path, Kafka → Spark Structured Streaming → Kafka → persistence consumer →
PostgreSQL. Code: `streaming/kafka/client.py`, `streaming/spark/`, `backend/app/streaming/`,
`infrastructure/kafka/create_topics.sh`, `docker-compose.yml`. All figures are measured on one development machine
(see §9).

## 1. Kafka

Apache Kafka 3.9.1 in **KRaft** mode, a **single broker** (node 1 is both broker and controller), with the log on the
named volume `kafka-data`.

| Setting | Value | Why |
|---|---|---|
| Listeners | `INTERNAL://kafka:29092` (containers), `EXTERNAL://127.0.0.1:9092` (host tools and tests) | advertising `127.0.0.1` instead of `localhost` avoids a ~2 s IPv6 stall per connection on Windows |
| Replication factor / min ISR | 1 / 1 | single broker |
| `log.flush.interval.messages` | 1 (fsync every message) | durability on one broker; costs throughput (see §9) |
| `auto.create.topics.enable` | false | topics come only from `kafka-init` |
| Heap / container memory | 512 MiB / 1 GiB | |

### Topics

Created by `kafka-init` (idempotent). The definitions mirror `TOPIC_SPECS` in `streaming/kafka/client.py`. An optional
`FRAUDSTREAM_TOPIC_PREFIX` lets integration tests use isolated topics.

| Topic | Partitions | Retention | Producer → consumer | Content |
|---|---|---|---|---|
| `transactions.raw` | 6 | 7 d | API (Simulation Lab, ingest), benchmarks → Spark | raw events, key = `customer_id` |
| `transactions.validated` | 6 | 3 d | Spark `validated-dedup` → (monitoring) | schema-valid, watermark-deduplicated events |
| `transactions.normal` | 6 | 3 d | Spark `score` → persistence consumer | scored LOW events |
| `transactions.suspicious` | 6 | 7 d | Spark `score` → persistence consumer | scored MEDIUM/HIGH events |
| `fraud.alerts` | 3 | 30 d | persistence consumer → (notification) | alert-created notifications, emitted after the DB commit |
| `fraud.dlq` | 1 | 30 d | Spark, persistence consumer | dead letters with `dlq_reason`, `dlq_stage`, `source_topic` headers |
| `features.windows` | 3 | 1 d | Spark window queries → (monitoring) | per-customer windowed aggregates |

Keying by `customer_id` keeps each customer's events ordered within one partition.

### Producer configuration

`producer_config()` in `streaming/kafka/client.py`:

```text
acks=all  enable.idempotence=true  linger.ms=5  compression.type=lz4
message.timeout.ms=30000  retries=10  retry.backoff.ms=200
```

Producer retries cannot create broker-side duplicates within a producer session. A record that is not acknowledged
within 30 s is reported to the caller as failed. `POST /transactions/ingest` waits for the acknowledgement before
returning 202 and returns 503 if Kafka is unavailable.

### Consumer configuration (persistence consumer, fallback processor)

```text
enable.auto.commit=false  enable.auto.offset.store=false  auto.offset.reset=earliest
isolation.level=read_committed  partition.assignment.strategy=cooperative-sticky
session.timeout.ms=30000  max.poll.interval.ms=300000
```

Groups: `fraudstream-persistence` (persistence consumer) and `fraudstream-processor` (fallback processor only). The
fallback group is shown as inactive while Spark runs, and its lag is not counted as pipeline lag.

## 2. Spark Structured Streaming

| Item | Value |
|---|---|
| Version | PySpark **4.0.5**, `spark-sql-kafka-0-10_2.13:4.0.5` (jars pre-fetched at image build), OpenJDK 17, Python 3.12 |
| Deployment | **local mode**: `SPARK_MASTER=local[8]` (8 threads in **one JVM**) inside **one container**. There is no Spark cluster, standalone master, YARN or Kubernetes. |
| Resources | container limit 3 GiB, driver memory 1 GiB |
| Trigger | `processingTime = 2 seconds` (`SPARK_TRIGGER_INTERVAL`) |
| Source | `startingOffsets=earliest` (applies only to a **new** checkpoint), `failOnDataLoss=true`, `maxOffsetsPerTrigger=5000` |
| Shuffle partitions | 2 (= state-store partitions; fixed by the checkpoint) |
| State store | RocksDB (`RocksDBStateStoreProvider`) |
| Watermark | `10 minutes` (`SPARK_WATERMARK_DELAY`) |
| Spark UI | `127.0.0.1:4040`, kill actions disabled (`spark.ui.killEnabled=false`) |
| Mounts | `backend/`, `ml/`, `streaming/`, `simulator/`, the health-check script — all **read-only**. Not `.env`. |
| Python workers | custom daemon `streaming/spark/daemon.py` pre-imports the ML stack and loads the models before forking (removed 4–6 s warm-up batches) |

### Queries

`streaming/spark/fraud_stream_job.py` starts six queries, each with its own checkpoint under
`${SPARK_CHECKPOINT_DIR:-/checkpoints/v2}`:

| Query | Path | Output |
|---|---|---|
| **`score`** | raw → Python validation with the **shared Pydantic schema** → `groupBy(customer_id).applyInPandasWithState` → per-customer state (behaviour history + seen event ids) → features → ML inference → risk engine | `transactions.normal` / `transactions.suspicious`; invalid → `fraud.dlq` (stage `spark-validation`); events > 10 min behind the customer's latest event → `fraud.dlq` (`LATE_EVENT_BEYOND_TOLERANCE`, stage `spark-scoring`) |
| `validated-dedup` | raw → JVM-native validation (mirrors the Pydantic schema, including rejection of unknown fields) → `withWatermark(event_time)` → `dropDuplicatesWithinWatermark(event_id)` | `transactions.validated` |
| `window-tumbling_1m` | JVM validation → watermark → 1-minute tumbling window per customer | `features.windows` |
| `window-tumbling_15m` | 15-minute tumbling window (spend totals) | `features.windows` |
| `window-sliding_5m` | 5-minute window, 1-minute slide (velocity) | `features.windows` |
| `window-sliding_1h` | 1-hour window, 15-minute slide (behaviour) | `features.windows` |

Window queries use `update` output mode. Rows later than the watermark are dropped by Spark and counted
(`rows_dropped_by_watermark` in the progress snapshot). Parity between the JVM validator and the Pydantic schema is
asserted by the Spark integration tests.

The `score` query emits records in exactly the same format as the Phase 8 Python processor
(`processor=spark-structured-streaming`), so the persistence consumer does not know which processor produced them.

### Checkpoints

- Stored in the named volume `spark-checkpoints` (`/checkpoints/v2`): source offsets (`offsets/`), commit log
  (`commits/`) and the RocksDB state store.
- Source offsets live **in the checkpoint**, not in a Kafka consumer group.
- The shuffle-partition count is fixed by a query's checkpoint. Changing it requires a new `SPARK_CHECKPOINT_DIR`.
  Old directories (e.g. `/checkpoints/v1`) are kept and never deleted automatically.
- A **new** checkpoint with `SPARK_STARTING_OFFSETS=latest` skips everything produced before its first start.
  The default `earliest` replays the retained topic.
- The Streaming Infrastructure page and `/infrastructure/status` show, per query, the last committed batch, its age,
  the planned batch and the number of uncommitted batches (`streaming/spark/checkpoint.py`, a read-only listing).

### Progress and health

Every 5 s the job writes a snapshot to Redis `fraudstream:spark:progress` and to `/tmp/spark-progress.json`. The
snapshot contains the batch id, input and processed rows/s, duration breakdown, state rows, rows dropped by the
watermark, source end offsets, exceptions and checkpoint info. On shutdown it writes a `terminated` snapshot. The
container is healthy only while the file is < 45 s old **and** every query is active without an exception. The file
is deleted at start-up so a restarted container is not reported healthy too early.

Graceful shutdown: SIGTERM sets a flag. The job finishes the current batch, stops its queries and exits 0
(`stop_grace_period: 60s`).

## 3. Persistence consumer

`backend/app/streaming/persistence_consumer.py`, run as `python -m app.streaming.persistence_consumer`.

1. `consume(num_messages=200, timeout=0.2)` from `transactions.normal` and `transactions.suspicious`.
2. Decode and validate each record (scored payload, decision keys, event schema). A record that can never succeed
   (empty, malformed JSON, schema-invalid) is a **poison message**: it is produced to `fraud.dlq` with headers, and the
   offset is committed only after the DLQ record is acknowledged.
3. `persist_scored_event` writes the customer and merchant (insert-if-absent), transaction, prediction, and — for MEDIUM/HIGH —
   the alert with an `ALERT_CREATED` audit row, in **one database transaction**.
4. After the commit: `fraud.alerts` notification for new alerts, flush (must be acknowledged), then a **synchronous
   offset commit**, then publish the live message to Redis `fraudstream:live`.
5. Transient database errors (connection loss, PostgreSQL down) are retried with capped exponential backoff (max
   10 s). The offset is **not** advanced and nothing is skipped. The heartbeat file keeps being written, so the
   container stays healthy while it waits instead of entering a crash loop.

Counters (`consumed`, `created`, `duplicates`, `alerts`, `dead_lettered`, `db_retries`, `commits`) are published with
the Redis heartbeat and shown on the Streaming Infrastructure page.

## 4. Deduplication

| Layer | Mechanism | Scope |
|---|---|---|
| Producer | idempotent producer | retries within one producer session |
| Spark `score` | seen event ids in per-customer RocksDB state (bounded at 1,000 ids per customer) | duplicates that reach the scoring query |
| Spark `validated-dedup` | `dropDuplicatesWithinWatermark(event_id)` | within the 10-minute watermark |
| Persistence | event-id check + `UNIQUE(event_id)`, `UNIQUE(predictions.transaction_id)`, `UNIQUE(alerts.transaction_id)` | **all** re-deliveries, including Spark replays after a crash |

## 5. Dead-letter handling

| Stage | Reasons | Destination |
|---|---|---|
| `api-ingest` | schema validation failure on `POST /transactions/ingest` | 422 to the caller + `dead_letters` table |
| `spark-validation` | malformed or schema-invalid raw event | `fraud.dlq` |
| `spark-scoring` | `LATE_EVENT_BEYOND_TOLERANCE` | `fraud.dlq` |
| `persistence` | `EMPTY_PAYLOAD`, `MALFORMED_JSON`, `SCORED_SCHEMA: …`, event-schema errors | `fraud.dlq` |

`fraud.dlq` records keep the original key and value, with headers `dlq_reason`, `dlq_stage`, `source_topic` and
`dlq_at`. There is no automatic re-processing; dead letters are for inspection. The Streaming Infrastructure page
shows the retained DLQ count, database dead letters and recent reasons.

## 6. Delivery semantics

At-least-once from Spark to Kafka, plus idempotent persistence, gives **effectively-once rows in PostgreSQL**. It is
**not** end-to-end exactly-once: Kafka transactions are not used, and a Spark replay can put a scored record on the
topic twice (the second copy is counted as a duplicate by the consumer). Every Phase 10 and 12 crash, outage and
benchmark run ended with 0 lost and 0 duplicate rows.

## 7. Recovery behaviour

| Failure | Behaviour (verified) |
|---|---|
| Kafka down | API ingestion and simulation → 503 naming Kafka (no local fallback). Spark and consumer reconnect after the restart. Run 2 (20.5 s outage at 25 ev/s): 1,750 / 1,750 acknowledged and persisted exactly once, lag 0 after 49.2 s. Run 1: 194 sends timed out at the producer and were **not** persisted; every acknowledged event was. |
| Spark crash (SIGKILL) | `restart: unless-stopped` → resumes from the checkpoint; 200 events produced during the outage persisted exactly once; status shows the new application id |
| Spark stopped | dashboard shows NOT RUNNING within seconds; nothing else processes the events; a backlog of 3,000 drained exactly once in 53–78 s after start |
| Consumer crash | restarted by policy; 300 events around the crash persisted exactly once (offsets were never committed ahead of the DB) |
| PostgreSQL down | consumer blocks and retries, stays healthy; Spark keeps scoring into Kafka; all events persisted once after the restart |
| Redis down | circuit breaker: requests at normal speed after one tripping call; persistence continues; processor status `UNKNOWN`; Spark keeps the last Redis risk configuration; feed recovers 16–21 s after Redis returns |

Procedures: [OPERATIONS.md §5](OPERATIONS.md#5-failure-recovery). Diagram: [DIAGRAMS.md §D](DIAGRAMS.md#d-failure-recovery).

## 8. The fallback processor

`backend/app/streaming/processor.py` (profile `python-processor`) consumes `transactions.raw` with the
`fraudstream-processor` group and runs the same shared detection code. It exists for development and comparison
only. It refuses to start while Spark reports progress, Spark refuses while it runs, and the dashboard labels it
"python-kafka processor (non-Spark)".

## 9. Throughput measurements

Phase 12, isolated stack, one machine (16 vCPU / 7.6 GiB Docker VM). Latencies are stage-separated on the Docker VM
clock. Source: [phase12/RELIABILITY_REPORT.md §2](phase12/RELIABILITY_REPORT.md).

| Offered (ev/s, 60 s) | Persisted | Spark stage p50 / p95 | Persistence p50 / p95 | **End-to-end p50 / p95** |
|---|---|---|---|---|
| 10.5 | 10.5 | 2.36 / 3.24 s | 0.21 / 0.33 s | **2.53 / 3.42 s** |
| 26.3 | 27.0 | 2.24 / 3.17 s | 0.33 / 0.57 s | **2.58 / 3.53 s** |
| 50.8 | 50.1 | 2.18 / 3.13 s | 0.47 / 0.82 s | **2.65 / 3.74 s** |
| 104.5 | 104.0 | 2.74 / 3.97 s | 1.07 / 2.03 s | **3.79 / 5.57 s** |
| 153.2 | 115.1 (falls behind) | 5.34 / 13.5 s | 4.73 / 13.2 s | 10.4 / 24.0 s |

- Sustained 600 s at 112.6 ev/s → 105.5 ev/s persisted, backlog grows (p95 57 s). **Sustained capacity ≈ 100–105
  ev/s on this machine.**
- Burst of 5,000: Spark processed it in one 14.7 s micro-batch (≈ 340 ev/s, bounded by `maxOffsetsPerTrigger=5000`);
  the consumer persisted 217 ev/s.
- HTTP ingestion through one API process: ≈ 65 accepted ev/s.

## 10. Backpressure and limitations

- **No end-to-end backpressure.** Kafka absorbs bursts. Spark reads at most 5,000 offsets per trigger. Beyond
  ~100 ev/s sustained, the consumer lag and then the Spark lag grow until the load stops. There is no producer
  throttling and no autoscaling.
- **Bottleneck order** under sustained load: (1) the persistence consumer, where database work alone does 259 ev/s
  per-event and 353 ev/s batched in a microbenchmark, plus per-batch Kafka waits; (2) Kafka fsync-per-message (ack p50
  19 → 147 ms); (3) Spark memory (2.7–3.05 GiB of 3 GiB, no OOM).
- Single broker with RF=1: broker restarts are survived, disk loss is not.
- A Spark crash shorter than the 30 s heartbeat freshness window can be invisible on the dashboard.
- Kafka "msgs/s" per topic is the difference between the API's two most recent status checks (n/a until a second
  check). The Spark rows/s chart is sampled by the browser while the page is open and is not stored.
