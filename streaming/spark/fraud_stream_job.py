"""FraudStream Spark Structured Streaming job.

    transactions.raw -+- score            : Python validation with the SHARED Pydantic schema; invalid
                      |                     records -> fraud.dlq; valid records grouped by customer_id ->
                      |                     applyInPandasWithState (per-customer behavioural state in the
                      |                     checkpointed state store; event-id dedup; lateness; shared
                      |                     features, ML inference and risk engine)
                      |                                       -> transactions.normal | .suspicious
                      |                                          (late events -> fraud.dlq)
                      +- validated-dedup  : JVM-native validation + event-time watermark +
                      |                     dropDuplicatesWithinWatermark(event_id) -> transactions.validated
                      +- window-*         : JVM-native validation + event-time tumbling/sliding windows
                                            with watermark   -> features.windows

Python UDFs run in ONE query (score); the other queries parse JSON natively in the JVM with
checks mirroring streaming/schemas/events.py (verified by integration tests: records rejected
by the Python validator never reach the validated topic or the window aggregates).

Every query has its own checkpoint directory under CHECKPOINT_DIR, so a restarted job resumes
from the last committed Kafka offsets and restores its state (feature history, dedup state,
window aggregates). Kafka output is AT-LEAST-ONCE (a batch replayed after a crash can be
re-emitted); the persistence consumer's idempotent writes absorb such duplicates.

Spark runs in local mode inside the container (SPARK_MASTER, default local[4]).

Run (inside the fraudstream-spark image):  python -m streaming.spark.fraud_stream_job
"""
from __future__ import annotations

import ast
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.kafka.client import Topics
from streaming.spark import logic
from streaming.spark.checkpoint import checkpoint_info

log = logging.getLogger("fraudstream.spark")
PROGRESS_KEY = "fraudstream:spark:progress"

WINDOW_SPECS: dict[str, tuple[str, str | None]] = {
    "tumbling_1m": ("1 minute", None),        # transaction velocity
    "sliding_5m": ("5 minutes", "1 minute"),  # 5-minute velocity, updated every minute
    "tumbling_15m": ("15 minutes", None),     # 15-minute spending totals
    "sliding_1h": ("1 hour", "15 minutes"),   # one-hour behavioural aggregates
}

VALIDATION_SCHEMA = StructType([
    StructField("key", BinaryType()), StructField("value", BinaryType()), StructField("kafka_ts", TimestampType()),
    StructField("src_partition", IntegerType()), StructField("src_offset", LongType()),
    StructField("valid", BooleanType()), StructField("reason", StringType()), StructField("event_id", StringType()),
    StructField("customer_id", StringType()), StructField("event_time", TimestampType()),
    StructField("amount", DoubleType()), StructField("status", StringType()),
])
SCORE_SCHEMA = StructType([StructField(c, StringType()) for c in logic.SCORE_COLUMNS])
STATE_SCHEMA = StructType([StructField("state_json", StringType())])


STATE_STORE_PROVIDERS = {
    "rocksdb": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    "hdfs": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
}


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("fraudstream-structured-streaming")
            .master(env("SPARK_MASTER", "local[4]"))
            .config("spark.jars.packages", env("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5"))
            .config("spark.jars.ivy", env("SPARK_IVY_DIR", "/opt/ivy"))
            # NOTE: the number of shuffle (= state store) partitions is fixed by a query's checkpoint;
            # changing it only affects NEW checkpoints.
            .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "2"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.streaming.stateStore.providerClass", STATE_STORE_PROVIDERS[env("STATE_STORE", "rocksdb")])
            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # workers are forked from a daemon that has already imported the ML stack and loaded models
            .config("spark.python.daemon.module", env("SPARK_PYTHON_DAEMON", "streaming.spark.daemon"))
            .config("spark.python.worker.reuse", "true")
            .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "1g"))
            .config("spark.ui.showConsoleProgress", "false")
            # Phase 12: the Spark UI has no authentication (published on 127.0.0.1 only); do not let it kill jobs/stages
            .config("spark.ui.killEnabled", "false")
            .getOrCreate())


def read_raw(spark: SparkSession, topics: Topics, bootstrap: str) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topics.raw)
            .option("startingOffsets", env("STARTING_OFFSETS", "earliest"))   # only used for a NEW checkpoint
            .option("failOnDataLoss", env("FAIL_ON_DATA_LOSS", "true"))
            .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "5000"))
            .load()
            .select(F.col("key"), F.col("value"), F.col("timestamp").alias("kafka_ts"),
                    F.col("partition").alias("src_partition"), F.col("offset").alias("src_offset")))


def validated(raw: DataFrame) -> DataFrame:
    """Python validation with the shared Pydantic schema (used by the score query only)."""
    return raw.mapInPandas(lambda it: (logic.validate_frame(pdf) for pdf in it), VALIDATION_SCHEMA)


_LOC = StructType([StructField("city", StringType()), StructField("country", StringType()),
                   StructField("lat", DoubleType()), StructField("lon", DoubleType()),
                   StructField("simulated", BooleanType())])
EVENT_JSON_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()), StructField("account_id", StringType()),
    StructField("merchant_id", StringType()), StructField("merchant_name", StringType()),
    StructField("amount", DoubleType()), StructField("currency", StringType()),
    StructField("event_time", StringType()), StructField("transaction_type", StringType()),
    StructField("merchant_category", StringType()), StructField("channel", StringType()),
    StructField("device_id", StringType()), StructField("ip_address", StringType()),
    StructField("location", _LOC), StructField("status", StringType()),
    StructField("schema_version", StringType()), StructField("synthetic", BooleanType()),
    StructField("source", StringType()),
])
_ALLOWED_KEYS = [f.name for f in EVENT_JSON_SCHEMA.fields] + ["ground_truth"]


def _len_between(c: Any, lo: int, hi: int) -> Any:
    return F.length(c).between(lo, hi)


def native_valid(raw: DataFrame) -> DataFrame:
    """JVM-native parsing with checks mirroring streaming/schemas/events.py (no Python workers)."""
    from streaming.schemas.events import MERCHANT_CATEGORIES, PaymentChannel, TransactionStatus, TransactionType

    js = F.col("value").cast("string")
    e = F.col("e")
    ts = F.to_timestamp(e.event_time)
    d = raw.select(F.col("value"), F.col("kafka_ts"), F.from_json(js, EVENT_JSON_SCHEMA).alias("e"),
                   F.json_object_keys(js).alias("keys"))
    ok = (e.isNotNull()
          & F.coalesce(F.forall("keys", lambda k: k.isin(*_ALLOWED_KEYS)), F.lit(False))   # extra="forbid"
          & _len_between(e.event_id, 8, 64) & _len_between(e.transaction_id, 4, 64)
          & _len_between(e.customer_id, 1, 64) & _len_between(e.account_id, 1, 64)
          & _len_between(e.merchant_id, 1, 64)
          & (e.amount > 0) & (e.amount <= 1e7) & (F.round(e.amount, 2) == e.amount)
          & e.currency.rlike("^[A-Z]{3}$")
          & e.event_time.rlike("(Z|[+-][0-9]{2}:?[0-9]{2})$") & ts.isNotNull()
          & e.transaction_type.isin(*[t.value for t in TransactionType])
          & e.merchant_category.isin(*MERCHANT_CATEGORIES)
          & e.channel.isin(*[c.value for c in PaymentChannel])
          & e.status.isin(*[st.value for st in TransactionStatus])
          & _len_between(e.device_id, 1, 64) & _len_between(e.ip_address, 3, 45)
          & e.location.isNotNull() & e.location.lat.between(-90, 90) & e.location.lon.between(-180, 180)
          & _len_between(e.location.city, 1, 80) & _len_between(e.location.country, 2, 2)
          & F.coalesce(F.split(e.schema_version, "[.]").getItem(0) == "1", F.lit(True)))
    return d.where(ok).select(F.col("value"), F.col("kafka_ts"), e.event_id.alias("event_id"),
                              e.customer_id.alias("customer_id"), ts.alias("event_time"),
                              e.amount.alias("amount"), e.status.alias("status"))


def kafka_sink(df: DataFrame, name: str, checkpoint_root: str, bootstrap: str, *, topic: str | None = None,
               mode: str = "append", trigger: str = "2 seconds") -> Any:
    w = (df.writeStream.format("kafka").queryName(name).outputMode(mode)
         .option("kafka.bootstrap.servers", bootstrap)
         .option("kafka.acks", "all").option("kafka.enable.idempotence", "true")
         .option("checkpointLocation", f"{checkpoint_root}/{name}")
         .trigger(processingTime=trigger))
    if topic:
        w = w.option("topic", topic)
    return w.start()


def hdr(*pairs: tuple[str, Any]) -> Any:
    return F.array(*[F.struct(F.lit(k).alias("key"), F.coalesce(v.cast("string"), F.lit("")).cast("binary").alias("value"))
                     for k, v in pairs])


def start_queries(spark: SparkSession, topics: Topics, bootstrap: str, checkpoint_root: str) -> list[Any]:
    trigger = env("TRIGGER_INTERVAL", "1 second")
    window_trigger = env("WINDOW_TRIGGER_INTERVAL", "5 seconds")
    wm = env("WATERMARK_DELAY", "10 minutes")
    queries = []

    # 1) score: Python validation (shared schema) + stateful per-customer scoring.
    #    Invalid records form the group logic.INVALID_KEY and are dead-lettered by the same query.
    def score_fn(key, pdf_iter, state):  # type: ignore[no-untyped-def]
        return logic.score_group_iter(key, pdf_iter, state, topics)

    v = validated(read_raw(spark, topics, bootstrap))
    scored = (v.select("customer_id", "value", "kafka_ts", "src_partition", "src_offset", "event_time", "valid", "reason")
              .groupBy("customer_id")
              .applyInPandasWithState(score_fn, SCORE_SCHEMA, STATE_SCHEMA, "append", GroupStateTimeout.NoTimeout))
    scored_out = scored.select(
        F.col("topic"), F.col("key").cast("binary").alias("key"), F.col("value").cast("binary").alias("value"),
        F.when(F.col("h_reason").isNull(),
               hdr(("event_id", F.col("h_event_id")), ("processor", F.lit(logic.PROCESSOR_NAME))))
         .otherwise(hdr(("dlq_reason", F.col("h_reason")), ("dlq_stage", F.col("h_stage")),
                        ("source_topic", F.lit(topics.raw)), ("source_partition", F.col("h_src_partition")),
                        ("source_offset", F.col("h_src_offset")), ("event_id", F.col("h_event_id")),
                        ("dlq_at", F.current_timestamp())))
         .alias("headers"))
    queries.append(kafka_sink(scored_out, "score", checkpoint_root, bootstrap, trigger=trigger))   # topic per row

    # 2) event-time watermark + event-id deduplication -> transactions.validated (JVM only)
    good = native_valid(read_raw(spark, topics, bootstrap))
    dedup = (good.withWatermark("event_time", wm).dropDuplicatesWithinWatermark(["event_id"])
             .select(F.col("customer_id").cast("binary").alias("key"), F.col("value"),
                     hdr(("event_id", F.col("event_id")), ("processor", F.lit(logic.PROCESSOR_NAME))).alias("headers")))
    queries.append(kafka_sink(dedup, "validated-dedup", checkpoint_root, bootstrap, topic=topics.validated, trigger=trigger))

    # 3) event-time behavioural window aggregates (late rows beyond the watermark are dropped; JVM only)
    enabled = [w.strip() for w in env("WINDOWS", ",".join(WINDOW_SPECS)).split(",") if w.strip()]
    for name in enabled:
        duration, slide = WINDOW_SPECS[name]
        win = F.window("event_time", duration, slide) if slide else F.window("event_time", duration)
        agg = (native_valid(read_raw(spark, topics, bootstrap)).withWatermark("event_time", wm)
               .groupBy(win.alias("w"), F.col("customer_id"))
               .agg(F.count(F.lit(1)).alias("tx_count"),
                    F.sum(F.when(F.col("status") == "APPROVED", F.col("amount")).otherwise(0.0)).alias("amount_sum"),
                    F.sum(F.when(F.col("status").isin("FAILED", "DECLINED"), 1).otherwise(0)).alias("failed_count")))
        out = agg.select(F.col("customer_id").cast("binary").alias("key"), F.to_json(F.struct(
            F.lit(name).alias("window_type"), F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
            F.col("customer_id"), F.col("tx_count"), F.round("amount_sum", 2).alias("amount_sum"),
            F.col("failed_count"))).cast("binary").alias("value"))
        queries.append(kafka_sink(out, f"window-{name}", checkpoint_root, bootstrap, topic=topics.windows,
                                  mode="update", trigger=window_trigger))
    return queries


def prewarm_python_workers(spark: SparkSession) -> None:
    """Load models/rules in the (reused) Python workers before streaming starts, so the first
    live micro-batches do not pay the model-loading cost (measured: 4-5 s first batches)."""
    n = spark.sparkContext.defaultParallelism
    t0 = time.perf_counter()
    try:
        labels = (spark.sparkContext.parallelize(range(n), n)
                  .map(lambda _: (logic.get_watcher().scorer.supervised.label
                                  if logic.get_watcher().scorer.supervised else None)).collect())
        log.info("pre-warmed %d python workers in %.1fs (model=%s)", n, time.perf_counter() - t0, labels[0])
    except Exception as exc:  # noqa: BLE001 - not fatal: workers load lazily instead
        log.warning("python worker pre-warm failed: %s", exc)


# ------------------------------------------------------------------ progress reporting
def _progress_snapshot(spark: SparkSession, queries: list[Any]) -> dict[str, Any]:
    checkpoint_root = env("CHECKPOINT_DIR", "/checkpoints")
    qs = []
    for q in queries:
        p = q.lastProgress or {}
        src = (p.get("sources") or [{}])[0]
        end = src.get("endOffset")
        if isinstance(end, str):   # JSON in Spark 3.x; a Python-repr string (single quotes) in PySpark 4
            try:
                end = json.loads(end)
            except json.JSONDecodeError:
                try:
                    end = ast.literal_eval(end)
                except (ValueError, SyntaxError):
                    pass
        qs.append({
            "name": q.name, "id": str(q.id), "run_id": str(q.runId), "active": q.isActive,
            "status": (q.status or {}).get("message"), "batch_id": p.get("batchId"),
            "num_input_rows": p.get("numInputRows"), "input_rows_per_second": p.get("inputRowsPerSecond"),
            "processed_rows_per_second": p.get("processedRowsPerSecond"),
            "trigger_ms": (p.get("durationMs") or {}).get("triggerExecution"), "timestamp": p.get("timestamp"),
            "duration_ms": dict(p.get("durationMs") or {}),
            "source_end_offsets": end,
            "state_rows": sum(s.get("numRowsTotal", 0) for s in p.get("stateOperators", []) or []),
            "rows_dropped_by_watermark": sum(s.get("numRowsDroppedByWatermark", 0) for s in p.get("stateOperators", []) or []),
            "event_time_watermark": (p.get("eventTime") or {}).get("watermark"),
            "exception": str(q.exception()) if not q.isActive and q.exception() else None,
            "checkpoint": checkpoint_info(f"{checkpoint_root}/{q.name}"),
        })
    return {"processor": logic.PROCESSOR_NAME, "spark_version": spark.version,
            "master": spark.sparkContext.master, "app_id": spark.sparkContext.applicationId,
            "checkpoint_root": checkpoint_root, "reported_at": time.time(), "queries": qs}


def report_progress(spark: SparkSession, queries: list[Any], redis_client: Any, stop: threading.Event,
                    path: str | None) -> None:
    while not stop.wait(5):
        snap = _progress_snapshot(spark, queries)
        if redis_client is not None:
            try:
                redis_client.set(PROGRESS_KEY, json.dumps(snap, default=str), ex=60)
            except Exception as exc:  # noqa: BLE001
                log.warning("could not publish spark progress: %s", exc.__class__.__name__)
        if path:
            try:
                with open(path, "w") as fh:
                    json.dump(snap, fh, default=str)
            except OSError:
                pass


def _remove_progress_file(path: str | None) -> None:
    if path:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning("could not remove stale progress file %s: %s", path, exc)


def _python_processor_active(redis_client: Any) -> bool:
    if redis_client is None:
        return False
    try:
        raw = redis_client.get("fraudstream:processor:heartbeat")
    except Exception:  # noqa: BLE001
        return False
    return bool(raw) and time.time() - json.loads(raw).get("ts", 0) < 30


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        log.error("KAFKA_BOOTSTRAP_SERVERS is required")
        return 2
    topics = Topics.from_env()
    checkpoint_root = env("CHECKPOINT_DIR", "/checkpoints")
    redis_client = None
    if os.environ.get("REDIS_URL"):
        from app.services.redis_guard import guarded_from_url

        redis_client = guarded_from_url(os.environ["REDIS_URL"], "redis (spark driver)")
    if _python_processor_active(redis_client) and env("ALLOW_CONCURRENT_PROCESSORS", "0") != "1":
        log.error("the python-kafka processor is running (fresh heartbeat); refusing to start a second stream "
                  "processor on the same topics. Stop it or set ALLOW_CONCURRENT_PROCESSORS=1.")
        return 3
    # A progress file left by a previous run survives a container *restart* (same filesystem) and would
    # make the health check report this new, still-starting job healthy for up to its max age.
    progress_file = os.environ.get("PROGRESS_FILE")
    _remove_progress_file(progress_file)
    spark = build_spark()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))
    log.info("spark %s master=%s topics prefix=%r checkpoints=%s", spark.version, spark.sparkContext.master,
             topics.prefix, checkpoint_root)
    prewarm_python_workers(spark)
    queries = start_queries(spark, topics, bootstrap, checkpoint_root)
    stop = threading.Event()
    reporter = threading.Thread(target=report_progress, daemon=True,
                                args=(spark, queries, redis_client, stop, progress_file))
    reporter.start()

    shutdown = threading.Event()

    def _shutdown(*_: Any) -> None:
        # Only set a flag: calling into Py4J from a signal handler while the main thread is
        # blocked in a Py4J call causes "reentrant call" errors (observed in Phase 9).
        shutdown.set()
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    exit_code = 0
    try:
        while not shutdown.is_set():
            if spark.streams.awaitAnyTermination(1):     # a query terminated (failed or stopped)
                break
        if shutdown.is_set():
            log.info("shutdown requested: stopping streaming queries gracefully")
        failed = [q for q in queries if q.exception() is not None]
        if failed:
            for q in failed:
                log.error("query %s failed: %s", q.name, q.exception())
            exit_code = 1
    except Exception as exc:  # noqa: BLE001
        log.exception("streaming terminated with error: %s", exc)
        exit_code = 1
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        stop.set()
        reporter.join(timeout=10)
        _remove_progress_file(progress_file)   # a stopped job must not look healthy
        if redis_client is not None:
            try:
                snap = _progress_snapshot(spark, queries)
                snap["terminated"] = True
                redis_client.set(PROGRESS_KEY, json.dumps(snap, default=str), ex=60)
            except Exception:  # noqa: BLE001
                pass
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
