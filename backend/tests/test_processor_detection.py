"""The API must name the stream processor that is ACTUALLY running (never assume Spark)."""
from __future__ import annotations

import json
import time

from app.services.streaming_status import detect_stream_processor, spark_status


class FakeRedis:
    def __init__(self, data: dict[str, dict]) -> None:
        self.data = {k: json.dumps(v) for k, v in data.items()}

    def get(self, k: str):  # noqa: ANN201
        return self.data.get(k)


def spark(age: float = 1, **kw: object) -> dict:
    return {"fraudstream:spark:progress": {"reported_at": time.time() - age, "queries": [{"name": "score"}], **kw}}


def py(age: float = 1) -> dict:
    return {"fraudstream:processor:heartbeat": {"ts": time.time() - age, "processor": "python-kafka"}}


def test_no_redis_or_no_heartbeat_reports_not_running():
    assert detect_stream_processor(None).startswith("NOT RUNNING")
    assert detect_stream_processor(FakeRedis({})).startswith("NOT RUNNING")


def test_fresh_spark_progress_reports_spark():
    assert detect_stream_processor(FakeRedis(spark())) == "Spark Structured Streaming"


def test_stale_terminated_or_failed_spark_is_not_reported_as_running():
    assert spark_status(FakeRedis(spark(age=120)), {})["status"] == "STALE"
    assert spark_status(FakeRedis(spark(terminated=True)), {})["status"] == "STOPPED"
    failed = spark()
    failed["fraudstream:spark:progress"]["queries"] = [{"name": "score", "exception": "boom"}]
    assert spark_status(FakeRedis(failed), {})["status"] == "FAILED"
    for data in (spark(age=120), spark(terminated=True), failed):
        assert detect_stream_processor(FakeRedis(data)).startswith("NOT RUNNING")


def test_python_processor_reported_with_explicit_non_spark_label():
    assert detect_stream_processor(FakeRedis(py())) == "python-kafka processor (non-Spark)"
    assert detect_stream_processor(FakeRedis(py(age=120))).startswith("NOT RUNNING")


def test_redis_error_is_reported_as_unknown_not_as_not_running():
    """Phase 12: Spark keeps processing during a Redis outage; the API cannot read its heartbeat, so it must
    say UNKNOWN instead of claiming the processor is not running."""
    class DownRedis:
        def get(self, k: str):  # noqa: ANN201
            raise ConnectionError("redis down")

    assert detect_stream_processor(DownRedis()).startswith("UNKNOWN (Redis unavailable")
    assert detect_stream_processor(None).startswith("NOT RUNNING")          # Redis not configured: unchanged
