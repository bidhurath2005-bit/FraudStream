"""Simulation control (start / pause / resume / stop / inject / replay).

Sinks:
* LOCAL profile: events go straight into the in-process LocalPipeline.
* DISTRIBUTED profile: events are produced to Kafka ``transactions.raw``. If Kafka is
  unreachable the run FAILS with an explicit error — it never falls back to the local
  pipeline.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from app.database.session import session_scope
from app.models.entities import SimulationRun
from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.kafka.client import KafkaUnavailableError, Topics, check_kafka, encode
from streaming.schemas.events import TransactionEvent

log = logging.getLogger("fraudstream.simulation")


class KafkaSink:
    def __init__(self, producer: Any, topic: str | None = None) -> None:
        self.producer = producer
        self.topic = topic or Topics.from_env().raw
        self.delivered = 0
        self.failed = 0
        self.last_error: str | None = None

    def _cb(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
        else:
            self.delivered += 1

    def send(self, ev: TransactionEvent) -> None:
        now_ms = int(time.time() * 1000)
        while True:
            try:
                self.producer.produce(self.topic, key=ev.customer_id.encode(), value=encode(ev.to_wire()),
                                      headers={"event_id": ev.event_id, "schema_version": ev.schema_version,
                                               "producer": "fraudstream-simulator"},
                                      timestamp=now_ms, on_delivery=self._cb)
                break
            except BufferError:
                self.producer.poll(0.2)  # local queue full: apply backpressure instead of dropping
        self.producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self.producer.flush(timeout)


class SimulationManager:
    def __init__(self, runtime: Any) -> None:
        self.rt = runtime
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._resume = threading.Event()
        self._resume.set()
        self._lock = threading.Lock()
        self.state: dict[str, Any] = {"status": "IDLE"}
        self._kafka_sink: KafkaSink | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------ sinks
    def _sink(self) -> Any:
        if self.rt.settings.profile == "local":
            return lambda ev: self.rt.pipeline.process(ev, ingested_at=datetime.now(timezone.utc))
        check_kafka(self.rt.settings.kafka_bootstrap_servers)  # raises KafkaUnavailableError
        self._kafka_sink = KafkaSink(self.rt.kafka_producer())
        return self._kafka_sink.send

    # ------------------------------------------------------------------ control
    def start(self, config: SimulationConfig, started_by: Any, label: str | None = None) -> dict[str, Any]:
        with self._lock:
            if self.running:
                raise RuntimeError("a simulation is already running; stop it first")
            if self.rt.settings.profile == "local" and not self.rt.scorer.ready:
                log.warning("starting simulation without an active ML model: rules-only scoring")
            sink = self._sink()
            run_uid = str(uuid.uuid4())
            with session_scope() as db:
                db.add(SimulationRun(run_uid=run_uid, config=asdict(config), profile=self.rt.settings.profile,
                                     status="RUNNING", started_by_id=started_by.id if started_by else None))
            self._stop.clear()
            self._resume.set()
            self.state = {"status": "RUNNING", "run_uid": run_uid, "config": asdict(config), "label": label,
                          "profile": self.rt.settings.profile, "started_at": datetime.now(timezone.utc).isoformat(),
                          "events_generated": 0, "events_suspicious_injected": 0, "errors": 0, "sink": (
                              "in-process pipeline" if self.rt.settings.profile == "local" else f"kafka:{Topics.from_env().raw}")}
            self._thread = threading.Thread(target=self._run, args=(config, sink, run_uid), daemon=True,
                                            name="simulation")
            self._thread.start()
            return dict(self.state)

    def pause(self) -> dict[str, Any]:
        if not self.running or self.state["status"] != "RUNNING":
            raise RuntimeError("no running simulation to pause")
        self._resume.clear()
        self.state["status"] = "PAUSED"
        return dict(self.state)

    def resume(self) -> dict[str, Any]:
        if not self.running or self.state["status"] != "PAUSED":
            raise RuntimeError("simulation is not paused")
        self.state["status"] = "RUNNING"
        self._resume.set()
        return dict(self.state)

    def stop(self) -> dict[str, Any]:
        if not self.running:
            raise RuntimeError("no simulation is running")
        self._stop.set()
        self._resume.set()
        if self._thread:
            self._thread.join(timeout=35)
        return dict(self.state)

    def status(self) -> dict[str, Any]:
        st = dict(self.state)
        if self._kafka_sink:
            st["kafka"] = {"delivered": self._kafka_sink.delivered, "failed": self._kafka_sink.failed,
                           "last_error": self._kafka_sink.last_error}
        return st

    # ------------------------------------------------------------------ worker
    def _run(self, config: SimulationConfig, sink: Any, run_uid: str) -> None:
        gen = TransactionGenerator(config, datetime.now(timezone.utc))
        wall0 = time.monotonic()
        paused_total = 0.0
        error: str | None = None
        sent = sent_suspicious = 0   # events actually handed to the sink (not merely generated)
        try:
            for ev in gen.events():
                if self._stop.is_set():
                    break
                if not self._resume.is_set():
                    t_pause = time.monotonic()
                    self._resume.wait()
                    paused_total += time.monotonic() - t_pause
                    if self._stop.is_set():
                        break
                if paused_total:
                    ev = ev.model_copy(update={"event_time": ev.event_time + timedelta(seconds=paused_total)})
                # event_time already includes the paused offset, so wall-clock target = wall0 + due
                due = (ev.event_time - gen.start_time).total_seconds()
                delay = wall0 + due - time.monotonic()
                while delay > 0 and not self._stop.is_set():
                    self._stop.wait(min(delay, 0.5))
                    delay = wall0 + due - time.monotonic()
                if self._stop.is_set():
                    break
                try:
                    sink(ev)
                    sent += 1
                    sent_suspicious += int(bool(ev.ground_truth and ev.ground_truth.is_suspicious))
                except KafkaUnavailableError:
                    raise
                except Exception as exc:  # noqa: BLE001 - count, surface, keep going
                    self.state["errors"] += 1
                    self.state["last_error"] = f"{exc.__class__.__name__}: {exc}"
                    if self.state["errors"] > 50:
                        raise RuntimeError(f"too many processing errors; last: {self.state['last_error']}") from exc
                self.state["events_generated"] = sent
                self.state["events_suspicious_injected"] = sent_suspicious
            if self._kafka_sink:
                left = self._kafka_sink.flush()
                if left or self._kafka_sink.failed:
                    error = f"kafka delivery failures: {self._kafka_sink.failed}, undelivered: {left}"
        except Exception as exc:  # noqa: BLE001
            error = f"{exc.__class__.__name__}: {exc}"
            log.exception("simulation failed")
        status = "FAILED" if error else ("STOPPED" if self._stop.is_set() else "COMPLETED")
        self.state.update(status=status, ended_at=datetime.now(timezone.utc).isoformat(), error=error,
                          events_generated=sent, events_suspicious_injected=sent_suspicious)
        try:
            with session_scope() as db:
                run = db.query(SimulationRun).filter_by(run_uid=run_uid).one()
                run.status, run.ended_at, run.error = status, datetime.now(timezone.utc), error
                run.events_generated = sent
                run.events_suspicious = sent_suspicious
        except Exception:  # noqa: BLE001
            log.exception("could not record simulation run end")

    # ------------------------------------------------------------------ one-off injection
    def inject(self, scenario: str, customer_id: str | None, seed: int | None, customer_count: int,
               population_seed: int = 1000) -> list[dict[str, Any]]:
        cfg = SimulationConfig(mode="custom", scenarios=[scenario], seed=seed if seed is not None else 42,
                               customer_count=customer_count, population_seed=population_seed)
        gen = TransactionGenerator(cfg, datetime.now(timezone.utc))
        events = gen.inject(scenario, customer_id)
        out = []
        if self.rt.settings.profile == "local":
            for ev in events:
                r = self.rt.pipeline.process(ev, ingested_at=datetime.now(timezone.utc))
                out.append({"event_id": ev.event_id, "transaction_id": ev.transaction_id, "status": r["status"],
                            "risk_score": r.get("decision", {}).get("risk_score"),
                            "risk_category": r.get("decision", {}).get("risk_category"),
                            "alert_uid": r.get("alert_uid")})
        else:
            check_kafka(self.rt.settings.kafka_bootstrap_servers)
            sink = KafkaSink(self.rt.kafka_producer())
            for ev in events:
                sink.send(ev)
            left = sink.flush(15)
            if left or sink.failed:
                raise KafkaUnavailableError(f"kafka delivery failed (failed={sink.failed}, undelivered={left})")
            out = [{"event_id": ev.event_id, "transaction_id": ev.transaction_id, "status": "published_to_kafka"}
                   for ev in events]
        return out
