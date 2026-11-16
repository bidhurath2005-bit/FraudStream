# FraudStream — Project overview

*A one-page technical summary for interviews and reviews. Every number links back to a measured result.*

## In one sentence

FraudStream is a real-time fraud-detection platform for **synthetic** payment transactions. Events flow through Kafka
into Spark Structured Streaming, where per-customer behavioural features, an XGBoost and Isolation Forest model and a
configurable rule engine score each one. Results are persisted exactly once into PostgreSQL and worked by analysts in
a React dashboard with investigations, analytics, reports and an audit trail.

## The problem it models

Fraud has to be detected while transactions stream, using **per-customer state**: how many transactions in the last
minute, how far an amount deviates from this customer's history, whether the device or city is new, how fast they
would have to travel. Decisions must be **explainable** (which rules fired, which features pushed the score) and
**auditable** (who changed a threshold, who closed a case). The pipeline must not lose or double-count events when
components fail.

## Architecture at a glance

```text
Simulator / API ──► Kafka (transactions.raw)
                        │
                        ▼
         Spark Structured Streaming (local[8], one container)
           validate → dedup → per-customer state (RocksDB) → features
           → XGBoost + Isolation Forest → risk engine (rules + noisy-OR)
                        │
                        ▼
      Kafka (transactions.normal / .suspicious, fraud.dlq, features.windows)
                        │
                        ▼
        Persistence consumer ──► PostgreSQL (one transaction per event)
                        │            then: fraud.alerts, offset commit
                        ▼
              Redis pub/sub ──► FastAPI SSE ──► nginx ──► React dashboard
```

Two explicit profiles: **distributed** (the above, 7 services in Docker Compose) and **local** (same scoring code
in-process on SQLite, for development). Diagrams: [DIAGRAMS.md](DIAGRAMS.md).

## What I would highlight technically

1. **One scoring library, three runtimes.** Feature engineering, inference and the risk engine are plain Python
   modules with no web or database imports. They run inside Spark's `applyInPandasWithState`, inside the API for the
   local profile, and in a fallback processor. Tests assert that the outputs are identical.
2. **Effectively-once persistence on an at-least-once stream.** Spark's Kafka sink can re-emit a batch after a crash.
   The consumer commits offsets only after the database commit, and `UNIQUE(event_id)` (plus one-prediction and
   one-alert-per-transaction constraints) absorbs replays. Crash tests of Spark, the consumer, Kafka and PostgreSQL
   ended with **0 lost and 0 duplicate rows**.
3. **Leakage-aware ML evaluation.** Temporal 60/20/20 split, 15 % of customers held out entirely, thresholds (max F1)
   and model choice (validation PR-AUC) decided on validation only, and ground-truth fields blocked from the feature
   path by a forbidden-field list.
4. **Explainable hybrid scoring.** The model score is mapped so its threshold equals 0.5, the anomaly score counts
   only above the 97th percentile, and ten rules in seven families contribute at most one rule per family. Everything
   is combined with noisy-OR into 0–100. Each alert shows the triggered rules and TreeSHAP feature contributions.
5. **Measured, not assumed.** Stage-separated latency benchmarks, failure injection on an isolated copy of the stack,
   and a backup → restore → verify cycle. Each one found real defects that were then fixed, including HTTP 500s under
   concurrent analytics (PostgreSQL `/dev/shm`), a 4 s penalty on every request during a Redis outage, a login
   rate-limit bypass via `X-Forwarded-For`, and an SSE stream that died after 15 s idle.
6. **Operational honesty in the UI.** The dashboard derives "Spark Structured Streaming / NOT RUNNING / UNKNOWN" from
   live heartbeats rather than from configuration, never falls back silently, and labels synthetic data, simulated
   locations and simulator ground truth wherever they appear.

## Key numbers (one development machine; not production capacity)

| | |
|---|---|
| End-to-end latency (Kafka → Spark → Kafka → PostgreSQL) | p50 / p95 **2.5 / 3.4 s** at 10 ev/s; **3.8 / 5.6 s** at 100 ev/s |
| Sustained throughput | **≈ 100–105 events/s** (600 s run); one API process ingests ≈ 65 req/s |
| Recovery | Kafka outage: all acknowledged events persisted once, lag 0 in ~50 s; full stack restart healthy in 22–45 s; restore from backup healthy in 43–48 s |
| Model (synthetic test split) | XGBoost PR-AUC 0.9996, precision 0.994, recall 0.993. **Optimistic**: the scenarios are separable by construction |
| Tests | 203 Python + 64 frontend unit tests; 25 container, 12 Kafka, 6 Spark integration tests; 12 Playwright E2E per profile |

Sources: [phase12/RELIABILITY_REPORT.md](phase12/RELIABILITY_REPORT.md), [DATA_AND_ML.md](DATA_AND_ML.md),
[../DEVELOPMENT_STATUS.md](../DEVELOPMENT_STATUS.md).

## Stack

React 18 + TypeScript + Vite + TanStack Query + Recharts · FastAPI + SQLAlchemy 2 + Alembic + Pydantic 2 ·
scikit-learn 1.5 + XGBoost 2.1 · Kafka 3.9 (KRaft) · PySpark 4.0 (RocksDB state) · PostgreSQL 16 · Redis 7 ·
nginx · Docker Compose · pytest, Vitest, Playwright.

## Honest limitations (say these before you are asked)

- **Synthetic data only.** Model metrics show the pipeline works; they do not show it would catch real fraud.
- **Single host, no HA.** One Kafka broker (RF=1), one PostgreSQL, one Redis; Spark in local mode, not a cluster.
- **No TLS, no Kafka authentication.** Safe only because everything is bound to 127.0.0.1.
- **Capacity** is about 100 events/s on a laptop, and the persistence consumer is the first bottleneck.
- **Drift** currently reads SIGNIFICANT because simulated live timing differs from the one-day training set. That is
  a measured, explained result, not a bug.

## Likely interview questions and short answers

- **Why Spark if it runs in local mode?** To use Structured Streaming's checkpointed state, watermarks and
  exactly-once source tracking with the same code that would run on a cluster. Deploying a real cluster is a separate
  step, and the documentation states it was not done.
- **Why not end-to-end exactly-once with Kafka transactions?** Idempotent writes at the system of record are simpler,
  were verified under crashes, and the Kafka-to-Kafka duplicates they leave are harmless because the consumer
  rejects them.
- **How do you know there is no leakage?** Temporal split, customer holdout, validation-only selection, a forbidden
  field list enforced in feature engineering, and features that count *prior* events only. The unseen-customer split
  scores close to the test split.
- **What would you do next?** CI, TLS and Kafka authentication, batched persistence transactions (1.36× measured in
  isolation), evaluation on a public dataset, and a feedback loop from analyst decisions into training labels.
