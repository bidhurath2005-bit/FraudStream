# FraudStream — benchmarks (Phase 12)

All numbers in `docs/phase12/RELIABILITY_REPORT.md` were **measured** with the scripts below on the machine described
there; each run writes a JSON file to `benchmarks/results/` (environment, container limits, raw distributions,
resource samples). They describe this single development machine, **not production capacity**.

## Target

Always the **isolated** stack (compose project `fraudstream-p12`, `.env.isolated`, dashboard port 18080) — never the
live one: the benchmarks write tens of thousands of rows. Every script refuses project `fraudstream` unless
`BENCH_ALLOW_LIVE=1`. Start it as described in `docs/phase12/BACKUP_AND_RESTORE.md` (§4) or with
`python scripts/isolated_env.py` + `docker compose -p fraudstream-p12 --env-file .env.isolated -f docker-compose.yml
-f infrastructure/isolated/docker-compose.isolated.yml --profile distributed up -d`.

The API benchmark needs the per-token API limit (1,200/min) and the login limit (10/min) raised in `.env.isolated`
(`API_RATE_LIMIT_PER_MINUTE=10000000`, `LOGIN_RATE_LIMIT_PER_MINUTE=100000`, then `up -d api`) — otherwise every
level is capped at 20 req/s by design. The limits in force are recorded in the result. Restore the defaults afterwards.

## Scripts

| Script | Measures | Notes |
|---|---|---|
| `stream_bench.py --rates 10,25,50,100,150 --duration 60` | paced Kafka ingestion levels: submitted / acknowledged / persisted rates, **separate** latencies (producer ack; raw→scored = Spark; scored→`processed_at` = persistence; raw→`processed_at` = end-to-end), Spark and consumer lag (sampled from `/infrastructure/status` every 3 s), errors, missing, duplicate rows, re-emitted scored records, dead letters, alerts, `docker stats`, volume growth | the measuring process runs **inside a container on the stack network**, so all timestamps come from the Docker VM clock (no host/VM skew); arrivals are Poisson (the simulator), so the offered rate is ≈ 5–13 % above the nominal one (scenario tails) — the *submitted* rate is what was actually sent |
| `stream_bench.py --rates 100 --duration 600` | sustained high load | |
| `stream_bench.py --burst 5000` | backlog drain: everything sent at once | max processing / persistence rate |
| `stream_bench.py --source api --rates … --concurrency N` | HTTP ingestion (`POST /transactions/ingest` inside the network) | accepted = HTTP 202 (the API waits for the Kafka acknowledgement) |
| `api_bench.py --levels 1,8,32 --seconds 6` | closed-loop concurrency per endpoint through nginx: throughput, p50/p95/p99, status codes, error rate, mean response size | client = httpx on the Windows host → nginx (127.0.0.1) → API; includes the Windows→VM port-forward latency |
| `report_bench.py` | 6 report types × 1 h / 7 d / 366 d: generation, CSV and PDF time and size, API memory peak; `/auth/me` probe latency every 100 ms (idle, during sequential generation, during 4 concurrent large reports) | |
| `persist_microbench.py` | `persist_scored_event` with one transaction per event vs one per batch (SAVEPOINT per event), throwaway DB | bottleneck analysis only |

Workload: synthetic events from the simulator (`mixed`, 5 % injected scenarios), each run in its own `BM<hex>-`
customer namespace; the customer population scales with the rate (`--customers-per-eps 200`, ≥ 2,000). The first
sustained run used a fixed 2,000-customer population: per-customer velocity rose with load and **40 %** of the events
were flagged (velocity rules) — kept as an *alert-storm* data point, not as the baseline.

Percentiles: nearest-rank; p99 only reported with ≥ 100 samples. "steady" persisted rate = (n − 1) / (last − first
`processed_at`).
