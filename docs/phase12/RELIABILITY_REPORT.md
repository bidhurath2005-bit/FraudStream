# FraudStream — Phase 12 reliability report

Every number below is **measured** unless labelled *estimated* or *untested*. Raw results (with distributions,
resource samples and the environment) are in `benchmarks/results/*.json`; methodology in `benchmarks/BENCHMARKS.md`.
This is one development machine. **None of it is a production capacity claim.**

## 1. Environment

| Item | Value |
|---|---|
| Host | Windows 11, Intel Core 7 240H, 16 logical CPUs, 15.7 GB RAM |
| Docker | Docker Desktop 29.9.2, WSL2 kernel 6.18.58.1, VM: 16 vCPUs, 7.61 GiB |
| Stack | isolated compose project `fraudstream-p12` (own network/volumes/ports/secrets), restored from a backup of the live database (15,727 transactions at the start, ~183k after the benchmarks) |
| Limits | Kafka 1 GiB (heap 512 MiB, `log.flush.interval.messages=1`), PostgreSQL 512 MiB, Redis 256 MiB, Spark 3 GiB (`local[8]`, driver 1 GiB, trigger 2 s, RocksDB state), API 1.5 GiB (1 uvicorn worker), consumer 512 MiB; no CPU limits |
| Workload | simulator events (`mixed`, 5 % injected scenarios), Poisson arrivals, own customer namespace per run |

## 2. Streaming pipeline (Kafka → Spark → Kafka → consumer → PostgreSQL)

Latencies are separate stages, all on the Docker VM clock: **ack** = producer send → broker ack (acks=all);
**stream** = raw record → scored record (Spark, incl. the 2 s trigger wait); **persist** = scored record → row
committed; **e2e** = raw record → row committed. p50 / p95 / p99 in ms.

### 2.1 Paced levels (60 s each; fixed 2,000-customer population)

| Target ev/s | Submitted | Persisted (steady) | Ack | Stream | Persist | **E2E** | Max lag Spark / consumer | Alerts | Lost / dup / DLQ |
|---|---|---|---|---|---|---|---|---|---|
| 10 (low) | 10.5 | 10.5 | 79 / 320 / 624 | 2,359 / 3,237 / 3,510 | 211 / 333 / 390 | **2,525 / 3,421 / 3,753** | 81 / 23 | 11.7 % | 0 / 0 / 0 |
| 25 (normal) | 26.3 | 27.0 | 41 / 140 / 229 | 2,242 / 3,173 / 3,409 | 332 / 569 / 677 | **2,575 / 3,533 / 3,857** | 189 / 37 | 7.3 % | 0 / 0 / 0 |
| 50 (elevated) | 50.8 | 50.1 | 25 / 79 / 454 | 2,179 / 3,129 / 3,447 | 470 / 824 / 990 | **2,650 / 3,738 / 4,128** | 376 / 101 | 11.6 % | 0 / 0 / 0 |
| 100 (high) | 104.5 | 104.0 | 19 / 47 / 458 | 2,737 / 3,967 / 4,492 | 1,069 / 2,031 / 2,488 | **3,786 / 5,570 / 6,435** | 818 / 320 | 12.0 % | 0 / 0 / 0 |
| 150 (overload) | 153.2 | **115.1** | 32 / 220 / 451 | 5,342 / 13,536 / 14,973 | 4,731 / 13,157 / 14,792 | **10,424 / 23,955 / 26,908** | 2,620 / 1,221 | 18.4 % | 0 / 0 / 0 |

Every event of every level was persisted exactly once (0 missing, 0 duplicate rows, 0 re-emitted scored records,
0 dead letters). Up to ~100 ev/s for 60 s the pipeline keeps up (lag drains within ~4 s after the load stops);
at 150 ev/s it falls behind (backlog drained 22.5 s after the load stopped).

### 2.2 Sustained high load (600 s)

| Run | Offered | Persisted (steady) | E2E p50 / p95 / p99 | Max lag Spark / consumer | Alert rate | Result |
|---|---|---|---|---|---|---|
| #1 — 2,000 customers | 113.3 ev/s (68,001) | 95.4 | 73.2 s / 132.0 s / 140.4 s | 5,876 / 12,458 | **40.4 %** (velocity rules) | does not keep up; all 68,001 persisted exactly once, drained 119 s after the load |
| #2 — 20,000 customers | 112.6 ev/s (67,572) | **105.5** | 31.5 s / 57.0 s / 62.4 s | 3,294 / 4,970 | 19.0 % | does not keep up; all persisted exactly once, drained 45 s after |

Run #1 is an **alert storm** created by the benchmark (a fixed population makes per-customer velocity grow with the
load); it is kept as a stress data point. **Sustained capacity on this machine: ≈ 100–105 ev/s** (measured: 104 ev/s
for 60 s keeps up; 112.6 ev/s for 600 s does not). The offered rate is ~13 % above the nominal 100 because the
simulator adds scenario tails — the nominal rate is not what was sent.

### 2.3 Backlog (burst of 5,000 events sent at once)

Spark processed the whole backlog in **one micro-batch of 14.7 s (≈ 340 ev/s)**; the consumer then persisted it at
**217 ev/s steady**; all 5,000 persisted 38.6 s after sending, exactly once; consumer lag peaked at 3,895.

### 2.4 HTTP ingestion (`POST /transactions/ingest`, 16 concurrent clients inside the network)

| Target | Accepted (202) | HTTP p50 / p95 / p99 ms | E2E p50 / p95 ms | Errors |
|---|---|---|---|---|
| 10 | 9.2 /s | 26.7 / 64.0 / 215.9 | 3,167 / 4,907 | 0 |
| 25 | 26.0 /s | 27.0 / 104.2 / 403.8 | 2,971 / 3,969 | 0 |
| 50 | 51.2 /s | 66.9 / 277.4 / 339.5 | 4,207 / 5,817 | 0 |
| 100 | **65.0 /s** (saturated) | 243.4 / 368.6 / 461.6 | 6,319 / 9,015 | 0 |

One API process (single uvicorn worker; each request waits for the Kafka acknowledgement) accepts **≈ 65 ev/s**,
API CPU up to ~1.5 cores. Bulk producers (the simulator) write to Kafka directly.

### 2.5 Bottlenecks (analysis)

1. **Persistence consumer** — its lag grows first under sustained load. Inside the network, the database work alone
   sustains 259 ev/s with one transaction per event and 353 ev/s with one transaction per 200-event batch
   (`persist_microbench.py`, 1.36×); the remainder is per-batch Kafka waits (alert `flush()` + synchronous offset
   commit) whose latency rises under load. Not changed in Phase 12: Spark is close to its own limit at the same rate,
   so the end-to-end gain would be marginal while the change would touch the exactly-once persistence path.
2. **Kafka broker durability setting** — `log.flush.interval.messages=1` (fsync per message, single broker) plus
   ~6 Kafka writes per event (validated, scored, four window outputs): producer ack p50 rose from 19 ms (100 ev/s,
   60 s) to 147–164 ms (sustained). Relaxing it is a **durability trade-off for the owner to decide**, not done.
3. **Spark memory** — 2.7–3.05 GiB of its 3 GiB limit under load; the cgroup hit `memory.max` 1,128 → 4,693 times
   (reclaim pressure), no OOM kill, no restart. Larger state or longer windows need a higher limit.
4. **API**: one process, synchronous handlers in a thread pool (GIL) — see §3.

## 3. API (through nginx, closed-loop concurrency 1 / 8 / 32)

### 3.1 Defect found and fixed: HTTP 500 under concurrent analytics

With ~190k transactions the planner chose **parallel hash joins**, whose shared hash tables live in `/dev/shm`
(Docker default 64 MB). Concurrent dashboard/analytics requests failed with `could not resize shared memory segment
… No space left on device` → **HTTP 500**. Invisible at the live stack's 16k rows; a latent failure there too.

| Endpoint | Error rate before (c=8 / c=32) | `/dev/shm` 256 MB only | + 1 GiB memory | **Final: 256 MB + `enable_parallel_hash=off`, 512 MiB** |
|---|---|---|---|---|
| /analytics/overview | 57 % / 77 % | 0 / 0 | 0 / 0 | **0 / 0** |
| /analytics/timeline | 89 % / 90 % | 0 / 0 | 0 / 0 | **0 / 0** |
| /analytics/patterns | 86 % / 84 % | 0 / 0 | 0 / 0 | **0 / 0** |
| /analytics/confusion | 75 % / 96 % | 0 / 0 | 0 / 0 | **0 / 0** |
| /analytics/merchants | 96 % / 99 % | 0 / 4 % | 0 / 3 % | **0 / 0** |
| /analytics/geo | 93 % / 95 % | 0 / 52 % | 0 / 55 % | **0 / 0** |

Change: `shm_size: 256m` and `command: postgres -c enable_parallel_hash=off` on the PostgreSQL service (memory limit
unchanged at 512 MiB — the 1 GiB experiment did not fix `geo` and was reverted). Workers now build private hash
tables; parallel scans and partial aggregates remain. Cost: some single-request latencies rose (e.g. `confusion`
p50 103 → 227 ms, `merchants` 248 → 474 ms at c=1, comparing the shm-only and final runs). Behaviour unchanged
otherwise (same queries, same results; full regression suites green).

### 3.2 Latency per endpoint

Final images and configuration (`benchmarks/results/20261115T180258Z-api.json`), ~189k transactions; ms; ok/s = successful requests per second. Last column: the first (pre-fix) run.

| Endpoint | c=1: ok/s · p50 · p95 | c=8: ok/s · p50 · p95 · p99 | c=32: ok/s · p95 · errors | Mean size | Before fixes, c=8: ok/s · errors |
|---|---|---|---|---|---|
| `GET /alerts/{id}` | 41.3 · 22.4 · 38.5 | 94.7 · 80.4 · 125.2 · 208.7 | 68.7 · 1193.2 · 0 % | 3,982 B | 146.5 · 0 % |
| `GET /alerts?size=25` | 14.1 · 67.2 · 95.7 | 17.1 · 245.6 · 1405.6 · 1921.5 | 11.0 · 4164.6 · 0 % | 26,084 B | 28.7 · 0 % |
| `GET /analytics/confusion` | 4.5 · 210.2 · 260.8 | 17.5 · 444.5 · 547.7 · 571.1 | 20.6 · 1754.2 · 0 % | 409 B | 8.2 · 75 % |
| `GET /analytics/geo` | 37.8 · 25.2 · 36.5 | 118.1 · 64.8 · 95.3 · 155.8 | 145.4 · 311.4 · 0 % | 6,364 B | 2.9 · 93 % |
| `GET /analytics/hourly` | 4.0 · 251.9 · 309.8 | 18.4 · 418.3 · 573.8 · 579.9 | 23.1 · 1670.4 · 0 % | 978 B | 8.6 · 0 % |
| `GET /analytics/merchants` | 2.0 · 509.0 · 587.8 | 6.8 · 1141.8 · 1517.3 · n/a | 8.7 · 4847.6 · 0 % | 4,279 B | 1.3 · 96 % |
| `GET /analytics/overview` | 0.8 · 471.4 · 4403.0 | 12.0 · 620.8 · 877.0 · n/a | 11.6 · 3307.6 · 0 % | 978 B | 4.3 · 57 % |
| `GET /analytics/patterns?minutes=1440` | 0.9 · 599.4 · 3082.3 | 3.3 · 2267.5 · 2868.0 · n/a | 3.1 · 10598.8 · 0 % | 2,488 B | 2.5 · 86 % |
| `GET /analytics/timeline?minutes=30` | 21.3 · 38.1 · 72.5 | 96.4 · 80.5 · 112.9 · 126.7 | 130.1 · 329.1 · 0 % | 2,989 B | 6.9 · 89 % |
| `GET /analytics/velocity` | 53.1 · 17.5 · 25.9 | 113.5 · 53.4 · 167.9 · 291.4 | 87.3 · 1001.2 · 0 % | 1,195 B | 18.3 · 0 % |
| `GET /audit?size=25` | 56.5 · 17.1 · 25.7 | 125.0 · 51.2 · 148.1 · 238.0 | 92.3 · 871.3 · 0 % | 5,369 B | 45.4 · 0 % |
| `GET /auth/me` | 83.1 · 11.0 · 18.0 | 118.8 · 56.0 · 139.2 · 175.0 | 111.7 · 666.5 · 0 % | 181 B | 160.9 · 0 % |
| `GET /health` | 51.3 · 19.5 · 28.2 | 90.4 · 64.7 · 219.5 · 401.8 | 108.0 · 785.6 · 0 % | 183 B | 173.1 · 0 % |
| `GET /infrastructure/status` | 14.1 · 44.8 · 64.7 | 44.0 · 71.5 · 1077.4 · 1107.2 | 47.5 · 1591.1 · 0 % | 7,971 B | 75.7 · 0 % |
| `GET /investigations/{id}` | 42.3 · 23.9 · 31.9 | 83.7 · 91.6 · 138.1 · 200.8 | 67.7 · 1124.7 · 0 % | 10,396 B | 120.7 · 0 % |
| `GET /investigations?size=25` | 47.7 · 19.9 · 30.0 | 90.4 · 83.4 · 158.9 · 188.6 | 72.6 · 783.3 · 0 % | 15,797 B | 136.3 · 0 % |
| `GET /models/active` | 100.6 · 9.6 · 15.8 | 162.3 · 36.0 · 124.2 · 208.5 | 114.8 · 789.6 · 0 % | 4,220 B | 48.6 · 0 % |
| `GET /models/live-inference` | 24.9 · 33.2 · 106.7 | 37.8 · 213.4 · 292.4 · 329.2 | 36.8 · 1062.2 · 0 % | 499 B | 23.2 · 0 % |
| `GET /reports/generated/{id}` | 67.1 · 14.4 · 23.5 | 119.4 · 49.3 · 152.8 · 266.0 | 83.9 · 1052.2 · 0 % | 15,417 B | 52.9 · 0 % |
| `GET /reports/generated?size=20` | 105.7 · 8.8 · 13.3 | 149.3 · 51.7 · 73.5 · 98.9 | 88.1 · 954.7 · 0 % | 8,516 B | 45.2 · 0 % |
| `GET /reports/types` | 127.1 · 7.4 · 11.3 | 240.9 · 31.6 · 51.0 · 75.6 | 163.0 · 546.7 · 0 % | 2,139 B | 50.6 · 0 % |
| `GET /risk/rules` | 72.1 · 13.3 · 21.7 | 119.4 · 49.5 · 168.7 · 286.5 | 84.6 · 1020.9 · 0 % | 3,783 B | 43.3 · 0 % |
| `GET /risk/settings` | 65.3 · 14.4 · 25.7 | 112.1 · 51.4 · 201.7 · 306.3 | 93.7 · 908.5 · 0 % | 489 B | 51.4 · 0 % |
| `GET /settings/drift` | 87.0 · 10.1 · 19.0 | 121.4 · 50.8 · 161.8 · 313.0 | 82.0 · 1153.8 · 0 % | 491 B | 48.3 · 0 % |
| `GET /system/status` | 122.1 · 8.0 · 11.1 | 137.3 · 45.4 · 142.6 · 249.9 | 85.2 · 998.1 · 0 % | 525 B | 170.4 · 0 % |
| `GET /transactions/{id}` | 53.5 · 17.5 · 25.7 | 108.9 · 68.5 · 114.0 · 153.8 | 77.8 · 1117.0 · 0 % | 2,785 B | — (benchmark bug in run 1) |
| `GET /transactions?size=25` | 32.1 · 30.4 · 39.8 | 90.1 · 85.5 · 117.9 · 199.4 | 65.2 · 1328.4 · 0 % | 21,037 B | 117.7 · 0 % |
| `POST /auth/login` | 8.2 · 107.7 · 188.6 | 29.5 · 260.8 · 358.1 · 457.2 | — (login: c ≤ 8) | 493 B | 49.5 · 0 % |
| `POST /transactions/ingest` | 14.7 · 28.1 · 38.5 | 31.2 · 152.9 · 522.0 · 1715.3 | 29.1 · 2667.7 · 0 % | 122 B | 16.9 · 0 % |

**0 errors on every endpoint at every level.** Throughput of simple endpoints differs between runs by up to ~2× in both directions (e.g. `/reports/types` 51 → 241 ok/s, `/health` 173 → 90 ok/s at c=8): background activity (autovacuum after ~190k inserts, Spark idle triggers) dominates. The Phase 12 code adds **40 µs per request** (JWT check in the rate-limit key) and **0.5 µs per Redis call** (circuit breaker), measured in-process — not a cause of those differences. `p99` is shown only with ≥ 100 samples (`n/a` otherwise).

Caveat: analytics endpoints aggregate "the last N minutes", so their cost depends on how much traffic is recent
(e.g. `/analytics/velocity` c=1: 3.7 req/s right after the sustained run, 45 req/s later). Login (Argon2id, by
design ~57 ms p50) caps at ~16/s per client at c=1 and ~50/s at c=8. Closed-loop throughput of the single API process
peaks at c=8 and **drops at c=32** for most endpoints (thread pool + GIL), i.e. more concurrency only adds queueing.

## 4. Reports (6 types; API memory; responsiveness)

| Type | Rows covered | Generate | CSV | PDF |
|---|---|---|---|---|
| fraud_activity | 57–59 aggregate rows over ~180k tx | 2.25–2.28 s | ≤ 0.03 s / 5 KB | 0.08–0.79 s / 53 KB |
| risk_analytics | 55 | 1.97–2.20 s | ≤ 0.02 s / 3 KB | 0.07 s / 51 KB |
| alert_summary | 43,269–45,312 alerts (10,000 stored) | 1.01–1.26 s | 0.27–0.40 s / 1.9 MB | 1.50–2.50 s / 212 KB |
| investigation | 0–27 cases | 0.02–0.06 s | ≤ 0.03 s | 0.03–0.25 s |
| model_performance | 6–14 | 0.64–0.80 s | ≤ 0.03 s | 0.05–0.09 s |
| system_activity | 43,662–46,247 audit events (10,000 stored) | 0.56–0.78 s | 0.16–0.22 s / 1.8 MB | 0.98–1.22 s / 119 KB |

Small / medium / large = 1 h / 7 d / 366 d periods; because almost all benchmark data is recent, the large result
sets already occur in the 1-hour period (rows above are the measured coverage). API memory peak **385 MiB** (of
1.5 GiB). Probe `GET /auth/me` every 100 ms: idle p50/p95/max **16 / 42 / 53 ms**; during sequential generation
**21 / 237 / 394 ms**; during 4 concurrent large reports + PDF **111 / 365 / 482 ms**; 0 errors.

**Decision:** synchronous generation slows other requests (GIL) but does not block the API; worst case ≈ 2.3 s +
2.5 s PDF. A job queue is **not** warranted at this scale. Smallest safe step if reports grow past ~10–30 s:
run `generate()` in a `ProcessPoolExecutor` (one worker) with the report stored as `PENDING` and the page polling
`GET /reports/generated/{id}` — no new infrastructure. *(Proposal, not implemented.)*

## 5. Resources and disk growth

| Service | CPU mean / max (%) at 100 ev/s | Memory max |
|---|---|---|
| Spark | 396 / 773 | 3,004 MiB (limit 3,072) |
| Kafka | 37 / 205 | 688 MiB (840 MiB sustained) |
| PostgreSQL | 19 / 40 | 167 MiB (392 MiB sustained) |
| Persistence consumer | 51 / 79 | 154 MiB |
| API | 2 / 24 (ingestion: 72 / 146) | 193 MiB (397 MiB ingestion) |
| Redis | 2 / 6 | 10–18 MiB |

Disk growth, sustained run #1 (68,001 events): Kafka **+455 MB**, PostgreSQL **+388 MB**, Spark checkpoints
**+207 MB** → ≈ 6.7 / 5.7 / 3.0 MB per 1,000 events (*estimated* linear rate). Kafka retention bounds its growth;
PostgreSQL grows without bound (no data retention policy for transactions).

## 6. Failure recovery (isolated stack)

| Scenario (isolated stack) | What was verified | Measured |
|---|---|---|
| Kafka stopped 20.5 s **during continuous production** (25 ev/s) | every **acknowledged** event persisted exactly once; no unacknowledged event appeared; consumer and Spark lag back to 0; Spark reported again | run 1: 1,750 produced → **1,556 acknowledged, all persisted exactly once**; 194 reported to the producer as `Message timed out` (broker unavailable longer than the 30 s delivery timeout) and **not** persisted; all acknowledged persisted **55.6 s** after the broker restarted, lag 0 after **56.0 s**. Run 2 (final code): **1,750 / 1,750 acknowledged and persisted exactly once**, 0 delivery failures; persisted **48.9 s**, lag 0 **49.2 s** after restart |
| Kafka stopped ≥ 20 s (existing test) | API answers 503 naming Kafka (no local fallback); new events exactly once after restart | PASS |
| Spark crash (SIGKILL) / graceful stop (existing tests) | restart policy, checkpoint recovery, exactly once, NOT RUNNING shown while stopped, never a silent fallback | PASS |
| **Spark backlog**: stopped while 3,000 events arrive | nothing processed meanwhile; drained from the checkpoint exactly once; lag 0 | run 1: first persisted **37.0 s** after start (job start-up), all **77.9 s**, drain ≈ **73 ev/s**; run 2: **28.5 s / 53.2 s / 122 ev/s** |
| PostgreSQL stopped (existing test) | consumer blocks and retries (no crash loop), API 503 `database=down`, all 200 events persisted exactly once after restart | PASS |
| **Redis stopped** | Redis shown `down`, processor `UNKNOWN` (not a false NOT RUNNING), login works (in-process limiter), **persistence continues**, consumer/Spark keep running; after restart processor and live feed recover | before the fix: every API request **+4 s** (`/system/status` 12 s), persistence **21 of 60 events in 90 s**; after: requests at baseline after one tripping call, **200 events persisted during the outage in 20.7 s / 13.6 s** (two runs); Spark reported **8.6 s / 7.2 s** and live feed delivering **20.8 s / 16.3 s** after Redis restarted |
| Persistence-consumer crash, API restart / recreate, nginx re-resolution (existing tests) | — | PASS |
| **Full stack** stop + `up -d` | DB rows, Spark checkpoint, artifacts survive; API, routes, live feed, processing recover | run 1 / run 2: stop **6.5 / 4.7 s**; all 7 services healthy **45.1 / 22.0 s** after `up`; login **46.8 / 22.4 s**; Spark reported **56.0 / 25.4 s**; first new event persisted and seen on the live feed **77.3 / 40.7 s**; transactions/alerts/investigations/reports identical (run 2: 196,184 / 48,431 / 25 / 80), audit +1 (the login); Spark score commit advanced (628 → 629); artifact SHA-256 identical |
| Restore from backup | — | see §7: 48 s to healthy on restored data, 10/10 verification checks |

Container suite on the isolated stack: **25 tests**; final full run **25 passed** (13 min 25 s); first full run 23 passed, 2 failed — the Redis-outage test (a real
defect, fixed: circuit breaker) and the artifact test (it compared against the repository directory while the
isolated stack mounts the *restored* copy; the test now compares against the directory actually mounted, assertion
unchanged). Both pass after the fixes; the final full run is listed in DEVELOPMENT_STATUS.md.

## 7. Backup and restore

See `docs/phase12/BACKUP_AND_RESTORE.md`: live backup 9.6 s (PostgreSQL dump 3.2 s), restore + verification of
counts, content fingerprints (16 tables), artifacts/datasets SHA-256, Kafka offsets and Spark checkpoints all
identical; application healthy 48 s after start on the restored data; `verify_restore.py` 10/10.

## 8. Remaining reliability limitations

- Single host; single Kafka broker (RF=1), single PostgreSQL, single Redis, Spark in local mode: **no HA**; Docker
  restart policies give process-level recovery only.
- Sustained capacity ≈ 100–105 ev/s; beyond it the backlog grows (no autoscaling, no backpressure to producers
  beyond Kafka buffering).
- A Spark crash shorter than the 30 s heartbeat window may never show as NOT RUNNING (Phase 10 limitation, kept).
- During a Redis outage the live feed (SSE) and processor status are unavailable (reported as UNKNOWN), rate limits
  fall back to per-process counters.
- Backups are local directories; no off-site copy, encryption or schedule. Kafka/Spark state is consistent only when
  backed up stopped.
- No data retention for transactions/predictions/audit; disk grows ≈ 12 MB per 1,000 events (*estimated*).
- *Untested:* multi-day soak, disk-full behaviour, host reboot during writes (power loss), PostgreSQL major upgrade.
