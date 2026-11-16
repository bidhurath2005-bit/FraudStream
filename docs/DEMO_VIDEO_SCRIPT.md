# FraudStream: two-minute demo video script

A recording plan for a short portfolio video (≈ 2:00). Every screen action uses a real page of the running
application in the **distributed profile**. For a longer live walkthrough with expected observations and fallback
steps, see [DEMO_GUIDE.md](DEMO_GUIDE.md).

> **Status:** this document is a script and storyboard. No video has been recorded yet.

---

## 1. Before recording (≈ 10 minutes, off camera)

**Stack**

```powershell
docker compose --env-file .env --profile distributed up -d
docker compose --env-file .env --profile distributed ps      # wait until every long-running service is (healthy)
Invoke-RestMethod http://127.0.0.1:8080/api/v1/health        # status ok, stream_processor "Spark Structured Streaming"
```

- Spark can take up to 2 minutes after a cold start. Start early.
- **Warm-up:** in Simulation Lab, run a `mixed` simulation for about 2 minutes so the Overview charts, Risk
  Analytics and the map are not empty. Then stop it.
- **Prepare one investigation take in advance** (see 0:55 below). The full investigation workflow takes longer than
  20 seconds on screen, so either record it separately and cut it in, or speed that segment up in editing.

**Screen**

- Browser window 1440 × 900 or larger, zoom 100 %, at `http://127.0.0.1:8080` (not `localhost`).
- Log in as the **analyst** before you start recording.
- Close other tabs, hide bookmarks, and turn off notifications.
- **Never show `.env`** or a terminal that prints secrets. Do not type passwords on camera.
- Open a second tab with the GitHub README scrolled to the architecture diagram (§4) for the closing shot.

**Recording tools (any of these)**

- OBS Studio (free): one Display or Window Capture source, 1920 × 1080, 30 fps, MP4.
- Windows Xbox Game Bar: `Win + Alt + R` starts and stops recording of the active window.
- Record the narration separately, or with a headset microphone, and keep the video under 2:10.

---

## 2. Timestamped script

The narration is about 290 words, which takes roughly 2 minutes at a calm pace. **Screen** is what to show;
**Say** is the voice-over.

### 0:00 – 0:12 · Opening introduction

**Screen:** Intelligence Overview. Hold still for 2 s, then move the cursor over the `FULL DISTRIBUTED MODE` and
`processor: Spark Structured Streaming` badges in the header.

**Say:**
> "This is FraudStream, a real-time fraud-detection platform I built on synthetic payment data. Transactions stream
> through Kafka, Spark scores them with machine learning and a rule engine, and analysts investigate the alerts here."

### 0:12 – 0:27 · Transaction generation

**Screen:** Click **Simulation Lab**.
1. Traffic generator: mode `mixed`, **10 tps**, duration **120 s** → **Start**. The run status starts counting events.
2. Inject a suspicious scenario: choose `account_takeover` → **Inject**.

**Say:**
> "The simulator generates customers, merchants and seven labelled fraud scenarios. I'll start normal traffic and
> inject an account takeover: a new device, a new location and unusual spending. The API produces these events
> straight to Kafka."

### 0:27 – 0:42 · Live streaming

**Screen:** Click **Live Transaction Stream** and let the rows arrive for about 5 s. The injected `account_takeover`
events appear as HIGH. Then click **Streaming Infrastructure** and point the cursor at the Spark query table (`score`,
`validated-dedup` and the window queries) and the consumer lag ≈ 0.

**Say:**
> "Scored events arrive in about three seconds. Spark Structured Streaming validates and deduplicates each event,
> builds per-customer features in checkpointed state, runs inference and writes the result back to Kafka. An
> idempotent consumer then stores it in PostgreSQL, so even a replayed record never becomes a duplicate row."

### 0:42 – 0:55 · Risk scoring and alert creation

**Screen:** Click **Investigation Center**, filter to **HIGH** and **NEW**, and open one of the `account_takeover`
alerts. Point at the *Decision & explanation* panel, then the **top feature contributions** bars.

**Say:**
> "Medium and high-risk events become alerts. Each score combines an XGBoost model, an Isolation Forest anomaly
> detector and ten explainable rules into a 0-to-100 score, with the model's actual feature contributions shown here."

### 0:55 – 1:15 · Analyst investigation

**Screen** (jump cut or speed up 2×):
1. Click **Start review** and add a short note, for example "New device and location; checking history."
2. Back in the Investigation Center, tick the alert → **Open investigation**.
3. In the investigation: **IN_PROGRESS** → **RESOLVED** with classification `CONFIRMED_SUSPICIOUS` and findings →
   **CLOSED**.
4. Hold on the closed case with its audit trail for 2 s.

**Say:**
> "An analyst reviews the alert, opens an investigation, records findings and closes the case. Every transition is
> enforced by the server and written to an audit trail."

### 1:15 – 1:35 · Dashboard analytics

**Screen:** Spend about 6 s on each page:
1. **Risk Analytics:** the scenario table and the *risk engine vs simulator labels* confusion matrix.
2. **Fraud Intelligence Map:** hover one city.
3. **Model Intelligence:** the registry and the *evaluation by split* table.

**Say:**
> "Risk Analytics compares what the engine flagged with the simulator's labels. The map shows simulated city
> centroids. Model Intelligence tracks versions, evaluation splits, live latency and drift. These metrics are measured
> on synthetic data, so they show the pipeline works, not real-world fraud performance."

### 1:35 – 2:00 · Closing architecture summary

**Screen:** Switch to the GitHub README tab, showing the architecture diagram. Slowly scroll from the simulator through
Kafka, Spark, PostgreSQL and Redis to the dashboard. End on the repository title.

**Say:**
> "Everything runs in Docker on one machine. Kafka, Spark in local mode, PostgreSQL, Redis, FastAPI and React. On my
> laptop it sustains about a hundred events per second, with zero lost or duplicated rows through broker, Spark and
> database outages. The limits are documented: a single host, no TLS, and synthetic data. The code and documentation
> are on GitHub. Thanks for watching."

---

## 3. Storyboard summary

| Time | Page / view | Key action | On-screen focus |
|---|---|---|---|
| 0:00 | Intelligence Overview | hold, hover header badges | `FULL DISTRIBUTED MODE`, Spark processor |
| 0:12 | Simulation Lab | Start `mixed` 10 tps; inject `account_takeover` | run status counter |
| 0:27 | Live Transaction Stream → Streaming Infrastructure | watch rows arrive; point at Spark queries and lag | HIGH rows; consumer lag ≈ 0 |
| 0:42 | Investigation Center → Alert detail | filter HIGH/NEW, open alert | explanation + feature contributions |
| 0:55 | Alert → Investigation | review, note, open, resolve, close | closed case + audit trail |
| 1:15 | Risk Analytics → Map → Model Intelligence | scroll and hover | confusion matrix, map, evaluation table |
| 1:35 | GitHub README | scroll the architecture diagram | the end-to-end flow |

## 4. Accuracy checklist for the narration

Keep the video consistent with the documented facts:

- Say **synthetic data**. Do not describe the model metrics as real-world detection performance.
- Say Spark runs in **local mode in one container**. Do not call it a cluster.
- "About three seconds" refers to the measured p50 / p95 of 2.5 / 3.4 s at 10 events/s. "About a hundred events per
  second" is the measured sustainable rate on one development machine, not production capacity.
- Don't say "exactly once" without qualification. Spark's Kafka output is **at-least-once**, so a replay can put a
  scored record on a topic twice. The idempotent persistence consumer (offsets committed only after the DB commit,
  plus an `event_id` check and unique constraints) keeps **one row per event in PostgreSQL**. That is effectively-once
  at the database, not end-to-end exactly-once.
- `BLOCK_SIMULATION` blocks nothing real.

Sources: [README](../README.md), [STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md),
[DATA_AND_ML.md](DATA_AND_ML.md), [phase12/RELIABILITY_REPORT.md](phase12/RELIABILITY_REPORT.md).

## 5. After recording

- Stop the simulation if it is still running, and stop the stack with
  `docker compose --env-file .env --profile distributed stop` (**not** `down -v`).
- The demo adds synthetic transactions, alerts and an investigation to the database. That is normal application data.
- Before publishing the video, watch it once and check that no secrets, `.env` contents, or personal browser data are
  visible.
