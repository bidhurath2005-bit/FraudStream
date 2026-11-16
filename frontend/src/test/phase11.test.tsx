/** Streaming Infrastructure + Model Intelligence pages. Fixtures mirror real API responses
 * captured from the running distributed stack (trimmed). */
import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { App } from "../App";
import { ANALYST, mockApi, renderApp, type Route } from "./utils";

afterEach(() => vi.unstubAllGlobals());

const ADMIN = { ...ANALYST, id: 1, username: "admin", role: "ADMIN" };

const q = (name: string, extra: object = {}) => ({
  name, id: `id-${name}`, run_id: `run-${name}`, active: true, status: "Waiting for next trigger", batch_id: 144, num_input_rows: 12,
  input_rows_per_second: 2.4, processed_rows_per_second: 30.5, trigger_ms: 420, timestamp: "2026-11-14T17:51:32.000Z",
  duration_ms: { addBatch: 380, latestOffset: 5, triggerExecution: 420 }, source_end_offsets: {}, state_rows: 7518,
  rows_dropped_by_watermark: 0, event_time_watermark: null, exception: null, lag_vs_raw_topic: 0,
  checkpoint: { location: `/checkpoints/v2/${name}`, exists: true, last_committed_batch: 143, last_planned_batch: 144, uncommitted_batches: 1,
    last_commit_at: 1794678690, last_commit_age_seconds: 4.2, has_state: true },
  ...extra,
});

const INFRA = {
  profile: "distributed", checked_at: "2026-11-14T17:51:40Z",
  database: { status: "UP", dialect: "postgresql", latency_ms: 0.28 }, redis: { status: "UP" },
  dead_letters_db: 2,
  recent_dead_letters: [{ id: 7, source: "spark-validation", reason: "SCHEMA_INVALID: amount", event_id: "ev-1", created_at: "2026-11-14T17:50:00Z" }],
  kafka: {
    status: "UP", brokers: 1, dlq_messages: 3,
    topics: [
      { name: "transactions.raw", exists: true, partitions: 6, end_offset_total: 18185, messages_retained: 18185, rate_per_sec_since_last_check: null, description: "Raw events" },
      { name: "fraud.dlq", exists: true, partitions: 1, end_offset_total: 3, messages_retained: 3, rate_per_sec_since_last_check: 0.5, description: "Dead letters" },
    ],
    consumer_groups: [
      { group: "fraudstream-processor", state: "EMPTY", members: 0, active: false, lag_by_topic: { "transactions.raw": 13205 }, total_lag: 13205, committed_partitions: 6 },
      { group: "fraudstream-persistence", state: "STABLE", members: 1, active: true, lag_by_topic: { "transactions.normal": 0, "transactions.suspicious": 4 }, total_lag: 4, committed_partitions: 12 },
    ],
  },
  spark: {
    status: "RUNNING", processor: "spark-structured-streaming", spark_version: "4.0.5", master: "local[8]", app_id: "local-1794678285301",
    checkpoint_root: "/checkpoints/v2", reported_at: 1794678695.69, age_seconds: 4.6,
    queries: [q("score", { lag_vs_raw_topic: 17 }), q("validated-dedup"), q("window-tumbling_1m", { rows_dropped_by_watermark: 5 })],
  },
  processor: { status: "NO_HEARTBEAT", unavailable_reason: "process has not reported" },
  consumer: { status: "UP", age_seconds: 0.5, group: "fraudstream-persistence", last_error: null,
    stats: { consumed: 100, created: 100, duplicates: 0, alerts: 5, dead_lettered: 0, db_retries: 0, commits: 2 } },
  stream_processor: "Spark Structured Streaming",
};
const OVERVIEW = {
  profile: "distributed", total_transactions: 9363, transactions_last_minute: 120, tps_last_minute: 2, risk_categories: {},
  suspicious_transactions: 0, alerts_total: 0, high_risk_open_alerts: 0,
  detection_latency_ms: { avg: 2900, p50: 2750, p95: 3870, p99: 4600, samples: 900, definition: "" },
  detection_rate: { value: null, numerator: 0, denominator: 0, definition: "", false_alert_rate: null, normal_labelled: 0 },
  model: null, model_error: null, simulation: "IDLE",
};
const streamRoutes = (infra: object = INFRA) => [
  { path: "/api/v1/infrastructure/status", body: infra },
  { path: "/api/v1/analytics/overview", body: OVERVIEW },
  { path: /analytics\/timeline/, body: { bucket_seconds: 30, points: [] } },
];

describe("Streaming Infrastructure page", () => {
  it("shows real Kafka, Spark, consumer-lag, checkpoint and DLQ values and honest n/a", async () => {
    mockApi(streamRoutes());
    renderApp(<App />, "/streaming");
    expect(await screen.findByRole("heading", { name: "Streaming Infrastructure" })).toBeInTheDocument();
    expect(await screen.findByTestId("health-spark")).toHaveTextContent("RUNNING");
    expect(screen.getByTestId("health-kafka")).toHaveTextContent("UP");
    expect(screen.getByTestId("health-processor")).toHaveTextContent("NOT USED");
    expect(screen.getByTestId("active-processor")).toHaveTextContent("Spark Structured Streaming");
    expect(screen.getByTestId("stat-spark-lag")).toHaveTextContent("17");
    expect(screen.getByTestId("stat-persist-lag")).toHaveTextContent("4");
    expect(screen.getByTestId("stat-e2e-p95")).toHaveTextContent("3870 ms");
    expect(screen.getByTestId("stat-dlq-kafka")).toHaveTextContent("3");
    expect(screen.getByTestId("stat-dlq-db")).toHaveTextContent("2");
    expect(screen.getByText("SCHEMA_INVALID: amount")).toBeInTheDocument();
    // a rate that needs two checks is shown as n/a, never as 0
    const topics = screen.getByTestId("kafka-topics");
    expect(within(within(topics).getByText("transactions.raw").closest("tr")!).getByText("n/a")).toBeInTheDocument();
    expect(within(topics).getByText("0.50")).toBeInTheDocument();
    // inactive fallback group is explained, not counted as lag
    expect(within(screen.getByTestId("consumer-groups")).getByText(/Inactive group/)).toBeInTheDocument();
    // checkpoint column
    expect(within(screen.getByTestId("query-row-score")).getByText(/#143/)).toBeInTheDocument();
  });

  it("filters queries, expands a query and refreshes on demand", async () => {
    const calls = mockApi(streamRoutes());
    renderApp(<App />, "/streaming");
    const table = await screen.findByTestId("spark-queries");
    expect(within(table).getAllByRole("row")).toHaveLength(4);
    await userEvent.selectOptions(screen.getByTestId("query-filter"), "window");
    expect(within(table).queryByText("score")).not.toBeInTheDocument();
    expect(within(table).getByText("window-tumbling_1m")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByTestId("query-filter"), "problem");
    expect(screen.getByText("No query matches this filter")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByTestId("query-filter"), "all");
    await userEvent.click(within(screen.getByTestId("spark-queries")).getByLabelText("Details for score"));
    const detail = await screen.findByTestId("query-detail-score");
    expect(within(detail).getByText("/checkpoints/v2/score")).toBeInTheDocument();
    expect(within(detail).getByText("addBatch")).toBeInTheDocument();
    const before = calls.filter((c) => c.url.includes("/infrastructure/status")).length;
    await userEvent.selectOptions(screen.getByTestId("refresh-interval"), "0");
    await userEvent.click(screen.getByTestId("refresh-now"));
    await waitFor(() => expect(calls.filter((c) => c.url.includes("/infrastructure/status")).length).toBeGreaterThan(before));
  });

  it("reports Spark NOT_REPORTING honestly", async () => {
    mockApi(streamRoutes({ ...INFRA, spark: { status: "NOT_REPORTING", unavailable_reason: "Spark job has not published progress (is it running?)" },
      stream_processor: "NOT RUNNING (no processor heartbeat)" }));
    renderApp(<App />, "/streaming");
    expect(await screen.findByTestId("health-spark")).toHaveTextContent("NOT REPORTING");
    expect(screen.getByTestId("active-processor")).toHaveTextContent("NOT RUNNING");
    expect(screen.getByText("Spark: NOT REPORTING")).toBeInTheDocument();
    expect(within(screen.getByTestId("stat-spark-input")).getByText("n/a")).toBeInTheDocument();
  });

  it("explains the local profile instead of showing Kafka/Spark tables", async () => {
    mockApi(streamRoutes({
      profile: "local", checked_at: "2026-11-14T17:51:40Z", database: { status: "UP", dialect: "sqlite", latency_ms: 0.1 },
      redis: { status: "NOT_CONFIGURED", note: "not used in the local profile" }, dead_letters_db: 0, recent_dead_letters: [],
      pipeline: { mode: "LOCAL in-process pipeline", counters: { processed: 42 }, feature_store_customers: 30, warm_started_events: 0 },
      kafka: { status: "NOT_USED", note: "Kafka is only used in the DISTRIBUTED profile" },
      spark: { status: "NOT_USED", note: "Spark is only used in the DISTRIBUTED profile" },
    }));
    renderApp(<App />, "/streaming");
    expect(await screen.findByText(/Kafka and Spark are not used/)).toBeInTheDocument();
    expect(screen.getByTestId("health-kafka")).toHaveTextContent("NOT USED");
    expect(screen.queryByTestId("kafka-topics")).not.toBeInTheDocument();
    expect(screen.getByText("No dead letters recorded")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("shows an error state with retry when the status endpoint fails", async () => {
    mockApi([{ path: "/api/v1/infrastructure/status", status: 500, body: { detail: "boom" } }, ...streamRoutes().slice(1)]);
    renderApp(<App />, "/streaming");
    expect(await screen.findByText("boom")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});

/* ------------------------------------------------------------------ models */
const split = (pr: number) => ({ threshold: 0.84, support: 24155, positives: 1211, precision: pr - 0.005, recall: pr - 0.007, f1: pr - 0.006,
  roc_auc: 0.9999, pr_auc: pr, false_positive_rate: 0.0003, confusion_matrix: { tn: 22937, fp: 7, fn: 9, tp: 1202 } });
const model = (id: number, name: string, kind: string, status: string, pr: number) => ({
  id, model_name: name, version: "v20261114T124627Z", kind, algorithm: name, status, feature_config_version: "fc-1.0",
  hyperparameters: { max_depth: 5 }, metrics: { validation: split(pr - 0.001), test: split(pr), unseen_customer_test: split(pr - 0.002) },
  inference_latency: { p50_ms: 8.31, p95_ms: 11.2, samples: 200 }, threshold: 0.8471, artifact_path: `models/${name}/v/model.joblib`,
  trained_at: "2026-11-14T12:47:22Z", activated_at: status === "ACTIVE" ? "2026-11-14T14:07:36Z" : null, experiment_id: 1,
  dataset: { name: "synthetic-fraudstream", version: "1.0", source: "synthetic", rows: 139517 },
});
const XGB = model(4, "xgboost", "supervised", "ACTIVE", 0.9996);
const LR = model(2, "logistic_regression", "supervised", "CANDIDATE", 0.9765);
const IF = model(1, "isolation_forest", "anomaly", "ACTIVE", 0.3204);
const ACTIVE = (writable: boolean) => ({ active: { supervised: XGB, anomaly: IF }, loaded_in_api: { supervised: "xgboost:v20261114T124627Z", anomaly: "isolation_forest:v20261114T124627Z" },
  model_error: null, feature_config_version: "fc-1.0", artifacts_writable: writable });
const cmpRow = (m: ReturnType<typeof model>) => ({ id: m.id, model_name: m.model_name, version: m.version, kind: m.kind, status: m.status, threshold: m.threshold,
  validation_pr_auc: m.metrics.validation.pr_auc, test: m.metrics.test, unseen_customer_test: m.metrics.unseen_customer_test, latency: m.inference_latency, dataset: "synthetic-fraudstream" });
const EXP = [{ id: 1, experiment_version: "v20261114T124627Z", status: "COMPLETED", triggered_by: "cli", started_at: "2026-11-14T12:46:27Z", finished_at: "2026-11-14T12:47:39Z",
  recommended: "xgboost", dataset: "synthetic-fraudstream", duration_seconds: 71.99, split: null }];
const modelRoutes = (writable = false, extra: Route[] = []): Route[] => [
  ...extra,
  { path: "/api/v1/models/active", body: ACTIVE(writable) },
  { path: "/api/v1/models", body: [XGB, LR, IF] },
  { path: "/api/v1/models/4", body: { ...XGB, live: { predictions: 9000, avg_inference_ms: 9.1 } } },
  { path: "/api/v1/models/2", body: { ...LR, live: { predictions: 0, avg_inference_ms: null } } },
  { path: "/api/v1/models/live-inference", body: { window_rows: 5000, total_predictions: 9363, definition: "Prediction.inference_ms",
    models: [{ model_version: "xgboost:v20261114T124627Z", samples: 5000, mean_ms: 9.4, p50_ms: 8.9, p95_ms: 14.2, p99_ms: 20.1, max_ms: 41, first_at: "2026-11-14T17:00:00Z", last_at: "2026-11-14T17:50:00Z" }] } },
  { path: "/api/v1/models/experiments", body: EXP },
  { path: "/api/v1/models/compare", body: { models: [cmpRow(IF), cmpRow(LR), cmpRow(XGB)], evaluation_note: "Synthetic-data results are optimistic." } },
];

describe("Model Intelligence page", () => {
  it("shows active models, read-only mode, registry with live latency and model detail (analyst: no admin actions)", async () => {
    mockApi(modelRoutes());
    renderApp(<App />, "/models");
    expect(await screen.findByRole("heading", { name: "Model Intelligence" })).toBeInTheDocument();
    const sup = await screen.findByTestId("active-supervised");
    expect(sup).toHaveTextContent("xgboost");
    expect(sup).toHaveTextContent("0.9996");
    expect(sup).toHaveTextContent("loaded in API");
    expect(screen.getByTestId("artifacts-mode")).toHaveTextContent("READ-ONLY");
    const row = await screen.findByTestId("model-row-xgboost");
    expect(row).toHaveTextContent("14 ms");          // live p95 (fmt.ms rounds ≥ 10 ms) from persisted predictions
    expect(within(screen.getByTestId("model-row-logistic_regression")).getByText("n/a")).toBeInTheDocument(); // no live predictions
    expect(screen.getByTestId("live-inference")).toHaveTextContent("5,000");
    await userEvent.click(row);
    const detail = await screen.findByTestId("model-detail");
    expect(within(detail).getByTestId("confusion-matrix")).toHaveTextContent("1,202");
    expect(within(detail).getByText("max_depth=5")).toBeInTheDocument();
    expect(screen.queryByTestId("activate-model")).not.toBeInTheDocument();
    await userEvent.click(screen.getByLabelText("Close model detail"));
    expect(screen.queryByTestId("model-detail")).not.toBeInTheDocument();
  });

  it("kind filter queries the backend", async () => {
    const calls = mockApi(modelRoutes());
    renderApp(<App />, "/models");
    await screen.findByTestId("models-table");
    await userEvent.selectOptions(screen.getByTestId("kind-filter"), "anomaly");
    await waitFor(() => expect(calls.some((c) => c.url.endsWith("/models?kind=anomaly"))).toBe(true));
  });

  it("disables activation for admins while artifacts are read-only", async () => {
    mockApi(modelRoutes(false), ADMIN);
    renderApp(<App />, "/models");
    await userEvent.click(await screen.findByTestId("model-row-logistic_regression"));
    expect(await screen.findByTestId("activate-model")).toBeDisabled();
    expect(screen.getByText(/Disabled: artifacts are read-only/)).toBeInTheDocument();
  });

  it("activates a version with an audited reason when artifacts are writable", async () => {
    const calls = mockApi(modelRoutes(true, [{ method: "POST", path: "/api/v1/models/2/activate", body: { activated: LR, previous: XGB } }]), ADMIN);
    renderApp(<App />, "/models");
    await userEvent.click(await screen.findByTestId("model-row-logistic_regression"));
    await userEvent.click(await screen.findByTestId("activate-model"));
    const confirm = screen.getByTestId("confirm-model-change");
    expect(confirm).toBeDisabled();
    await userEvent.type(screen.getByTestId("model-change-reason"), "shadow comparison");
    await userEvent.click(confirm);
    await waitFor(() => expect(calls.find((c) => c.method === "POST" && c.url.endsWith("/models/2/activate"))?.body).toEqual({ reason: "shadow comparison" }));
  });

  it("compares models from stored experiment results; toggles and split change work", async () => {
    mockApi(modelRoutes());
    renderApp(<App />, "/models?tab=compare");
    const table = await screen.findByTestId("compare-table");
    expect(within(table).getAllByRole("row")).toHaveLength(4);
    expect(within(table).getByText("recommended")).toBeInTheDocument();
    expect(within(table).getAllByText("0.9996").length).toBeGreaterThan(0);
    await userEvent.click(screen.getByTestId("compare-toggle-isolation_forest"));
    expect(within(screen.getByTestId("compare-table")).getAllByRole("row")).toHaveLength(3);
    await userEvent.selectOptions(screen.getByTestId("compare-split"), "unseen_customer_test");
    expect(within(screen.getByTestId("compare-table")).getAllByText("0.9976").length).toBeGreaterThan(0); // unseen PR-AUC of xgboost
    await userEvent.click(screen.getByTestId("compare-toggle-xgboost"));
    await userEvent.click(screen.getByTestId("compare-toggle-logistic_regression"));
    expect(screen.getByText("Select at least one model")).toBeInTheDocument();
  });

  it("drift: empty state, then a stored report with PSI per feature and a feature filter", async () => {
    const report = { rows: 2000, status: "SIGNIFICANT", max_psi: 8.29, thresholds: { moderate: 0.1, significant: 0.25 },
      features: { hour_of_day: { psi: 8.29, status: "SIGNIFICANT" }, tx_count_5m: { psi: 0.13, status: "MODERATE" }, amount: { psi: 0.017, status: "STABLE" } },
      score_psi: { psi: 0.746, status: "SIGNIFICANT" }, drifted_features: ["hour_of_day", "tx_count_5m"], model_version: "xgboost:v20261114T124627Z",
      labelled_performance: { rows: 2000, positives: 144, precision: 0.9638, recall: 0.9236, reference_test_precision: 0.9942, reference_test_recall: 0.9926, label_source: "simulator ground truth (synthetic)" },
      interpretation: "PSI indicates DATA drift" };
    let stored = false;
    const calls = mockApi([
      ...modelRoutes(),
      { path: "/api/v1/drift/history", get body() { return stored ? [{ id: 9, model_version: "xgboost:v20261114T124627Z", computed_at: "2026-11-14T18:00:00Z", rows: 2000, status: "SIGNIFICANT", max_psi: 8.29, drifted_features: ["hour_of_day"], score_psi: 0.746 }] : []; } },
      { path: "/api/v1/drift/current", get body() { stored = true; return report; } },
      { path: "/api/v1/drift/reports/9", body: { id: 9, computed_at: "2026-11-14T18:00:00Z", model_version: "xgboost:v20261114T124627Z", rows: 2000, status: "SIGNIFICANT", max_psi: 8.29, report } },
    ]);
    renderApp(<App />, "/models?tab=drift");
    expect(await screen.findByText("No stored drift reports yet")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("run-drift"));
    await waitFor(() => expect(calls.some((c) => c.url.endsWith("/drift/current?persist=true"))).toBe(true));
    const rep = await screen.findByTestId("drift-report");
    expect(rep).toHaveTextContent("0.7460 (SIGNIFICANT)");
    expect(rep).toHaveTextContent("0.9638 vs 0.9942");
    expect(within(screen.getByTestId("drift-history")).getByText("8.290")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByTestId("drift-feature-filter"), "STABLE");
    await waitFor(() => expect(screen.queryByText("No feature matches this filter")).not.toBeInTheDocument());
  });

  it("drift: insufficient data is reported and nothing is claimed", async () => {
    mockApi([...modelRoutes(), { path: "/api/v1/drift/history", body: [] },
      { path: "/api/v1/drift/current", body: { status: "INSUFFICIENT_DATA", rows: 12, min_rows: 200, features: {} } }]);
    renderApp(<App />, "/models?tab=drift");
    await userEvent.click(await screen.findByTestId("run-drift"));
    expect(await screen.findByTestId("drift-insufficient")).toHaveTextContent("12 of the 200 required");
  });

  it("experiments, datasets and artifact files with registry consistency and filter", async () => {
    mockApi([...modelRoutes(),
      { path: "/api/v1/models/experiments/1", body: { ...EXP[0], summary: {
        dataset: { name: "synthetic-fraudstream", version: "1.0", source: "synthetic", provenance: "Generated locally", rows: 139517, positives: 7064, positive_rate: 0.0506, content_sha256: "b57801b3b941b43fa897e9115ad3587803c1974321f1357540f190652d5ba3e9" },
        split: { strategy: "temporal (event_time ordered) + customer holdout", sizes: { train: 72034, test: 24155 }, positives: { train: 3678, test: 1211 } },
        recommended_supervised: "xgboost", selection_rule: "highest validation PR-AUC among supervised models (test set not used)" },
        model_versions: [{ id: 4, model_name: "xgboost", kind: "supervised", status: "ACTIVE" }] } },
      { path: "/api/v1/datasets", body: { datasets: [{ id: 1, name: "synthetic-fraudstream", version: "1.0", source: "synthetic", rows: 139517, positive_rate: 0.0506, provenance: "Generated locally", created_at: "2026-11-14T14:07:36Z", content_sha256: "b57801b3b941" }] } },
      { path: "/api/v1/models/artifacts", body: { artifacts_dir_writable: false, exists: true, note: "registry.json …",
        registry_active: { supervised: "xgboost:v20261114T124627Z", anomaly: "isolation_forest:v20261114T124627Z" },
        database_active: { supervised: "xgboost:v20261114T124627Z", anomaly: "isolation_forest:v20261114T124627Z" },
        files: [{ path: "registry.json", bytes: 1606, modified_at: "2026-11-14T18:17:00Z", sha256: "a".repeat(64) },
          { path: "models/xgboost/v20261114T124627Z/model.joblib", bytes: 812345, modified_at: "2026-11-14T18:17:00Z", sha256: "b".repeat(64) }] } },
    ]);
    renderApp(<App />, "/models?tab=artifacts");
    const exp = await screen.findByTestId("experiment-detail");
    expect(exp).toHaveTextContent("139,517");
    expect(exp).toHaveTextContent("72,034 rows · 3,678 positive");
    expect(await screen.findByTestId("datasets-table")).toHaveTextContent("5.06%");
    expect(await screen.findByTestId("registry-consistency")).toHaveTextContent("registry.json matches the database");
    expect(screen.getByTestId("artifacts-writable")).toHaveTextContent("read-only");
    expect(screen.getByText("793.3 KB")).toBeInTheDocument();
    await userEvent.type(screen.getByTestId("artifact-filter"), "joblib");
    expect(within(screen.getByTestId("artifacts-table")).queryByText("registry.json")).not.toBeInTheDocument();
  });

  it("tab selection is reflected in the URL and survives reload (deep link)", async () => {
    mockApi(modelRoutes());
    renderApp(<App />, "/models?tab=compare");
    expect(await screen.findByTestId("tab-compare")).toHaveAttribute("aria-selected", "true");
    await userEvent.click(screen.getByTestId("tab-registry"));
    expect(screen.getByTestId("tab-registry")).toHaveAttribute("aria-selected", "true");
    expect(await screen.findByTestId("models-table")).toBeInTheDocument();
  });
});
