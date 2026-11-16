export type Role = "ADMIN" | "ANALYST";
export type RiskCategory = "LOW" | "MEDIUM" | "HIGH";
export type AlertStatus = "NEW" | "UNDER_REVIEW" | "CONFIRMED_SUSPICIOUS" | "DISMISSED" | "CLOSED";
export type InvestigationStatus = "OPEN" | "IN_PROGRESS" | "RESOLVED" | "CLOSED";

export interface User {
  id: number;
  username: string;
  full_name: string;
  role: Role;
  is_active: boolean;
  created_at: string;
  last_login_at: string | null;
}

export interface Page<T> {
  items: T[];
  total: number;
  page: number;
  size: number;
}

export interface TriggeredRule {
  code: string;
  name: string;
  family: string;
  weight: number;
  detail: string;
}

export interface Transaction {
  id: number;
  event_id: string;
  transaction_id: string;
  customer_id: string;
  merchant_id: string;
  amount: number;
  currency: string;
  event_time: string;
  processed_at: string;
  transaction_type: string;
  merchant_category: string;
  channel: string;
  device_id: string;
  city: string;
  country: string;
  status: string;
  synthetic: boolean;
  pipeline_latency_ms: number | null;
  sim_is_suspicious: boolean | null;
  sim_scenario: string | null;
  risk_score?: number;
  risk_category?: RiskCategory;
  detection_method?: string;
  recommended_action?: string;
  model_score?: number | null;
}

export interface Prediction {
  model_version: string | null;
  anomaly_version: string | null;
  model_score: number | null;
  model_threshold: number | null;
  anomaly_score: number | null;
  risk_score: number;
  risk_category: RiskCategory;
  recommended_action: string;
  detection_method: string;
  triggered_rules: TriggeredRule[];
  components: Record<string, number>;
  features: Record<string, number | string>;
  contributions: { feature: string; contribution: number }[];
  explanation_method: string | null;
  explanation: string;
  inference_ms: number | null;
  predicted_at: string;
}

export interface AuditEntry {
  id: number;
  actor: string;
  action: string;
  entity_type: string;
  entity_id: string;
  field: string | null;
  old_value: string | null;
  new_value: string | null;
  details: Record<string, unknown>;
  created_at: string;
}

export interface Note {
  id: number;
  body: string;
  author: string;
  created_at: string;
}

export interface Alert {
  alert_uid: string;
  id: number;
  transaction_pk: number;
  transaction_id: string;
  customer_id: string;
  amount: number;
  currency: string;
  merchant_category: string;
  event_time: string;
  risk_score: number;
  risk_category: RiskCategory;
  detection_method: string;
  triggered_rules: TriggeredRule[];
  model_version: string | null;
  explanation: string;
  status: AlertStatus;
  assigned_to: User | null;
  investigation_id: number | null;
  created_at: string;
  updated_at: string;
}

export interface AlertDetail extends Alert {
  transaction: Transaction;
  prediction: Prediction | null;
  notes: Note[];
  audit: AuditEntry[];
  investigation: Investigation | null;
}

export interface Investigation {
  case_uid: string;
  id: number;
  title: string;
  customer_id: string;
  status: InvestigationStatus;
  priority: string;
  opened_by: string;
  assigned_to: User | null;
  final_classification: string | null;
  findings: string | null;
  created_at: string;
  updated_at: string;
  closed_at: string | null;
  alert_count: number;
  alerts?: Alert[];
}

export interface InvestigationDetail extends Investigation {
  alerts: Alert[];
  notes: Note[];
  audit: AuditEntry[];
  customer_transactions: Transaction[];
  related_alerts: Alert[];
}

export interface LiveTransaction {
  type: "transaction";
  id: number;
  event_id: string;
  transaction_id: string;
  customer_id: string;
  merchant_id: string;
  merchant_name: string;
  merchant_category: string;
  amount: number;
  currency: string;
  event_time: string;
  processed_at: string;
  status: string;
  channel: string;
  city: string;
  country: string;
  risk_score: number;
  risk_category: RiskCategory;
  detection_method: string;
  recommended_action: string;
  model_version: string | null;
  alert_uid: string | null;
  latency_ms: number | null;
  model_error: string | null;
}

export interface Overview {
  profile: "local" | "distributed";
  total_transactions: number;
  transactions_last_minute: number;
  tps_last_minute: number;
  risk_categories: Record<string, number>;
  suspicious_transactions: number;
  alerts_total: number;
  high_risk_open_alerts: number;
  detection_latency_ms: { avg: number | null; p50: number | null; p95: number | null; p99: number | null; samples: number; definition: string };
  detection_rate: { value: number | null; numerator: number; denominator: number; definition: string; false_alert_rate: number | null; normal_labelled: number };
  model: { label: string; test_pr_auc: number | null; test_recall: number | null; test_precision: number | null } | null;
  model_error: string | null;
  simulation: string;
}

export interface SystemStatus {
  profile: "local" | "distributed";
  profile_description: string;
  stream_processor?: string;
  environment: string;
  database: string;
  redis: string;
  live_broker: string;
  model: { supervised: string | null; anomaly: string | null; error: string | null };
  version: string;
  rate_limiter?: string; runtime_config_publish?: Record<string, string>; started_at?: string;
}

/* ---------------------------------------------------------------- streaming infrastructure */
export interface KafkaTopic {
  name: string; exists: boolean; description: string; partitions?: number; end_offset_total?: number;
  messages_retained?: number; rate_per_sec_since_last_check?: number | null;
}
export interface ConsumerGroup {
  group: string; state?: string; members?: number | null; active?: boolean; lag_by_topic?: Record<string, number>;
  total_lag?: number; committed_partitions?: number; error?: string;
}
export interface CheckpointInfo {
  location: string; exists: boolean; last_committed_batch?: number | null; last_planned_batch?: number | null;
  uncommitted_batches?: number | null; last_commit_at?: number | null; last_commit_age_seconds?: number | null; has_state?: boolean;
}
export interface SparkQuery {
  name: string; id: string; run_id: string; active: boolean; status: string | null; batch_id: number | null;
  num_input_rows: number | null; input_rows_per_second: number | null; processed_rows_per_second: number | null;
  trigger_ms: number | null; timestamp: string | null; duration_ms: Record<string, number>; state_rows: number;
  rows_dropped_by_watermark: number; event_time_watermark: string | null; exception: string | null;
  lag_vs_raw_topic?: number; checkpoint?: CheckpointInfo;
}
export interface SparkStatus {
  status: string; unavailable_reason?: string; processor?: string; spark_version?: string; master?: string; app_id?: string;
  checkpoint_root?: string; reported_at?: number; age_seconds?: number; queries?: SparkQuery[]; terminated?: boolean;
}
export interface Heartbeat {
  status: string; unavailable_reason?: string; age_seconds?: number; group?: string; last_error?: string | null;
  stats?: Record<string, number>;
}
export interface InfrastructureStatus {
  profile: "local" | "distributed"; checked_at: string;
  database: { status: string; dialect?: string; latency_ms?: number; error?: string };
  redis: { status: string; note?: string };
  dead_letters_db: number;
  recent_dead_letters: { id: number; source: string; reason: string; event_id: string | null; created_at: string }[];
  kafka: { status: string; note?: string; error?: string; brokers?: number; topics?: KafkaTopic[]; consumer_groups?: ConsumerGroup[]; dlq_messages?: number | null };
  spark: SparkStatus & { note?: string };
  processor?: Heartbeat; consumer?: Heartbeat; stream_processor?: string;
  pipeline?: { mode: string; counters: Record<string, number>; feature_store_customers: number; warm_started_events: number };
}

/* ---------------------------------------------------------------- model intelligence */
export interface SplitMetrics {
  threshold?: number; support?: number; positives?: number; precision?: number; recall?: number; f1?: number;
  roc_auc?: number; pr_auc?: number; false_positive_rate?: number; false_negative_rate?: number;
  confusion_matrix?: { tn: number; fp: number; fn: number; tp: number }; pr_auc_definition?: string;
}
export interface ModelVersion {
  id: number; model_name: string; version: string; kind: "supervised" | "anomaly"; algorithm: string; status: string;
  feature_config_version: string; hyperparameters: Record<string, unknown>;
  metrics: { validation?: SplitMetrics; test?: SplitMetrics; unseen_customer_test?: SplitMetrics };
  inference_latency: { mean_ms?: number; p50_ms?: number; p95_ms?: number; p99_ms?: number; samples?: number };
  threshold: number; artifact_path: string; trained_at: string; activated_at: string | null; experiment_id: number | null;
  dataset: { name: string; version: string; source: string; rows: number } | null;
  live?: { predictions: number; avg_inference_ms: number | null };
}
export interface ActiveModels {
  active: Partial<Record<"supervised" | "anomaly", ModelVersion>>;
  loaded_in_api: { supervised: string | null; anomaly: string | null };
  model_error: string | null; feature_config_version: string; artifacts_writable: boolean;
}
export interface CompareRow {
  id: number; model_name: string; version: string; kind: string; status: string; threshold: number;
  validation_pr_auc: number | null; test: SplitMetrics; unseen_customer_test: SplitMetrics;
  latency: ModelVersion["inference_latency"]; dataset: string | null;
}
export interface Experiment {
  id: number; experiment_version: string; status: string; triggered_by: string; started_at: string; finished_at: string | null;
  recommended: string | null; dataset: string | null; duration_seconds: number | null;
  split: { strategy: string; sizes: Record<string, number>; positives: Record<string, number>; train_end?: string; validation_end?: string } | null;
}
export interface ExperimentDetail {
  id: number; experiment_version: string; status: string; triggered_by: string; started_at: string; finished_at: string | null;
  summary: {
    dataset?: { name: string; version: string; source: string; provenance: string; rows: number; positives?: number; positive_rate?: number; content_sha256?: string; spec?: Record<string, unknown> };
    split?: Experiment["split"]; config?: Record<string, unknown>; recommended_supervised?: string; selection_rule?: string; duration_seconds?: number;
  };
  model_versions: { id: number; model_name: string; kind: string; status: string }[];
}
export interface LiveInference {
  window_rows: number; total_predictions: number; definition: string;
  models: { model_version: string; samples: number; mean_ms: number; p50_ms: number; p95_ms: number; p99_ms: number; max_ms: number; first_at: string; last_at: string }[];
}
export interface ArtifactListing {
  artifacts_dir_writable: boolean; exists: boolean; note: string;
  files: { path: string; bytes: number; modified_at: string; sha256: string }[];
  registry_active: Record<string, string>; database_active: Record<string, string>;
}
export interface DriftFeature { psi: number | null; status: string }
export interface DriftReport {
  rows: number; status: string; max_psi?: number | null; thresholds?: { moderate: number; significant: number };
  features?: Record<string, DriftFeature>; score_psi?: DriftFeature | null; drifted_features?: string[];
  model_version?: string; interpretation?: string; reason?: string; min_rows?: number;
  labelled_performance?: { rows: number; positives: number; precision: number | null; recall: number | null; reference_test_precision: number; reference_test_recall: number; label_source: string } | null;
}
export interface DriftHistoryRow {
  id: number; model_version: string; computed_at: string; rows: number; status: string; max_psi: number | null;
  drifted_features: string[]; score_psi: number | null;
}
export interface Dataset {
  id: number; name: string; version: string; source: string; rows: number; positive_rate: number | null;
  provenance: string; created_at: string; content_sha256: string | null;
}

/* ---------------------------------------------------------------- risk analytics (Phase 11B) */
export interface AnalyticsPeriod { window_minutes: number | null; basis: string; label: string }
export interface PatternRow {
  scenario: string; count: number; flagged: number; flag_rate: number | null; avg_risk: number | null;
  labelled_suspicious?: number;
}
export interface PatternsResponse {
  scenarios: PatternRow[]; rule_hits: { rule: string; count: number }[];
  rule_hits_basis?: { flagged_predictions_examined: number; cap: number; definition: string };
  period?: AnalyticsPeriod; note: string;
}
export interface ConfusionResponse {
  risk_engine_vs_simulator: { tp: number; fp: number; fn: number; tn: number; precision: number | null; recall: number | null; false_positive_rate: number | null };
  analyst_outcomes: Record<string, number>; period?: AnalyticsPeriod; definition: string;
}
export interface MerchantCategoryRow { category: string; count: number; volume: number; flagged: number; flag_rate: number; avg_risk: number | null }
export interface TopMerchantRow {
  merchant_id: string; category: string; count: number; flagged: number; flag_rate?: number; avg_risk?: number | null; max_risk?: number | null;
}
export interface MerchantsResponse {
  categories: MerchantCategoryRow[]; top_flagged_merchants: TopMerchantRow[]; period?: AnalyticsPeriod; category?: string | null; ranking?: string;
}
export interface HourlyResponse { hours: { hour: number; count: number; flagged: number }[]; timezone: string; period?: AnalyticsPeriod }
export interface VelocityResponse { window_minutes: number; customers: { customer_id: string; count: number; amount: number; max_risk: number | null }[] }
export interface GeoPoint {
  city: string; country: string; lat: number; lon: number; count: number; high: number; medium: number; avg_risk: number | null; simulated: boolean;
}
export interface GeoResponse { window_minutes: number; points: GeoPoint[]; without_location: number; available: boolean; label: string }
export interface LastChange { by: string; at: string; action: string }
export interface RiskSettingsValues {
  medium_threshold: number; high_threshold: number; model_weight: number; anomaly_weight: number;
  anomaly_min_percentile: number; block_simulation_enabled: boolean;
}
export interface RiskSettingsResponse {
  settings: { medium_threshold: number; high_threshold: number; [k: string]: unknown }; defaults: Record<string, unknown>;
  updated_at?: string | null; last_change?: LastChange | null; editable_by?: string; used_by?: string | null;
  changed?: boolean; propagation?: Record<string, "ok" | "error"> | null;
}
export interface RiskRule {
  code: string; name: string; description: string; family: string; weight: number; params: Record<string, number>; enabled: boolean; updated_at: string;
  last_change?: LastChange | null; editable?: boolean;
  param_bounds?: Record<string, { integer: boolean; min: number; min_inclusive: boolean; max: number }>;
  changed_fields?: string[]; propagation?: Record<string, "ok" | "error"> | null;
}

/* ---------------------------------------------------------------- settings (Phase 11C) */
export interface SettingDoc {
  key: string; value: Record<string, number | string | boolean>; defaults: Record<string, number | string | boolean>;
  updated_at: string | null; last_change: LastChange | null; used_by: string | null; editable_by: string;
  bounds: Record<string, { min: number; max: number }> | null;
}

/* ---------------------------------------------------------------- reports (Phase 11C) */
export type ReportTypeKey = "fraud_activity" | "risk_analytics" | "alert_summary" | "investigation" | "model_performance" | "system_activity";
export type ReportFilterKey = "risk_level" | "alert_status" | "investigation_status";
export interface ReportTypeInfo {
  key: ReportTypeKey; title: string; description: string; period_basis: string;
  filters: { key: ReportFilterKey; options: string[] }[]; admin_only: boolean; available: boolean;
}
export interface ReportCatalogue {
  types: ReportTypeInfo[]; max_period_days: number; detail_row_cap: number; pdf_row_cap: number; preview_rows: number; persisted: boolean;
}
export type ReportValueFormat = "int" | "pct" | "ratio" | "score" | "money" | "float2" | "ms" | "hours" | "datetime" | "text" | "longtext";
export interface ReportMetric { key: string; label: string; value: number | string | null; format: ReportValueFormat; hint?: string }
export interface ReportSection {
  id: string; title: string; description: string | null; columns: { key: string; label: string; format: ReportValueFormat }[];
  rows: Record<string, unknown>[]; total_rows: number; truncated: boolean; stored_rows: number; note?: string | null;
}
export interface ReportContent {
  report_uid: string; report_type: ReportTypeKey; title: string; generated_at: string; generated_by: string; profile: string;
  period: { start: string; end: string; basis: string; label: string }; filters: Partial<Record<ReportFilterKey, string>>;
  summary: ReportMetric[]; sections: ReportSection[]; limitations: string[]; data_sources: string[];
}
export interface ReportMeta {
  report_uid: string; report_type: ReportTypeKey; title: string; period_start: string; period_end: string; period_label: string;
  filters: Partial<Record<ReportFilterKey, string>>; status: "COMPLETED" | "FAILED"; error: string | null; row_count: number;
  generated_by: string; created_at: string;
}
export interface ReportDetail extends ReportMeta { content: ReportContent | null }
