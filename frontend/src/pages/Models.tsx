import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, Lock, RotateCcw, X } from "lucide-react";
import { api, download } from "../services/api";
import { useAuth } from "../services/auth";
import {
  EmptyState, ErrorState, HealthPill, KV, PageHeader, Panel, QueryState, Select, StatTile, Tabs, Unavailable,
  cx, fmt, fmtBytes,
} from "../components/ui";
import { ModelMetricBars, PsiBars, TimeSeries, modelColor } from "../charts/Charts";
import type {
  ActiveModels, ArtifactListing, CompareRow, Dataset, DriftHistoryRow, DriftReport, Experiment, ExperimentDetail,
  LiveInference, ModelVersion, SplitMetrics,
} from "../types/api";

type Tab = "registry" | "compare" | "drift" | "artifacts";
const TABS: { id: Tab; label: string }[] = [
  { id: "registry", label: "Models & versions" }, { id: "compare", label: "Comparison" },
  { id: "drift", label: "Drift monitoring" }, { id: "artifacts", label: "Experiments & artifacts" },
];
const SYNTHETIC_NOTE = "Metrics were produced by the training pipeline on the synthetic dataset's held-out splits; synthetic results are optimistic and not representative of real-world fraud.";

const f4 = (n: number | null | undefined) => fmt.num(n, 4);

export function ModelsPage() {
  const [params, setParams] = useSearchParams();
  const tab = (TABS.some((t) => t.id === params.get("tab")) ? params.get("tab") : "registry") as Tab;
  const setTab = (t: Tab) => setParams((p) => { const n = new URLSearchParams(p); n.set("tab", t); return n; }, { replace: true });
  const active = useQuery({ queryKey: ["models-active"], queryFn: () => api<ActiveModels>("/models/active"), refetchInterval: 30000 });
  return (
    <div>
      <PageHeader title="Model Intelligence"
        subtitle="Registered model versions, offline evaluation, live inference latency and drift — all read from the model registry, stored experiment results and persisted predictions."
        actions={<>
          <button className="btn btn-secondary" data-testid="download-model-report" onClick={() => void download("/reports/model-evaluation?format=md", "model-evaluation.md")}>Evaluation report (.md)</button>
        </>} />
      <ActiveSummary q={active} />
      <Tabs label="Model intelligence sections" tabs={TABS} value={tab} onChange={setTab} />
      {tab === "registry" && <Registry active={active.data} />}
      {tab === "compare" && <Compare />}
      {tab === "drift" && <Drift />}
      {tab === "artifacts" && <Artifacts />}
    </div>
  );
}

/* ------------------------------------------------------------------ active models */
function ActiveSummary({ q }: { q: ReturnType<typeof useQuery<ActiveModels>> }) {
  return (
    <QueryState q={q}>
      {q.data && (
        <>
          {q.data.model_error && (
            <div role="alert" className="panel border-coral-500/40 px-4 py-3 mb-4 text-sm text-coral-400">
              Model not loaded in the API — scoring is rules-only: {q.data.model_error}
            </div>
          )}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
            {(["supervised", "anomaly"] as const).map((kind) => {
              const m = q.data!.active[kind];
              const loaded = q.data!.loaded_in_api[kind];
              const t = m?.metrics.test;
              return (
                <Panel key={kind} title={`Active ${kind} model`} actions={m && <HealthPill state={m.status} />}>
                  {!m ? <EmptyState title={`No active ${kind} model`} /> : (
                    <div data-testid={`active-${kind}`}>
                      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                        <span className="num text-stream-400 text-base">{m.model_name}</span>
                        <span className="num text-xs text-mist-300">{m.version}</span>
                        <span className={cx("text-[11px]", loaded === `${m.model_name}:${m.version}` ? "text-emerald-400" : "text-coral-400")}>
                          {loaded === `${m.model_name}:${m.version}` ? "loaded in API" : `API has loaded: ${loaded ?? "nothing"}`}
                        </span>
                      </div>
                      <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 mt-3">
                        <StatTile compact label="PR-AUC" value={f4(t?.pr_auc)} />
                        <StatTile compact label="Precision" value={f4(t?.precision)} />
                        <StatTile compact label="Recall" value={f4(t?.recall)} />
                        <StatTile compact label="F1" value={f4(t?.f1)} />
                        <StatTile compact label="p50 latency" value={fmt.ms(m.inference_latency.p50_ms)} hint="offline, per row" />
                      </div>
                      <p className="text-[11px] text-mist-400 mt-2">
                        Test split · threshold <span className="num">{m.threshold.toFixed(4)}</span> · trained {fmt.dt(m.trained_at)} · dataset {m.dataset ? `${m.dataset.name} v${m.dataset.version} (${fmt.num(m.dataset.rows)} rows)` : "unknown"}
                      </p>
                    </div>
                  )}
                </Panel>
              );
            })}
          </div>
          <div className={cx("panel px-4 py-2 mb-4 text-xs flex items-center gap-2", q.data.artifacts_writable ? "text-mist-300" : "text-amber-400")} data-testid="artifacts-mode">
            <Lock className="h-3.5 w-3.5 shrink-0" />
            {q.data.artifacts_writable
              ? "Model artifacts are writable in this deployment: administrators can activate or roll back model versions."
              : "Model artifacts are mounted READ-ONLY (default): activation, rollback and training are disabled. Set ARTIFACTS_MOUNT_MODE=rw to allow them."}
            <span className="ml-auto num text-mist-400 hidden sm:inline">feature config {q.data.feature_config_version}</span>
          </div>
        </>
      )}
    </QueryState>
  );
}

/* ------------------------------------------------------------------ registry */
function Registry({ active }: { active?: ActiveModels }) {
  const [kind, setKind] = useState("all");
  const [selected, setSelected] = useState<number | null>(null);
  const models = useQuery({ queryKey: ["models", kind], queryFn: () => api<ModelVersion[]>(`/models${kind === "all" ? "" : `?kind=${kind}`}`) });
  const live = useQuery({ queryKey: ["live-inference"], queryFn: () => api<LiveInference>("/models/live-inference?limit=5000"), refetchInterval: 15000 });
  const liveByVersion = new Map((live.data?.models ?? []).map((m) => [m.model_version, m]));
  return (
    <>
      <Panel className="mb-4" title="Registered model versions"
        actions={<Select label="" value={kind} onChange={(v) => { setKind(v); setSelected(null); }} testId="kind-filter"
          options={[{ value: "all", label: "All kinds" }, { value: "supervised", label: "Supervised" }, { value: "anomaly", label: "Anomaly" }]} />}>
        <QueryState q={models} empty={!models.data?.length && <EmptyState title="No model versions registered" hint="Train models with the training pipeline; the API imports them from ml/artifacts at startup." />}>
          <div className="overflow-x-auto">
            <table className="w-full" data-testid="models-table">
              <thead><tr>
                <th className="th">Model</th><th className="th">Version</th><th className="th">Kind</th><th className="th">Status</th>
                <th className="th">PR-AUC</th><th className="th">Precision</th><th className="th">Recall</th><th className="th">F1</th>
                <th className="th">Offline p95</th><th className="th">Live p95</th><th className="th">Live n</th>
              </tr></thead>
              <tbody>
                {models.data?.map((m) => {
                  const t = m.metrics.test ?? {};
                  const lv = liveByVersion.get(`${m.model_name}:${m.version}`);
                  return (
                    <tr key={m.id} className={cx("hover:bg-ink-850 cursor-pointer", selected === m.id && "bg-ink-850")} onClick={() => setSelected(m.id)} data-testid={`model-row-${m.model_name}`}>
                      <td className="td"><span className="inline-flex items-center gap-2"><span aria-hidden className="h-2.5 w-2.5 rounded-sm" style={{ background: modelColor(m.model_name) }} />
                        <button className="num text-stream-400 hover:underline">{m.model_name}</button></span></td>
                      <td className="td num text-mist-300">{m.version}</td>
                      <td className="td">{m.kind}</td>
                      <td className="td"><HealthPill state={m.status} /></td>
                      <td className="td num">{f4(t.pr_auc)}</td><td className="td num">{f4(t.precision)}</td>
                      <td className="td num">{f4(t.recall)}</td><td className="td num">{f4(t.f1)}</td>
                      <td className="td num">{fmt.ms(m.inference_latency.p95_ms)}</td>
                      <td className="td num">{lv ? fmt.ms(lv.p95_ms) : <Unavailable reason="no live predictions by this version in the window" />}</td>
                      <td className="td num">{lv ? fmt.num(lv.samples) : "0"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="text-[11px] text-mist-400 mt-2">{SYNTHETIC_NOTE} Offline latency was measured by the training pipeline on {`${models.data?.[0]?.inference_latency.samples ?? "?"}`} single-row predictions. Click a row for details.</p>
        </QueryState>
      </Panel>
      {selected != null && <ModelDetail id={selected} active={active} onClose={() => setSelected(null)} />}
      <LivePanel q={live} />
    </>
  );
}

function LivePanel({ q }: { q: ReturnType<typeof useQuery<LiveInference>> }) {
  return (
    <Panel title="Live inference latency (persisted predictions)" actions={<Activity className="h-4 w-4 text-mist-400" />}>
      <QueryState q={q} empty={!q.data?.models.length && <EmptyState title="No live predictions with inference timing yet" hint="Run traffic from the Simulation Lab." />}>
        {q.data && (
          <>
            <div className="overflow-x-auto">
              <table className="w-full" data-testid="live-inference">
                <thead><tr><th className="th">Model version</th><th className="th">Samples</th><th className="th">Mean</th><th className="th">p50</th><th className="th">p95</th><th className="th">p99</th><th className="th">Max</th><th className="th">Window</th></tr></thead>
                <tbody>
                  {q.data.models.map((m) => (
                    <tr key={m.model_version}>
                      <td className="td num text-mist-100">{m.model_version}</td><td className="td num">{fmt.num(m.samples)}</td>
                      <td className="td num">{fmt.ms(m.mean_ms)}</td><td className="td num">{fmt.ms(m.p50_ms)}</td><td className="td num">{fmt.ms(m.p95_ms)}</td>
                      <td className="td num">{fmt.ms(m.p99_ms)}</td><td className="td num">{fmt.ms(m.max_ms)}</td>
                      <td className="td num text-mist-300 text-xs">{fmt.dt(m.first_at)} → {fmt.time(m.last_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-[11px] text-mist-400 mt-2">Most recent {fmt.num(q.data.window_rows)} of {fmt.num(q.data.total_predictions)} predictions. {q.data.definition}</p>
          </>
        )}
      </QueryState>
    </Panel>
  );
}

function MetricsTable({ m }: { m: ModelVersion }) {
  const splits: [string, SplitMetrics | undefined][] = [["validation", m.metrics.validation], ["test", m.metrics.test], ["unseen customers", m.metrics.unseen_customer_test]];
  const rows: [string, keyof SplitMetrics][] = [["PR-AUC", "pr_auc"], ["ROC-AUC", "roc_auc"], ["Precision", "precision"], ["Recall", "recall"], ["F1", "f1"], ["False-positive rate", "false_positive_rate"], ["Support", "support"], ["Positives", "positives"]];
  return (
    <div className="overflow-x-auto">
      <table className="w-full">
        <thead><tr><th className="th">Metric</th>{splits.map(([n]) => <th key={n} className="th">{n}</th>)}</tr></thead>
        <tbody>
          {rows.map(([label, k]) => (
            <tr key={k}><td className="td text-mist-300">{label}</td>
              {splits.map(([n, s]) => <td key={n} className="td num">{s?.[k] == null ? "n/a" : k === "support" || k === "positives" ? fmt.num(s[k] as number) : f4(s[k] as number)}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Confusion({ cm }: { cm?: SplitMetrics["confusion_matrix"] }) {
  if (!cm) return <EmptyState title="No confusion matrix recorded" />;
  const cell = (label: string, v: number, tone: string) => (
    <div className={cx("rounded-md border px-3 py-2 text-center", tone)}><div className="text-[11px] text-mist-300">{label}</div><div className="num text-lg text-mist-100">{fmt.num(v)}</div></div>
  );
  return (
    <div className="grid grid-cols-[auto_1fr_1fr] gap-2 text-xs items-center" data-testid="confusion-matrix">
      <span /><span className="text-center text-mist-400">predicted normal</span><span className="text-center text-mist-400">predicted suspicious</span>
      <span className="text-mist-400">actual normal</span>{cell("TN", cm.tn, "border-ink-600 bg-ink-850")}{cell("FP", cm.fp, "border-amber-500/40 bg-amber-500/5")}
      <span className="text-mist-400">actual suspicious</span>{cell("FN", cm.fn, "border-coral-500/40 bg-coral-500/5")}{cell("TP", cm.tp, "border-emerald-500/40 bg-emerald-500/5")}
    </div>
  );
}

function ModelDetail({ id, active, onClose }: { id: number; active?: ActiveModels; onClose: () => void }) {
  const { isAdmin } = useAuth();
  const qc = useQueryClient();
  const q = useQuery({ queryKey: ["model", id], queryFn: () => api<ModelVersion>(`/models/${id}`) });
  const [reason, setReason] = useState("");
  const [confirm, setConfirm] = useState<"activate" | "rollback" | null>(null);
  const writable = !!active?.artifacts_writable;
  const act = useMutation({
    mutationFn: () => confirm === "rollback"
      ? api("/models/rollback", { body: { kind: q.data!.kind, reason } })
      : api(`/models/${id}/activate`, { body: { reason } }),
    onSuccess: () => { setConfirm(null); setReason(""); void qc.invalidateQueries({ queryKey: ["models"] }); void qc.invalidateQueries({ queryKey: ["models-active"] }); void qc.invalidateQueries({ queryKey: ["model"] }); },
  });
  const m = q.data;
  return (
    <Panel className="mb-4" title={m ? `${m.model_name} · ${m.version}` : "Model detail"}
      actions={<button className="btn btn-ghost p-1" aria-label="Close model detail" onClick={onClose}><X className="h-4 w-4" /></button>}>
      <QueryState q={q}>
        {m && (
          <div data-testid="model-detail">
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
              <div>
                <div className="panel-title mb-1">Version</div>
                <KV k="status" v={<HealthPill state={m.status} />} />
                <KV k="algorithm" v={m.algorithm} />
                <KV k="kind" v={m.kind} />
                <KV k="decision threshold" v={<span className="num">{m.threshold.toFixed(6)}</span>} />
                <KV k="feature config" v={m.feature_config_version} />
                <KV k="trained" v={fmt.dt(m.trained_at)} />
                <KV k="activated" v={fmt.dt(m.activated_at)} />
                <KV k="dataset" v={m.dataset ? `${m.dataset.name} v${m.dataset.version}` : "unknown"} />
                <KV k="artifact" v={<span className="num text-xs">{m.artifact_path}</span>} />
                <KV k="live predictions" v={<span className="num">{fmt.num(m.live?.predictions)} · avg {fmt.ms(m.live?.avg_inference_ms)}</span>} />
              </div>
              <div className="xl:col-span-2 space-y-4">
                <div><div className="panel-title mb-1">Evaluation by split</div><MetricsTable m={m} /></div>
                <div><div className="panel-title mb-2">Confusion matrix · test split</div><Confusion cm={m.metrics.test?.confusion_matrix} /></div>
              </div>
            </div>
            <div className="mt-4">
              <div className="panel-title mb-1">Hyperparameters</div>
              <div className="flex flex-wrap gap-2">
                {Object.entries(m.hyperparameters).map(([k, v]) => <span key={k} className="rounded bg-ink-850 border border-ink-700 px-2 py-0.5 text-[11px] num">{k}={String(v)}</span>)}
              </div>
            </div>
            {isAdmin && (
              <div className="mt-4 border-t border-ink-800 pt-3">
                <div className="flex flex-wrap gap-2 items-center">
                  <button className="btn btn-primary" data-testid="activate-model" disabled={!writable || m.status === "ACTIVE"} onClick={() => setConfirm("activate")}
                    title={!writable ? "Artifacts are mounted read-only" : m.status === "ACTIVE" ? "Already active" : ""}>Activate this version</button>
                  <button className="btn btn-secondary" data-testid="rollback-model" disabled={!writable || m.status !== "ACTIVE"} onClick={() => setConfirm("rollback")}
                    title={!writable ? "Artifacts are mounted read-only" : m.status !== "ACTIVE" ? "Only the active version can be rolled back" : ""}>
                    <RotateCcw className="h-4 w-4" /> Roll back {m.kind}
                  </button>
                  {!writable && <span className="text-xs text-amber-400">Disabled: artifacts are read-only in this deployment.</span>}
                </div>
                {confirm && (
                  <form className="mt-3 flex flex-col sm:flex-row gap-2 sm:items-end" onSubmit={(e) => { e.preventDefault(); act.mutate(); }}>
                    <label className="flex-1 text-xs text-mist-300 flex flex-col gap-1">
                      Reason for {confirm === "activate" ? "activating" : "rolling back"} (audited, min. 5 characters)
                      <input className="input" data-testid="model-change-reason" value={reason} onChange={(e) => setReason(e.target.value)} maxLength={500} />
                    </label>
                    <button className="btn btn-danger" type="submit" disabled={reason.trim().length < 5 || act.isPending} data-testid="confirm-model-change">Confirm</button>
                    <button className="btn btn-ghost" type="button" onClick={() => { setConfirm(null); act.reset(); }}>Cancel</button>
                  </form>
                )}
                {act.isError && <p role="alert" className="text-xs text-coral-400 mt-2">{(act.error as Error).message}</p>}
              </div>
            )}
          </div>
        )}
      </QueryState>
    </Panel>
  );
}

/* ------------------------------------------------------------------ comparison */
const COMPARE_METRICS = [
  { key: "pr_auc", label: "PR-AUC" }, { key: "precision", label: "Precision" }, { key: "recall", label: "Recall" },
  { key: "f1", label: "F1" }, { key: "roc_auc", label: "ROC-AUC" },
];
type SplitKey = "test" | "unseen_customer_test";

function Compare() {
  const exps = useQuery({ queryKey: ["experiments"], queryFn: () => api<Experiment[]>("/models/experiments") });
  const [exp, setExp] = useState("");
  const [split, setSplit] = useState<SplitKey>("test");
  const [hidden, setHidden] = useState<Set<number>>(new Set());
  const expVersion = exp || exps.data?.[0]?.experiment_version || "";
  const cmp = useQuery({
    queryKey: ["compare", expVersion], enabled: !!expVersion,
    queryFn: () => api<{ models: CompareRow[]; evaluation_note: string }>(`/models/compare?experiment=${encodeURIComponent(expVersion)}`),
  });
  const rows = cmp.data?.models ?? [];
  const shown = rows.filter((r) => !hidden.has(r.id));
  const best = useMemo(() => {
    const out: Record<string, number> = {};
    COMPARE_METRICS.forEach(({ key }) => {
      const vals = shown.map((r) => (r[split] as Record<string, number | undefined>)[key]).filter((v): v is number => v != null);
      if (vals.length) out[key] = Math.max(...vals);
    });
    return out;
  }, [shown, split]);
  const recommended = exps.data?.find((e) => e.experiment_version === expVersion)?.recommended;
  return (
    <QueryState q={exps} empty={!exps.data?.length && <EmptyState title="No experiments recorded" hint="Model comparison uses stored experiment results; none exist yet." />}>
      <Panel className="mb-4" title="Model comparison · stored experiment results"
        actions={<div className="flex flex-wrap gap-2">
          <Select label="" value={expVersion} onChange={(v) => { setExp(v); setHidden(new Set()); }} testId="compare-experiment"
            options={(exps.data ?? []).map((e) => ({ value: e.experiment_version, label: e.experiment_version }))} />
          <Select label="" value={split} onChange={(v) => setSplit(v as SplitKey)} testId="compare-split"
            options={[{ value: "test", label: "Temporal test split" }, { value: "unseen_customer_test", label: "Unseen-customer split" }]} />
        </div>}>
        <QueryState q={cmp} empty={!rows.length && <EmptyState title="This experiment has no registered model versions" />}>
          <div className="flex flex-wrap gap-3 mb-3" role="group" aria-label="Models shown">
            {rows.map((r) => (
              <label key={r.id} className="inline-flex items-center gap-2 text-xs text-mist-200 cursor-pointer">
                <input type="checkbox" checked={!hidden.has(r.id)} data-testid={`compare-toggle-${r.model_name}`}
                  onChange={() => setHidden((h) => { const n = new Set(h); if (n.has(r.id)) n.delete(r.id); else n.add(r.id); return n; })} />
                <span aria-hidden className="h-2.5 w-2.5 rounded-sm" style={{ background: modelColor(r.model_name) }} />
                <span className="num">{r.model_name}</span> <span className="text-mist-400">({r.kind})</span>
              </label>
            ))}
          </div>
          {shown.length === 0 ? <EmptyState title="Select at least one model" /> : (
            <>
              <div data-testid="compare-chart">
                <ModelMetricBars metrics={COMPARE_METRICS}
                  models={shown.map((r) => ({ name: r.model_name, values: r[split] as Record<string, number | undefined> }))} />
              </div>
              <div className="overflow-x-auto mt-3">
                <table className="w-full" data-testid="compare-table">
                  <thead><tr><th className="th">Model</th><th className="th">Status</th><th className="th">Val PR-AUC</th>
                    {COMPARE_METRICS.map((m) => <th key={m.key} className="th">{m.label}</th>)}
                    <th className="th">FPR</th><th className="th">Threshold</th><th className="th">p50 / p95</th></tr></thead>
                  <tbody>
                    {shown.map((r) => {
                      const s = r[split] as Record<string, number | undefined>;
                      return (
                        <tr key={r.id}>
                          <td className="td num"><span className="inline-flex items-center gap-2"><span aria-hidden className="h-2.5 w-2.5 rounded-sm" style={{ background: modelColor(r.model_name) }} />{r.model_name}
                            {r.model_name === recommended && <span className="text-[10px] rounded border border-stream-500/40 text-stream-400 px-1">recommended</span>}</span></td>
                          <td className="td"><HealthPill state={r.status} /></td>
                          <td className="td num">{f4(r.validation_pr_auc)}</td>
                          {COMPARE_METRICS.map((m) => (
                            <td key={m.key} className={cx("td num", s[m.key] != null && s[m.key] === best[m.key] && shown.length > 1 && "text-emerald-400 font-semibold")}>{f4(s[m.key])}</td>
                          ))}
                          <td className="td num">{f4(s.false_positive_rate)}</td>
                          <td className="td num">{r.threshold.toFixed(4)}</td>
                          <td className="td num">{fmt.ms(r.latency.p50_ms)} / {fmt.ms(r.latency.p95_ms)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <p className="text-[11px] text-mist-400 mt-2">Best value per column among the shown models is highlighted. Selection rule used by training: highest validation PR-AUC among supervised models (test split not used). The anomaly model is unsupervised and is not a candidate for the supervised slot. {cmp.data?.evaluation_note}</p>
            </>
          )}
        </QueryState>
      </Panel>
    </QueryState>
  );
}

/* ------------------------------------------------------------------ drift */
function Drift() {
  const qc = useQueryClient();
  const [selected, setSelected] = useState<number | null>(null);
  const [featureFilter, setFeatureFilter] = useState("all");
  const hist = useQuery({ queryKey: ["drift-history"], queryFn: () => api<DriftHistoryRow[]>("/drift/history?limit=100") });
  const run = useMutation({
    mutationFn: () => api<DriftReport>("/drift/current?persist=true"),
    onSuccess: async () => { await qc.invalidateQueries({ queryKey: ["drift-history"] }); setSelected(null); },
  });
  const latestId = hist.data?.[0]?.id;
  const reportId = selected ?? latestId ?? null;
  const rep = useQuery({
    queryKey: ["drift-report", reportId], enabled: reportId != null,
    queryFn: () => api<{ id: number; computed_at: string; model_version: string; rows: number; status: string; report: DriftReport }>(`/drift/reports/${reportId}`),
  });
  const trend = [...(hist.data ?? [])].reverse().map((h) => ({
    label: new Date(h.computed_at).toLocaleString("en-GB", { hour12: false, month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }),
    max_psi: h.max_psi, score_psi: h.score_psi,
  }));
  const r = rep.data?.report;
  const featRows = Object.entries(r?.features ?? {})
    .filter(([, v]) => v.psi != null && (featureFilter === "all" || (featureFilter === "drifted" ? v.status !== "STABLE" : v.status === featureFilter)))
    .map(([feature, v]) => ({ feature, psi: v.psi as number, status: v.status }))
    .sort((a, b) => b.psi - a.psi);
  return (
    <>
      <Panel className="mb-4" title="Drift checks"
        actions={<button className="btn btn-primary" data-testid="run-drift" disabled={run.isPending} onClick={() => run.mutate()}>
          {run.isPending ? "Computing…" : "Run drift check now"}</button>}>
        <p className="text-xs text-mist-400 mb-3">
          A check compares the active supervised model's training reference profile with the features of its most recent live predictions (PSI per feature and on the model score) and stores the result.
          PSI measures <b>data</b> drift only; concept drift needs labelled outcomes (simulator ground truth, shown with each report).
        </p>
        {run.isError && <ErrorState error={run.error} />}
        {run.data?.status === "INSUFFICIENT_DATA" && (
          <div role="status" data-testid="drift-insufficient" className="text-sm text-amber-400 mb-2">
            Not enough live predictions by the active model: {fmt.num(run.data.rows)} of the {fmt.num(run.data.min_rows)} required. Nothing was stored.
          </div>
        )}
        <QueryState q={hist} empty={!hist.data?.length && <EmptyState title="No stored drift reports yet" hint="Run a drift check to compute and store one from live predictions." />}>
          <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">
            <div className="xl:col-span-3">
              <div className="panel-title mb-1">PSI over stored checks</div>
              {trend.length < 2 ? <EmptyState title="One stored check so far" hint="The trend appears after a second check." /> :
                <TimeSeries points={trend} height={200} yLabel={(v) => v.toFixed(2)}
                  series={[{ key: "max_psi", name: "max feature PSI", color: "#3987e5" }, { key: "score_psi", name: "model-score PSI", color: "#d95926" }]} />}
            </div>
            <div className="xl:col-span-2 overflow-x-auto max-h-64 overflow-y-auto">
              <table className="w-full" data-testid="drift-history">
                <thead><tr><th className="th">Computed</th><th className="th">Status</th><th className="th">Max PSI</th><th className="th">Rows</th></tr></thead>
                <tbody>
                  {hist.data?.map((h) => (
                    <tr key={h.id} onClick={() => setSelected(h.id)} className={cx("cursor-pointer hover:bg-ink-850", h.id === reportId && "bg-ink-850")}>
                      <td className="td num text-mist-300"><button className="hover:underline text-left">{fmt.dt(h.computed_at)}</button></td>
                      <td className="td"><HealthPill state={h.status} /></td>
                      <td className="td num">{h.max_psi == null ? "—" : h.max_psi.toFixed(3)}</td>
                      <td className="td num">{fmt.num(h.rows)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </QueryState>
      </Panel>
      {reportId != null && (
        <Panel title={rep.data ? `Report #${rep.data.id} · ${fmt.dt(rep.data.computed_at)} · ${rep.data.model_version}` : "Drift report"}
          actions={<Select label="" value={featureFilter} onChange={setFeatureFilter} testId="drift-feature-filter"
            options={[{ value: "all", label: "All features" }, { value: "drifted", label: "Drifted (moderate+)" }, { value: "SIGNIFICANT", label: "Significant" }, { value: "MODERATE", label: "Moderate" }, { value: "STABLE", label: "Stable" }]} />}>
          <QueryState q={rep}>
            {r && (
              <div className="grid grid-cols-1 xl:grid-cols-3 gap-4" data-testid="drift-report">
                <div className="xl:col-span-2">
                  <div className="flex flex-wrap gap-4 text-xs text-mist-300 mb-2">
                    {(["STABLE", "MODERATE", "SIGNIFICANT"] as const).map((s) => (
                      <span key={s} className="inline-flex items-center gap-1.5"><HealthPill state={s} /> {s === "STABLE" ? `< ${r.thresholds?.moderate}` : s === "MODERATE" ? `${r.thresholds?.moderate}–${r.thresholds?.significant}` : `≥ ${r.thresholds?.significant}`}</span>
                    ))}
                  </div>
                  {featRows.length ? <PsiBars rows={featRows} moderate={r.thresholds?.moderate ?? 0.1} significant={r.thresholds?.significant ?? 0.25} />
                    : <EmptyState title="No feature matches this filter" />}
                  <p className="text-[11px] text-mist-400 mt-1">√-scaled x-axis. Dashed lines mark the moderate / significant thresholds.</p>
                </div>
                <div>
                  <div className="panel-title mb-1">Summary</div>
                  <KV k="status" v={<HealthPill state={r.status} />} />
                  <KV k="rows compared" v={fmt.num(r.rows)} />
                  <KV k="max feature PSI" v={r.max_psi == null ? "—" : r.max_psi.toFixed(4)} />
                  <KV k="model-score PSI" v={r.score_psi?.psi == null ? "—" : `${r.score_psi.psi.toFixed(4)} (${r.score_psi.status})`} />
                  <KV k="drifted features" v={fmt.num(r.drifted_features?.length ?? 0)} />
                  <div className="panel-title mt-4 mb-1">Labelled performance on the same window</div>
                  {r.labelled_performance ? (
                    <>
                      <KV k="precision (live vs test)" v={<span className="num">{f4(r.labelled_performance.precision)} vs {f4(r.labelled_performance.reference_test_precision)}</span>} />
                      <KV k="recall (live vs test)" v={<span className="num">{f4(r.labelled_performance.recall)} vs {f4(r.labelled_performance.reference_test_recall)}</span>} />
                      <KV k="labelled rows / positives" v={<span className="num">{fmt.num(r.labelled_performance.rows)} / {fmt.num(r.labelled_performance.positives)}</span>} />
                      <p className="text-[11px] text-mist-400 mt-1">Labels: {r.labelled_performance.label_source}. Uses the model's own threshold (model score only, not the hybrid risk decision).</p>
                    </>
                  ) : <p className="text-xs text-mist-400">No labelled positives in this window — performance cannot be estimated.</p>}
                  {r.interpretation && <p className="text-[11px] text-mist-400 mt-3">{r.interpretation}</p>}
                </div>
              </div>
            )}
          </QueryState>
        </Panel>
      )}
    </>
  );
}

/* ------------------------------------------------------------------ experiments & artifacts */
function Artifacts() {
  const exps = useQuery({ queryKey: ["experiments"], queryFn: () => api<Experiment[]>("/models/experiments") });
  const [expId, setExpId] = useState<number | null>(null);
  const shownExp = expId ?? exps.data?.[0]?.id ?? null;
  const detail = useQuery({ queryKey: ["experiment", shownExp], enabled: shownExp != null, queryFn: () => api<ExperimentDetail>(`/models/experiments/${shownExp}`) });
  const ds = useQuery({ queryKey: ["datasets"], queryFn: () => api<{ datasets: Dataset[] }>("/datasets") });
  const art = useQuery({ queryKey: ["artifacts"], queryFn: () => api<ArtifactListing>("/models/artifacts") });
  const [fileFilter, setFileFilter] = useState("");
  const files = (art.data?.files ?? []).filter((f) => f.path.toLowerCase().includes(fileFilter.toLowerCase()));
  const consistent = art.data && Object.entries(art.data.database_active).every(([k, v]) => art.data!.registry_active[k] === v)
    && Object.keys(art.data.registry_active).length === Object.keys(art.data.database_active).length;
  const s = detail.data?.summary;
  return (
    <>
      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4 mb-4">
        <Panel title="Experiment history" className="xl:col-span-2">
          <QueryState q={exps} empty={!exps.data?.length && <EmptyState title="No experiments recorded" />}>
            <div className="overflow-x-auto">
              <table className="w-full" data-testid="experiments-table">
                <thead><tr><th className="th">Experiment</th><th className="th">Status</th><th className="th">Started</th><th className="th">Duration</th><th className="th">Recommended</th></tr></thead>
                <tbody>
                  {exps.data?.map((e) => (
                    <tr key={e.id} onClick={() => setExpId(e.id)} className={cx("cursor-pointer hover:bg-ink-850", e.id === shownExp && "bg-ink-850")}>
                      <td className="td num"><button className="text-stream-400 hover:underline">{e.experiment_version}</button></td>
                      <td className="td"><HealthPill state={e.status === "COMPLETED" ? "OK" : e.status} /></td>
                      <td className="td num text-mist-300">{fmt.dt(e.started_at)}</td>
                      <td className="td num">{e.duration_seconds != null ? `${e.duration_seconds.toFixed(0)} s` : "—"}</td>
                      <td className="td num">{e.recommended ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </QueryState>
        </Panel>
        <Panel title={detail.data ? `Experiment ${detail.data.experiment_version}` : "Experiment detail"} className="xl:col-span-3">
          {shownExp == null ? <EmptyState title="Select an experiment" /> : (
            <QueryState q={detail}>
              {s && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4" data-testid="experiment-detail">
                  <div>
                    <div className="panel-title mb-1">Training dataset</div>
                    <KV k="name / version" v={`${s.dataset?.name ?? "?"} v${s.dataset?.version ?? "?"}`} />
                    <KV k="source" v={s.dataset?.source ?? "—"} />
                    <KV k="rows / positives" v={<span className="num">{fmt.num(s.dataset?.rows)} / {fmt.num(s.dataset?.positives)}</span>} />
                    <KV k="positive rate" v={fmt.pct(s.dataset?.positive_rate, 2)} />
                    <KV k="sha256" v={<span className="num text-xs">{s.dataset?.content_sha256?.slice(0, 16) ?? "—"}…</span>} />
                    <p className="text-[11px] text-mist-400 mt-1">{s.dataset?.provenance}</p>
                  </div>
                  <div>
                    <div className="panel-title mb-1">Split · {s.split?.strategy}</div>
                    {Object.entries(s.split?.sizes ?? {}).map(([k, v]) => (
                      <KV key={k} k={k.replace(/_/g, " ")} v={<span className="num">{fmt.num(v)} rows · {fmt.num(s.split?.positives?.[k])} positive</span>} />
                    ))}
                    <KV k="train ends" v={fmt.dt(s.split?.train_end)} />
                    <KV k="validation ends" v={fmt.dt(s.split?.validation_end)} />
                    <div className="panel-title mt-3 mb-1">Selection</div>
                    <KV k="recommended" v={s.recommended_supervised ?? "—"} />
                    <p className="text-[11px] text-mist-400 mt-1">{s.selection_rule}</p>
                    <div className="flex flex-wrap gap-2 mt-2">
                      {detail.data!.model_versions.map((m) => <span key={m.id} className="rounded bg-ink-850 border border-ink-700 px-2 py-0.5 text-[11px] num">{m.model_name} · {m.status}</span>)}
                    </div>
                  </div>
                </div>
              )}
            </QueryState>
          )}
        </Panel>
      </div>

      <Panel className="mb-4" title="Registered datasets">
        <QueryState q={ds} empty={!ds.data?.datasets.length && <EmptyState title="No datasets registered" />}>
          <div className="overflow-x-auto">
            <table className="w-full" data-testid="datasets-table">
              <thead><tr><th className="th">Name</th><th className="th">Version</th><th className="th">Source</th><th className="th">Rows</th><th className="th">Positive rate</th><th className="th">SHA-256</th><th className="th">Provenance</th></tr></thead>
              <tbody>
                {ds.data?.datasets.map((d) => (
                  <tr key={d.id}><td className="td num">{d.name}</td><td className="td num">{d.version}</td><td className="td">{d.source}</td>
                    <td className="td num">{fmt.num(d.rows)}</td><td className="td num">{fmt.pct(d.positive_rate, 2)}</td>
                    <td className="td num text-xs text-mist-300" title={d.content_sha256 ?? ""}>{d.content_sha256?.slice(0, 12) ?? "—"}</td>
                    <td className="td text-xs text-mist-300 whitespace-normal">{d.provenance}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </QueryState>
      </Panel>

      <Panel title="Model artifacts on disk"
        actions={<input className="input py-1.5 w-48" placeholder="Filter files" aria-label="Filter artifact files" value={fileFilter} onChange={(e) => setFileFilter(e.target.value)} data-testid="artifact-filter" />}>
        <QueryState q={art} empty={art.data && !art.data.exists && <EmptyState title="Artifacts directory not found" />}>
          {art.data && (
            <>
              <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs mb-3">
                <span className={art.data.artifacts_dir_writable ? "text-mist-300" : "text-amber-400"} data-testid="artifacts-writable">
                  <Lock className="h-3.5 w-3.5 inline mr-1" />{art.data.artifacts_dir_writable ? "writable" : "read-only mount"}
                </span>
                <span className="text-mist-300">active in registry.json: <span className="num text-mist-100">{Object.entries(art.data.registry_active).map(([k, v]) => `${k}=${v}`).join(", ") || "none"}</span></span>
                <span data-testid="registry-consistency" className={consistent ? "text-emerald-400" : "text-coral-400"}>
                  {consistent ? "registry.json matches the database" : `MISMATCH — database: ${Object.entries(art.data.database_active).map(([k, v]) => `${k}=${v}`).join(", ")}`}
                </span>
              </div>
              {files.length === 0 ? <EmptyState title="No file matches this filter" /> : (
                <div className="overflow-x-auto">
                  <table className="w-full" data-testid="artifacts-table">
                    <thead><tr><th className="th">Path</th><th className="th">Size</th><th className="th">Modified</th><th className="th">SHA-256</th></tr></thead>
                    <tbody>
                      {files.map((f) => (
                        <tr key={f.path}><td className="td num text-mist-100">{f.path}</td><td className="td num">{fmtBytes(f.bytes)}</td>
                          <td className="td num text-mist-300">{fmt.dt(f.modified_at)}</td><td className="td num text-xs text-mist-300" title={f.sha256}>{f.sha256.slice(0, 16)}</td></tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              <p className="text-[11px] text-mist-400 mt-2">{files.length} of {art.data.files.length} files. {art.data.note}</p>
            </>
          )}
        </QueryState>
      </Panel>
    </>
  );
}
