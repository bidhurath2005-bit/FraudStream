import { useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Lock, Pencil, RefreshCw } from "lucide-react";
import { ApiError, api, getAccessToken, qs } from "../services/api";
import { useAuth } from "../services/auth";
import {
  ConfirmDialog, EmptyState, HealthPill, KV, Notice, PageHeader, Pagination, Panel, QueryState, Tabs, cx, fmt,
} from "../components/ui";
import type {
  ActiveModels, AuditEntry, Dataset, InfrastructureStatus, LastChange, Page, RiskRule, RiskSettingsResponse,
  RiskSettingsValues, SettingDoc, SystemStatus, User,
} from "../types/api";

type Tab = "rules" | "scoring" | "drift" | "account" | "system" | "audit";
const TABS: { id: Tab; label: string }[] = [
  { id: "rules", label: "Risk rules" }, { id: "scoring", label: "Risk scoring" }, { id: "drift", label: "Drift monitoring" },
  { id: "account", label: "Account & access" }, { id: "system", label: "System" }, { id: "audit", label: "Settings audit trail" },
];

const errText = (e: unknown) => (e instanceof ApiError && e.status === 0 ? "Cannot reach the FraudStream API." : e instanceof Error ? e.message : String(e));
const isConflict = (e: unknown) => e instanceof ApiError && e.status === 409;
const lastChangeText = (lc: LastChange | null | undefined, updatedAt?: string | null) =>
  lc ? `${fmt.dt(lc.at)} by ${lc.by}` : updatedAt ? `${fmt.dt(updatedAt)} (initial value — no change recorded)` : "—";

export function SettingsPage() {
  const [params, setParams] = useSearchParams();
  const tab = (TABS.some((t) => t.id === params.get("tab")) ? params.get("tab") : "rules") as Tab;
  const setTab = (t: Tab) => setParams((p) => { const n = new URLSearchParams(p); n.set("tab", t); return n; }, { replace: true });
  const { isAdmin } = useAuth();
  return (
    <div>
      <PageHeader title="Settings"
        subtitle="Risk-scoring configuration and drift thresholds stored in the database, your account, and read-only system information. Changes are validated, permission-checked and audited by the API." />
      {!isAdmin && (
        <div className="mb-4"><Notice tone="info" testId="readonly-notice">
          <span className="inline-flex items-start gap-1.5"><Lock className="h-4 w-4 mt-0.5 shrink-0" />
            You are signed in as an analyst: configuration is shown read-only. Changing rules, scoring or drift settings requires the
            administrator role — the API rejects (and audits) changes from other roles.</span>
        </Notice></div>
      )}
      <Tabs label="Settings sections" tabs={TABS} value={tab} onChange={setTab} />
      {tab === "rules" && <RulesTab />}
      {tab === "scoring" && <ScoringTab />}
      {tab === "drift" && <DriftTab />}
      {tab === "account" && <AccountTab />}
      {tab === "system" && <SystemTab />}
      {tab === "audit" && <AuditTab />}
    </div>
  );
}

/* ------------------------------------------------------------------ shared */
type Diff = { field: string; from: string; to: string };
function DiffList({ diffs }: { diffs: Diff[] }) {
  return (
    <ul className="space-y-1 mb-3" data-testid="change-list">
      {diffs.map((d) => (
        <li key={d.field} className="text-sm break-words"><span className="text-mist-300">{d.field}</span>: <span className="num text-coral-400 line-through">{d.from}</span>{" → "}
          <span className="num text-emerald-400">{d.to}</span></li>
      ))}
    </ul>
  );
}

function NumberField({ label, value, onChange, error, hint, step = "any", testId, disabled }: {
  label: string; value: string; onChange: (v: string) => void; error?: string | null; hint?: ReactNode; step?: string; testId?: string; disabled?: boolean;
}) {
  return (
    <label className="flex flex-col gap-1 text-xs text-mist-300 min-w-0">
      {label}
      <input className={cx("input py-1.5 num", error && "border-coral-500")} inputMode="decimal" type="number" step={step} value={value}
        onChange={(e) => onChange(e.target.value)} aria-invalid={!!error} data-testid={testId} disabled={disabled} />
      {error ? <span className="text-coral-400 text-[11px]" role="alert">{error}</span> : hint ? <span className="text-mist-400 text-[11px]">{hint}</span> : null}
    </label>
  );
}

function ResultNotice({ m }: { m: { tone: "ok" | "error"; text: ReactNode; conflict?: boolean; onReload?: () => void } | null }) {
  if (!m) return null;
  return (
    <div className="mt-3"><Notice tone={m.tone} testId="save-status">
      {m.text}
      {m.conflict && m.onReload && <button className="btn btn-secondary text-xs px-2 py-1 ml-2" onClick={m.onReload}><RefreshCw className="h-3 w-3" />Reload current values</button>}
    </Notice></div>
  );
}

type Msg = Parameters<typeof ResultNotice>[0]["m"];

const propagationText = (p: Record<string, string> | null | undefined) =>
  p ? ` Published runtime config — ${Object.entries(p).map(([k, v]) => `${k === "redis" ? "Redis (read by Spark)" : "file"}: ${v}`).join(", ")}.` : "";

/* ------------------------------------------------------------------ rules */
function RulesTab() {
  const q = useQuery({ queryKey: ["risk-rules"], queryFn: () => api<RiskRule[]>("/risk/rules") });
  const { isAdmin } = useAuth();
  const [editing, setEditing] = useState<string | null>(null);
  return (
    <div className="flex flex-col gap-4">
      <Panel title="How rules contribute to the risk score">
        <p className="text-sm text-mist-200 leading-relaxed">
          Each enabled rule that triggers contributes its <b>weight</b> (0–1). Rules are grouped into signal <b>families</b>; only the
          strongest triggered rule per family counts, so correlated signals are not double-counted. Contributions are combined with the
          model and anomaly components by noisy-OR into a 0–100 risk score (a heuristic, not a probability). Rule parameters are the
          thresholds each check compares against. Changes apply to new transactions immediately in the API and are published to the
          Spark job; transactions already scored keep their stored result.
        </p>
      </Panel>
      <QueryState q={q} empty={q.data?.length === 0 ? <EmptyState title="No risk rules configured" /> : false}>
        <div className="flex flex-col gap-3" data-testid="rules-list">
          {q.data?.map((r) => editing === r.code
            ? <RuleEditor key={r.code} rule={r} onDone={() => setEditing(null)} />
            : <RuleCard key={r.code} rule={r} canEdit={isAdmin && r.editable !== false && editing === null} onEdit={() => setEditing(r.code)} />)}
        </div>
      </QueryState>
    </div>
  );
}

function RuleCard({ rule: r, canEdit, onEdit }: { rule: RiskRule; canEdit: boolean; onEdit: () => void }) {
  const params = Object.entries(r.params);
  return (
    <section className="panel p-4 min-w-0" data-testid={`rule-${r.code}`} aria-label={r.name}>
      <div className="flex flex-wrap items-start gap-2 justify-between">
        <div className="min-w-0">
          <h3 className="text-sm font-semibold text-mist-100 break-words">{r.name}</h3>
          <div className="text-[11px] text-mist-400 num break-all">{r.code} · family: {r.family}</div>
        </div>
        <div className="flex items-center gap-2">
          <span data-testid={`rule-state-${r.code}`} className={cx("inline-flex rounded border px-1.5 py-0.5 text-[11px] font-semibold",
            r.enabled ? "text-emerald-400 border-emerald-500/40 bg-emerald-500/10" : "text-mist-300 border-ink-500 bg-ink-800")}>
            {r.enabled ? "ENABLED" : "DISABLED"}</span>
          {canEdit && <button className="btn btn-secondary text-xs px-2 py-1" onClick={onEdit} data-testid={`rule-edit-${r.code}`}><Pencil className="h-3 w-3" />Edit</button>}
        </div>
      </div>
      <p className="text-sm text-mist-200 mt-2 break-words">{r.description}</p>
      <div className="flex flex-wrap gap-x-6 gap-y-1 mt-2 text-xs">
        <span className="text-mist-400">Weight <span className="num text-mist-100" data-testid={`rule-weight-${r.code}`}>{r.weight.toFixed(2)}</span></span>
        {params.length ? params.map(([k, v]) => (
          <span key={k} className="text-mist-400">{k} <span className="num text-mist-100" data-testid={`rule-param-${r.code}-${k}`}>{v}</span></span>
        )) : <span className="text-mist-400">no parameters</span>}
        <span className="text-mist-400">Last updated <span className="text-mist-200">{lastChangeText(r.last_change, r.updated_at)}</span></span>
      </div>
    </section>
  );
}

function boundHint(b?: { integer: boolean; min: number; min_inclusive: boolean; max: number }) {
  if (!b) return undefined;
  return `${b.integer ? "whole number, " : ""}${b.min_inclusive ? "≥" : ">"} ${b.min} and ≤ ${b.max}`;
}
function checkBound(raw: string, b?: { integer: boolean; min: number; min_inclusive: boolean; max: number }): string | null {
  if (raw.trim() === "") return "required";
  const v = Number(raw);
  if (!Number.isFinite(v)) return "must be a number";
  if (!b) return v < 0 ? "must be ≥ 0" : null;
  if (b.integer && !Number.isInteger(v)) return "must be a whole number";
  if (b.min_inclusive ? v < b.min : v <= b.min) return `must be ${b.min_inclusive ? "≥" : ">"} ${b.min}`;
  if (v > b.max) return `must be ≤ ${b.max}`;
  return null;
}

function RuleEditor({ rule: r, onDone }: { rule: RiskRule; onDone: () => void }) {
  const qc = useQueryClient();
  const [enabled, setEnabled] = useState(r.enabled);
  const [weight, setWeight] = useState(String(r.weight));
  const [params, setParams] = useState<Record<string, string>>(Object.fromEntries(Object.entries(r.params).map(([k, v]) => [k, String(v)])));
  const [confirm, setConfirm] = useState(false);
  const [msg, setMsg] = useState<Msg>(null);
  const errors: Record<string, string | null> = {
    weight: weight.trim() === "" ? "required" : !Number.isFinite(Number(weight)) ? "must be a number" : Number(weight) < 0 || Number(weight) > 1 ? "must be between 0 and 1" : null,
    ...Object.fromEntries(Object.entries(params).map(([k, v]) => [k, checkBound(v, r.param_bounds?.[k])])),
  };
  const valid = Object.values(errors).every((e) => !e);
  const diffs: Diff[] = [];
  if (enabled !== r.enabled) diffs.push({ field: "enabled", from: String(r.enabled), to: String(enabled) });
  if (valid && Number(weight) !== r.weight) diffs.push({ field: "weight", from: String(r.weight), to: String(Number(weight)) });
  const changedParams = valid ? Object.fromEntries(Object.entries(params).filter(([k, v]) => Number(v) !== r.params[k]).map(([k, v]) => [k, Number(v)])) : {};
  Object.entries(changedParams).forEach(([k, v]) => diffs.push({ field: `params.${k}`, from: String(r.params[k]), to: String(v) }));
  const save = useMutation({
    mutationFn: () => api<RiskRule>(`/risk/rules/${encodeURIComponent(r.code)}`, {
      method: "PATCH",
      body: {
        ...(enabled !== r.enabled ? { enabled } : {}),
        ...(Number(weight) !== r.weight ? { weight: Number(weight) } : {}),
        ...(Object.keys(changedParams).length ? { params: changedParams } : {}),
        expected_updated_at: r.updated_at,
      },
    }),
    onSuccess: (res) => {
      setConfirm(false);
      qc.setQueryData<RiskRule[]>(["risk-rules"], (old) => old?.map((x) => (x.code === res.code ? res : x)));
      void qc.invalidateQueries({ queryKey: ["risk-rules"] });
      void qc.invalidateQueries({ queryKey: ["settings-audit"] });
      setMsg({ tone: "ok", text: `Saved ${r.name}: ${res.changed_fields?.join(", ") || "no change"}.${propagationText(res.propagation)}` });
    },
    onError: (e) => {
      setConfirm(false);
      setMsg({ tone: "error", text: isConflict(e) ? `Not saved — ${errText(e)}` : `Not saved: ${errText(e)}`, conflict: isConflict(e),
        onReload: () => { void qc.invalidateQueries({ queryKey: ["risk-rules"] }); onDone(); } });
    },
  });
  return (
    <section className="panel p-4 border-stream-500/40 min-w-0" data-testid={`rule-editor-${r.code}`} aria-label={`Edit ${r.name}`}>
      <h3 className="text-sm font-semibold text-mist-100 break-words">Edit: {r.name} <span className="num text-[11px] text-mist-400">{r.code}</span></h3>
      <p className="text-xs text-mist-400 mt-1 break-words">{r.description}</p>
      <form className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 mt-3" onSubmit={(e) => { e.preventDefault(); if (valid && diffs.length) setConfirm(true); }}>
        <label className="flex items-center gap-2 text-sm text-mist-200 sm:col-span-2 lg:col-span-4">
          <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} data-testid="rule-enabled" />
          Rule enabled
        </label>
        <NumberField label="Weight (0–1)" value={weight} onChange={setWeight} error={errors.weight} step="0.01" testId="rule-weight-input" hint="contribution when the rule triggers" />
        {Object.keys(params).map((k) => (
          <NumberField key={k} label={k} value={params[k]} onChange={(v) => setParams((p) => ({ ...p, [k]: v }))} error={errors[k]}
            hint={boundHint(r.param_bounds?.[k])} step={r.param_bounds?.[k]?.integer ? "1" : "any"} testId={`rule-param-input-${k}`} />
        ))}
        <div className="flex flex-wrap gap-2 sm:col-span-2 lg:col-span-4">
          <button type="submit" className="btn btn-primary" disabled={!valid || diffs.length === 0} data-testid="rule-review">Review changes</button>
          <button type="button" className="btn btn-secondary" onClick={onDone}>{msg?.tone === "ok" ? "Close" : "Cancel"}</button>
          {valid && diffs.length === 0 && <span className="text-xs text-mist-400 self-center">No changes yet.</span>}
        </div>
      </form>
      <ResultNotice m={msg} />
      <ConfirmDialog open={confirm} title={`Change ${r.name}?`} confirmLabel="Save changes" tone="danger" busy={save.isPending}
        onConfirm={() => save.mutate()} onCancel={() => setConfirm(false)} testId="confirm-rule">
        <DiffList diffs={diffs} />
        <p className="text-xs text-mist-400">This changes how new transactions are scored (API and Spark). The change is recorded in the audit trail with your username.</p>
      </ConfirmDialog>
    </section>
  );
}

/* ------------------------------------------------------------------ scoring */
const SCORING_FIELDS: { key: keyof RiskSettingsValues; label: string; hint: string; step: string }[] = [
  { key: "medium_threshold", label: "MEDIUM threshold (risk score)", hint: "scores ≥ this are MEDIUM (review)", step: "0.5" },
  { key: "high_threshold", label: "HIGH threshold (risk score)", hint: "scores ≥ this are HIGH; must be > MEDIUM and ≤ 100", step: "0.5" },
  { key: "model_weight", label: "Model weight (0–1)", hint: "weight of the supervised model component", step: "0.01" },
  { key: "anomaly_weight", label: "Anomaly weight (0–1)", hint: "weight of the anomaly-detector component", step: "0.01" },
  { key: "anomaly_min_percentile", label: "Anomaly minimum percentile [0–1)", hint: "only anomalies above this training percentile contribute", step: "0.01" },
];

export function validateScoring(v: Record<string, string>): Record<string, string | null> {
  const n = (k: string) => Number(v[k]);
  const e: Record<string, string | null> = {};
  for (const f of SCORING_FIELDS) e[f.key] = v[f.key].trim() === "" || !Number.isFinite(n(f.key)) ? "must be a number" : null;
  if (!e.medium_threshold && (n("medium_threshold") < 1 || n("medium_threshold") > 99)) e.medium_threshold = "must be between 1 and 99";
  if (!e.high_threshold && (n("high_threshold") <= n("medium_threshold") || n("high_threshold") > 100)) e.high_threshold = "must be > MEDIUM threshold and ≤ 100";
  for (const k of ["model_weight", "anomaly_weight"]) if (!e[k] && (n(k) < 0 || n(k) > 1)) e[k] = "must be between 0 and 1";
  if (!e.anomaly_min_percentile && (n("anomaly_min_percentile") < 0 || n("anomaly_min_percentile") >= 1)) e.anomaly_min_percentile = "must be ≥ 0 and < 1";
  return e;
}

function ScoringTab() {
  const q = useQuery({ queryKey: ["risk-settings"], queryFn: () => api<RiskSettingsResponse>("/risk/settings") });
  const { isAdmin } = useAuth();
  const [msg, setMsg] = useState<Msg>(null);   // owned here: the form remounts when a save changes updated_at
  return (
    <>
      <QueryState q={q}>
        {q.data && <ScoringForm key={q.data.updated_at ?? "x"} data={q.data} editable={isAdmin} setMsg={setMsg} />}
      </QueryState>
      <ResultNotice m={msg} />
    </>
  );
}

function ScoringForm({ data, editable, setMsg }: { data: RiskSettingsResponse; editable: boolean; setMsg: (m: Msg) => void }) {
  const qc = useQueryClient();
  const cur = data.settings as unknown as RiskSettingsValues;
  const defaults = data.defaults as unknown as RiskSettingsValues;
  const init = () => Object.fromEntries(SCORING_FIELDS.map((f) => [f.key, String(cur[f.key])]));
  const [vals, setVals] = useState<Record<string, string>>(init);
  const [block, setBlock] = useState(cur.block_simulation_enabled);
  const [confirm, setConfirm] = useState(false);
  const errors = validateScoring(vals);
  const valid = Object.values(errors).every((e) => !e);
  const diffs: Diff[] = SCORING_FIELDS.filter((f) => valid && Number(vals[f.key]) !== cur[f.key])
    .map((f) => ({ field: f.key, from: String(cur[f.key]), to: String(Number(vals[f.key])) }));
  if (block !== cur.block_simulation_enabled) diffs.push({ field: "block_simulation_enabled", from: String(cur.block_simulation_enabled), to: String(block) });
  const save = useMutation({
    mutationFn: () => api<RiskSettingsResponse>("/risk/settings", {
      method: "PUT",
      body: { ...Object.fromEntries(SCORING_FIELDS.map((f) => [f.key, Number(vals[f.key])])), block_simulation_enabled: block,
        expected_updated_at: data.updated_at },
    }),
    onSuccess: (res) => {
      setConfirm(false);
      qc.setQueryData(["risk-settings"], res);
      void qc.invalidateQueries({ queryKey: ["settings-audit"] });
      const s = res.settings as unknown as RiskSettingsValues;
      setMsg({ tone: "ok", text: res.changed
        ? `Saved. Active configuration: MEDIUM ≥ ${s.medium_threshold}, HIGH ≥ ${s.high_threshold}, model weight ${s.model_weight}, anomaly weight ${s.anomaly_weight}.${propagationText(res.propagation)}`
        : "No change: the submitted values equal the active configuration." });
    },
    onError: (e) => {
      setConfirm(false);
      setMsg({ tone: "error", text: `Not saved — ${errText(e)}. The previous configuration is still active.`, conflict: isConflict(e),
        onReload: () => void qc.invalidateQueries({ queryKey: ["risk-settings"] }) });
    },
  });
  return (
    <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,20rem)] gap-4 items-start">
      <Panel title="Risk scoring configuration (active)">
        <form className="grid grid-cols-1 sm:grid-cols-2 gap-3" data-testid="scoring-form" onSubmit={(e) => { e.preventDefault(); if (valid && diffs.length) setConfirm(true); }}>
          {SCORING_FIELDS.map((f) => (
            <NumberField key={f.key} label={f.label} value={vals[f.key]} onChange={(v) => setVals((s) => ({ ...s, [f.key]: v }))}
              error={editable ? errors[f.key] : null} step={f.step} testId={`scoring-${f.key}`} disabled={!editable}
              hint={<>{f.hint} · default <span className="num">{String(defaults[f.key])}</span></>} />
          ))}
          <label className="flex items-start gap-2 text-sm text-mist-200 sm:col-span-2">
            <input type="checkbox" className="mt-1" checked={block} onChange={(e) => setBlock(e.target.checked)} disabled={!editable} data-testid="scoring-block" />
            <span>HIGH risk recommends <b>BLOCK_SIMULATION</b> (otherwise REVIEW). Affects the educational simulation only — nothing real is ever blocked.
              <span className="block text-[11px] text-mist-400">default {String(defaults.block_simulation_enabled)}</span></span>
          </label>
          {editable && (
            <div className="flex flex-wrap gap-2 sm:col-span-2">
              <button type="submit" className="btn btn-primary" disabled={!valid || diffs.length === 0} data-testid="scoring-review">Review changes</button>
              <button type="button" className="btn btn-secondary" disabled={diffs.length === 0 && valid} onClick={() => { setVals(init()); setBlock(cur.block_simulation_enabled); setMsg(null); }}>Discard</button>
            </div>
          )}
        </form>
      </Panel>
      <Panel title="About this configuration">
        <KV k="Used by" v={data.used_by ?? "—"} />
        <KV k="Last updated" v={<span data-testid="scoring-last-change">{lastChangeText(data.last_change, data.updated_at)}</span>} />
        <KV k="Editable by" v={data.editable_by ?? "ADMIN"} />
        <p className="text-xs text-mist-400 mt-3 leading-relaxed">Categories: LOW &lt; MEDIUM threshold ≤ MEDIUM &lt; HIGH threshold ≤ HIGH. Already-scored transactions keep their stored category; only new transactions use a changed configuration.</p>
      </Panel>
      <ConfirmDialog open={confirm} title="Change the risk-scoring configuration?" confirmLabel="Save configuration" tone="danger" busy={save.isPending}
        onConfirm={() => save.mutate()} onCancel={() => setConfirm(false)} testId="confirm-scoring">
        <DiffList diffs={diffs} />
        <p className="text-xs text-mist-400">New transactions are scored with these values immediately (API and Spark). Recorded in the audit trail with your username.</p>
      </ConfirmDialog>
    </div>
  );
}

/* ------------------------------------------------------------------ drift */
const DRIFT_FIELDS = [
  { key: "moderate", label: "Moderate PSI threshold", hint: "PSI ≥ this is MODERATE drift", step: "0.01" },
  { key: "significant", label: "Significant PSI threshold", hint: "PSI ≥ this is SIGNIFICANT; must be > moderate and ≤ 10", step: "0.01" },
  { key: "window_rows", label: "Window (most recent predictions)", hint: "", step: "1" },
  { key: "min_rows", label: "Minimum rows for a check", hint: "fewer rows → INSUFFICIENT_DATA", step: "1" },
] as const;

function DriftTab() {
  const q = useQuery({ queryKey: ["setting", "drift"], queryFn: () => api<SettingDoc>("/settings/drift") });
  const { isAdmin } = useAuth();
  const [msg, setMsg] = useState<Msg>(null);   // owned here: the form remounts when a save changes updated_at
  return (
    <>
      <QueryState q={q}>{q.data && <DriftForm key={q.data.updated_at ?? "x"} doc={q.data} editable={isAdmin} setMsg={setMsg} />}</QueryState>
      <ResultNotice m={msg} />
    </>
  );
}

function DriftForm({ doc, editable, setMsg }: { doc: SettingDoc; editable: boolean; setMsg: (m: Msg) => void }) {
  const qc = useQueryClient();
  const cur = doc.value as Record<string, number>;
  const [vals, setVals] = useState<Record<string, string>>(Object.fromEntries(DRIFT_FIELDS.map((f) => [f.key, String(cur[f.key])])));
  const [confirm, setConfirm] = useState(false);
  const n = (k: string) => Number(vals[k]);
  const errors: Record<string, string | null> = {};
  for (const f of DRIFT_FIELDS) errors[f.key] = vals[f.key].trim() === "" || !Number.isFinite(n(f.key)) ? "must be a number" : null;
  for (const k of ["window_rows", "min_rows"] as const) {
    const b = doc.bounds?.[k];
    if (!errors[k] && !Number.isInteger(n(k))) errors[k] = "must be a whole number";
    else if (!errors[k] && b && (n(k) < b.min || n(k) > b.max)) errors[k] = `must be between ${b.min} and ${b.max}`;
  }
  if (!errors.moderate && n("moderate") <= 0) errors.moderate = "must be > 0";
  if (!errors.significant && (n("significant") <= n("moderate") || n("significant") > 10)) errors.significant = "must be > moderate and ≤ 10";
  if (!errors.min_rows && !errors.window_rows && n("min_rows") > n("window_rows")) errors.min_rows = "must not exceed the window";
  const valid = Object.values(errors).every((e) => !e);
  const changed = valid ? Object.fromEntries(DRIFT_FIELDS.filter((f) => n(f.key) !== cur[f.key]).map((f) => [f.key, n(f.key)])) : {};
  const diffs = Object.entries(changed).map(([k, v]) => ({ field: k, from: String(cur[k]), to: String(v) }));
  const save = useMutation({
    mutationFn: () => api<Record<string, unknown>>("/settings/drift", { method: "PUT", body: { value: changed, expected_updated_at: doc.updated_at } }),
    onSuccess: () => {
      setConfirm(false);
      void qc.invalidateQueries({ queryKey: ["setting", "drift"] });
      void qc.invalidateQueries({ queryKey: ["settings-audit"] });
      setMsg({ tone: "ok", text: `Saved drift settings (${diffs.map((d) => `${d.field} ${d.to}`).join(", ")}). Used by the next drift check.` });
    },
    onError: (e) => {
      setConfirm(false);
      setMsg({ tone: "error", text: `Not saved — ${errText(e)}. The previous settings are still active.`, conflict: isConflict(e),
        onReload: () => void qc.invalidateQueries({ queryKey: ["setting", "drift"] }) });
    },
  });
  return (
    <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,20rem)] gap-4 items-start">
      <Panel title="Drift monitoring thresholds (active)">
        <form className="grid grid-cols-1 sm:grid-cols-2 gap-3" data-testid="drift-form" onSubmit={(e) => { e.preventDefault(); if (valid && diffs.length) setConfirm(true); }}>
          {DRIFT_FIELDS.map((f) => {
            const b = doc.bounds?.[f.key];
            return (
              <NumberField key={f.key} label={f.label} value={vals[f.key]} onChange={(v) => setVals((s) => ({ ...s, [f.key]: v }))}
                error={editable ? errors[f.key] : null} step={f.step} testId={`drift-${f.key}`} disabled={!editable}
                hint={<>{f.hint}{b ? `${f.hint ? " · " : ""}${b.min}–${b.max}` : ""} · default <span className="num">{String(doc.defaults[f.key])}</span></>} />
            );
          })}
          {editable && (
            <div className="flex flex-wrap gap-2 sm:col-span-2">
              <button type="submit" className="btn btn-primary" disabled={!valid || diffs.length === 0} data-testid="drift-review">Review changes</button>
            </div>
          )}
        </form>
      </Panel>
      <Panel title="About this setting">
        <KV k="Used by" v={doc.used_by ?? "not read by any component"} />
        <KV k="Last updated" v={<span data-testid="drift-last-change">{lastChangeText(doc.last_change, doc.updated_at)}</span>} />
        <KV k="Editable by" v={doc.editable_by} />
        <p className="text-xs text-mist-400 mt-3">Affects drift checks only (status thresholds and sample size) — not risk scoring.</p>
      </Panel>
      <ConfirmDialog open={confirm} title="Change the drift thresholds?" confirmLabel="Save" busy={save.isPending}
        onConfirm={() => save.mutate()} onCancel={() => setConfirm(false)} testId="confirm-drift">
        <DiffList diffs={diffs} />
        <p className="text-xs text-mist-400">Applies to the next drift check. Recorded in the audit trail.</p>
      </ConfirmDialog>
    </div>
  );
}

/* ------------------------------------------------------------------ account */
/** Reads only the `exp` claim of the in-memory access token (the token itself is never displayed). */
export function tokenExpiry(token: string | null): Date | null {
  try {
    const part = token?.split(".")[1];
    if (!part) return null;
    const json = JSON.parse(atob(part.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(part.length / 4) * 4, "=")));
    return typeof json.exp === "number" ? new Date(json.exp * 1000) : null;
  } catch {
    return null;
  }
}

const CAPABILITIES: [string, boolean, boolean][] = [
  ["View dashboards, live stream, analytics, map, models, streaming status", true, true],
  ["Triage alerts, notes, open and resolve investigations", true, true],
  ["Run simulations and inject scenarios (Simulation Lab)", true, true],
  ["Generate and download reports (except system activity)", true, true],
  ["Generate system activity reports and read the audit trail", false, true],
  ["Change risk rules, risk scoring and drift settings", false, true],
  ["Activate / roll back models, upload datasets", false, true],
  ["Re-open closed alerts and investigations; reassign others' work", false, true],
  ["Create and modify user accounts (API only — no UI)", false, true],
];

function AccountTab() {
  const me = useQuery({ queryKey: ["me"], queryFn: () => api<User>("/auth/me") });
  const { isAdmin } = useAuth();
  const users = useQuery({ queryKey: ["users"], queryFn: () => api<User[]>("/users"), enabled: isAdmin });
  // recomputed whenever /auth/me is refetched (the token may have been refreshed meanwhile)
  const exp = useMemo(() => tokenExpiry(getAccessToken()), [me.dataUpdatedAt]);
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 items-start">
      <Panel title="Your account">
        <QueryState q={me}>
          {me.data && (
            <div data-testid="account-info">
              <KV k="Username" v={<span className="num">{me.data.username}</span>} />
              <KV k="Full name" v={me.data.full_name || "—"} />
              <KV k="Role" v={<span data-testid="account-role">{me.data.role}</span>} />
              <KV k="Account status" v={me.data.is_active ? "active" : "inactive"} />
              <KV k="Account created" v={fmt.dt(me.data.created_at)} />
              <KV k="Last sign-in" v={fmt.dt(me.data.last_login_at)} />
            </div>
          )}
        </QueryState>
      </Panel>
      <Panel title="Session">
        <KV k="Access token expires" v={<span data-testid="session-expiry">{exp ? fmt.dt(exp.toISOString()) : "—"}</span>} />
        <KV k="Renewal" v="automatic, via an httpOnly refresh cookie" />
        <p className="text-xs text-mist-400 mt-3 leading-relaxed">The access token is kept in memory only and is never shown. Signing out revokes the refresh token on the server.
          No per-account preferences are stored by the backend, so there are none to edit here.</p>
      </Panel>
      <Panel title="Role permissions (enforced by the API)" className="lg:col-span-2">
        <div className="overflow-x-auto">
          <table className="w-full text-left" data-testid="permissions-table">
            <thead><tr><th className="th">Capability</th><th className="th">Analyst</th><th className="th">Admin</th></tr></thead>
            <tbody>
              {CAPABILITIES.map(([c, a, ad]) => (
                <tr key={c}><td className="td whitespace-normal min-w-[14rem]">{c}</td>
                  <td className={cx("td", a ? "text-emerald-400" : "text-mist-400")}>{a ? "yes" : "no"}</td>
                  <td className={cx("td", ad ? "text-emerald-400" : "text-mist-400")}>{ad ? "yes" : "no"}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      {isAdmin && (
        <Panel title="Accounts (read-only)" className="lg:col-span-2">
          <p className="text-xs text-mist-400 mb-2">Accounts are managed through the API (<code className="num">/users</code>); this page does not create or edit them.</p>
          <QueryState q={users}>
            <div className="overflow-x-auto">
              <table className="w-full text-left" data-testid="users-table">
                <thead><tr><th className="th">Username</th><th className="th">Role</th><th className="th">Status</th><th className="th">Last sign-in</th></tr></thead>
                <tbody>{users.data?.map((u) => (
                  <tr key={u.id}><td className="td num">{u.username}</td><td className="td">{u.role}</td><td className="td">{u.is_active ? "active" : "inactive"}</td>
                    <td className="td num text-xs">{fmt.dt(u.last_login_at)}</td></tr>
                ))}</tbody>
              </table>
            </div>
          </QueryState>
        </Panel>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ system */
function SystemTab() {
  const sys = useQuery({ queryKey: ["system-status"], queryFn: () => api<SystemStatus>("/system/status"), refetchInterval: 30000 });
  const infra = useQuery({ queryKey: ["infrastructure"], queryFn: () => api<InfrastructureStatus>("/infrastructure/status"), refetchInterval: 30000 });
  const models = useQuery({ queryKey: ["models-active"], queryFn: () => api<ActiveModels>("/models/active") });
  const ds = useQuery({ queryKey: ["datasets"], queryFn: () => api<{ datasets: Dataset[] }>("/datasets") });
  const unused = useQuery({
    queryKey: ["settings-unused"],
    queryFn: async () => (await Promise.all(["simulation_defaults", "preferences"].map((k) => api<SettingDoc>(`/settings/${k}`)))).filter((d) => !d.used_by),
  });
  return (
    <div className="flex flex-col gap-4">
      <Notice tone="info">Read-only system information reported by the running API. Nothing on this tab is configurable from the dashboard; deployment settings come from the environment and are not displayed.</Notice>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 items-start">
        <Panel title="Application">
          <QueryState q={sys}>
            {sys.data && (
              <div data-testid="system-app">
                <KV k="Profile" v={<span data-testid="system-profile">{sys.data.profile}</span>} />
                <KV k="Description" v={sys.data.profile_description} />
                <KV k="Environment" v={sys.data.environment} />
                <KV k="API version" v={sys.data.version} />
                <KV k="API started" v={fmt.dt(sys.data.started_at)} />
                <KV k="Stream processor" v={sys.data.stream_processor ?? "—"} />
                <KV k="Live feed broker" v={sys.data.live_broker} />
                <KV k="Rate limiter" v={sys.data.rate_limiter ?? "—"} />
                <KV k="Runtime config published" v={Object.entries(sys.data.runtime_config_publish ?? {}).map(([k, v]) => `${k}: ${v === "ok" ? "ok" : "error"}`).join(", ") || "—"} />
              </div>
            )}
          </QueryState>
        </Panel>
        <Panel title="Services">
          <QueryState q={infra}>
            {infra.data && (
              <div data-testid="system-services">
                <KV k="Database" v={<span className="flex items-center gap-2 justify-end"><HealthPill state={infra.data.database.status} testId="sys-db" />
                  <span className="text-xs text-mist-300">{infra.data.database.dialect ?? ""}{infra.data.database.latency_ms != null ? ` · ${fmt.ms(infra.data.database.latency_ms)}` : ""}</span></span>} />
                <KV k="Redis" v={<HealthPill state={infra.data.redis.status} testId="sys-redis" />} />
                <KV k="Kafka" v={<span className="flex items-center gap-2 justify-end"><HealthPill state={infra.data.kafka.status} testId="sys-kafka" />
                  {infra.data.kafka.brokers != null && <span className="text-xs text-mist-300">{infra.data.kafka.brokers} broker(s), {infra.data.kafka.topics?.length ?? 0} topics</span>}</span>} />
                <KV k="Spark" v={<span className="flex items-center gap-2 justify-end"><HealthPill state={infra.data.spark.status} testId="sys-spark" />
                  {infra.data.spark.master && <span className="text-xs text-mist-300 num">{infra.data.spark.spark_version} · {infra.data.spark.master}</span>}</span>} />
                {infra.data.consumer && <KV k="Persistence consumer" v={<HealthPill state={infra.data.consumer.status} />} />}
                {(infra.data.kafka.note || infra.data.spark.note) && <p className="text-xs text-mist-400 mt-2">{infra.data.kafka.note} {infra.data.spark.note}</p>}
              </div>
            )}
          </QueryState>
        </Panel>
        <Panel title="Active models">
          <QueryState q={models}>
            {models.data && (
              <div data-testid="system-models">
                {(["supervised", "anomaly"] as const).map((k) => {
                  const m = models.data!.active[k];
                  return <KV key={k} k={k === "supervised" ? "Supervised model" : "Anomaly model"} v={m ? <span className="num">{m.model_name}:{m.version}</span> : "none"} />;
                })}
                <KV k="Loaded in the API" v={<span className="num">{models.data.loaded_in_api.supervised ?? "—"}</span>} />
                <KV k="Feature config" v={models.data.feature_config_version} />
                <KV k="Model artifacts" v={models.data.artifacts_writable ? "writable" : "read-only (activation disabled)"} />
                {models.data.model_error && <p className="text-xs text-coral-400 mt-2">{models.data.model_error}</p>}
              </div>
            )}
          </QueryState>
        </Panel>
        <Panel title="Data sources">
          <QueryState q={ds} empty={ds.data && ds.data.datasets.length === 0 ? <EmptyState title="No registered datasets" /> : false}>
            <div className="overflow-x-auto">
              <table className="w-full text-left" data-testid="system-datasets">
                <thead><tr><th className="th">Dataset</th><th className="th">Source</th><th className="th text-right">Rows</th><th className="th">SHA-256</th></tr></thead>
                <tbody>{ds.data?.datasets.map((d) => (
                  <tr key={d.id}><td className="td">{d.name} <span className="num text-[11px] text-mist-400">{d.version}</span></td><td className="td">{d.source}</td>
                    <td className="td num text-right">{fmt.num(d.rows)}</td><td className="td num text-[11px]">{d.content_sha256?.slice(0, 12) ?? "—"}</td></tr>
                ))}</tbody>
              </table>
            </div>
            <p className="text-xs text-mist-400 mt-2">Live transactions come from the built-in simulator (synthetic).</p>
          </QueryState>
        </Panel>
      </div>
      {unused.data && unused.data.length > 0 && (
        <p className="text-xs text-mist-400" data-testid="unused-settings">
          Stored but not read by any component (so not offered for editing): {unused.data.map((d) => d.key).join(", ")}.
        </p>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ audit */
function AuditTab() {
  const { isAdmin } = useAuth();
  const [page, setPage] = useState(1);
  const size = 25;
  const q = useQuery({
    queryKey: ["settings-audit", page], enabled: isAdmin,
    queryFn: () => api<Page<AuditEntry>>(`/audit${qs({ entity_type: "risk_rule,system_setting", page, size })}`),
  });
  if (!isAdmin) {
    return <Panel title="Settings audit trail"><Notice tone="info" testId="audit-restricted">The audit trail is restricted to administrators (enforced by the API).</Notice></Panel>;
  }
  return (
    <Panel title="Settings audit trail" actions={<button className="btn btn-ghost text-xs px-2 py-1" onClick={() => void q.refetch()}><RefreshCw className="h-3 w-3" />Refresh</button>}>
      <p className="text-xs text-mist-400 mb-2">Every change to risk rules and settings, plus rejected (validation, conflict) and denied (wrong role) attempts. Newest first.</p>
      <QueryState q={q} empty={q.data?.items.length === 0 ? <EmptyState title="No configuration changes recorded yet" /> : false}>
        {q.data && (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-left" data-testid="audit-table">
                <thead><tr><th className="th">Time</th><th className="th">Actor</th><th className="th">Action</th><th className="th">Setting</th>
                  <th className="th">Field</th><th className="th">Previous value</th><th className="th">New value / reason</th></tr></thead>
                <tbody>{q.data.items.map((a) => {
                  const failed = a.action.endsWith("_REJECTED") || a.action === "CONFIG_CHANGE_DENIED";
                  const reason = (a.details as { reason?: string }).reason;
                  return (
                    <tr key={a.id} data-testid={`audit-${a.id}`}>
                      <td className="td num text-xs">{fmt.dt(a.created_at)}</td>
                      <td className="td">{a.actor}</td>
                      <td className="td"><span className={cx("text-[11px] font-semibold", failed ? "text-coral-400" : "text-emerald-400")}>{a.action}</span></td>
                      <td className="td num text-xs">{a.entity_type}:{a.entity_id}</td>
                      <td className="td text-xs">{a.field ?? "—"}</td>
                      <td className="td num text-[11px] whitespace-pre-wrap break-all min-w-[10rem] max-w-[22rem]">{a.old_value ?? "—"}</td>
                      <td className="td num text-[11px] whitespace-pre-wrap break-all min-w-[10rem] max-w-[22rem]">{failed ? (reason ?? "denied: insufficient role") : a.new_value ?? "—"}</td>
                    </tr>
                  );
                })}</tbody>
              </table>
            </div>
            <Pagination page={page} size={size} total={q.data.total} onPage={setPage} />
          </>
        )}
      </QueryState>
    </Panel>
  );
}
