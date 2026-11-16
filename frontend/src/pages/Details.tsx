import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useForm } from "react-hook-form";
import { api } from "../services/api";
import { useAuth } from "../services/auth";
import { EmptyState, KV, PageHeader, Panel, QueryState, RiskBadge, StatusBadge, cx, fmt } from "../components/ui";
import type { AlertDetail, AlertStatus, AuditEntry, InvestigationDetail, Note, Prediction } from "../types/api";

const ALERT_NEXT: Record<AlertStatus, AlertStatus[]> = {
  NEW: ["UNDER_REVIEW", "DISMISSED"],
  UNDER_REVIEW: ["CONFIRMED_SUSPICIOUS", "DISMISSED", "NEW"],
  CONFIRMED_SUSPICIOUS: ["CLOSED", "UNDER_REVIEW"],
  DISMISSED: ["CLOSED", "UNDER_REVIEW"],
  CLOSED: ["UNDER_REVIEW"],
};

export function AuditTrail({ entries }: { entries: AuditEntry[] }) {
  if (!entries.length) return <EmptyState title="No audit entries" />;
  return (
    <ol className="space-y-2 text-xs" data-testid="audit-trail">
      {entries.map((a) => (
        <li key={a.id} className="flex gap-3 border-l-2 border-ink-600 pl-3">
          <span className="num text-mist-400 shrink-0">{fmt.dt(a.created_at)}</span>
          <span><b className="text-mist-100">{a.actor}</b> · <span className="text-stream-400">{a.action}</span>
            {a.field && <> · {a.field}: <span className="text-mist-400">{a.old_value ?? "∅"}</span> → <span className="text-mist-100">{a.new_value ?? "∅"}</span></>}</span>
        </li>
      ))}
    </ol>
  );
}

function Notes({ notes, onAdd, pending }: { notes: Note[]; onAdd: (b: string) => void; pending: boolean }) {
  const { register, handleSubmit, reset } = useForm<{ body: string }>();
  return (
    <div>
      <ul className="space-y-2 mb-3" data-testid="notes">
        {notes.map((n) => (
          <li key={n.id} className="rounded-md bg-ink-850 border border-ink-700 px-3 py-2 text-sm">
            <div className="text-[11px] text-mist-400 mb-1"><b className="text-mist-200">{n.author}</b> · {fmt.dt(n.created_at)}</div>
            <p className="whitespace-pre-wrap">{n.body}</p>
          </li>
        ))}
        {!notes.length && <li className="text-xs text-mist-400">No notes yet.</li>}
      </ul>
      <form onSubmit={handleSubmit((f) => { onAdd(f.body); reset(); })} className="flex flex-col gap-2">
        <textarea className="input min-h-[70px]" placeholder="Add an analyst note…" data-testid="note-input"
          {...register("body", { required: true, maxLength: 5000 })} />
        <button className="btn btn-secondary self-end" type="submit" disabled={pending} data-testid="add-note">Add note</button>
      </form>
    </div>
  );
}

function Explanation({ p }: { p: Prediction }) {
  const max = Math.max(...p.contributions.map((c) => Math.abs(c.contribution)), 1e-9);
  return (
    <div className="space-y-3">
      <p className="text-sm leading-relaxed text-mist-200" data-testid="explanation">{p.explanation}</p>
      <div className="grid sm:grid-cols-2 gap-x-6">
        <KV k="Model" v={<span className="num">{p.model_version ?? "unavailable (rules only)"}</span>} />
        <KV k="Model score / threshold" v={<span className="num">{p.model_score?.toFixed(4) ?? "—"} / {p.model_threshold?.toFixed(4) ?? "—"}</span>} />
        <KV k="Anomaly percentile" v={<span className="num">{p.anomaly_score != null ? fmt.pct(p.anomaly_score, 2) : "—"}</span>} />
        <KV k="Recommended action" v={p.recommended_action} />
        <KV k="Inference" v={fmt.ms(p.inference_ms)} />
        <KV k="Explanation method" v={p.explanation_method ?? "—"} />
      </div>
      {p.triggered_rules.length > 0 && (
        <div>
          <div className="panel-title mb-1">Triggered rules</div>
          <ul className="text-sm space-y-1">
            {p.triggered_rules.map((r) => <li key={r.code}><span className="num text-amber-400">{r.code}</span> <span className="text-mist-400">({r.family}, w={r.weight})</span> — {r.detail}</li>)}
          </ul>
        </div>
      )}
      {p.contributions.length > 0 && (
        <div>
          <div className="panel-title mb-1">Top feature contributions ({p.explanation_method})</div>
          <ul className="space-y-1">
            {p.contributions.map((c) => (
              <li key={c.feature} className="grid grid-cols-[150px_1fr_70px] items-center gap-2 text-xs">
                <span className="num truncate">{c.feature}</span>
                <span className="h-2 bg-ink-800 rounded relative">
                  <span className={cx("absolute top-0 h-2 rounded", c.contribution >= 0 ? "bg-coral-500 left-1/2" : "bg-emerald-500 right-1/2")}
                    style={{ width: `${(Math.abs(c.contribution) / max) * 50}%` }} />
                </span>
                <span className="num text-right">{c.contribution >= 0 ? "+" : ""}{c.contribution.toFixed(3)}</span>
              </li>
            ))}
          </ul>
          <p className="text-[11px] text-mist-400 mt-1">Positive values push towards higher risk. Values come from the deployed model for this exact prediction.</p>
        </div>
      )}
    </div>
  );
}

export function AlertDetailPage() {
  const { uid = "" } = useParams();
  const { user } = useAuth();
  const qc = useQueryClient();
  const q = useQuery({ queryKey: ["alert", uid], queryFn: () => api<AlertDetail>(`/alerts/${uid}`) });
  const [err, setErr] = useState<string | null>(null);
  const done = { onSuccess: () => { setErr(null); qc.invalidateQueries({ queryKey: ["alert", uid] }); qc.invalidateQueries({ queryKey: ["alerts"] }); },
    onError: (e: Error) => setErr(e.message) };
  const setStatus = useMutation({ mutationFn: (s: AlertStatus) => api(`/alerts/${uid}/status`, { method: "PATCH", body: { status: s } }), ...done });
  const assignMe = useMutation({ mutationFn: () => api(`/alerts/${uid}/assign`, { body: { user_id: user?.id } }), ...done });
  const addNote = useMutation({ mutationFn: (body: string) => api(`/alerts/${uid}/notes`, { body: { body } }), ...done });
  const a = q.data;
  return (
    <div>
      <PageHeader title={`Alert ${uid.slice(0, 8)}`} subtitle={<Link className="text-stream-400 hover:underline" to="/investigations">← Investigation Center</Link>}
        actions={a && <><RiskBadge category={a.risk_category} score={a.risk_score} /><StatusBadge status={a.status} /></>} />
      <QueryState q={q}>
        {a && (
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <div className="xl:col-span-2 space-y-4">
              <Panel title="Decision & explanation">{a.prediction ? <Explanation p={a.prediction} /> : <EmptyState title="No prediction record" />}</Panel>
              <Panel title="Transaction (synthetic)">
                <div className="grid sm:grid-cols-2 gap-x-6">
                  <KV k="Transaction" v={<span className="num">{a.transaction.transaction_id}</span>} />
                  <KV k="Customer" v={<span className="num">{a.customer_id}</span>} />
                  <KV k="Amount" v={<span className="num">{fmt.money(a.transaction.amount, a.transaction.currency)}</span>} />
                  <KV k="Merchant" v={`${a.transaction.merchant_id} · ${a.transaction.merchant_category}`} />
                  <KV k="Event time (UTC)" v={<span className="num">{a.transaction.event_time}</span>} />
                  <KV k="Channel / type" v={`${a.transaction.channel} / ${a.transaction.transaction_type}`} />
                  <KV k="Device" v={<span className="num">{a.transaction.device_id}</span>} />
                  <KV k="Simulated location" v={`${a.transaction.city}, ${a.transaction.country}`} />
                  <KV k="Auth status" v={a.transaction.status} />
                  <KV k="Simulator ground truth" v={a.transaction.sim_scenario ? `${a.transaction.sim_scenario} (evaluation only)` : "—"} />
                </div>
              </Panel>
              <Panel title="Audit trail"><AuditTrail entries={a.audit} /></Panel>
            </div>
            <div className="space-y-4">
              <Panel title="Actions">
                <div className="space-y-3">
                  <KV k="Assignee" v={a.assigned_to?.username ?? "unassigned"} />
                  {a.investigation && <KV k="Investigation" v={<Link className="text-stream-400 hover:underline" to={`/investigations/${a.investigation.case_uid}`}>{a.investigation.title}</Link>} />}
                  {a.assigned_to?.id !== user?.id && a.status !== "CLOSED" && (
                    <button className="btn btn-secondary w-full" onClick={() => assignMe.mutate()} disabled={assignMe.isPending}>Assign to me</button>
                  )}
                  <div className="flex flex-wrap gap-2" data-testid="status-actions">
                    {ALERT_NEXT[a.status].map((s) => (
                      <button key={s} className={cx("btn", s === "CONFIRMED_SUSPICIOUS" ? "btn-danger" : "btn-secondary")}
                        disabled={setStatus.isPending} onClick={() => setStatus.mutate(s)}>
                        {s === "UNDER_REVIEW" && a.status === "NEW" ? "Start review" : `→ ${s.replace(/_/g, " ").toLowerCase()}`}
                      </button>
                    ))}
                  </div>
                  {err && <p role="alert" className="text-xs text-coral-400">{err}</p>}
                  <p className="text-[11px] text-mist-400">Confirming marks the activity as suspicious for this educational simulation; it does not establish fraud.</p>
                </div>
              </Panel>
              <Panel title="Analyst notes"><Notes notes={a.notes} onAdd={(b) => addNote.mutate(b)} pending={addNote.isPending} /></Panel>
              <Panel title="Customer"><Link className="text-stream-400 text-sm hover:underline" to={`/investigations?tab=alerts&q=${a.customer_id}&status=`}>All alerts for {a.customer_id}</Link></Panel>
            </div>
          </div>
        )}
      </QueryState>
    </div>
  );
}

type Resolve = { final_classification: string; findings: string };

export function InvestigationDetailPage() {
  const { uid = "" } = useParams();
  const qc = useQueryClient();
  const q = useQuery({ queryKey: ["investigation", uid], queryFn: () => api<InvestigationDetail>(`/investigations/${uid}`) });
  const [err, setErr] = useState<string | null>(null);
  const done = { onSuccess: () => { setErr(null); qc.invalidateQueries({ queryKey: ["investigation", uid] }); },
    onError: (e: Error) => setErr(e.message) };
  const setStatus = useMutation({ mutationFn: (body: Record<string, string>) => api(`/investigations/${uid}/status`, { method: "PATCH", body }), ...done });
  const addNote = useMutation({ mutationFn: (body: string) => api(`/investigations/${uid}/notes`, { body: { body } }), ...done });
  const { register, handleSubmit, formState } = useForm<Resolve>({ defaultValues: { final_classification: "CONFIRMED_SUSPICIOUS" } });
  const inv = q.data;
  return (
    <div>
      <PageHeader title={inv?.title ?? "Investigation"} subtitle={<Link className="text-stream-400 hover:underline" to="/investigations?tab=cases">← Investigations</Link>}
        actions={inv && <StatusBadge status={inv.status} />} />
      <QueryState q={q}>
        {inv && (
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <div className="xl:col-span-2 space-y-4">
              <Panel title={`Linked alerts (${inv.alerts.length})`}>
                <table className="w-full"><thead><tr><th className="th">Alert</th><th className="th">Risk</th><th className="th">Rules</th><th className="th">Status</th></tr></thead>
                  <tbody>{inv.alerts.map((a) => (
                    <tr key={a.alert_uid}><td className="td"><Link className="num text-stream-400 hover:underline" to={`/alerts/${a.alert_uid}`}>{a.alert_uid.slice(0, 8)}</Link></td>
                      <td className="td"><RiskBadge category={a.risk_category} score={a.risk_score} /></td>
                      <td className="td text-xs text-mist-300">{a.triggered_rules.map((r) => r.code).join(", ") || "—"}</td>
                      <td className="td"><StatusBadge status={a.status} /></td></tr>))}</tbody></table>
              </Panel>
              <Panel title={`Customer ${inv.customer_id} · recent transactions (synthetic)`}>
                <div className="overflow-x-auto max-h-80 overflow-y-auto">
                  <table className="w-full"><thead className="sticky top-0 bg-ink-900"><tr><th className="th">Event time</th><th className="th">Amount</th><th className="th">Merchant</th><th className="th">Device</th><th className="th">City</th><th className="th">Risk</th></tr></thead>
                    <tbody>{inv.customer_transactions.map((t) => (
                      <tr key={t.id}><td className="td num text-mist-300">{fmt.dt(t.event_time)}</td><td className="td num">{fmt.money(t.amount, t.currency)}</td>
                        <td className="td text-xs">{t.merchant_category}</td><td className="td num text-xs">{t.device_id}</td><td className="td text-xs">{t.city}</td>
                        <td className="td">{t.risk_category && <RiskBadge category={t.risk_category} score={t.risk_score} />}</td></tr>))}</tbody></table>
                </div>
              </Panel>
              <Panel title="Audit trail"><AuditTrail entries={inv.audit} /></Panel>
            </div>
            <div className="space-y-4">
              <Panel title="Case">
                <KV k="Priority" v={inv.priority} /><KV k="Opened by" v={inv.opened_by} /><KV k="Assignee" v={inv.assigned_to?.username ?? "—"} />
                <KV k="Opened" v={fmt.dt(inv.created_at)} /><KV k="Outcome" v={inv.final_classification?.replace(/_/g, " ") ?? "—"} />
                {inv.findings && <p className="text-sm mt-2 whitespace-pre-wrap" data-testid="findings">{inv.findings}</p>}
                <div className="flex flex-wrap gap-2 mt-3" data-testid="inv-actions">
                  {inv.status === "OPEN" && <button className="btn btn-secondary" onClick={() => setStatus.mutate({ status: "IN_PROGRESS" })}>Start work</button>}
                  {inv.status === "RESOLVED" && <button className="btn btn-secondary" onClick={() => setStatus.mutate({ status: "CLOSED" })}>Close case</button>}
                  {inv.status === "RESOLVED" && <button className="btn btn-ghost" onClick={() => setStatus.mutate({ status: "IN_PROGRESS" })}>Re-open work</button>}
                </div>
                {inv.status === "IN_PROGRESS" && (
                  <form className="mt-3 space-y-2" onSubmit={handleSubmit((f) => setStatus.mutate({ status: "RESOLVED", ...f }))}>
                    <div className="panel-title">Record final findings</div>
                    <select className="input" {...register("final_classification")} data-testid="classification">
                      <option value="CONFIRMED_SUSPICIOUS">Confirmed suspicious</option>
                      <option value="NOT_SUSPICIOUS">Not suspicious</option>
                      <option value="INCONCLUSIVE">Inconclusive</option>
                    </select>
                    <textarea className="input min-h-[90px]" placeholder="Findings (min. 10 characters)" data-testid="findings-input"
                      {...register("findings", { required: "Findings are required", minLength: { value: 10, message: "At least 10 characters" } })} />
                    {formState.errors.findings && <p className="text-xs text-coral-400">{formState.errors.findings.message}</p>}
                    <button className="btn btn-primary w-full" type="submit" disabled={setStatus.isPending} data-testid="resolve">Resolve investigation</button>
                  </form>
                )}
                {err && <p role="alert" className="text-xs text-coral-400 mt-2">{err}</p>}
              </Panel>
              <Panel title="Investigation notes"><Notes notes={inv.notes} onAdd={(b) => addNote.mutate(b)} pending={addNote.isPending} /></Panel>
            </div>
          </div>
        )}
      </QueryState>
    </div>
  );
}
