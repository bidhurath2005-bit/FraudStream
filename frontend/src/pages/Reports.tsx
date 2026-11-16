import { useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Download, FileText, Lock } from "lucide-react";
import { ApiError, api, download, qs } from "../services/api";
import {
  EmptyState, Notice, PageHeader, Pagination, Panel, QueryState, Select, StatusBadge, Unavailable, cx,
} from "../components/ui";
import type {
  Page, ReportCatalogue, ReportDetail, ReportFilterKey, ReportMeta, ReportSection, ReportTypeKey,
  ReportValueFormat,
} from "../types/api";

/* ------------------------------------------------------------------ formatting (mirrors the PDF renderer) */
export function fmtReportValue(v: unknown, f: ReportValueFormat): string | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  switch (f) {
    case "int": return Number.isFinite(n) ? Math.round(n).toLocaleString("en-US") : String(v);
    case "pct": return `${(n * 100).toFixed(1)}%`;
    case "ratio": return n.toFixed(4);
    case "score": return n.toFixed(1);
    case "money":
    case "float2": return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    case "ms": return `${n.toFixed(1)} ms`;
    case "hours": return `${n.toFixed(1)} h`;
    case "datetime": {
      const d = new Date(String(v));
      return Number.isNaN(d.getTime()) ? String(v) : d.toISOString().replace("T", " ").slice(0, 19);
    }
    default: return String(v);
  }
}

const FILTER_LABEL: Record<ReportFilterKey, string> = {
  risk_level: "Risk level", alert_status: "Alert status", investigation_status: "Investigation status",
};
const PRESETS = [
  { id: "24h", label: "Last 24 hours", hours: 24 }, { id: "7d", label: "Last 7 days", hours: 24 * 7 },
  { id: "30d", label: "Last 30 days", hours: 24 * 30 }, { id: "custom", label: "Custom dates (UTC)", hours: 0 },
] as const;
type PresetId = (typeof PRESETS)[number]["id"];

const DAY = 86_400_000;
const utcDate = (d: Date) => d.toISOString().slice(0, 10);

/** The exact UTC period sent to the server (end exclusive), or a validation message. */
export function resolvePeriod(preset: PresetId, from: string, to: string, maxDays: number, now = new Date()):
  { start: Date; end: Date } | { error: string } {
  if (preset !== "custom") {
    const hours = PRESETS.find((p) => p.id === preset)!.hours;
    const end = new Date(Math.ceil(now.getTime() / 60_000) * 60_000);   // next whole minute: includes "now"
    return { start: new Date(end.getTime() - hours * 3_600_000), end };
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) return { error: "Choose a start and an end date." };
  const start = new Date(`${from}T00:00:00Z`);
  const end = new Date(new Date(`${to}T00:00:00Z`).getTime() + DAY);   // the end date is included in full
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return { error: "Invalid date." };
  if (start >= end) return { error: "The start date must not be after the end date." };
  if (end.getTime() - start.getTime() > maxDays * DAY) return { error: `The period must not exceed ${maxDays} days.` };
  return { start, end };
}
const fmtUtc = (d: Date) => `${d.toISOString().slice(0, 16).replace("T", " ")} UTC`;

function errMsg(e: unknown) {
  if (e instanceof ApiError && e.status === 0) return "Cannot reach the FraudStream API.";
  return e instanceof Error ? e.message : String(e);
}

/* ------------------------------------------------------------------ page */
export function ReportsPage() {
  const [params, setParams] = useSearchParams();
  const qc = useQueryClient();
  const selected = params.get("report");
  const select = (uid: string | null) => setParams((p) => {
    const n = new URLSearchParams(p);
    if (uid) n.set("report", uid); else n.delete("report");
    return n;
  }, { replace: false });

  const cat = useQuery({ queryKey: ["report-types"], queryFn: () => api<ReportCatalogue>("/reports/types"), staleTime: 300_000 });
  const detail = useQuery({
    queryKey: ["report", selected], enabled: !!selected,
    queryFn: () => api<ReportDetail>(`/reports/generated/${encodeURIComponent(selected!)}`),
  });

  return (
    <div>
      <PageHeader title="Reports"
        subtitle="Reports are computed by the server from persisted transactions, alerts, investigations, model records and the audit trail, stored as a snapshot, and exported as CSV or PDF." />
      <QueryState q={cat}>
        {cat.data && (
          <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,22rem)_minmax(0,1fr)] gap-4 items-start">
            <Generator cat={cat.data} onGenerated={(r) => {
              qc.setQueryData(["report", r.report_uid], r);
              void qc.invalidateQueries({ queryKey: ["report-history"] });
              select(r.report_uid);
            }} />
            <div className="min-w-0">
              {!selected && (
                <Panel title="Report preview">
                  <EmptyState title="No report selected" hint="Generate a report, or open one from the history below." />
                </Panel>
              )}
              {selected && (
                <QueryState q={detail}>
                  {detail.data && <ReportView r={detail.data} cat={cat.data} onClose={() => select(null)} />}
                </QueryState>
              )}
            </div>
          </div>
        )}
      </QueryState>
      <History cat={cat.data} selected={selected} onOpen={select} />
    </div>
  );
}

/* ------------------------------------------------------------------ generator */
function Generator({ cat, onGenerated }: { cat: ReportCatalogue; onGenerated: (r: ReportDetail) => void }) {
  const firstAvailable = cat.types.find((t) => t.available)?.key ?? "fraud_activity";
  const [type, setType] = useState<ReportTypeKey>(firstAvailable);
  const [preset, setPreset] = useState<PresetId>("7d");
  const today = utcDate(new Date());
  const [from, setFrom] = useState(utcDate(new Date(Date.now() - 6 * DAY)));
  const [to, setTo] = useState(today);
  const [filters, setFilters] = useState<Partial<Record<ReportFilterKey, string>>>({});
  const spec = cat.types.find((t) => t.key === type)!;
  const period = resolvePeriod(preset, from, to, cat.max_period_days);
  const gen = useMutation({
    mutationFn: () => {
      if ("error" in period) throw new Error(period.error);
      const applied = Object.fromEntries(spec.filters.map((f) => [f.key, filters[f.key] || undefined]).filter(([, v]) => v));
      return api<ReportDetail>("/reports", { body: { report_type: type, start: period.start.toISOString(), end: period.end.toISOString(), ...applied } });
    },
    onSuccess: (r) => onGenerated(r),
  });
  return (
    <Panel title="Generate a report">
      <form className="flex flex-col gap-3" onSubmit={(e) => { e.preventDefault(); gen.mutate(); }} aria-label="Generate a report">
        <label className="flex flex-col gap-1 text-xs text-mist-300">
          Report type
          <select className="input py-1.5" value={type} data-testid="report-type"
            onChange={(e) => { setType(e.target.value as ReportTypeKey); setFilters({}); gen.reset(); }}>
            {cat.types.map((t) => (
              <option key={t.key} value={t.key} disabled={!t.available}>{t.title}{t.admin_only ? " (admin only)" : ""}</option>
            ))}
          </select>
        </label>
        <p className="text-xs text-mist-400 leading-relaxed" data-testid="report-type-description">
          {spec.description} <span className="text-mist-300">Period basis: <code className="num">{spec.period_basis}</code>.</span>
        </p>
        {cat.types.some((t) => !t.available) && (
          <p className="text-[11px] text-mist-400 flex items-start gap-1.5"><Lock className="h-3 w-3 mt-0.5 shrink-0" />
            System activity reports read the audit trail and require the administrator role (enforced by the API).</p>
        )}
        <Select label="Reporting period" value={preset} onChange={(v) => setPreset(v as PresetId)} testId="report-preset"
          options={PRESETS.map((p) => ({ value: p.id, label: p.label }))} />
        {preset === "custom" && (
          <div className="grid grid-cols-2 gap-2">
            <label className="flex flex-col gap-1 text-xs text-mist-300 min-w-0">From (UTC)
              <input type="date" className="input py-1.5 min-w-0" value={from} max={to} onChange={(e) => setFrom(e.target.value)} data-testid="report-from" />
            </label>
            <label className="flex flex-col gap-1 text-xs text-mist-300 min-w-0">To (UTC, inclusive)
              <input type="date" className="input py-1.5 min-w-0" value={to} min={from} onChange={(e) => setTo(e.target.value)} data-testid="report-to" />
            </label>
          </div>
        )}
        <div className="text-xs" data-testid="report-period-preview">
          {"error" in period
            ? <span role="alert" className="text-coral-400">{period.error}</span>
            : <span className="text-mist-300 num">{fmtUtc(period.start)} → {fmtUtc(period.end)} <span className="text-mist-400">(end exclusive)</span></span>}
        </div>
        {spec.filters.length > 0 ? spec.filters.map((f) => (
          <Select key={f.key} label={`${FILTER_LABEL[f.key]} (server filter)`} value={filters[f.key] ?? ""} testId={`report-filter-${f.key}`}
            onChange={(v) => setFilters((s) => ({ ...s, [f.key]: v }))}
            options={[{ value: "", label: "All" }, ...f.options.map((o) => ({ value: o, label: o.replace(/_/g, " ") }))]} />
        )) : <p className="text-[11px] text-mist-400">This report type has no filters other than the period.</p>}
        <button type="submit" className="btn btn-primary" disabled={gen.isPending || "error" in period || !spec.available} data-testid="report-generate">
          <FileText className="h-4 w-4" />{gen.isPending ? "Generating…" : "Generate report"}
        </button>
        {gen.isPending && <span role="status" className="text-xs text-mist-400">The server is querying the database…</span>}
        {gen.isError && <Notice tone="error" testId="report-error">Report not generated: {errMsg(gen.error)}</Notice>}
        {gen.data?.status === "FAILED" && <Notice tone="error" testId="report-error">The report failed: {gen.data.error}</Notice>}
        <p className="text-[11px] text-mist-400 leading-relaxed">
          Generated reports are stored (history below). Detail tables keep up to {cat.detail_row_cap.toLocaleString()} rows; the
          preview shows {cat.preview_rows}, the PDF {cat.pdf_row_cap.toLocaleString()} per table, the CSV every stored row.
        </p>
      </form>
    </Panel>
  );
}

/* ------------------------------------------------------------------ export buttons */
function useExport() {
  const [state, setState] = useState<{ busy: string | null; msg: { tone: "ok" | "error"; text: string } | null }>({ busy: null, msg: null });
  const run = async (key: string, path: string, fallback: string) => {
    setState({ busy: key, msg: null });
    try {
      const r = await download(path, fallback);
      setState({ busy: null, msg: { tone: "ok", text: `Downloaded ${r.filename} (${(r.bytes / 1024).toFixed(1)} KB).` } });
    } catch (e) {
      setState({ busy: null, msg: { tone: "error", text: `Download failed: ${errMsg(e)}` } });
    }
  };
  return { ...state, run };
}

/* ------------------------------------------------------------------ report view */
function ReportView({ r, cat, onClose }: { r: ReportDetail; cat: ReportCatalogue; onClose: () => void }) {
  const ex = useExport();
  const c = r.content;
  const base = `/reports/generated/${encodeURIComponent(r.report_uid)}/download`;
  if (r.status !== "COMPLETED" || !c) {
    return (
      <Panel title={r.title} actions={<button className="btn btn-ghost text-xs" onClick={onClose}>Close</button>}>
        <Notice tone="error" testId="report-failed">This report failed to generate ({r.error ?? "no details"}); there is nothing to export.</Notice>
      </Panel>
    );
  }
  const filters = Object.entries(c.filters);
  return (
    <div className="flex flex-col gap-4 min-w-0" data-testid="report-view">
      <Panel title="Report" actions={<button className="btn btn-ghost text-xs" onClick={onClose}>Close</button>}>
        <h2 className="text-lg font-semibold text-mist-100 break-words" data-testid="report-title">{c.title}</h2>
        <dl className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-1 mt-2 text-sm">
          <Meta k="Report ID" v={<span className="num break-all" data-testid="report-uid">{c.report_uid}</span>} />
          <Meta k="Generated" v={<span className="num">{fmtReportValue(c.generated_at, "datetime")} UTC · {c.generated_by}</span>} />
          <Meta k="Reporting period" v={<span className="num" data-testid="report-period">{c.period.label}</span>} />
          <Meta k="Period basis" v={<code className="num text-xs">{c.period.basis}</code>} />
          <Meta k="Filters" v={<span data-testid="report-filters">{filters.length ? filters.map(([k, v]) => `${FILTER_LABEL[k as ReportFilterKey] ?? k}: ${v}`).join(" · ") : "none"}</span>} />
          <Meta k="Profile" v={c.profile} />
        </dl>
        <div className="flex flex-wrap gap-2 mt-3">
          <button className="btn btn-secondary" disabled={!!ex.busy} data-testid="export-csv"
            onClick={() => void ex.run("csv", `${base}?format=csv`, "report.csv")}>
            <Download className="h-4 w-4" />{ex.busy === "csv" ? "Preparing CSV…" : "CSV (all sections)"}</button>
          <button className="btn btn-secondary" disabled={!!ex.busy} data-testid="export-pdf"
            onClick={() => void ex.run("pdf", `${base}?format=pdf`, "report.pdf")}>
            <Download className="h-4 w-4" />{ex.busy === "pdf" ? "Preparing PDF…" : "PDF"}</button>
        </div>
        {ex.msg && <div className="mt-2"><Notice tone={ex.msg.tone} testId="export-status">{ex.msg.text}</Notice></div>}
      </Panel>

      <Panel title="Summary">
        <div className="grid grid-cols-1 min-[420px]:grid-cols-2 lg:grid-cols-3 gap-2" data-testid="report-summary">
          {c.summary.map((m) => {
            const v = fmtReportValue(m.value, m.format);
            return (
              <div key={m.key} className="rounded-lg border border-ink-700 bg-ink-850 px-3 py-2 min-w-0" data-testid={`metric-${m.key}`}>
                <div className="text-[11px] text-mist-400 leading-snug break-words">{m.label}</div>
                <div className="num text-base font-semibold text-mist-100 mt-0.5 break-all">{v ?? <Unavailable reason="no data in the period" />}</div>
                {m.hint && <div className="text-[11px] text-mist-400 mt-0.5 break-words">{m.hint}</div>}
              </div>
            );
          })}
        </div>
      </Panel>

      {c.sections.map((s) => (
        <SectionTable key={s.id} s={s} cat={cat} onCsv={() => void ex.run(`csv-${s.id}`, `${base}?format=csv&section=${encodeURIComponent(s.id)}`, `${s.id}.csv`)}
          busy={ex.busy === `csv-${s.id}`} disabled={!!ex.busy} />
      ))}

      <Panel title="Limitations and data sources">
        <ul className="list-disc pl-5 text-sm text-mist-200 space-y-1" data-testid="report-limitations">
          {c.limitations.map((l) => <li key={l}>{l}</li>)}
          <li>Data basis: <code className="num text-xs">{c.period.basis}</code>.</li>
        </ul>
      </Panel>
    </div>
  );
}

function Meta({ k, v }: { k: string; v: ReactNode }) {
  return (
    <div className="flex flex-col min-w-0 py-1 border-b border-ink-800">
      <dt className="text-[11px] uppercase tracking-wider text-mist-400">{k}</dt>
      <dd className="text-mist-100 min-w-0 break-words">{v}</dd>
    </div>
  );
}

function SectionTable({ s, cat, onCsv, busy, disabled }: { s: ReportSection; cat: ReportCatalogue; onCsv: () => void; busy: boolean; disabled: boolean }) {
  const shown = s.rows.length;
  return (
    <Panel title={s.title} actions={
      <button className="btn btn-ghost text-xs px-2 py-1" onClick={onCsv} disabled={disabled || s.total_rows === 0} data-testid={`section-csv-${s.id}`}
        aria-label={`Download ${s.title} as CSV`}>
        <Download className="h-3.5 w-3.5" />{busy ? "…" : "CSV"}
      </button>}>
      <div data-testid={`section-${s.id}`}>
        {s.description && <p className="text-xs text-mist-400 mb-2">{s.description}</p>}
        {shown === 0 ? (
          <p className="text-sm text-mist-300 py-3" data-testid={`section-empty-${s.id}`}>No rows for this period and filters.</p>
        ) : (
          <div className="overflow-x-auto max-h-[28rem] overflow-y-auto rounded border border-ink-800">
            <table className="w-full text-left">
              <thead className="sticky top-0 bg-ink-900"><tr>{s.columns.map((col) => <th key={col.key} className="th">{col.label}</th>)}</tr></thead>
              <tbody>
                {s.rows.map((row, i) => (
                  <tr key={i}>
                    {s.columns.map((col) => {
                      const v = row[col.key];
                      const txt = fmtReportValue(v, col.format);
                      const numeric = !["text", "longtext", "datetime"].includes(col.format);
                      return (
                        <td key={col.key} className={cx("td align-top", numeric && "num text-right",
                          col.format === "longtext" && "whitespace-pre-wrap break-words min-w-[14rem] max-w-[28rem]")}>
                          {col.key === "status" && typeof v === "string" ? <StatusBadge status={v} /> : txt ?? <span className="text-mist-400">—</span>}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {(s.total_rows > shown) && (
          <p className="text-[11px] text-mist-400 mt-2" data-testid={`section-count-${s.id}`}>
            Preview shows {shown.toLocaleString()} of {s.total_rows.toLocaleString()} rows.
            {s.truncated ? ` The report stores the first ${s.stored_rows.toLocaleString()} (detail cap ${cat.detail_row_cap.toLocaleString()}).` : ""}
            {" "}The CSV contains every stored row; the PDF lists up to {cat.pdf_row_cap.toLocaleString()}.
          </p>
        )}
      </div>
    </Panel>
  );
}

/* ------------------------------------------------------------------ history */
function History({ cat, selected, onOpen }: { cat?: ReportCatalogue; selected: string | null; onOpen: (uid: string) => void }) {
  const [page, setPage] = useState(1);
  const [type, setType] = useState("");
  const size = 10;
  const q = useQuery({
    queryKey: ["report-history", page, type], placeholderData: keepPreviousData,
    queryFn: () => api<Page<ReportMeta>>(`/reports/generated${qs({ page, size, report_type: type })}`),
  });
  const ex = useExport();
  const types = useMemo(() => cat?.types.filter((t) => t.available) ?? [], [cat]);
  return (
    <Panel className="mt-4" title="Report history" actions={
      <select className="input py-1 text-xs w-auto max-w-[12rem]" aria-label="Filter history by report type (server filter)" value={type}
        data-testid="history-type" onChange={(e) => { setType(e.target.value); setPage(1); }}>
        <option value="">All types</option>
        {types.map((t) => <option key={t.key} value={t.key}>{t.title}</option>)}
      </select>}>
      <QueryState q={q} empty={q.data && q.data.items.length === 0 ? <EmptyState title="No reports generated yet" hint={type ? "No report of this type has been generated." : "Generated reports are stored and listed here."} /> : false}>
        {q.data && (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-left" data-testid="history-table">
                <thead><tr>
                  <th className="th">Report</th><th className="th">Period</th><th className="th">Generated</th><th className="th">By</th>
                  <th className="th">Status</th><th className="th text-right">Rows</th><th className="th">Actions</th>
                </tr></thead>
                <tbody>
                  {q.data.items.map((r) => (
                    <tr key={r.report_uid} className={cx(selected === r.report_uid && "bg-ink-850")} data-testid={`history-${r.report_uid}`}>
                      <td className="td"><div className="text-mist-100">{r.title}</div><div className="num text-[11px] text-mist-400">{r.report_uid.slice(0, 8)}</div></td>
                      <td className="td num text-xs">{fmtReportValue(r.period_start, "datetime")?.slice(0, 16)} → {fmtReportValue(r.period_end, "datetime")?.slice(0, 16)}</td>
                      <td className="td num text-xs">{fmtReportValue(r.created_at, "datetime")}</td>
                      <td className="td">{r.generated_by}</td>
                      <td className="td"><span className={cx("inline-flex rounded border px-1.5 py-0.5 text-[11px] font-semibold",
                        r.status === "COMPLETED" ? "text-emerald-400 border-emerald-500/40 bg-emerald-500/10" : "text-coral-400 border-coral-500/40 bg-coral-500/10")}>{r.status}</span></td>
                      <td className="td num text-right">{r.row_count.toLocaleString()}</td>
                      <td className="td">
                        <div className="flex gap-1">
                          <button className="btn btn-ghost text-xs px-2 py-1" onClick={() => onOpen(r.report_uid)} data-testid={`history-open-${r.report_uid}`}>View</button>
                          {r.status === "COMPLETED" && (["csv", "pdf"] as const).map((f) => (
                            <button key={f} className="btn btn-ghost text-xs px-2 py-1" disabled={!!ex.busy}
                              aria-label={`Download ${r.title} ${r.report_uid.slice(0, 8)} as ${f.toUpperCase()}`}
                              onClick={() => void ex.run(`${f}-${r.report_uid}`, `/reports/generated/${encodeURIComponent(r.report_uid)}/download?format=${f}`, `report.${f}`)}>
                              {ex.busy === `${f}-${r.report_uid}` ? "…" : f.toUpperCase()}
                            </button>
                          ))}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {ex.msg && <div className="mt-2"><Notice tone={ex.msg.tone}>{ex.msg.text}</Notice></div>}
            <Pagination page={page} size={size} total={q.data.total} onPage={setPage} />
          </>
        )}
      </QueryState>
      {q.isFetching && !q.isLoading && <span role="status" className="text-[11px] text-mist-400">updating…</span>}
    </Panel>
  );
}

