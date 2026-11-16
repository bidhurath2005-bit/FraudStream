import { useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { FolderPlus } from "lucide-react";
import { api, download, qs } from "../services/api";
import { EmptyState, PageHeader, Pagination, Panel, QueryState, RiskBadge, Select, StatusBadge, cx, fmt } from "../components/ui";
import type { Alert, Investigation, Page } from "../types/api";

function AlertsTab() {
  const [sp, setSp] = useSearchParams();
  const page = Number(sp.get("page") ?? 1);
  const filters = {
    status: sp.get("status") ?? "NEW,UNDER_REVIEW", risk_category: sp.get("risk") ?? "", assigned: sp.get("assigned") ?? "any",
    q: sp.get("q") ?? "", sort: sp.get("sort") ?? "created_at",
  };
  const set = (k: string, v: string) => {
    const n = new URLSearchParams(sp);
    if (v) n.set(k, v); else n.delete(k);
    if (k !== "page") n.delete("page");
    setSp(n, { replace: true });
  };
  const alerts = useQuery({
    queryKey: ["alerts", filters, page],
    queryFn: () => api<Page<Alert>>(`/alerts${qs({ ...filters, page, size: 20, order: "desc" })}`),
    refetchInterval: 8000,
  });
  const [selected, setSelected] = useState<Record<string, Alert>>({});
  const [title, setTitle] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const nav = useNavigate();
  const qc = useQueryClient();
  const create = useMutation({
    mutationFn: () => api<Investigation>("/investigations", { body: { alert_uids: Object.keys(selected), title, priority: "HIGH" } }),
    onSuccess: (inv) => { qc.invalidateQueries({ queryKey: ["alerts"] }); nav(`/investigations/${inv.case_uid}`); },
    onError: (e: Error) => setErr(e.message),
  });
  const sel = Object.values(selected);
  const sameCustomer = new Set(sel.map((a) => a.customer_id)).size === 1;

  return (
    <Panel>
      <div className="flex flex-wrap gap-3 items-end mb-3">
        <Select label="Status" value={filters.status} onChange={(v) => set("status", v)} testId="alert-status-filter" options={[
          { value: "NEW,UNDER_REVIEW", label: "Open (new + under review)" }, { value: "NEW", label: "New" },
          { value: "UNDER_REVIEW", label: "Under review" }, { value: "CONFIRMED_SUSPICIOUS", label: "Confirmed suspicious" },
          { value: "DISMISSED", label: "Dismissed" }, { value: "CLOSED", label: "Closed" }, { value: "", label: "Any status" }]} />
        <Select label="Risk" value={filters.risk_category} onChange={(v) => set("risk", v)} options={[
          { value: "", label: "Any" }, { value: "HIGH", label: "HIGH" }, { value: "MEDIUM", label: "MEDIUM" }]} />
        <Select label="Assignee" value={filters.assigned} onChange={(v) => set("assigned", v)} options={[
          { value: "any", label: "Anyone" }, { value: "me", label: "Me" }, { value: "unassigned", label: "Unassigned" }]} />
        <Select label="Sort" value={filters.sort} onChange={(v) => set("sort", v)} options={[
          { value: "created_at", label: "Newest" }, { value: "risk_score", label: "Highest risk" }, { value: "updated_at", label: "Recently updated" }]} />
        <label className="flex flex-col gap-1 text-xs text-mist-300 w-56">Search
          <input className="input py-1.5" defaultValue={filters.q} placeholder="alert, customer, tx id, rule"
            onKeyDown={(e) => e.key === "Enter" && set("q", (e.target as HTMLInputElement).value)} data-testid="alert-search" />
        </label>
        <button className="btn btn-ghost ml-auto text-xs" onClick={() => void download(`/reports/alerts.csv${qs({ status: filters.status })}`, "alerts.csv")}>Export CSV</button>
      </div>
      {sel.length > 0 && (
        <div className="flex flex-wrap items-center gap-2 mb-3 rounded-md border border-stream-500/30 bg-stream-500/5 px-3 py-2 text-sm">
          <span>{sel.length} selected</span>
          {!sameCustomer && <span className="text-amber-400 text-xs">Investigations group alerts of a single customer.</span>}
          <input className="input py-1 w-64" placeholder="Investigation title" value={title} onChange={(e) => setTitle(e.target.value)} data-testid="inv-title" />
          <button className="btn btn-primary py-1" disabled={!sameCustomer || title.trim().length < 3 || create.isPending}
            onClick={() => { setErr(null); create.mutate(); }} data-testid="open-investigation">
            <FolderPlus className="h-4 w-4" /> Open investigation
          </button>
          <button className="btn btn-ghost py-1" onClick={() => setSelected({})}>Clear</button>
          {err && <span role="alert" className="text-coral-400 text-xs">{err}</span>}
        </div>
      )}
      <QueryState q={alerts} empty={!alerts.data?.items.length && <EmptyState title="No alerts match these filters" />}>
        <div className="overflow-x-auto">
          <table className="w-full" data-testid="alerts-table">
            <thead><tr><th className="th w-8"></th><th className="th">Created</th><th className="th">Alert</th><th className="th">Customer</th>
              <th className="th">Amount</th><th className="th">Risk</th><th className="th">Rules</th><th className="th">Method</th>
              <th className="th">Status</th><th className="th">Assignee</th></tr></thead>
            <tbody>
              {alerts.data?.items.map((a) => (
                <tr key={a.alert_uid} className={cx("hover:bg-ink-850", selected[a.alert_uid] && "bg-stream-500/5")}>
                  <td className="td"><input type="checkbox" aria-label={`select ${a.alert_uid}`} disabled={a.investigation_id != null}
                    checked={!!selected[a.alert_uid]} onChange={(e) => setSelected((s) => {
                      const n = { ...s };
                      if (e.target.checked) n[a.alert_uid] = a; else delete n[a.alert_uid];
                      return n;
                    })} /></td>
                  <td className="td num text-mist-300">{fmt.dt(a.created_at)}</td>
                  <td className="td"><Link className="num text-stream-400 hover:underline" to={`/alerts/${a.alert_uid}`}>{a.alert_uid.slice(0, 8)}</Link></td>
                  <td className="td num">{a.customer_id}</td>
                  <td className="td num">{fmt.money(a.amount, a.currency)}</td>
                  <td className="td"><RiskBadge category={a.risk_category} score={a.risk_score} /></td>
                  <td className="td text-xs text-mist-300 max-w-[220px] truncate" title={a.triggered_rules.map((r) => r.code).join(", ")}>
                    {a.triggered_rules.map((r) => r.code).join(", ") || "—"}</td>
                  <td className="td text-xs text-mist-300">{a.detection_method}</td>
                  <td className="td"><StatusBadge status={a.status} />{a.investigation_id && <span className="ml-1 text-[10px] text-mist-400">in case</span>}</td>
                  <td className="td text-xs text-mist-300">{a.assigned_to?.username ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {alerts.data && <Pagination page={page} size={20} total={alerts.data.total} onPage={(p) => set("page", String(p))} />}
      </QueryState>
    </Panel>
  );
}

function InvestigationsTab() {
  const [status, setStatus] = useState("OPEN,IN_PROGRESS,RESOLVED");
  const [page, setPage] = useState(1);
  const invs = useQuery({
    queryKey: ["investigations", status, page],
    queryFn: () => api<Page<Investigation>>(`/investigations${qs({ status, page, size: 20 })}`),
  });
  return (
    <Panel>
      <div className="flex gap-3 mb-3">
        <Select label="Status" value={status} onChange={(v) => { setStatus(v); setPage(1); }} options={[
          { value: "OPEN,IN_PROGRESS,RESOLVED", label: "Active" }, { value: "OPEN", label: "Open" }, { value: "IN_PROGRESS", label: "In progress" },
          { value: "RESOLVED", label: "Resolved" }, { value: "CLOSED", label: "Closed" }, { value: "", label: "Any" }]} />
      </div>
      <QueryState q={invs} empty={!invs.data?.items.length && <EmptyState title="No investigations" hint="Select alerts in the Alerts tab and open an investigation." />}>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead><tr><th className="th">Updated</th><th className="th">Case</th><th className="th">Title</th><th className="th">Customer</th>
              <th className="th">Alerts</th><th className="th">Priority</th><th className="th">Status</th><th className="th">Assignee</th><th className="th">Outcome</th></tr></thead>
            <tbody>
              {invs.data?.items.map((i) => (
                <tr key={i.case_uid} className="hover:bg-ink-850">
                  <td className="td num text-mist-300">{fmt.dt(i.updated_at)}</td>
                  <td className="td"><Link className="num text-stream-400 hover:underline" to={`/investigations/${i.case_uid}`}>{i.case_uid.slice(0, 8)}</Link></td>
                  <td className="td max-w-[260px] truncate">{i.title}</td>
                  <td className="td num">{i.customer_id}</td>
                  <td className="td num">{i.alert_count}</td>
                  <td className="td text-xs">{i.priority}</td>
                  <td className="td"><StatusBadge status={i.status} /></td>
                  <td className="td text-xs text-mist-300">{i.assigned_to?.username ?? "—"}</td>
                  <td className="td text-xs text-mist-300">{i.final_classification?.replace(/_/g, " ") ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {invs.data && <Pagination page={page} size={20} total={invs.data.total} onPage={setPage} />}
      </QueryState>
    </Panel>
  );
}

export function InvestigationCenterPage() {
  const [sp, setSp] = useSearchParams();
  const tab = sp.get("tab") ?? "alerts";
  return (
    <div>
      <PageHeader title="Investigation Center" subtitle="Human-in-the-loop review. Alerts are model/rule estimates — analysts decide the outcome." />
      <div className="flex gap-1 mb-4 border-b border-ink-800" role="tablist">
        {[["alerts", "Alerts"], ["cases", "Investigations"]].map(([k, l]) => (
          <button key={k} role="tab" aria-selected={tab === k} onClick={() => setSp({ tab: k })}
            className={cx("px-4 py-2 text-sm -mb-px border-b-2", tab === k ? "border-stream-500 text-stream-400" : "border-transparent text-mist-300 hover:text-mist-100")}>
            {l}
          </button>
        ))}
      </div>
      {tab === "alerts" ? <AlertsTab /> : <InvestigationsTab />}
    </div>
  );
}
