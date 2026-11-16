import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Pause, Play, Trash2 } from "lucide-react";
import { useLiveStream, type StreamState } from "../hooks/useLiveStream";
import { EmptyState, PageHeader, Panel, RiskBadge, Select, cx, fmt } from "../components/ui";

const stateLabel: Record<StreamState, [string, string]> = {
  connecting: ["Connecting…", "text-amber-400"],
  open: ["Live", "text-emerald-400"],
  closed: ["Disconnected — reconnecting", "text-amber-400"],
  error: ["Connection error — retrying", "text-coral-400"],
  unavailable: ["Live feed unavailable", "text-coral-400"],
};

export function LiveStreamPage() {
  const [paused, setPaused] = useState(false);
  const [risk, setRisk] = useState("ALL");
  const [q, setQ] = useState("");
  const { rows, state, detail, missed, clear } = useLiveStream(300, paused);
  const filtered = useMemo(() => rows.filter((r) =>
    (risk === "ALL" || r.risk_category === risk || (risk === "FLAGGED" && r.risk_category !== "LOW")) &&
    (!q || [r.customer_id, r.transaction_id, r.merchant_name, r.city].some((v) => v?.toLowerCase().includes(q.toLowerCase())))),
  [rows, risk, q]);
  const [label, color] = stateLabel[state];

  return (
    <div>
      <PageHeader title="Live Transaction Stream"
        subtitle={<>Server-Sent Events from the scoring pipeline · <span data-testid="stream-state" className={color}>{label}</span>{detail && state !== "open" ? <span className="text-mist-400"> ({detail})</span> : null}</>}
        actions={<>
          <button className="btn btn-secondary" onClick={() => setPaused((p) => !p)} data-testid="pause-toggle">
            {paused ? <><Play className="h-4 w-4" /> Resume{missed ? ` (${missed} new)` : ""}</> : <><Pause className="h-4 w-4" /> Pause</>}
          </button>
          <button className="btn btn-ghost" onClick={clear}><Trash2 className="h-4 w-4" /> Clear</button>
        </>} />
      <Panel>
        <div className="flex flex-wrap gap-3 mb-3 items-end">
          <Select label="Risk" value={risk} onChange={setRisk} testId="risk-filter" options={[
            { value: "ALL", label: "All" }, { value: "FLAGGED", label: "Flagged (MEDIUM+HIGH)" },
            { value: "HIGH", label: "HIGH" }, { value: "MEDIUM", label: "MEDIUM" }, { value: "LOW", label: "LOW" }]} />
          <label className="flex flex-col gap-1 text-xs text-mist-300 w-64">Search
            <input className="input py-1.5" placeholder="customer, transaction, merchant, city" value={q} onChange={(e) => setQ(e.target.value)} />
          </label>
          <span className="ml-auto text-xs text-mist-400 num">{filtered.length} shown · {rows.length} buffered</span>
        </div>
        {filtered.length === 0 ? (
          <EmptyState title={rows.length ? "No events match the filters" : "Waiting for transactions"}
            hint={rows.length ? undefined : "Start a simulation in the Simulation Lab; scored events appear here in real time."} />
        ) : (
          <div className="overflow-x-auto max-h-[70vh] overflow-y-auto">
            <table className="w-full" data-testid="live-table">
              <thead className="sticky top-0 bg-ink-900"><tr>
                <th className="th">Processed</th><th className="th">Customer</th><th className="th">Merchant</th><th className="th">Amount</th>
                <th className="th">Location (sim.)</th><th className="th">Status</th><th className="th">Risk</th><th className="th">Method</th>
                <th className="th">Action</th><th className="th">Latency</th><th className="th">Alert</th></tr></thead>
              <tbody>
                {filtered.map((r) => (
                  <tr key={r.event_id} className={cx("hover:bg-ink-850", r.risk_category === "HIGH" && "bg-coral-500/[0.04]")}>
                    <td className="td num text-mist-300">{fmt.time(r.processed_at)}</td>
                    <td className="td num">{r.customer_id}</td>
                    <td className="td"><span className="text-mist-100">{r.merchant_name || r.merchant_id}</span> <span className="text-mist-400 text-xs">{r.merchant_category}</span></td>
                    <td className="td num">{fmt.money(r.amount, r.currency)}</td>
                    <td className="td text-mist-300">{r.city}, {r.country}</td>
                    <td className={cx("td text-xs", r.status === "APPROVED" ? "text-mist-300" : "text-amber-400")}>{r.status}</td>
                    <td className="td"><RiskBadge category={r.risk_category} score={r.risk_score} /></td>
                    <td className="td text-xs text-mist-300">{r.detection_method}</td>
                    <td className="td text-xs text-mist-300">{r.recommended_action}</td>
                    <td className="td num text-xs text-mist-400">{fmt.ms(r.latency_ms)}</td>
                    <td className="td">{r.alert_uid ? <Link className="text-stream-400 text-xs hover:underline" to={`/alerts/${r.alert_uid}`}>view</Link> : <span className="text-mist-400 text-xs">—</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </div>
  );
}
