import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../services/api";
import { EmptyState, PageHeader, Panel, QueryState, RiskBadge, StatTile, StatusBadge, fmt } from "../components/ui";
import { FlaggedChart, RiskHistogram, RiskLegend, VolumeChart } from "../charts/Charts";
import type { Alert, Overview, Page } from "../types/api";

type Timeline = { bucket_seconds: number; points: { t: string; total: number; medium: number; high: number; avg_latency_ms: number | null }[] };
type Dist = { bins: { from: number; to: number; count: number }[]; detection_methods: Record<string, number> };
type RiskSettings = { settings: { medium_threshold: number; high_threshold: number } };

export function OverviewPage() {
  const ov = useQuery({ queryKey: ["overview"], queryFn: () => api<Overview>("/analytics/overview"), refetchInterval: 5000 });
  const tl = useQuery({ queryKey: ["timeline"], queryFn: () => api<Timeline>("/analytics/timeline?minutes=30&bucket_seconds=30"), refetchInterval: 10000 });
  const dist = useQuery({ queryKey: ["risk-dist"], queryFn: () => api<Dist>("/analytics/risk-distribution"), refetchInterval: 15000 });
  const rs = useQuery({ queryKey: ["risk-settings"], queryFn: () => api<RiskSettings>("/risk/settings") });
  const alerts = useQuery({ queryKey: ["alerts", "recent"], queryFn: () => api<Page<Alert>>("/alerts?size=8"), refetchInterval: 5000 });
  const o = ov.data;
  const med = rs.data?.settings.medium_threshold ?? 40;
  const high = rs.data?.settings.high_threshold ?? 70;

  return (
    <div>
      <PageHeader title="Intelligence Overview"
        subtitle={o ? <>Profile <b className="text-mist-100">{o.profile.toUpperCase()}</b> · simulation {o.simulation.toLowerCase()} · all values computed from persisted data</> : "Loading live metrics"} />
      <QueryState q={ov}>
        {o && (
          <>
            {o.model_error && (
              <div role="alert" className="panel border-coral-500/40 px-4 py-3 mb-4 text-sm text-coral-400">
                Model unavailable — decisions are rules-only: {o.model_error}
              </div>
            )}
            <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 mb-4">
              <StatTile testId="stat-total" label="Processed" value={fmt.num(o.total_transactions)} tone="cyan" hint={`${fmt.num(o.transactions_last_minute)} in the last minute`} />
              <StatTile label="Throughput" value={`${o.tps_last_minute.toFixed(2)}/s`} hint="persisted, 60 s window" />
              <StatTile testId="stat-suspicious" label="Suspicious" value={fmt.num(o.suspicious_transactions)} tone="amber" hint={`MEDIUM+HIGH · ${fmt.num(o.alerts_total)} alerts`} />
              <StatTile testId="stat-high-open" label="Open high-risk" value={fmt.num(o.high_risk_open_alerts)} tone="coral" hint="HIGH alerts, NEW or UNDER REVIEW" />
              <StatTile label="Detection rate" value={fmt.pct(o.detection_rate.value)}
                hint={`${fmt.num(o.detection_rate.numerator)} / ${fmt.num(o.detection_rate.denominator)} injected (synthetic labels)`} tone="emerald" />
              <StatTile label="Latency p95" value={fmt.ms(o.detection_latency_ms.p95)}
                hint={`p50 ${fmt.ms(o.detection_latency_ms.p50)} · last 15 min · n=${fmt.num(o.detection_latency_ms.samples)}`} />
            </div>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 mb-4">
              <Panel title="Transaction volume · last 30 min (30 s buckets)" className="xl:col-span-2">
                <QueryState q={tl} empty={!tl.data?.points.length && <EmptyState title="No transactions in the last 30 minutes" hint="Start a simulation from the Simulation Lab." />}>
                  {tl.data && <VolumeChart points={tl.data.points} />}
                </QueryState>
              </Panel>
              <Panel title="Active model">
                {o.model ? (
                  <div className="text-sm space-y-2">
                    <div className="num text-stream-400 break-all">{o.model.label}</div>
                    <div className="grid grid-cols-3 gap-2 pt-1">
                      <StatTile compact label="PR-AUC" value={fmt.num(o.model.test_pr_auc, 4)} />
                      <StatTile compact label="Precision" value={fmt.num(o.model.test_precision, 4)} />
                      <StatTile compact label="Recall" value={fmt.num(o.model.test_recall, 4)} />
                    </div>
                    <p className="text-[11px] text-mist-400">Held-out temporal test split on the synthetic training dataset; not real-world performance.</p>
                  </div>
                ) : <EmptyState title="No active model" hint="Train models (scripts/train_models.ps1) — scoring falls back to rules only." />}
              </Panel>
            </div>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 mb-4">
              <Panel title="Flagged per bucket" actions={<RiskLegend medium={med} high={high} />} className="xl:col-span-2">
                {tl.data?.points.length ? <FlaggedChart points={tl.data.points} /> : <EmptyState title="No data yet" />}
              </Panel>
              <Panel title="Risk score distribution (all time)">
                <QueryState q={dist} empty={!dist.data?.bins.some((b) => b.count) && <EmptyState title="No scored transactions" />}>
                  {dist.data && <RiskHistogram bins={dist.data.bins} medium={med} high={high} height={180} />}
                </QueryState>
                <p className="text-[11px] text-mist-400 mt-1">√-scaled y-axis; bins of 5 risk points.</p>
              </Panel>
            </div>
            <Panel title="Recent alerts" actions={<Link className="text-xs text-stream-400 hover:underline" to="/investigations">Open Investigation Center →</Link>}>
              <QueryState q={alerts} empty={!alerts.data?.items.length && <EmptyState title="No alerts yet" />}>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead><tr><th className="th">Created</th><th className="th">Alert</th><th className="th">Customer</th><th className="th">Amount</th><th className="th">Risk</th><th className="th">Method</th><th className="th">Status</th></tr></thead>
                    <tbody>
                      {alerts.data?.items.map((a) => (
                        <tr key={a.alert_uid} className="hover:bg-ink-850">
                          <td className="td num text-mist-300">{fmt.time(a.created_at)}</td>
                          <td className="td"><Link className="num text-stream-400 hover:underline" to={`/alerts/${a.alert_uid}`}>{a.alert_uid.slice(0, 8)}</Link></td>
                          <td className="td num">{a.customer_id}</td>
                          <td className="td num">{fmt.money(a.amount, a.currency)}</td>
                          <td className="td"><RiskBadge category={a.risk_category} score={a.risk_score} /></td>
                          <td className="td text-mist-300">{a.detection_method}</td>
                          <td className="td"><StatusBadge status={a.status} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </QueryState>
            </Panel>
          </>
        )}
      </QueryState>
    </div>
  );
}
