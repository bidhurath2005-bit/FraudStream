import { useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { api, qs } from "../services/api";
import {
  EmptyState, KV, PageHeader, Panel, QueryState, RiskBadge, Select, StatTile, Unavailable, cx, fmt,
} from "../components/ui";
import { HBars, HourlyBars } from "../charts/Charts";
import type {
  ConfusionResponse, HourlyResponse, MerchantsResponse, Page, PatternsResponse, RiskCategory, RiskSettingsResponse, VelocityResponse,
} from "../types/api";

/** Server-side analysis windows (``minutes`` on every endpoint used below; "" = all time). */
export const PERIODS = [
  { value: "60", label: "Last hour" }, { value: "360", label: "Last 6 hours" }, { value: "1440", label: "Last 24 hours" },
  { value: "10080", label: "Last 7 days" }, { value: "43200", label: "Last 30 days" }, { value: "", label: "All time" },
];
const VELOCITY_WINDOWS = [
  { value: "5", label: "5 min" }, { value: "15", label: "15 min" }, { value: "60", label: "1 hour" },
  { value: "360", label: "6 hours" }, { value: "1440", label: "24 hours" },
];
const STALE = 30_000;
const pctFmt = (v: number) => `${(v * 100).toFixed(0)}%`;

function categoryOf(score: number | null | undefined, med: number, high: number): RiskCategory | null {
  if (score == null) return null;
  return score >= high ? "HIGH" : score >= med ? "MEDIUM" : "LOW";
}

/** Updating indicator: previous results stay visible (dimmed) while the new filter's request runs. */
function Updating({ on }: { on: boolean }) {
  return on ? <span role="status" className="text-[11px] text-mist-400">updating…</span> : null;
}

export function RiskAnalyticsPage() {
  const [params, setParams] = useSearchParams();
  const period = params.get("period") ?? "1440";
  const category = params.get("category") ?? "";
  const vwin = params.get("vwin") ?? "60";
  const set = (k: string, v: string) => setParams((p) => { const n = new URLSearchParams(p); if (v) n.set(k, v); else n.delete(k); return n; }, { replace: true });
  // "" (all time) must be distinguishable from "not set" in the URL
  const setPeriod = (v: string) => setParams((p) => { const n = new URLSearchParams(p); n.set("period", v); return n; }, { replace: true });
  const minutes = period || undefined;
  const qc = useQueryClient();
  const opts = { staleTime: STALE, placeholderData: keepPreviousData };

  const settings = useQuery({ queryKey: ["risk-settings"], queryFn: () => api<RiskSettingsResponse>("/risk/settings"), staleTime: STALE });
  const merchants = useQuery({ ...opts, queryKey: ["risk", "merchants", period], queryFn: () => api<MerchantsResponse>(`/analytics/merchants${qs({ minutes, limit: 15 })}`) });
  const merchantsCat = useQuery({ ...opts, enabled: !!category, queryKey: ["risk", "merchants", period, category],
    queryFn: () => api<MerchantsResponse>(`/analytics/merchants${qs({ minutes, limit: 15, category })}`) });
  const patterns = useQuery({ ...opts, queryKey: ["risk", "patterns", period], queryFn: () => api<PatternsResponse>(`/analytics/patterns${qs({ minutes })}`) });
  const confusion = useQuery({ ...opts, queryKey: ["risk", "confusion", period], queryFn: () => api<ConfusionResponse>(`/analytics/confusion${qs({ minutes })}`) });
  const hourly = useQuery({ ...opts, queryKey: ["risk", "hourly", period], queryFn: () => api<HourlyResponse>(`/analytics/hourly${qs({ minutes })}`) });
  // `since` is computed when the request is made, so every (re)fetch uses the current window
  const alerts = useQuery({ ...opts, queryKey: ["risk", "alerts", period], queryFn: () => api<Page<unknown>>(
    `/alerts${qs({ size: 1, status: "", since: period ? new Date(Date.now() - Number(period) * 60_000).toISOString() : undefined })}`) });
  const velocity = useQuery({ ...opts, queryKey: ["risk", "velocity", vwin], queryFn: () => api<VelocityResponse>(`/analytics/velocity${qs({ minutes: vwin, limit: 15 })}`) });

  const med = settings.data?.settings.medium_threshold;
  const high = settings.data?.settings.high_threshold;
  const periodLabel = PERIODS.find((p) => p.value === period)?.label ?? "All time";
  const categories = merchants.data?.categories ?? [];
  const totals = useMemo(() => ({
    count: categories.reduce((a, c) => a + c.count, 0), flagged: categories.reduce((a, c) => a + c.flagged, 0),
    volume: categories.reduce((a, c) => a + c.volume, 0),
  }), [categories]);
  const scenarios = patterns.data?.scenarios ?? [];
  const injected = scenarios.filter((s) => (s.labelled_suspicious ?? 0) > 0);
  const topPattern = [...injected].sort((a, b) => b.flagged - a.flagged)[0];
  const topRule = patterns.data?.rule_hits[0];
  const anyLoading = [merchants, patterns, confusion, hourly, alerts].some((q) => q.isFetching);

  return (
    <div>
      <PageHeader title="Risk Analytics"
        subtitle={<>Aggregates computed by the backend from persisted, scored transactions · period: <b className="text-mist-100" data-testid="period-label">{periodLabel}</b> (by processing time) · all labels come from the <span className="text-amber-400">synthetic simulator</span></>}
        actions={<>
          <Select label="Period" value={period} onChange={setPeriod} options={PERIODS} testId="period-filter" />
          <Select label="Merchant category" value={category} onChange={(v) => set("category", v)} testId="category-filter"
            options={[{ value: "", label: "All categories" }, ...[...categories].sort((a, b) => a.category.localeCompare(b.category))
              .map((c) => ({ value: c.category, label: c.category.replace(/_/g, " ") })),
              ...(category && !categories.some((c) => c.category === category) ? [{ value: category, label: category }] : [])]} />
          <button className="btn btn-secondary self-end" data-testid="risk-refresh" disabled={anyLoading}
            onClick={() => void qc.invalidateQueries({ queryKey: ["risk"] })}>
            <RefreshCw className={cx("h-4 w-4", anyLoading && "animate-spin")} /> Refresh
          </button>
        </>} />

      {/* F. summary */}
      <QueryState q={merchants} empty={merchants.data && totals.count === 0 && (
        <EmptyState title={`No scored transactions in this period (${periodLabel.toLowerCase()})`} hint="Choose a longer period or generate traffic in the Simulation Lab." />)}>
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 mb-4" data-testid="risk-summary">
          <StatTile testId="sum-analysed" label="Transactions analysed" tone="cyan" value={fmt.num(totals.count)} hint={`${fmt.money(totals.volume)} volume`} />
          <StatTile testId="sum-flagged" label="Flagged MEDIUM/HIGH" tone="amber" value={fmt.num(totals.flagged)}
            hint={totals.count ? `${fmt.pct(totals.flagged / totals.count, 2)} of analysed` : "—"} />
          <StatTile testId="sum-alerts" label="Fraud alerts raised" tone="coral"
            value={alerts.isError ? <Unavailable reason="alerts request failed" /> : fmt.num(alerts.data?.total)}
            hint={period ? "created in the period" : "all time"} />
          <StatTile testId="sum-confirmed" label="Analyst-confirmed" value={confusion.data ? fmt.num(confusion.data.analyst_outcomes.CONFIRMED_SUSPICIOUS ?? 0) : "—"}
            hint={confusion.data ? `${fmt.num(confusion.data.analyst_outcomes.DISMISSED ?? 0)} dismissed` : "alert decisions"} />
          <StatTile testId="sum-top-pattern" label="Most-flagged injected pattern" compact={false}
            value={topPattern ? <span className="block text-base leading-snug whitespace-normal break-words">{topPattern.scenario.replace(/_/g, " ")}</span> : <Unavailable reason="no injected suspicious scenarios in this period" />}
            hint={topPattern ? `${fmt.num(topPattern.flagged)} flagged · avg risk ${fmt.num(topPattern.avg_risk, 1)}` : "simulator scenarios"} />
          <StatTile testId="sum-top-rule" label="Most-triggered rule"
            value={topRule ? <span className="block text-sm leading-snug whitespace-normal [overflow-wrap:anywhere]">{topRule.rule}</span> : <Unavailable reason="no rule triggers on flagged transactions in this period" />}
            hint={topRule ? `${fmt.num(topRule.count)} triggers` : "risk-engine rules"} />
        </div>
      </QueryState>

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4 mb-4">
        <PatternsPanel q={patterns} className="xl:col-span-3" />
        <ConfusionPanel q={confusion} className="xl:col-span-2" />
      </div>
      <MerchantPanel all={merchants} cat={merchantsCat} category={category} med={med} high={high} />
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <HourlyPanel q={hourly} />
        <VelocityPanel q={velocity} vwin={vwin} setVwin={(v) => set("vwin", v)} med={med} high={high} />
      </div>
    </div>
  );
}

type Q<T> = { data?: T; isLoading: boolean; isError: boolean; error: unknown; refetch: () => unknown; isPlaceholderData?: boolean; isFetching?: boolean };

/* ------------------------------------------------------------------ A. patterns */
function PatternsPanel({ q, className }: { q: Q<PatternsResponse>; className?: string }) {
  const rows = [...(q.data?.scenarios ?? [])].sort((a, b) => b.count - a.count);
  return (
    <Panel className={className} title="Fraud patterns (simulator scenarios)" actions={<Updating on={!!q.isPlaceholderData} />}>
      <QueryState q={q} empty={!rows.length && <EmptyState title="No simulator-labelled transactions in this period" />}>
        <p className="text-xs text-mist-400 mb-3" data-testid="pattern-legend">
          Three different things: <b className="text-mist-200">labelled suspicious</b> = the simulator injected the pattern (synthetic ground truth);
          <b className="text-mist-200"> flagged</b> = the risk engine scored it MEDIUM/HIGH (a detection, not proof of fraud);
          <b className="text-mist-200"> confirmed</b> = an analyst decision on an alert (shown in the confusion panel). Benign scenarios
          (labelled suspicious = 0) that are flagged are false alerts.
        </p>
        <div className={cx("overflow-x-auto", q.isPlaceholderData && "opacity-60")}>
          <table className="w-full" data-testid="patterns-table">
            <thead><tr><th className="th">Scenario</th><th className="th">Transactions</th><th className="th">Labelled suspicious</th><th className="th">Flagged</th><th className="th">Flag rate</th><th className="th">Avg risk</th></tr></thead>
            <tbody>
              {rows.map((s) => {
                const benign = s.labelled_suspicious === 0;
                return (
                  <tr key={s.scenario} className="hover:bg-ink-850">
                    <td className="td"><span className="num text-mist-100">{s.scenario}</span>
                      <span className={cx("ml-2 rounded border px-1 text-[10px]", benign ? "border-ink-500 text-mist-400" : "border-amber-500/40 text-amber-400")}>
                        {s.labelled_suspicious == null ? "label n/a" : benign ? "benign" : "injected"}</span></td>
                    <td className="td num">{fmt.num(s.count)}</td>
                    <td className="td num">{s.labelled_suspicious == null ? <Unavailable reason="not reported by this API version" /> : fmt.num(s.labelled_suspicious)}</td>
                    <td className={cx("td num", benign && s.flagged > 0 && "text-amber-400")}>{fmt.num(s.flagged)}</td>
                    <td className="td num">{fmt.pct(s.flag_rate)}</td>
                    <td className="td num">{fmt.num(s.avg_risk, 1)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
          <div>
            <div className="panel-title mb-1">Flag rate by scenario</div>
            <HBars testId="pattern-chart" rows={rows.map((s) => ({ label: s.scenario, value: s.flag_rate ?? 0, hint: `${fmt.num(s.flagged)} of ${fmt.num(s.count)}` }))}
              valueLabel="flag rate" format={pctFmt} domainMax={1} />
          </div>
          <div>
            <div className="panel-title mb-1">Most common suspicious behaviours (rule triggers)</div>
            {q.data?.rule_hits.length ? (
              <HBars testId="rule-chart" rows={q.data.rule_hits.slice(0, 10).map((r) => ({ label: r.rule, value: r.count }))}
                valueLabel="triggers" format={(v) => Math.round(v).toLocaleString()} color="#199e70" labelWidth={170} />
            ) : <EmptyState title="No rules triggered on flagged transactions in this period" />}
            {q.data?.rule_hits_basis && (
              <p className="text-[11px] text-mist-400 mt-1">Counted over {fmt.num(q.data.rule_hits_basis.flagged_predictions_examined)} flagged predictions
                (cap {fmt.num(q.data.rule_hits_basis.cap)}); one prediction can trigger several rules.</p>
            )}
          </div>
        </div>
      </QueryState>
    </Panel>
  );
}

/* ------------------------------------------------------------------ B. confusion matrix */
function ConfusionPanel({ q, className }: { q: Q<ConfusionResponse>; className?: string }) {
  const c = q.data?.risk_engine_vs_simulator;
  const total = c ? c.tp + c.fp + c.fn + c.tn : 0;
  const f1 = c?.precision != null && c?.recall != null && c.precision + c.recall > 0 ? (2 * c.precision * c.recall) / (c.precision + c.recall) : null;
  const cell = (label: string, desc: string, v: number, tone: string, testId: string) => (
    <div className={cx("rounded-md border px-3 py-2", tone)} data-testid={testId}>
      <div className="text-[11px] text-mist-300">{label}</div>
      <div className="num text-xl text-mist-100">{fmt.num(v)}</div>
      <div className="text-[10px] text-mist-400">{desc}</div>
    </div>
  );
  return (
    <Panel className={className} title="Risk engine vs simulator labels" actions={<Updating on={!!q.isPlaceholderData} />}>
      <QueryState q={q} empty={c && total === 0 && <EmptyState title="No labelled transactions in this period" hint="Only simulator transactions carry ground-truth labels." />}>
        {c && (
          <div className={cx(q.isPlaceholderData && "opacity-60")}>
            <div className="grid grid-cols-[auto_1fr_1fr] gap-2 items-center text-xs" data-testid="risk-confusion">
              <span /><span className="text-center text-mist-400">flagged (MEDIUM/HIGH)</span><span className="text-center text-mist-400">not flagged (LOW)</span>
              <span className="text-mist-400">labelled suspicious</span>
              {cell("True positive", "suspicious & flagged", c.tp, "border-emerald-500/40 bg-emerald-500/5", "cm-tp")}
              {cell("False negative", "suspicious, missed", c.fn, "border-coral-500/40 bg-coral-500/5", "cm-fn")}
              <span className="text-mist-400">labelled normal</span>
              {cell("False positive", "normal, flagged", c.fp, "border-amber-500/40 bg-amber-500/5", "cm-fp")}
              {cell("True negative", "normal & not flagged", c.tn, "border-ink-600 bg-ink-850", "cm-tn")}
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-3">
              <StatTile compact testId="cm-precision" label="Precision" value={fmt.num(c.precision, 3)} />
              <StatTile compact testId="cm-recall" label="Recall" value={fmt.num(c.recall, 3)} />
              <StatTile compact testId="cm-f1" label="F1" value={fmt.num(f1, 3)} hint="from P and R" />
              <StatTile compact label="False-pos. rate" value={fmt.num(c.false_positive_rate, 4)} />
            </div>
            <dl className="mt-3 text-[11px] text-mist-400 space-y-1">
              <div><b className="text-mist-300">Precision</b> — of the transactions the engine flagged, the share the simulator labelled suspicious (how many alerts are worth an analyst's time).</div>
              <div><b className="text-mist-300">Recall</b> — of the labelled-suspicious transactions, the share the engine flagged (how much injected fraud is caught).</div>
              <div><b className="text-mist-300">F1</b> — harmonic mean of precision and recall (computed here from the two values above).</div>
            </dl>
            <div className="mt-3 border-t border-ink-800 pt-2">
              <div className="panel-title mb-1">Analyst outcomes on alerts</div>
              <KV k="confirmed suspicious" v={<span className="num">{fmt.num(q.data!.analyst_outcomes.CONFIRMED_SUSPICIOUS ?? 0)}</span>} />
              <KV k="dismissed" v={<span className="num">{fmt.num(q.data!.analyst_outcomes.DISMISSED ?? 0)}</span>} />
            </div>
            <p className="text-[11px] text-amber-400/90 mt-2" data-testid="synthetic-note">
              {fmt.num(total)} labelled transactions. Labels are SYNTHETIC simulator ground truth — these numbers describe detection of injected patterns, not real-world fraud performance.
            </p>
          </div>
        )}
      </QueryState>
    </Panel>
  );
}

/* ------------------------------------------------------------------ C. merchants */
function MerchantPanel({ all, cat, category, med, high }: {
  all: Q<MerchantsResponse>; cat: Q<MerchantsResponse>; category: string; med?: number; high?: number;
}) {
  const cats = [...(all.data?.categories ?? [])].sort((a, b) => b.flag_rate - a.flag_rate || b.count - a.count);
  const topQ = category ? cat : all;
  const top = topQ.data?.top_flagged_merchants ?? [];
  return (
    <Panel className="mb-4" title="Merchant risk" actions={<Updating on={!!all.isPlaceholderData || !!cat.isPlaceholderData} />}>
      <QueryState q={all} empty={!cats.length && <EmptyState title="No merchant activity in this period" />}>
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <div className="min-w-0">
            <div className="panel-title mb-1">Flag rate by merchant category</div>
            <HBars testId="category-chart" rows={cats.map((c) => ({ label: c.category, value: c.flag_rate, hint: `${fmt.num(c.flagged)} of ${fmt.num(c.count)}` }))}
              valueLabel="flag rate" format={pctFmt} domainMax={1} color="#d95926" />
            <div className="overflow-x-auto mt-2">
              <table className="w-full" data-testid="category-table">
                <thead><tr><th className="th">Category</th><th className="th">Transactions</th><th className="th">Volume</th><th className="th">Flagged</th><th className="th">Flag rate</th><th className="th">Avg risk</th></tr></thead>
                <tbody>
                  {cats.map((c) => (
                    <tr key={c.category} className={cx("hover:bg-ink-850", c.category === category && "bg-ink-850")}>
                      <td className="td">{c.category.replace(/_/g, " ")}</td><td className="td num">{fmt.num(c.count)}</td>
                      <td className="td num">{fmt.money(c.volume)}</td><td className="td num">{fmt.num(c.flagged)}</td>
                      <td className="td num">{fmt.pct(c.flag_rate)}</td><td className="td num">{fmt.num(c.avg_risk, 1)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          <div className="min-w-0">
            <div className="panel-title mb-1">Top merchants by flagged transactions{category ? ` · ${category.replace(/_/g, " ")}` : ""}</div>
            <QueryState q={topQ} empty={!top.length && <EmptyState title="No merchants with transactions for this filter" />}>
              <div className={cx("overflow-x-auto", topQ.isPlaceholderData && "opacity-60")}>
                <table className="w-full" data-testid="merchant-table">
                  <thead><tr><th className="th">#</th><th className="th">Merchant id</th><th className="th">Category</th><th className="th">Tx</th><th className="th">Flagged</th><th className="th">Rate</th><th className="th">Avg / max risk</th></tr></thead>
                  <tbody>
                    {top.map((m, i) => {
                      const c = med != null && high != null ? categoryOf(m.max_risk, med, high) : null;
                      return (
                        <tr key={m.merchant_id} className="hover:bg-ink-850">
                          <td className="td num text-mist-400">{i + 1}</td>
                          <td className="td num text-mist-100 max-w-[12rem] truncate" title={m.merchant_id}>{m.merchant_id}</td>
                          <td className="td">{m.category.replace(/_/g, " ")}</td>
                          <td className="td num">{fmt.num(m.count)}</td><td className="td num">{fmt.num(m.flagged)}</td>
                          <td className="td num">{m.flag_rate == null ? <Unavailable reason="not reported by this API version" /> : fmt.pct(m.flag_rate)}</td>
                          <td className="td num">{m.avg_risk == null ? "—" : fmt.num(m.avg_risk, 1)} / {c ? <RiskBadge category={c} score={m.max_risk!} /> : fmt.num(m.max_risk, 1)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <p className="text-[11px] text-mist-400 mt-1">{topQ.data?.ranking ?? "Ranked by flagged transactions."} Merchant ids are synthetic; the analytics API does not return merchant names.</p>
            </QueryState>
          </div>
        </div>
      </QueryState>
    </Panel>
  );
}

/* ------------------------------------------------------------------ D. hourly */
function HourlyPanel({ q }: { q: Q<HourlyResponse> }) {
  const hours = q.data?.hours ?? [];
  const total = hours.reduce((a, h) => a + h.count, 0);
  const active = hours.filter((h) => h.count > 0);
  const peak = [...active].sort((a, b) => b.flagged / b.count - a.flagged / a.count)[0];
  return (
    <Panel title="Hourly risk (hour of event time, UTC)" actions={<Updating on={!!q.isPlaceholderData} />}>
      <QueryState q={q} empty={q.data && total === 0 && <EmptyState title="No transactions in this period" />}>
        <div className={cx("overflow-hidden", q.isPlaceholderData && "opacity-60")} data-testid="hourly-chart"><HourlyBars hours={hours} /></div>
        <p className="text-[11px] text-mist-400 mt-1" data-testid="hourly-note">
          All 24 hours are shown; an hour with no bar had no transactions in the period (a real zero, not missing data).
          {` ${active.length} of 24 hours have activity.`}
          {peak && ` Highest flag rate: ${String(peak.hour).padStart(2, "0")}:00 UTC (${fmt.pct(peak.flagged / peak.count)} of ${fmt.num(peak.count)}).`}
          {" "}Simulated traffic is stamped with the time it is generated, so hours reflect when the simulator ran.
        </p>
      </QueryState>
    </Panel>
  );
}

/* ------------------------------------------------------------------ E. velocity */
function VelocityPanel({ q, vwin, setVwin, med, high }: { q: Q<VelocityResponse>; vwin: string; setVwin: (v: string) => void; med?: number; high?: number }) {
  const rows = q.data?.customers ?? [];
  const w = q.data?.window_minutes ?? Number(vwin);
  return (
    <Panel title="Transaction velocity (most active customers)" actions={<>
      <Updating on={!!q.isPlaceholderData} />
      <Select label="" value={vwin} onChange={setVwin} options={VELOCITY_WINDOWS} testId="velocity-window" />
    </>}>
      <QueryState q={q} empty={!rows.length && <EmptyState title={`No transactions processed in the last ${VELOCITY_WINDOWS.find((v) => v.value === vwin)?.label ?? `${vwin} min`}`} />}>
        <p className="text-xs text-mist-400 mb-2" data-testid="velocity-window-note">
          Window: transactions <b className="text-mist-200">processed in the last {w} minutes</b> (rolling, independent of the page period; the API supports up to 24 h). Top 15 customers by count; rate = count ÷ window.
        </p>
        <div className={cx("overflow-x-auto", q.isPlaceholderData && "opacity-60")}>
          <table className="w-full" data-testid="velocity-table">
            <thead><tr><th className="th">Customer</th><th className="th">Tx</th><th className="th">Tx / min</th><th className="th">Amount</th><th className="th">Max risk</th><th className="th" /></tr></thead>
            <tbody>
              {rows.map((c) => {
                const cat = med != null && high != null ? categoryOf(c.max_risk, med, high) : null;
                return (
                  <tr key={c.customer_id} className="hover:bg-ink-850">
                    <td className="td num text-mist-100">{c.customer_id}</td><td className="td num">{fmt.num(c.count)}</td>
                    <td className="td num">{(c.count / w).toFixed(c.count / w < 1 ? 3 : 1)}</td>
                    <td className="td num">{fmt.money(c.amount)}</td>
                    <td className="td">{cat ? <RiskBadge category={cat} score={c.max_risk!} /> : fmt.num(c.max_risk, 1)}</td>
                    <td className="td"><Link className="text-xs text-stream-400 hover:underline" to={`/investigations?status=&q=${encodeURIComponent(c.customer_id)}`}>alerts →</Link></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </QueryState>
    </Panel>
  );
}
