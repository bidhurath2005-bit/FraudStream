/**
 * Chart components. Colours: stream cyan for volume (single series, no legend needed);
 * risk bands use the STATUS palette (emerald/amber/coral) and are always labelled
 * LOW/MEDIUM/HIGH in text so colour is never the only cue. One y-axis per chart.
 */
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";

const AXIS = { stroke: "#3a4560", tick: { fill: "#94a0b8", fontSize: 11 }, tickLine: false };
const GRID = { stroke: "#1c2438", strokeDasharray: "0", vertical: false };
const TIP = {
  // long labels (e.g. rule codes) wrap instead of widening the tooltip beyond its chart
  contentStyle: { background: "#0f1422", border: "1px solid #273049", borderRadius: 8, fontSize: 12, color: "#e6ebf5",
    whiteSpace: "normal" as const, overflowWrap: "anywhere" as const },
  wrapperStyle: { maxWidth: "min(16rem, 90%)", zIndex: 10 },
  labelStyle: { color: "#94a0b8" },
  cursor: { stroke: "#3a4560", fill: "rgba(58,69,96,0.25)" },
};
export const RISK_COLORS = { LOW: "#10b981", MEDIUM: "#f59e0b", HIGH: "#f2554a" } as const;

export function VolumeChart({ points, height = 220 }: {
  points: { t: string; total: number; medium: number; high: number }[]; height?: number;
}) {
  const data = points.map((p) => ({ ...p, label: new Date(p.t).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" }) }));
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="vol" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#18c8dc" stopOpacity={0.28} />
            <stop offset="100%" stopColor="#18c8dc" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid {...GRID} />
        <XAxis dataKey="label" {...AXIS} minTickGap={24} />
        <YAxis {...AXIS} allowDecimals={false} width={48} />
        <Tooltip {...TIP} formatter={(v: number, n: string) => [v.toLocaleString(), n === "total" ? "Transactions" : n]} />
        <Area type="monotone" dataKey="total" name="total" stroke="#18c8dc" strokeWidth={2} fill="url(#vol)" isAnimationActive={false} />
      </AreaChart>
    </ResponsiveContainer>
  );
}

export function FlaggedChart({ points, height = 140 }: {
  points: { t: string; medium: number; high: number }[]; height?: number;
}) {
  const data = points.map((p) => ({ ...p, label: new Date(p.t).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" }) }));
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }} barCategoryGap={2}>
        <CartesianGrid {...GRID} />
        <XAxis dataKey="label" {...AXIS} minTickGap={24} />
        <YAxis {...AXIS} allowDecimals={false} width={48} />
        <Tooltip {...TIP} />
        <Bar dataKey="medium" name="MEDIUM" stackId="f" fill={RISK_COLORS.MEDIUM} isAnimationActive={false} />
        <Bar dataKey="high" name="HIGH" stackId="f" fill={RISK_COLORS.HIGH} radius={[4, 4, 0, 0]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export function RiskHistogram({ bins, medium, high, height = 220 }: {
  bins: { from: number; to: number; count: number }[]; medium: number; high: number; height?: number;
}) {
  const band = (from: number) => (from >= high ? "HIGH" : from >= medium ? "MEDIUM" : "LOW");
  const data = bins.map((b) => ({ ...b, label: `${b.from}`, band: band(b.from) }));
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }} barCategoryGap={2}>
        <CartesianGrid {...GRID} />
        <XAxis dataKey="label" {...AXIS} interval={3} />
        <YAxis {...AXIS} allowDecimals={false} width={48} scale="sqrt" />
        <Tooltip {...TIP} formatter={(v: number) => [v.toLocaleString(), "Transactions"]}
          labelFormatter={(_l, p) => (p?.[0] ? `Risk ${p[0].payload.from}–${p[0].payload.to} · ${p[0].payload.band}` : "")} />
        <Bar dataKey="count" radius={[4, 4, 0, 0]} isAnimationActive={false}>
          {data.map((d) => <Cell key={d.from} fill={RISK_COLORS[d.band as keyof typeof RISK_COLORS]} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

export function RiskLegend({ medium, high }: { medium: number; high: number }) {
  const items = [["LOW", `< ${medium}`], ["MEDIUM", `${medium}–${high}`], ["HIGH", `≥ ${high}`]] as const;
  return (
    <div className="flex flex-wrap gap-4 text-xs text-mist-300">
      {items.map(([k, r]) => (
        <span key={k} className="inline-flex items-center gap-1.5">
          <span className="h-2.5 w-2.5 rounded-sm" style={{ background: RISK_COLORS[k] }} /> {k} <span className="text-mist-400 num">{r}</span>
        </span>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ Phase 11A charts */

/** Model identity colours: fixed slot per model NAME (never per rank), validated on the
 * #0b0f19 surface (dark reference palette slots 1-4, adjacent CVD ΔE ≥ 8.4). */
export const MODEL_COLORS: Record<string, string> = {
  logistic_regression: "#3987e5", random_forest: "#d95926", xgboost: "#199e70", isolation_forest: "#c98500",
};
const FALLBACK_MODEL_COLOR = "#94a0b8";
export const modelColor = (name: string) => MODEL_COLORS[name] ?? FALLBACK_MODEL_COLOR;

export const STATUS_COLORS = { STABLE: "#10b981", MODERATE: "#f59e0b", SIGNIFICANT: "#f2554a" } as const;

/** Grouped bars: one group per metric, one bar per model (legend + table view on the page). */
export function ModelMetricBars({ metrics, models, height = 260 }: {
  metrics: { key: string; label: string }[];
  models: { name: string; values: Record<string, number | null | undefined> }[];
  height?: number;
}) {
  const data = metrics.map((m) => {
    const row: Record<string, number | string | null> = { metric: m.label };
    models.forEach((mod) => { row[mod.name] = mod.values[m.key] ?? null; });
    return row;
  });
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }} barCategoryGap="22%" barGap={2}>
        <CartesianGrid {...GRID} />
        <XAxis dataKey="metric" {...AXIS} />
        <YAxis {...AXIS} domain={[0, 1]} width={40} tickFormatter={(v: number) => v.toFixed(1)} />
        <Tooltip {...TIP} formatter={(v: unknown, n: string) => [typeof v === "number" ? v.toFixed(4) : "n/a", n]} />
        <Legend wrapperStyle={{ fontSize: 12, color: "#c4cddf" }} iconType="square" iconSize={10} />
        {models.map((m) => (
          <Bar key={m.name} dataKey={m.name} name={m.name} fill={modelColor(m.name)} radius={[4, 4, 0, 0]} isAnimationActive={false} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

/** Horizontal PSI bars per feature, coloured by drift status (status is also given in text). */
export function PsiBars({ rows, moderate, significant, height }: {
  rows: { feature: string; psi: number; status: string }[]; moderate: number; significant: number; height?: number;
}) {
  const h = height ?? Math.max(160, rows.length * 22 + 40);
  return (
    <ResponsiveContainer width="100%" height={h}>
      <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 0 }} barCategoryGap={3}>
        <CartesianGrid stroke="#1c2438" horizontal={false} />
        <XAxis type="number" {...AXIS} scale="sqrt" domain={[0, "auto"]} tickFormatter={(v: number) => v.toFixed(2)} />
        <YAxis type="category" dataKey="feature" {...AXIS} width={150} interval={0} />
        <Tooltip {...TIP} formatter={(v: number, _n, p) => [`${v.toFixed(4)} · ${p.payload.status}`, "PSI"]} />
        <ReferenceLine x={moderate} stroke="#f59e0b" strokeDasharray="4 3" label={{ value: "moderate", fill: "#94a0b8", fontSize: 10, position: "top" }} />
        <ReferenceLine x={significant} stroke="#f2554a" strokeDasharray="4 3" label={{ value: "significant", fill: "#94a0b8", fontSize: 10, position: "top" }} />
        <Bar dataKey="psi" radius={[0, 4, 4, 0]} isAnimationActive={false}>
          {rows.map((r) => <Cell key={r.feature} fill={STATUS_COLORS[r.status as keyof typeof STATUS_COLORS] ?? "#3a4560"} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

/** Generic time series: up to 2 series on ONE shared y-scale (legend when > 1). */
export function TimeSeries({ points, series, height = 200, yLabel, yDomain }: {
  points: Record<string, number | string | null>[];
  series: { key: string; name: string; color: string }[];
  height?: number; yLabel?: (v: number) => string; yDomain?: [number | "auto", number | "auto"];
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid {...GRID} />
        <XAxis dataKey="label" {...AXIS} minTickGap={28} />
        <YAxis {...AXIS} width={48} domain={yDomain ?? [0, "auto"]} tickFormatter={yLabel} />
        <Tooltip {...TIP} formatter={(v: unknown, n: string) => [typeof v !== "number" ? "—" : (yLabel ? yLabel(v) : v.toLocaleString()), n]} />
        {series.length > 1 && <Legend wrapperStyle={{ fontSize: 12, color: "#c4cddf" }} iconType="plainline" />}
        {series.map((s) => (
          <Line key={s.key} type="monotone" dataKey={s.key} name={s.name} stroke={s.color} strokeWidth={2}
            dot={points.length < 30 ? { r: 3, strokeWidth: 0, fill: s.color } : false} connectNulls={false} isAnimationActive={false} />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

/* ------------------------------------------------------------------ Phase 11B charts */
/** Single-series horizontal bars (long category names on the y-axis; values in the tooltip and the table view). */
export function HBars({ rows, valueLabel, format, color = "#3987e5", height, domainMax, labelWidth = 150, testId }: {
  rows: { label: string; value: number; hint?: string }[]; valueLabel: string; format: (v: number) => string;
  color?: string; height?: number; domainMax?: number; labelWidth?: number; testId?: string;
}) {
  const h = height ?? Math.max(120, rows.length * 24 + 30);
  return (
    // overflow-hidden: the chart is the boundary — an absolutely positioned (even hidden) tooltip must not widen the page
    <div data-testid={testId} className="overflow-hidden">
      <ResponsiveContainer width="100%" height={h}>
        <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 12, left: 4, bottom: 0 }} barCategoryGap={4}>
          <CartesianGrid stroke="#1c2438" horizontal={false} />
          <XAxis type="number" {...AXIS} domain={[0, domainMax ?? "auto"]} tickFormatter={format} />
          <YAxis type="category" dataKey="label" {...AXIS} width={labelWidth} interval={0}
            tickFormatter={(v: string) => (v.length > 22 ? `${v.slice(0, 21)}…` : v)} />
          <Tooltip {...TIP} formatter={(v: unknown, _n, p) => [typeof v === "number" ? format(v) : "—", p?.payload?.hint ? `${valueLabel} · ${p.payload.hint}` : valueLabel]} />
          <Bar dataKey="value" fill={color} radius={[0, 4, 4, 0]} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

/** 24 hourly buckets, stacked: not flagged + flagged (same unit, one axis). */
export function HourlyBars({ hours, height = 220 }: { hours: { hour: number; count: number; flagged: number }[]; height?: number }) {
  const data = hours.map((h) => ({
    label: String(h.hour).padStart(2, "0"), not_flagged: h.count - h.flagged, flagged: h.flagged, total: h.count,
  }));
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }} barCategoryGap={2}>
        <CartesianGrid {...GRID} />
        <XAxis dataKey="label" {...AXIS} interval={1} />
        <YAxis {...AXIS} allowDecimals={false} width={48} />
        <Tooltip {...TIP} labelFormatter={(l) => `${l}:00–${l}:59 UTC`}
          formatter={(v: unknown, n: string, p) => {
            const tot = p?.payload?.total ?? 0;
            return [typeof v === "number" ? `${v.toLocaleString()}${n === "flagged (MEDIUM/HIGH)" && tot ? ` (${((v / tot) * 100).toFixed(1)}%)` : ""}` : "—", n];
          }} />
        <Legend wrapperStyle={{ fontSize: 12, color: "#c4cddf" }} iconType="square" iconSize={10} />
        <Bar dataKey="not_flagged" name="not flagged (LOW)" stackId="h" fill="#3987e5" isAnimationActive={false} />
        <Bar dataKey="flagged" name="flagged (MEDIUM/HIGH)" stackId="h" fill="#d95926" radius={[4, 4, 0, 0]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}
