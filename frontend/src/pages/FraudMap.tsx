import { useMemo, useRef, useState, type MouseEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { MapPin, RefreshCw } from "lucide-react";
import { api, qs } from "../services/api";
import { EmptyState, KV, PageHeader, Panel, QueryState, Select, StatTile, cx, fmt } from "../components/ui";
import type { GeoPoint, GeoResponse, RiskSettingsResponse } from "../types/api";

const PERIODS = [
  { value: "60", label: "Last hour" }, { value: "360", label: "Last 6 hours" }, { value: "1440", label: "Last 24 hours" },
  { value: "10080", label: "Last 7 days" },             // /analytics/geo accepts at most 10080 minutes
];
const STALE = 30_000;

// Plot area: equirectangular (x = lon, y = -lat), cropped to latitudes 75°N … 55°S where every simulator city lies.
const LAT_TOP = 75, LAT_BOTTOM = -55, W = 360, H = LAT_TOP - LAT_BOTTOM;
const px = (lon: number) => lon + 180;
const py = (lat: number) => LAT_TOP - lat;

/** Sequential single-hue ramp (muted ink -> red) for the flagged share of a city's transactions. */
const RAMP_LO = [58, 69, 96], RAMP_HI = [230, 103, 103];
function rampColor(t: number) {
  const c = RAMP_LO.map((lo, i) => Math.round(lo + (RAMP_HI[i] - lo) * Math.max(0, Math.min(1, t))));
  return `rgb(${c[0]},${c[1]},${c[2]})`;
}
const flaggedShare = (p: GeoPoint) => (p.count ? (p.high + p.medium) / p.count : 0);
const HOVER_RADIUS = 10;   // plot units (degrees): nearest city centre within this distance wins
const keyOf = (p: GeoPoint) => `${p.city}|${p.country}`;

export function FraudMapPage() {
  const [params, setParams] = useSearchParams();
  const period = params.get("period") ?? "1440";
  const risk = params.get("risk") ?? "all";
  const country = params.get("country") ?? "";
  const set = (k: string, v: string) => setParams((p) => { const n = new URLSearchParams(p); if (v) n.set(k, v); else n.delete(k); return n; }, { replace: true });
  const [selected, setSelected] = useState<string | null>(null);
  const [hover, setHover] = useState<GeoPoint | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  const settings = useQuery({ queryKey: ["risk-settings"], queryFn: () => api<RiskSettingsResponse>("/risk/settings"), staleTime: STALE });
  const med = settings.data?.settings.medium_threshold;
  const high = settings.data?.settings.high_threshold;
  // risk-level filter is SERVER-side (min_risk); it needs the configured thresholds first
  const minRisk = risk === "all" ? 0 : risk === "high" ? high : med;
  const geo = useQuery({
    queryKey: ["geo", period, minRisk], enabled: minRisk != null, staleTime: STALE, placeholderData: keepPreviousData,
    queryFn: () => api<GeoResponse>(`/analytics/geo${qs({ minutes: period, min_risk: minRisk })}`),
  });
  const points = geo.data?.points ?? [];
  const countries = useMemo(() => [...new Set(points.map((p) => p.country))].sort(), [points]);
  const shown = useMemo(() => (country ? points.filter((p) => p.country === country) : points), [points, country]);
  const maxCount = Math.max(1, ...shown.map((p) => p.count));
  // colour domain = 0 … highest observed flagged share (stated in the legend), so differences between cities are visible
  const maxShare = Math.max(0, ...shown.map(flaggedShare));
  const shareColor = (p: GeoPoint) => rampColor(maxShare > 0 ? flaggedShare(p) / maxShare : 0);
  const totals = useMemo(() => shown.reduce((a, p) => ({ count: a.count + p.count, high: a.high + p.high, medium: a.medium + p.medium }), { count: 0, high: 0, medium: 0 }), [shown]);
  const byCountry = useMemo(() => {
    const m = new Map<string, { country: string; cities: number; count: number; high: number; medium: number }>();
    shown.forEach((p) => {
      const r = m.get(p.country) ?? { country: p.country, cities: 0, count: 0, high: 0, medium: 0 };
      r.cities += 1; r.count += p.count; r.high += p.high; r.medium += p.medium; m.set(p.country, r);
    });
    return [...m.values()].sort((a, b) => b.high + b.medium - (a.high + a.medium) || b.count - a.count);
  }, [shown]);
  const ranked = [...shown].sort((a, b) => b.high + b.medium - (a.high + a.medium) || b.count - a.count);
  const sel = shown.find((p) => `${p.city}|${p.country}` === selected) ?? null;
  const labelled = new Set(ranked.slice(0, 8).map((p) => `${p.city}|${p.country}`));
  /** Bubbles of nearby cities overlap (London/Manchester are < 3 units apart), so hover/click pick the city whose
   * CENTRE is nearest to the pointer: every city stays reachable regardless of drawing order. */
  const nearest = (e: MouseEvent<SVGElement>): GeoPoint | null => {
    const svg = svgRef.current;
    const ctm = svg?.getScreenCTM?.();
    if (!svg || !ctm) return null;
    const pt = new DOMPoint(e.clientX, e.clientY).matrixTransform(ctm.inverse());
    let best: GeoPoint | null = null, bestD = HOVER_RADIUS;
    for (const p of shown) {
      const d = Math.hypot(px(p.lon) - pt.x, py(p.lat) - pt.y);
      if (d < bestD) { best = p; bestD = d; }
    }
    return best;
  };
  const periodLabel = PERIODS.find((p) => p.value === period)?.label ?? `${period} min`;
  const riskLabel = risk === "all" ? "all transactions" : risk === "high" ? `HIGH only (risk ≥ ${high ?? "…"})` : `MEDIUM + HIGH (risk ≥ ${med ?? "…"})`;

  return (
    <div>
      <PageHeader title="Fraud Intelligence Map"
        subtitle={<>Where scored transactions occurred, by <span className="text-amber-400">simulated</span> city · {periodLabel.toLowerCase()} · {riskLabel}</>}
        actions={<>
          <Select label="Period" value={period} onChange={(v) => set("period", v)} options={PERIODS} testId="map-period" />
          <Select label="Risk level (server filter)" value={risk} onChange={(v) => set("risk", v === "all" ? "" : v)} testId="map-risk"
            options={[{ value: "all", label: "All transactions" }, { value: "medium", label: "MEDIUM + HIGH" }, { value: "high", label: "HIGH only" }]} />
          <Select label="Country (view filter)" value={country} onChange={(v) => { set("country", v); setSelected(null); }} testId="map-country"
            options={[{ value: "", label: "All countries" }, ...countries.map((c) => ({ value: c, label: c })),
              ...(country && !countries.includes(country) ? [{ value: country, label: country }] : [])]} />
          <button className="btn btn-secondary self-end" data-testid="map-refresh" disabled={geo.isFetching} onClick={() => void geo.refetch()}>
            <RefreshCw className={cx("h-4 w-4", geo.isFetching && "animate-spin")} /> Refresh
          </button>
        </>} />

      <div role="note" data-testid="geo-disclaimer" className="panel border-amber-500/40 px-4 py-3 mb-4 text-xs text-amber-400 leading-relaxed">
        <b>{geo.data?.label ?? "SIMULATED GEOGRAPHY — synthetic locations, not real customers"}.</b> Every point is the public centroid of one of the
        simulator's cities, attached to synthetic transactions; it is not a customer address, a merchant address, or the location of a confirmed
        fraud incident. "Flagged" means the risk engine scored a transaction MEDIUM/HIGH — a detection, not confirmed fraud. No basemap is drawn:
        the plot shows latitude/longitude only.
      </div>

      <QueryState q={{ ...geo, isLoading: geo.isLoading || settings.isLoading, isError: geo.isError || settings.isError, error: geo.error ?? settings.error,
        refetch: () => { void settings.refetch(); return geo.refetch(); } }}
        empty={geo.data && !geo.data.available && (
          <EmptyState title={`No located transactions for ${riskLabel} in the ${periodLabel.toLowerCase()}`}
            hint={geo.data.without_location ? `${fmt.num(geo.data.without_location)} matching transactions have no coordinates and cannot be placed.` : "Choose a longer period or a lower risk level, or generate traffic in the Simulation Lab."} />)}>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          <StatTile testId="geo-total" label="Transactions shown" tone="cyan" value={fmt.num(totals.count)} hint={`${shown.length} cities${country ? ` in ${country}` : ""}`} />
          <StatTile testId="geo-flagged" label="Flagged MEDIUM/HIGH" tone="amber" value={fmt.num(totals.high + totals.medium)}
            hint={totals.count ? `${fmt.pct((totals.high + totals.medium) / totals.count)} of shown` : "—"} />
          <StatTile testId="geo-high" label="HIGH risk" tone="coral" value={fmt.num(totals.high)} />
          <StatTile testId="geo-unlocated" label="Without coordinates" value={fmt.num(geo.data?.without_location)} hint="not placed on the map (never guessed)" />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 mb-4">
          <Panel title="Transactions by simulated city" className="xl:col-span-2"
            actions={geo.isPlaceholderData ? <span role="status" className="text-[11px] text-mist-400">updating…</span> : null}>
            {shown.length === 0 ? <EmptyState title={`No cities in ${country}`} hint="The country filter only narrows the points returned for the current period and risk level." /> : (
              <>
                <div className={cx("relative w-full overflow-hidden rounded-lg border border-ink-700 bg-ink-950", geo.isPlaceholderData && "opacity-60")}
                  data-testid="geo-map" onMouseLeave={() => setHover(null)}>
                  <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} className="block w-full h-auto" role="img"
                    aria-label={`Bubble plot of ${shown.length} simulated cities by latitude and longitude; bubble size = transactions, colour = flagged share`}>
                    {/* graticule */}
                    {[-150, -120, -90, -60, -30, 0, 30, 60, 90, 120, 150].map((lon) => (
                      <line key={`lon${lon}`} x1={px(lon)} x2={px(lon)} y1={0} y2={H} stroke={lon === 0 ? "#273049" : "#141a2b"} strokeWidth={0.4} />
                    ))}
                    {[60, 30, 0, -30].map((lat) => (
                      <g key={`lat${lat}`}>
                        <line x1={0} x2={W} y1={py(lat)} y2={py(lat)} stroke={lat === 0 ? "#273049" : "#141a2b"} strokeWidth={0.4} />
                        <text x={2} y={py(lat) - 1.5} fontSize={4} fill="#6b7790">{lat === 0 ? "0°" : `${Math.abs(lat)}°${lat > 0 ? "N" : "S"}`}</text>
                      </g>
                    ))}
                    {[...shown].sort((a, b) => b.count - a.count).map((p) => {
                      const key = `${p.city}|${p.country}`;
                      const r = 1.5 + Math.sqrt(p.count / maxCount) * 6;
                      return (
                        <g key={key} data-testid={`geo-point-${p.city}`} style={{ cursor: "pointer" }}
                          onMouseEnter={() => setHover(p)} onClick={() => setSelected(key === selected ? null : key)}>
                          <circle cx={px(p.lon)} cy={py(p.lat)} r={r} fill={shareColor(p)} fillOpacity={0.85}
                            stroke={key === selected ? "#e6ebf5" : "#0b0f19"} strokeWidth={key === selected ? 0.8 : 0.5} />
                          {labelled.has(key) && <text x={px(p.lon) + r + 1} y={py(p.lat) + 1.4} fontSize={4} fill="#c4cddf">{p.city}</text>}
                        </g>
                      );
                    })}
                    {/* nearest-centre hover/click layer (on top of all bubbles) */}
                    <rect data-testid="geo-hit-layer" x={0} y={0} width={W} height={H} fill="transparent"
                      style={{ cursor: hover ? "pointer" : "default" }}
                      onMouseMove={(e) => setHover(nearest(e))}
                      onClick={(e) => { const p = nearest(e); if (p) setSelected(keyOf(p) === selected ? null : keyOf(p)); }} />
                  </svg>
                  {hover && (
                    <div role="tooltip" data-testid="geo-tooltip"
                      className="pointer-events-none absolute z-10 max-w-[14rem] rounded-md border border-ink-600 bg-ink-850/95 px-2.5 py-1.5 text-xs text-mist-100 shadow-panel"
                      style={{ left: `${Math.min(70, (px(hover.lon) / W) * 100)}%`, top: `${Math.min(75, (py(hover.lat) / H) * 100)}%`, transform: "translate(8px, 8px)" }}>
                      <div className="font-semibold">{hover.city}, {hover.country} <span className="text-[10px] text-amber-400">simulated</span></div>
                      <div className="num text-mist-300">{fmt.num(hover.count)} tx · {fmt.num(hover.high)} HIGH · {fmt.num(hover.medium)} MEDIUM</div>
                      <div className="num text-mist-300">flagged {fmt.pct(flaggedShare(hover))} · avg risk {fmt.num(hover.avg_risk, 1)}</div>
                    </div>
                  )}
                </div>
                <div className="flex flex-wrap items-center gap-x-6 gap-y-2 mt-2 text-xs text-mist-300" data-testid="geo-legend">
                  <span className="inline-flex items-center gap-2">Flagged share
                    <span className="h-2.5 w-24 rounded-sm" style={{ background: `linear-gradient(to right, ${rampColor(0)}, ${rampColor(1)})` }} />
                    <span className="num text-mist-400" data-testid="geo-legend-range">0% → {fmt.pct(maxShare)}</span>
                    <span className="text-mist-400">(scale ends at the highest share shown)</span></span>
                  <span className="inline-flex items-center gap-2">Size = transactions
                    <span className="inline-flex items-end gap-1"><span className="h-1.5 w-1.5 rounded-full bg-mist-400" /><span className="h-3 w-3 rounded-full bg-mist-400" /></span>
                    <span className="num text-mist-400">max {fmt.num(maxCount)}</span></span>
                  <span className="text-mist-400">Hover for values · click to select · labels on the 8 most-flagged cities</span>
                </div>
              </>
            )}
          </Panel>
          <Panel title={sel ? `${sel.city}, ${sel.country}` : "Selected city"} actions={<MapPin className="h-4 w-4 text-mist-400" />}>
            {!sel ? <EmptyState title="Select a city on the map or in the table" /> : (
              <div data-testid="geo-detail">
                <KV k="transactions" v={<span className="num">{fmt.num(sel.count)}</span>} />
                <KV k="HIGH / MEDIUM" v={<span className="num">{fmt.num(sel.high)} / {fmt.num(sel.medium)}</span>} />
                <KV k="flagged share" v={<span className="num">{fmt.pct(flaggedShare(sel))}</span>} />
                <KV k="average risk" v={<span className="num">{fmt.num(sel.avg_risk, 1)}</span>} />
                <KV k="coordinates" v={<span className="num">{sel.lat.toFixed(3)}, {sel.lon.toFixed(3)}</span>} />
                <KV k="location type" v={sel.simulated ? <span className="text-amber-400">simulated city centroid</span> : "reported by source"} />
                <p className="text-[11px] text-mist-400 mt-2">Counts cover {riskLabel} in the {periodLabel.toLowerCase()}.</p>
              </div>
            )}
          </Panel>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
          <Panel title="Cities ranked by flagged transactions" className="xl:col-span-2">
            <div className="overflow-x-auto max-h-96 overflow-y-auto">
              <table className="w-full" data-testid="geo-table">
                <thead><tr><th className="th">#</th><th className="th">City</th><th className="th">Country</th><th className="th">Tx</th><th className="th">HIGH</th><th className="th">MEDIUM</th><th className="th">Flagged</th><th className="th">Avg risk</th></tr></thead>
                <tbody>
                  {ranked.map((p, i) => {
                    const key = `${p.city}|${p.country}`;
                    return (
                      <tr key={key} onClick={() => setSelected(key)} className={cx("cursor-pointer hover:bg-ink-850", key === selected && "bg-ink-850")}>
                        <td className="td num text-mist-400">{i + 1}</td>
                        <td className="td"><button className="text-stream-400 hover:underline">{p.city}</button></td>
                        <td className="td">{p.country}</td><td className="td num">{fmt.num(p.count)}</td>
                        <td className="td num">{fmt.num(p.high)}</td><td className="td num">{fmt.num(p.medium)}</td>
                        <td className="td num">{fmt.pct(flaggedShare(p))}</td><td className="td num">{fmt.num(p.avg_risk, 1)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Panel>
          <Panel title="By country (aggregated from the cities above)">
            <div className="overflow-x-auto">
              <table className="w-full" data-testid="geo-country-table">
                <thead><tr><th className="th">Country</th><th className="th">Cities</th><th className="th">Tx</th><th className="th">Flagged</th></tr></thead>
                <tbody>
                  {byCountry.map((c) => (
                    <tr key={c.country} className="hover:bg-ink-850">
                      <td className="td"><button className="text-stream-400 hover:underline" onClick={() => { set("country", c.country); setSelected(null); }}>{c.country}</button></td>
                      <td className="td num">{c.cities}</td><td className="td num">{fmt.num(c.count)}</td>
                      <td className="td num">{fmt.num(c.high + c.medium)} <span className="text-mist-400">({fmt.pct(c.count ? (c.high + c.medium) / c.count : 0)})</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>
        </div>
      </QueryState>
    </div>
  );
}
