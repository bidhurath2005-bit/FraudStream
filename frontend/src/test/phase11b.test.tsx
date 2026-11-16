/** Risk Analytics + Fraud Intelligence Map. Fixtures are trimmed copies of real responses captured from
 * the running distributed stack (Phase 11B). */
import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { App } from "../App";
import { mockApi, renderApp, type Route } from "./utils";

afterEach(() => vi.unstubAllGlobals());

const PERIOD = (m: number | null) => ({ window_minutes: m, basis: "transactions.processed_at", label: m ? `last ${m} minutes` : "all time" });
const PATTERNS = {
  scenarios: [
    { scenario: "normal", count: 9678, flagged: 240, flag_rate: 0.0248, avg_risk: 3.68, labelled_suspicious: 0 },
    { scenario: "large_amount", count: 245, flagged: 218, flag_rate: 0.8898, avg_risk: 78.1, labelled_suspicious: 245 },
    { scenario: "failed_attempts", count: 195, flagged: 192, flag_rate: 0.9846, avg_risk: 88.6, labelled_suspicious: 195 },
    { scenario: "impossible_travel_anchor", count: 121, flagged: 3, flag_rate: 0.0248, avg_risk: 2.7, labelled_suspicious: 0 },
  ],
  rule_hits: [{ rule: "IMPOSSIBLE_TRAVEL", count: 251 }, { rule: "NEW_LOCATION", count: 225 }],
  rule_hits_basis: { flagged_predictions_examined: 1166, cap: 20000, definition: "" },
  period: PERIOD(1440), note: "Scenario labels come from the simulator (synthetic ground truth).",
};
const CONFUSION = {
  risk_engine_vs_simulator: { tp: 877, fp: 289, fn: 64, tn: 9824, precision: 0.7521, recall: 0.9320, false_positive_rate: 0.0286 },
  analyst_outcomes: { CONFIRMED_SUSPICIOUS: 13 }, period: PERIOD(1440),
  definition: "flagged = risk category MEDIUM or HIGH; labels = simulator ground truth (synthetic)",
};
const MERCHANTS = {
  categories: [
    { category: "gift_cards", count: 200, volume: 67148.04, flagged: 200, flag_rate: 1.0, avg_risk: 92.83 },
    { category: "electronics", count: 1228, volume: 203772.7, flagged: 200, flag_rate: 0.1629, avg_risk: 15.9 },
    { category: "grocery", count: 1071, volume: 84621.12, flagged: 27, flag_rate: 0.0252, avg_risk: 3.31 },
  ],
  top_flagged_merchants: [
    { merchant_id: "M00031", category: "gift_cards", count: 60, flagged: 60, flag_rate: 1, avg_risk: 93.1, max_risk: 99.2 },
    { merchant_id: "M00007", category: "electronics", count: 140, flagged: 31, flag_rate: 0.2214, avg_risk: 18.2, max_risk: 97.4 },
  ],
  period: PERIOD(1440), category: null, ranking: "merchants ordered by number of MEDIUM/HIGH-scored transactions, then transaction count",
};
const HOURLY = { hours: Array.from({ length: 24 }, (_, h) => ({ hour: h, count: h === 17 ? 2000 : h === 18 ? 499 : 0, flagged: h === 17 ? 300 : h === 18 ? 127 : 0 })), timezone: "UTC (event_time)", period: PERIOD(1440) };
const VELOCITY = { window_minutes: 60, customers: [{ customer_id: "CT187a37-C000197", count: 20, amount: 1202.11, max_risk: 96.12 }, { customer_id: "C000017", count: 4, amount: 90.5, max_risk: 12.0 }] };
const SETTINGS = { settings: { medium_threshold: 40, high_threshold: 70 }, defaults: {} };

const riskRoutes = (over: Route[] = []): Route[] => [
  ...over,
  { path: "/api/v1/risk/settings", body: SETTINGS },
  { path: /\/analytics\/merchants\?.*category=/, body: { ...MERCHANTS, categories: [MERCHANTS.categories[0]], top_flagged_merchants: [MERCHANTS.top_flagged_merchants[0]], category: "gift_cards" } },
  { path: "/api/v1/analytics/merchants", body: MERCHANTS },
  { path: "/api/v1/analytics/patterns", body: PATTERNS },
  { path: "/api/v1/analytics/confusion", body: CONFUSION },
  { path: "/api/v1/analytics/hourly", body: HOURLY },
  { path: "/api/v1/analytics/velocity", body: VELOCITY },
  { path: "/api/v1/alerts", body: { items: [], total: 1166, page: 1, size: 1 } },
];

describe("Risk Analytics page", () => {
  it("summarises real backend aggregates without inventing values", async () => {
    mockApi(riskRoutes());
    renderApp(<App />, "/risk");
    expect(await screen.findByRole("heading", { name: "Risk Analytics" })).toBeInTheDocument();
    expect(await screen.findByTestId("sum-analysed")).toHaveTextContent("2,499");           // 200 + 1228 + 1071
    expect(screen.getByTestId("sum-flagged")).toHaveTextContent("427");                    // 200 + 200 + 27
    expect(await screen.findByTestId("sum-alerts")).toHaveTextContent("1,166");
    expect(screen.getByTestId("sum-confirmed")).toHaveTextContent("13");
    expect(screen.getByTestId("sum-top-pattern")).toHaveTextContent("large amount");       // most flagged INJECTED pattern
    expect(screen.getByTestId("sum-top-rule")).toHaveTextContent("IMPOSSIBLE_TRAVEL");
    expect(screen.getByTestId("period-label")).toHaveTextContent("Last 24 hours");
    // patterns distinguish injected vs benign by the label count, not by name
    const pt = screen.getByTestId("patterns-table");
    expect(within(within(pt).getByText("impossible_travel_anchor").closest("tr")!).getByText("benign")).toBeInTheDocument();
    expect(within(within(pt).getByText("large_amount").closest("tr")!).getByText("injected")).toBeInTheDocument();
    expect(screen.getByTestId("pattern-legend")).toHaveTextContent("confirmed");
    // confusion matrix + derived F1: 2·877/(2·877+289+64) = 0.8325 -> "0.832"
    expect(screen.getByTestId("cm-tp")).toHaveTextContent("877");
    expect(screen.getByTestId("cm-fp")).toHaveTextContent("289");
    expect(screen.getByTestId("cm-fn")).toHaveTextContent("64");
    expect(screen.getByTestId("cm-tn")).toHaveTextContent("9,824");
    expect(screen.getByTestId("cm-f1")).toHaveTextContent("0.832");
    expect(screen.getByTestId("synthetic-note")).toHaveTextContent("SYNTHETIC");
    expect(within(screen.getByTestId("merchant-table")).getByText("M00031")).toBeInTheDocument();
    expect(screen.getByTestId("hourly-note")).toHaveTextContent("2 of 24 hours have activity");
    expect(screen.getByTestId("velocity-window-note")).toHaveTextContent("last 60 minutes");
    expect(within(screen.getByTestId("velocity-table")).getByText("0.333")).toBeInTheDocument();   // 20 tx / 60 min
  });

  it("period, category and velocity filters are sent to the server and kept in the URL", async () => {
    const calls = mockApi(riskRoutes());
    renderApp(<App />, "/risk");
    await screen.findByTestId("sum-analysed");
    await userEvent.selectOptions(screen.getByTestId("period-filter"), "60");
    await waitFor(() => {
      for (const ep of ["patterns", "confusion", "merchants", "hourly"]) {
        expect(calls.some((c) => c.url.includes(`/analytics/${ep}?`) && c.url.includes("minutes=60"))).toBe(true);
      }
      expect(calls.some((c) => c.url.includes("/alerts?") && c.url.includes("since="))).toBe(true);
    });
    expect(screen.getByTestId("period-label")).toHaveTextContent("Last hour");
    await userEvent.selectOptions(screen.getByTestId("period-filter"), "");
    await waitFor(() => expect(calls.some((c) => c.url.endsWith("/analytics/patterns"))).toBe(true));     // all time: no minutes
    expect(screen.getByTestId("period-label")).toHaveTextContent("All time");
    await userEvent.selectOptions(screen.getByTestId("category-filter"), "gift_cards");
    await waitFor(() => expect(calls.some((c) => c.url.includes("category=gift_cards"))).toBe(true));
    await waitFor(() => expect(within(screen.getByTestId("merchant-table")).queryByText("M00007")).not.toBeInTheDocument());
    await userEvent.selectOptions(screen.getByTestId("velocity-window"), "1440");
    await waitFor(() => expect(calls.some((c) => c.url.includes("/analytics/velocity?minutes=1440"))).toBe(true));
  });

  it("a slow response for an old period never replaces the newer period's results", async () => {
    mockApi(riskRoutes([
      { path: /\/analytics\/merchants\?minutes=60&/, delay: 400, body: { ...MERCHANTS, categories: [{ ...MERCHANTS.categories[0], count: 111 }] } },
      { path: /\/analytics\/merchants\?minutes=360&/, body: { ...MERCHANTS, categories: [{ ...MERCHANTS.categories[0], count: 222 }] } },
    ]));
    renderApp(<App />, "/risk");
    await screen.findByTestId("sum-analysed");
    await userEvent.selectOptions(screen.getByTestId("period-filter"), "60");      // slow
    await userEvent.selectOptions(screen.getByTestId("period-filter"), "360");     // fast
    await waitFor(() => expect(screen.getByTestId("sum-analysed")).toHaveTextContent("222"));
    await new Promise((r) => setTimeout(r, 600));                                   // the slow 60-min reply has now arrived
    expect(screen.getByTestId("sum-analysed")).toHaveTextContent("222");
  });

  it("empty period: explicit empty states, zero hours are not drawn as data", async () => {
    mockApi(riskRoutes([
      { path: "/api/v1/analytics/merchants", body: { categories: [], top_flagged_merchants: [], period: PERIOD(60) } },
      { path: "/api/v1/analytics/patterns", body: { scenarios: [], rule_hits: [], period: PERIOD(60), note: "" } },
      { path: "/api/v1/analytics/confusion", body: { ...CONFUSION, risk_engine_vs_simulator: { tp: 0, fp: 0, fn: 0, tn: 0, precision: null, recall: null, false_positive_rate: null }, analyst_outcomes: {} } },
      { path: "/api/v1/analytics/hourly", body: { ...HOURLY, hours: HOURLY.hours.map((h) => ({ ...h, count: 0, flagged: 0 })) } },
      { path: "/api/v1/analytics/velocity", body: { window_minutes: 60, customers: [] } },
    ]));
    renderApp(<App />, "/risk?period=60");
    expect(await screen.findByText(/No scored transactions in this period/)).toBeInTheDocument();
    expect(screen.getByText("No simulator-labelled transactions in this period")).toBeInTheDocument();
    expect(screen.getByText("No labelled transactions in this period")).toBeInTheDocument();
    expect(screen.getByText("No transactions in this period")).toBeInTheDocument();
    expect(screen.getByText(/No transactions processed in the last 1 hour/)).toBeInTheDocument();
    expect(screen.queryByTestId("hourly-chart")).not.toBeInTheDocument();
  });

  it("one failing endpoint shows its own error with retry; the rest still render", async () => {
    mockApi(riskRoutes([{ path: "/api/v1/analytics/patterns", status: 500, body: { detail: "patterns exploded" } }]));
    renderApp(<App />, "/risk");
    expect(await screen.findByText("patterns exploded")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Retry" }).length).toBeGreaterThan(0);
    expect(await screen.findByTestId("cm-tp")).toHaveTextContent("877");
    expect(screen.getByTestId("sum-top-pattern")).toHaveTextContent("n/a");
  });

  it("older API responses without the new fields render n/a instead of crashing or showing 0", async () => {
    mockApi(riskRoutes([
      { path: "/api/v1/analytics/patterns", body: { ...PATTERNS, scenarios: PATTERNS.scenarios.map(({ labelled_suspicious: _l, ...s }) => s), rule_hits_basis: undefined, period: undefined } },
      { path: "/api/v1/analytics/merchants", body: { categories: MERCHANTS.categories, top_flagged_merchants: [{ merchant_id: "M1", category: "retail", count: 3, flagged: 1 }] } },
    ]));
    renderApp(<App />, "/risk");
    const pt = await screen.findByTestId("patterns-table");
    expect(within(pt).getAllByText("label n/a").length).toBe(4);
    expect(within(screen.getByTestId("merchant-table")).getAllByText("n/a").length).toBeGreaterThan(0);
  });
});

/* ------------------------------------------------------------------ map */
const GEO = {
  window_minutes: 1440, without_location: 0, available: true, label: "SIMULATED GEOGRAPHY — synthetic locations, not real customers",
  points: [
    { city: "Manchester", country: "GB", lat: 53.481, lon: -2.243, count: 378, high: 46, medium: 10, avg_risk: 15.7, simulated: true },
    { city: "London", country: "GB", lat: 51.507, lon: -0.128, count: 300, high: 10, medium: 5, avg_risk: 8.1, simulated: true },
    { city: "Cairo", country: "EG", lat: 30.044, lon: 31.236, count: 260, high: 20, medium: 7, avg_risk: 10.0, simulated: true },
  ],
};
const mapRoutes = (over: Route[] = []): Route[] => [...over, { path: "/api/v1/risk/settings", body: SETTINGS }, { path: "/api/v1/analytics/geo", body: GEO }];

describe("Fraud Intelligence Map", () => {
  it("plots the backend's city aggregates with legend, disclaimer, tooltip and selection", async () => {
    mockApi(mapRoutes());
    renderApp(<App />, "/map");
    expect(await screen.findByRole("heading", { name: "Fraud Intelligence Map" })).toBeInTheDocument();
    expect(screen.getByTestId("geo-disclaimer")).toHaveTextContent("not a customer address");
    expect(await screen.findByTestId("geo-total")).toHaveTextContent("938");
    expect(screen.getByTestId("geo-flagged")).toHaveTextContent("98");          // (46+10)+(10+5)+(20+7)
    expect(screen.getByTestId("geo-legend")).toHaveTextContent("Flagged share");
    // colour scale ends at the highest observed share: Manchester 56/378 = 14.8% (London 5.0%, Cairo 10.4%)
    expect(screen.getByTestId("geo-legend-range")).toHaveTextContent("0% → 14.8%");
    const map = screen.getByTestId("geo-map");
    expect(map.querySelectorAll("circle").length).toBe(3);                     // one bubble per city (hover = nearest centre)
    expect(screen.getByTestId("geo-hit-layer")).toBeInTheDocument();
    fireEvent.mouseEnter(screen.getByTestId("geo-point-Manchester"));
    expect(screen.getByTestId("geo-tooltip")).toHaveTextContent("378 tx · 46 HIGH · 10 MEDIUM");
    expect(screen.getByTestId("geo-tooltip")).toHaveTextContent("simulated");
    fireEvent.click(screen.getByTestId("geo-point-Cairo"));
    expect(screen.getByTestId("geo-detail")).toHaveTextContent("simulated city centroid");
    expect(screen.getByTestId("geo-detail")).toHaveTextContent("20 / 7");
    const rows = within(screen.getByTestId("geo-table")).getAllByRole("row");
    expect(rows[1]).toHaveTextContent("Manchester");                           // ranked by flagged count
  });

  it("risk level and period are server-side filters (min_risk from configured thresholds)", async () => {
    const calls = mockApi(mapRoutes());
    renderApp(<App />, "/map");
    await screen.findByTestId("geo-total");
    expect(calls.some((c) => c.url.includes("/analytics/geo?minutes=1440&min_risk=0"))).toBe(true);
    await userEvent.selectOptions(screen.getByTestId("map-risk"), "high");
    await waitFor(() => expect(calls.some((c) => c.url.includes("min_risk=70"))).toBe(true));
    await userEvent.selectOptions(screen.getByTestId("map-risk"), "medium");
    await waitFor(() => expect(calls.some((c) => c.url.includes("min_risk=40"))).toBe(true));
    await userEvent.selectOptions(screen.getByTestId("map-period"), "10080");
    await waitFor(() => expect(calls.some((c) => c.url.includes("minutes=10080"))).toBe(true));
  });

  it("country is a view filter over returned points (no new request)", async () => {
    const calls = mockApi(mapRoutes());
    renderApp(<App />, "/map");
    await screen.findByTestId("geo-total");
    const before = calls.filter((c) => c.url.includes("/analytics/geo")).length;
    await userEvent.selectOptions(screen.getByTestId("map-country"), "EG");
    expect(screen.getByTestId("geo-total")).toHaveTextContent("260");
    expect(screen.getByTestId("geo-map").querySelectorAll("circle").length).toBe(1);
    expect(calls.filter((c) => c.url.includes("/analytics/geo")).length).toBe(before);
    await userEvent.click(within(screen.getByTestId("geo-country-table")).getByText("EG"));
    expect(screen.getByTestId("geo-total")).toHaveTextContent("260");
  });

  it("explains unavailable geography and never fabricates locations", async () => {
    mockApi(mapRoutes([{ path: "/api/v1/analytics/geo", body: { ...GEO, points: [], available: false, without_location: 12 } }]));
    renderApp(<App />, "/map");
    expect(await screen.findByText(/No located transactions/)).toBeInTheDocument();
    expect(screen.getByText(/12 matching transactions have no coordinates/)).toBeInTheDocument();
    expect(screen.queryByTestId("geo-map")).not.toBeInTheDocument();
  });

  it("shows an error state with retry", async () => {
    mockApi(mapRoutes([{ path: "/api/v1/analytics/geo", status: 503, body: { detail: "database unavailable" } }]));
    renderApp(<App />, "/map");
    expect(await screen.findByText("database unavailable")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});
