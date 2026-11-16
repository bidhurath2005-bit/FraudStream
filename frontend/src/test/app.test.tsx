import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { App } from "../App";
import { NAV } from "../layouts/AppLayout";
import { parseSSEChunk } from "../services/sse";
import { mockApi, renderApp } from "./utils";

afterEach(() => vi.unstubAllGlobals());

const ALERT = {
  alert_uid: "a1b2c3d4-0000-0000-0000-000000000001", id: 1, transaction_pk: 10, transaction_id: "TX1", customer_id: "C000042",
  amount: 1234.5, currency: "USD", merchant_category: "electronics", event_time: "2026-01-01T00:00:00Z", risk_score: 82.5,
  risk_category: "HIGH", detection_method: "HYBRID",
  triggered_rules: [{ code: "NEW_DEVICE_HIGH_SPEND", name: "", family: "device", weight: 0.35, detail: "" }],
  model_version: "xgboost:v1", explanation: "Risk 82.5/100 (HIGH).", status: "NEW", assigned_to: null, investigation_id: null,
  created_at: "2026-01-01T00:00:01Z", updated_at: "2026-01-01T00:00:01Z",
};
const OVERVIEW = {
  profile: "local", total_transactions: 1500, transactions_last_minute: 600, tps_last_minute: 10,
  risk_categories: { LOW: 1400, MEDIUM: 60, HIGH: 40 }, suspicious_transactions: 100, alerts_total: 100, high_risk_open_alerts: 12,
  detection_latency_ms: { avg: 20, p50: 18, p95: 40, p99: 60, samples: 1000, definition: "" },
  detection_rate: { value: 0.9, numerator: 90, denominator: 100, definition: "", false_alert_rate: 0.01, normal_labelled: 1400 },
  model: { label: "xgboost:v1", test_pr_auc: 0.99, test_recall: 0.98, test_precision: 0.97 }, model_error: null, simulation: "RUNNING",
};
const OVERVIEW_ROUTES = [
  { path: "/api/v1/analytics/overview", body: OVERVIEW },
  { path: /analytics\/timeline/, body: { points: [] } },
  { path: "/api/v1/analytics/risk-distribution", body: { bins: [], detection_methods: {} } },
  { path: "/api/v1/risk/settings", body: { settings: { medium_threshold: 40, high_threshold: 70 } } },
];

describe("SSE parser", () => {
  it("parses events, ignores comments, keeps the partial remainder", () => {
    const { messages, rest } = parseSSEChunk(": keep-alive\n\nevent: transaction\ndata: {\"a\":1}\n\nevent: status\ndata: {\"b\"");
    expect(messages).toEqual([{ event: "transaction", data: { a: 1 } }]);
    expect(rest).toBe("event: status\ndata: {\"b\"");
  });
});

describe("authentication", () => {
  it("redirects to login and validates the form", async () => {
    mockApi([], null);
    renderApp(<App />, "/");
    const btn = await screen.findByRole("button", { name: "Sign in" });
    await userEvent.click(btn);
    expect(await screen.findByText("Username is required")).toBeInTheDocument();
  });

  it("shows a generic error on failed login", async () => {
    mockApi([{ method: "POST", path: "/api/v1/auth/login", status: 401, body: { detail: "invalid username or password" } }], null);
    renderApp(<App />, "/login");
    await userEvent.type(await screen.findByLabelText("Username"), "x");
    await userEvent.type(screen.getByLabelText("Password"), "y");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Invalid username or password.");
  });

  it("logs in and lands on the overview", async () => {
    const calls = mockApi([
      { method: "POST", path: "/api/v1/auth/login", body: { access_token: "abc", user: { id: 2, username: "analyst", role: "ANALYST" } } },
      ...OVERVIEW_ROUTES,
      { path: /\/api\/v1\/alerts/, body: { items: [], total: 0, page: 1, size: 8 } },
    ], null);
    renderApp(<App />, "/login");
    await userEvent.type(await screen.findByLabelText("Username"), "analyst");
    await userEvent.type(screen.getByLabelText("Password"), "secret-password");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText("Intelligence Overview", { selector: "h1" })).toBeInTheDocument();
    expect(calls.find((c) => c.url.endsWith("/auth/login"))?.body).toEqual({ username: "analyst", password: "secret-password" });
  });
});

describe("navigation & overview", () => {
  it("renders every nav link, the profile badge and overview numbers from the API", async () => {
    mockApi([...OVERVIEW_ROUTES, { path: /\/api\/v1\/alerts/, body: { items: [ALERT], total: 1, page: 1, size: 8 } }]);
    renderApp(<App />, "/");
    const nav = await screen.findAllByRole("navigation", { name: "Main" });
    for (const item of NAV) expect(within(nav[0]).getByText(item.label)).toBeInTheDocument();
    expect(await screen.findByTestId("stat-total")).toHaveTextContent("1,500");
    expect(screen.getByTestId("stat-high-open")).toHaveTextContent("12");
    expect(await screen.findByTestId("profile-badge")).toHaveTextContent("LOCAL DEVELOPMENT MODE");
    expect(await screen.findByText("C000042")).toBeInTheDocument();
    expect(screen.getByText("No transactions in the last 30 minutes")).toBeInTheDocument();
  });

  it("shows an error state with retry when the API fails", async () => {
    mockApi([
      { path: "/api/v1/analytics/overview", status: 500, body: { detail: "internal server error" } },
      { path: "/api/v1/risk/settings", body: { settings: {} } },
      { path: /alerts|timeline|risk-distribution/, body: {} },
    ]);
    renderApp(<App />, "/");
    expect(await screen.findByText("internal server error")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });

  it("navigates between pages via the sidebar", async () => {
    mockApi([...OVERVIEW_ROUTES, { path: /\/api\/v1\/alerts/, body: { items: [], total: 0, page: 1, size: 20 } },
      { path: "/api/v1/simulation/status", body: { status: "IDLE", model_ready: true } },
      { path: /simulation\/runs/, body: [] }]);
    renderApp(<App />, "/");
    const nav = (await screen.findAllByRole("navigation", { name: "Main" }))[0];
    await userEvent.click(within(nav).getByText("Simulation Lab"));
    expect(await screen.findByText("Simulation Lab", { selector: "h1" })).toBeInTheDocument();
    await userEvent.click(within(nav).getByText("Investigation Center"));
    expect(await screen.findByText("Investigation Center", { selector: "h1" })).toBeInTheDocument();
  });
});

describe("investigation center", () => {
  it("lists alerts and sends filter changes to the API", async () => {
    const calls = mockApi([{ path: /\/api\/v1\/alerts\?/, body: { items: [ALERT], total: 1, page: 1, size: 20 } }]);
    renderApp(<App />, "/investigations");
    const table = await screen.findByTestId("alerts-table");
    expect(within(table).getByText("C000042")).toBeInTheDocument();
    expect(within(table).getByText("NEW_DEVICE_HIGH_SPEND")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByTestId("alert-status-filter"), "CONFIRMED_SUSPICIOUS");
    await waitFor(() => expect(calls.some((c) => c.url.includes("status=CONFIRMED_SUSPICIOUS"))).toBe(true));
  });

  it("shows the empty state", async () => {
    mockApi([{ path: /\/api\/v1\/alerts\?/, body: { items: [], total: 0, page: 1, size: 20 } }]);
    renderApp(<App />, "/investigations");
    expect(await screen.findByText("No alerts match these filters")).toBeInTheDocument();
  });

  it("opens an investigation from selected alerts", async () => {
    const calls = mockApi([
      { path: /\/api\/v1\/alerts\?/, body: { items: [ALERT], total: 1, page: 1, size: 20 } },
      { method: "POST", path: "/api/v1/investigations", status: 201, body: { case_uid: "case-1" } },
      { path: "/api/v1/investigations/case-1", body: {
        case_uid: "case-1", id: 1, title: "Takeover case", status: "OPEN", alerts: [], notes: [], audit: [], customer_transactions: [],
        related_alerts: [], priority: "HIGH", opened_by: "analyst", assigned_to: null, created_at: "", updated_at: "", closed_at: null,
        final_classification: null, findings: null, customer_id: "C000042", alert_count: 0 } },
    ]);
    renderApp(<App />, "/investigations");
    await userEvent.click(await screen.findByLabelText(`select ${ALERT.alert_uid}`));
    await userEvent.type(screen.getByTestId("inv-title"), "Takeover case");
    await userEvent.click(screen.getByTestId("open-investigation"));
    await waitFor(() => expect(calls.find((c) => c.method === "POST" && c.url.endsWith("/investigations"))?.body)
      .toEqual({ alert_uids: [ALERT.alert_uid], title: "Takeover case", priority: "HIGH" }));
    expect(await screen.findByText("Start work")).toBeInTheDocument();
  });

  it("alert detail: starting a review sends PATCH", async () => {
    const calls = mockApi([
      { path: `/api/v1/alerts/${ALERT.alert_uid}`, body: { ...ALERT, prediction: null, notes: [], audit: [], investigation: null,
        transaction: { transaction_id: "TX1", amount: 1, currency: "USD", merchant_id: "M", merchant_category: "x", event_time: "",
          channel: "", transaction_type: "", device_id: "", city: "", country: "", status: "", sim_scenario: null } } },
      { method: "PATCH", path: `/api/v1/alerts/${ALERT.alert_uid}/status`, body: ALERT },
    ]);
    renderApp(<App />, `/alerts/${ALERT.alert_uid}`);
    await userEvent.click(await screen.findByRole("button", { name: "Start review" }));
    await waitFor(() => expect(calls.find((c) => c.method === "PATCH")?.body).toEqual({ status: "UNDER_REVIEW" }));
  });
});

describe("simulation lab", () => {
  it("submits a validated configuration", async () => {
    const calls = mockApi([
      { path: "/api/v1/simulation/status", body: { status: "IDLE", model_ready: true } },
      { path: /simulation\/runs/, body: [] },
      { method: "POST", path: "/api/v1/simulation/start", status: 201, body: { status: "RUNNING" } },
    ]);
    renderApp(<App />, "/simulation");
    const tps = await screen.findByTestId("sim-tps");
    await userEvent.clear(tps);
    await userEvent.type(tps, "25");
    await userEvent.click(screen.getByTestId("sim-start"));
    await waitFor(() => expect(calls.find((c) => c.url.endsWith("/simulation/start"))?.body).toMatchObject({ mode: "mixed", tps: 25, seed: 42 }));
  });
});

describe("live stream", () => {
  it("renders transactions pushed over SSE and filters them", async () => {
    const tx = (id: string, cat: string) => "event: transaction\ndata: " + JSON.stringify({
      type: "transaction", event_id: id, transaction_id: id, customer_id: `C-${id}`, merchant_id: "M", merchant_name: "Shop",
      merchant_category: "retail", amount: 10, currency: "USD", processed_at: "2026-01-01T00:00:00Z", status: "APPROVED", city: "Paris",
      country: "FR", risk_score: cat === "HIGH" ? 90 : 5, risk_category: cat, detection_method: "ML", recommended_action: "ALLOW",
      alert_uid: null, latency_ms: 12 }) + "\n\n";
    mockApi([{ path: "/api/v1/stream/transactions", stream: ["event: status\ndata: {\"live\":\"connected\"}\n\n", tx("e1", "LOW"), tx("e2", "HIGH")] }]);
    renderApp(<App />, "/live");
    expect(await screen.findByText("C-e1")).toBeInTheDocument();
    expect(screen.getByText("C-e2")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByTestId("risk-filter"), "HIGH");
    expect(screen.queryByText("C-e1")).not.toBeInTheDocument();
    expect(screen.getByText("C-e2")).toBeInTheDocument();
  });
});

describe("session expiry", () => {
  it("returns to the login screen when a 401 cannot be refreshed", async () => {
    mockApi([...OVERVIEW_ROUTES, { path: /\/api\/v1\/alerts/, body: { items: [], total: 0, page: 1, size: 8 } }]);
    renderApp(<App />, "/");
    expect(await screen.findByText("Intelligence Overview", { selector: "h1" })).toBeInTheDocument();
    // token now expired and refresh rejected
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ detail: "token expired" }), { status: 401 })));
    await waitFor(() => expect(screen.getByRole("button", { name: "Sign in" })).toBeInTheDocument(), { timeout: 10000 });
    expect(screen.getByText(/session expired/i)).toBeInTheDocument();
  }, 15000);
});
