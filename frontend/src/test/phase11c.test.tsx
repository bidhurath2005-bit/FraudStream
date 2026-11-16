/**
 * Phase 11C: Reports and Settings pages with real-shaped backend responses (fetch mocked at the
 * network boundary). Filters must reach the server, exports must use the server's file, errors and
 * empty states must be explicit, and configuration changes must be confirmed, minimal and concurrency-safe.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { App } from "../App";
import { ANALYST, mockApi, renderApp, type Route } from "./utils";
import { fmtReportValue, resolvePeriod } from "../pages/Reports";
import { tokenExpiry, validateScoring } from "../pages/Settings";
import { dispositionFilename } from "../services/api";

const ADMIN = { ...ANALYST, id: 1, username: "admin", role: "ADMIN" };
const UID = "4f0c7a2e-1111-4222-8333-944455556666";

const CATALOGUE = {
  types: [
    { key: "fraud_activity", title: "Fraud activity report", description: "Transactions persisted in the period.", period_basis: "transactions.processed_at",
      filters: [{ key: "risk_level", options: ["LOW", "MEDIUM", "HIGH"] }], admin_only: false, available: true },
    { key: "risk_analytics", title: "Risk analytics report", description: "Risk-score distribution.", period_basis: "transactions.processed_at",
      filters: [], admin_only: false, available: true },
    { key: "alert_summary", title: "Alert summary report", description: "Alerts raised in the period.", period_basis: "alerts.created_at",
      filters: [{ key: "risk_level", options: ["LOW", "MEDIUM", "HIGH"] }, { key: "alert_status", options: ["NEW", "UNDER_REVIEW", "CONFIRMED_SUSPICIOUS", "DISMISSED", "CLOSED"] }],
      admin_only: false, available: true },
    { key: "system_activity", title: "System activity report", description: "Audit trail events.", period_basis: "audit_logs.created_at",
      filters: [], admin_only: true, available: false },
  ],
  max_period_days: 366, detail_row_cap: 10000, pdf_row_cap: 500, preview_rows: 100, persisted: true,
};

const REPORT = {
  report_uid: UID, report_type: "alert_summary", title: "Alert summary report", period_start: "2026-11-08T00:00:00+00:00",
  period_end: "2026-11-15T00:00:00+00:00", period_label: "2026-11-08 00:00 UTC → 2026-11-15 00:00 UTC (end exclusive)",
  filters: { risk_level: "HIGH" }, status: "COMPLETED", error: null, row_count: 3, generated_by: "analyst", created_at: "2026-11-15T01:00:00+00:00",
  content: {
    report_uid: UID, report_type: "alert_summary", title: "Alert summary report", generated_at: "2026-11-15T01:00:00+00:00",
    generated_by: "analyst", profile: "distributed",
    period: { start: "2026-11-08T00:00:00+00:00", end: "2026-11-15T00:00:00+00:00", basis: "alerts.created_at",
      label: "2026-11-08 00:00 UTC → 2026-11-15 00:00 UTC (end exclusive)" },
    filters: { risk_level: "HIGH" },
    summary: [
      { key: "alerts", label: "Alerts raised", value: 1234, format: "int" },
      { key: "avg_risk", label: "Average risk score", value: null, format: "score" },
    ],
    sections: [
      { id: "by_status", title: "By status", description: null, columns: [{ key: "status", label: "Status", format: "text" }, { key: "alerts", label: "Alerts", format: "int" }],
        rows: [{ status: "NEW", alerts: 1234 }], total_rows: 1, truncated: false, stored_rows: 1 },
      { id: "alerts", title: "Alert list", description: "Newest first.", columns: [{ key: "alert_uid", label: "Alert id", format: "text" }, { key: "risk_score", label: "Score", format: "score" }],
        rows: [{ alert_uid: "a-1", risk_score: 91.234 }, { alert_uid: "a-2", risk_score: 88 }], total_rows: 1234, truncated: false, stored_rows: 1234 },
      { id: "by_rule", title: "By triggered rule", description: null, columns: [{ key: "rule", label: "Rule", format: "text" }], rows: [], total_rows: 0, truncated: false, stored_rows: 0 },
    ],
    limitations: ["All transactions are SYNTHETIC."], data_sources: ["alerts.created_at"],
  },
};
const META = (() => { const { content: _c, ...m } = REPORT; return m; })();

const reportRoutes = (over: Route[] = []): Route[] => [
  ...over,
  { path: "/api/v1/reports/types", body: CATALOGUE },
  { path: "/api/v1/reports/generated", body: { items: [META], total: 1, page: 1, size: 10 } },
  { path: `/api/v1/reports/generated/${UID}`, body: REPORT },
  { method: "POST", path: "/api/v1/reports", status: 201, body: REPORT },
];

beforeEach(() => {
  vi.stubGlobal("URL", Object.assign(URL, { createObjectURL: vi.fn(() => "blob:x"), revokeObjectURL: vi.fn() }));
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);   // jsdom cannot navigate to blob: URLs
});
afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

describe("report helpers", () => {
  it("resolves custom UTC dates to an end-exclusive period and validates it", () => {
    const p = resolvePeriod("custom", "2026-11-01", "2026-11-07", 366);
    expect("start" in p && p.start.toISOString()).toBe("2026-11-01T00:00:00.000Z");
    expect("end" in p && p.end.toISOString()).toBe("2026-11-08T00:00:00.000Z");
    expect(resolvePeriod("custom", "2026-11-08", "2026-11-07", 366)).toEqual({ error: "The start date must not be after the end date." });
    expect(resolvePeriod("custom", "2024-01-01", "2026-01-01", 366)).toEqual({ error: "The period must not exceed 366 days." });
    const now = new Date("2026-11-15T10:20:30Z");
    const d = resolvePeriod("24h", "", "", 366, now) as { start: Date; end: Date };
    expect(d.end.toISOString()).toBe("2026-11-15T10:21:00.000Z");
    expect(d.start.toISOString()).toBe("2026-11-14T10:21:00.000Z");
  });
  it("formats values like the PDF and never turns missing into zero", () => {
    expect(fmtReportValue(0.1234, "pct")).toBe("12.3%");
    expect(fmtReportValue(1234567, "int")).toBe("1,234,567");
    expect(fmtReportValue(null, "pct")).toBeNull();
    expect(fmtReportValue("2026-11-15T01:02:03+00:00", "datetime")).toBe("2026-11-15 01:02:03");
  });
  it("keeps only a safe file name from Content-Disposition", () => {
    expect(dispositionFilename('attachment; filename="fraudstream-alert-summary-20261108-20261115-4f0c7a2e.pdf"', "x.pdf"))
      .toBe("fraudstream-alert-summary-20261108-20261115-4f0c7a2e.pdf");
    expect(dispositionFilename('attachment; filename="../../etc/pass wd"', "x.csv")).toBe("pass_wd");
    expect(dispositionFilename(null, "fallback.csv")).toBe("fallback.csv");
  });
});

describe("Reports page", () => {
  it("offers only backend report types, shows each type's server filters and locks admin-only types for analysts", async () => {
    mockApi(reportRoutes());
    renderApp(<App />, "/reports");
    const type = await screen.findByTestId("report-type");
    expect(within(type).getByRole("option", { name: /System activity report \(admin only\)/ })).toBeDisabled();
    expect(screen.getByTestId("report-filter-risk_level")).toBeInTheDocument();
    expect(screen.queryByTestId("report-filter-alert_status")).toBeNull();
    await userEvent.selectOptions(type, "alert_summary");
    expect(screen.getByTestId("report-filter-alert_status")).toBeInTheDocument();
    await userEvent.selectOptions(type, "risk_analytics");
    expect(screen.queryByTestId("report-filter-risk_level")).toBeNull();
    expect(screen.getByText("This report type has no filters other than the period.")).toBeInTheDocument();
  });

  it("sends the period and only the applicable filters to the server, then previews the stored report", async () => {
    const calls = mockApi(reportRoutes());
    renderApp(<App />, "/reports");
    await userEvent.selectOptions(await screen.findByTestId("report-type"), "alert_summary");
    await userEvent.selectOptions(screen.getByTestId("report-preset"), "custom");
    fireEvent.change(screen.getByTestId("report-from"), { target: { value: "2026-11-08" } });
    fireEvent.change(screen.getByTestId("report-to"), { target: { value: "2026-11-14" } });
    expect(screen.getByTestId("report-period-preview")).toHaveTextContent("2026-11-08 00:00 UTC → 2026-11-15 00:00 UTC");
    await userEvent.selectOptions(screen.getByTestId("report-filter-risk_level"), "HIGH");
    await userEvent.click(screen.getByTestId("report-generate"));
    const post = await waitFor(() => { const c = calls.find((x) => x.method === "POST" && x.url.endsWith("/reports")); expect(c).toBeTruthy(); return c!; });
    expect(post.body).toEqual({ report_type: "alert_summary", start: "2026-11-08T00:00:00.000Z", end: "2026-11-15T00:00:00.000Z", risk_level: "HIGH" });
    expect(await screen.findByTestId("report-uid")).toHaveTextContent(UID);
    expect(screen.getByTestId("report-period")).toHaveTextContent("end exclusive");
    expect(screen.getByTestId("report-filters")).toHaveTextContent("Risk level: HIGH");
    expect(screen.getByTestId("metric-alerts")).toHaveTextContent("1,234");
    expect(screen.getByTestId("metric-avg_risk")).toHaveTextContent("n/a");                 // null is "n/a", not 0
    expect(within(screen.getByTestId("section-alerts")).getByText("91.2")).toBeInTheDocument();
    expect(screen.getByTestId("section-count-alerts")).toHaveTextContent("Preview shows 2 of 1,234 rows");
    expect(screen.getByTestId("section-empty-by_rule")).toHaveTextContent("No rows for this period and filters.");
    expect(screen.getByTestId("report-limitations")).toHaveTextContent("SYNTHETIC");
  });

  it("blocks an invalid custom period before anything is sent", async () => {
    const calls = mockApi(reportRoutes());
    renderApp(<App />, "/reports");
    await userEvent.selectOptions(await screen.findByTestId("report-preset"), "custom");
    fireEvent.change(screen.getByTestId("report-from"), { target: { value: "2026-11-10" } });
    fireEvent.change(screen.getByTestId("report-to"), { target: { value: "2026-11-01" } });
    expect(screen.getByTestId("report-period-preview")).toHaveTextContent("must not be after");
    expect(screen.getByTestId("report-generate")).toBeDisabled();
    expect(calls.some((c) => c.method === "POST" && c.url.endsWith("/reports"))).toBe(false);
  });

  it("shows the server's reason when generation is rejected", async () => {
    mockApi(reportRoutes([{ method: "POST", path: "/api/v1/reports", status: 422, body: { detail: "period must not exceed 366 days" } }]));
    renderApp(<App />, "/reports");
    await userEvent.click(await screen.findByTestId("report-generate"));
    expect(await screen.findByTestId("report-error")).toHaveTextContent("period must not exceed 366 days");
  });

  it("downloads the server's PDF/CSV with the server file name and reports failures", async () => {
    const calls = mockApi(reportRoutes([
      { path: new RegExp(`/reports/generated/${UID}/download\\?format=pdf`), body: { pdf: true },
        headers: { "Content-Disposition": 'attachment; filename="fraudstream-alert-summary-20261108-20261115-4f0c7a2e.pdf"' } },
      { path: new RegExp(`/reports/generated/${UID}/download\\?format=csv&section=alerts`), status: 409, body: { detail: "this report failed to generate" } },
    ]));
    renderApp(<App />, `/reports?report=${UID}`);
    await userEvent.click(await screen.findByTestId("export-pdf"));
    expect(await screen.findByTestId("export-status")).toHaveTextContent("Downloaded fraudstream-alert-summary-20261108-20261115-4f0c7a2e.pdf");
    expect(calls.some((c) => c.url.endsWith(`/reports/generated/${UID}/download?format=pdf`))).toBe(true);
    await userEvent.click(screen.getByTestId("section-csv-alerts"));
    await waitFor(() => expect(screen.getByTestId("export-status")).toHaveTextContent("Download failed: this report failed to generate"));
    expect(screen.getByTestId("section-csv-by_rule")).toBeDisabled();                      // nothing to export
  });

  it("treats an empty download as a failure", async () => {
    mockApi(reportRoutes([{ path: new RegExp(`/download\\?format=csv$`), status: 204 }]));
    renderApp(<App />, `/reports?report=${UID}`);
    await userEvent.click(await screen.findByTestId("export-csv"));
    expect(await screen.findByTestId("export-status")).toHaveTextContent("Download failed: The server returned an empty file");
  });

  it("shows a failed report without export actions, and filters history on the server", async () => {
    const calls = mockApi(reportRoutes([
      { path: `/api/v1/reports/generated/${UID}`, body: { ...REPORT, status: "FAILED", error: "report generation failed (OperationalError)", content: null } },
    ]));
    renderApp(<App />, `/reports?report=${UID}`);
    expect(await screen.findByTestId("report-failed")).toHaveTextContent("OperationalError");
    expect(screen.queryByTestId("export-pdf")).toBeNull();
    await screen.findByTestId("history-table");
    await userEvent.selectOptions(screen.getByTestId("history-type"), "alert_summary");
    await waitFor(() => expect(calls.some((c) => c.url.includes("/reports/generated?page=1&size=10&report_type=alert_summary"))).toBe(true));
  });

  it("explains an empty history", async () => {
    mockApi(reportRoutes([{ path: "/api/v1/reports/generated", body: { items: [], total: 0, page: 1, size: 10 } }]));
    renderApp(<App />, "/reports");
    expect(await screen.findByText("No reports generated yet")).toBeInTheDocument();
  });
});

/* ------------------------------------------------------------------ settings */
const RULES = [
  { code: "HIGH_VELOCITY_1M", name: "Excessive 1-minute velocity", description: "Prior transactions in the last 60 s >= count.", family: "velocity",
    weight: 0.35, params: { count: 4 }, enabled: true, updated_at: "2026-11-14T18:00:00.123456+00:00", editable: true, last_change: null,
    param_bounds: { count: { integer: true, min: 1, min_inclusive: true, max: 1000 } } },
  { code: "NEW_LOCATION", name: "New location", description: "City never seen for this customer.", family: "location",
    weight: 0.1, params: {}, enabled: true, updated_at: "2026-11-14T18:00:00+00:00", editable: true,
    last_change: { by: "admin", at: "2026-11-15T00:30:00+00:00", action: "RULE_UPDATED" }, param_bounds: {} },
];
const RISK = {
  settings: { medium_threshold: 40, high_threshold: 70, model_weight: 0.9, anomaly_weight: 0.3, anomaly_min_percentile: 0.97, block_simulation_enabled: true },
  defaults: { medium_threshold: 40, high_threshold: 70, model_weight: 0.9, anomaly_weight: 0.3, anomaly_min_percentile: 0.97, block_simulation_enabled: true },
  updated_at: "2026-11-14T18:00:00.5+00:00", last_change: null, editable_by: "ADMIN", used_by: "risk engine (API scoring, Spark job via the published runtime config)",
};
const DRIFT = { key: "drift", value: { moderate: 0.1, significant: 0.25, window_rows: 2000, min_rows: 200 },
  defaults: { moderate: 0.1, significant: 0.25, window_rows: 2000, min_rows: 200 }, updated_at: "2026-11-14T18:00:00+00:00",
  last_change: null, used_by: "drift check", editable_by: "ADMIN", bounds: { window_rows: { min: 50, max: 100000 }, min_rows: { min: 1, max: 100000 } } };
const settingsRoutes = (over: Route[] = []): Route[] => [
  ...over,
  { path: "/api/v1/risk/rules", body: RULES },
  { path: "/api/v1/risk/settings", body: RISK },
  { path: "/api/v1/settings/drift", body: DRIFT },
  { path: "/api/v1/auth/me", body: ADMIN },
  { path: "/api/v1/users", body: [ADMIN, ANALYST] },
  { path: "/api/v1/audit", body: { items: [
    { id: 9, actor: "analyst", action: "CONFIG_CHANGE_DENIED", entity_type: "system_setting", entity_id: "risk", field: null, old_value: null, new_value: null,
      details: { role: "ANALYST" }, created_at: "2026-11-15T01:00:00+00:00" },
    { id: 8, actor: "admin", action: "RISK_SETTINGS_REJECTED", entity_type: "system_setting", entity_id: "risk", field: null, old_value: null, new_value: null,
      details: { reason: "require 0 < medium_threshold < high_threshold <= 100" }, created_at: "2026-11-15T00:59:00+00:00" },
    { id: 7, actor: "admin", action: "RULE_UPDATED", entity_type: "risk_rule", entity_id: "HIGH_VELOCITY_1M", field: "params", old_value: '{"count": 4}',
      new_value: '{"count": 6}', details: {}, created_at: "2026-11-15T00:58:00+00:00" },
  ], total: 3, page: 1, size: 25 } },
];

describe("Settings page — analyst", () => {
  it("shows configuration read-only and does not call admin-only endpoints", async () => {
    const calls = mockApi(settingsRoutes());
    renderApp(<App />, "/settings");
    expect(await screen.findByTestId("readonly-notice")).toHaveTextContent("administrator role");
    expect(await screen.findByTestId("rule-HIGH_VELOCITY_1M")).toHaveTextContent("Excessive 1-minute velocity");
    expect(screen.getByTestId("rule-param-HIGH_VELOCITY_1M-count")).toHaveTextContent("4");
    expect(screen.getByTestId("rule-NEW_LOCATION")).toHaveTextContent("by admin");
    expect(screen.queryByTestId("rule-edit-HIGH_VELOCITY_1M")).toBeNull();
    await userEvent.click(screen.getByTestId("tab-scoring"));
    expect(await screen.findByTestId("scoring-medium_threshold")).toBeDisabled();
    expect(screen.queryByTestId("scoring-review")).toBeNull();
    await userEvent.click(screen.getByTestId("tab-audit"));
    expect(screen.getByTestId("audit-restricted")).toBeInTheDocument();
    expect(calls.some((c) => c.url.includes("/audit"))).toBe(false);
  });
});

describe("Settings page — admin", () => {
  it("edits a rule: validation, confirmation with the exact diff, minimal PATCH with the loaded version", async () => {
    const calls = mockApi(settingsRoutes([
      { method: "PATCH", path: "/api/v1/risk/rules/HIGH_VELOCITY_1M", body: { ...RULES[0], params: { count: 6 }, changed_fields: ["params"],
        propagation: { file: "ok", redis: "ok" }, updated_at: "2026-11-15T02:00:00+00:00" } },
    ]), ADMIN);
    renderApp(<App />, "/settings?tab=rules");
    await userEvent.click(await screen.findByTestId("rule-edit-HIGH_VELOCITY_1M"));
    const input = screen.getByTestId("rule-param-input-count");
    await userEvent.clear(input);
    await userEvent.type(input, "2.5");
    expect(screen.getByText("must be a whole number")).toBeInTheDocument();
    expect(screen.getByTestId("rule-review")).toBeDisabled();
    await userEvent.clear(input);
    await userEvent.type(input, "6");
    await userEvent.click(screen.getByTestId("rule-review"));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByTestId("change-list")).toHaveTextContent("params.count: 4 → 6");
    expect(calls.some((c) => c.method === "PATCH")).toBe(false);                            // nothing saved before confirmation
    await userEvent.click(within(dialog).getByTestId("confirm-rule-confirm"));
    expect(await screen.findByTestId("save-status")).toHaveTextContent("Saved Excessive 1-minute velocity: params");
    expect(screen.getByTestId("save-status")).toHaveTextContent("Redis (read by Spark): ok");
    const patch = calls.find((c) => c.method === "PATCH")!;
    expect(patch.body).toEqual({ params: { count: 6 }, expected_updated_at: "2026-11-14T18:00:00.123456+00:00" });
  });

  it("cancelling with Escape saves nothing; a stale edit (409) offers a reload", async () => {
    const calls = mockApi(settingsRoutes([
      { method: "PATCH", path: "/api/v1/risk/rules/HIGH_VELOCITY_1M", status: 409,
        body: { detail: "this rule was changed by someone else since you loaded it; reload and review the current values" } },
    ]), ADMIN);
    renderApp(<App />, "/settings");
    await userEvent.click(await screen.findByTestId("rule-edit-HIGH_VELOCITY_1M"));
    await userEvent.click(screen.getByTestId("rule-enabled"));
    await userEvent.click(screen.getByTestId("rule-review"));
    expect(await screen.findByRole("dialog")).toHaveTextContent("enabled: true → false");
    expect(screen.getByTestId("confirm-rule-cancel")).toHaveFocus();                     // safe default focus
    await userEvent.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(calls.some((c) => c.method === "PATCH")).toBe(false);
    await userEvent.click(screen.getByTestId("rule-review"));
    await userEvent.click(screen.getByTestId("confirm-rule-confirm"));
    const st = await screen.findByTestId("save-status");
    expect(st).toHaveTextContent("changed by someone else");
    expect(within(st).getByRole("button", { name: /Reload current values/ })).toBeInTheDocument();
  });

  it("risk scoring: client validation, full PUT with the loaded version, server rejection keeps the old config", async () => {
    const calls = mockApi(settingsRoutes([
      { method: "PUT", path: "/api/v1/risk/settings", status: 422, body: { detail: "require 0 < medium_threshold < high_threshold <= 100" } },
    ]), ADMIN);
    renderApp(<App />, "/settings?tab=scoring");
    const high = await screen.findByTestId("scoring-high_threshold");
    await userEvent.clear(high);
    await userEvent.type(high, "35");
    expect(screen.getByText("must be > MEDIUM threshold and ≤ 100")).toBeInTheDocument();
    expect(screen.getByTestId("scoring-review")).toBeDisabled();
    await userEvent.clear(high);
    await userEvent.type(high, "75");
    await userEvent.click(screen.getByTestId("scoring-review"));
    expect(await screen.findByRole("dialog")).toHaveTextContent("high_threshold: 70 → 75");
    await userEvent.click(screen.getByTestId("confirm-scoring-confirm"));
    expect(await screen.findByTestId("save-status")).toHaveTextContent("previous configuration is still active");
    const put = calls.find((c) => c.method === "PUT")!;
    expect(put.body).toEqual({ medium_threshold: 40, high_threshold: 75, model_weight: 0.9, anomaly_weight: 0.3, anomaly_min_percentile: 0.97,
      block_simulation_enabled: true, expected_updated_at: "2026-11-14T18:00:00.5+00:00" });
  });

  it("keeps the success message and shows the active configuration after a save changes updated_at (form remount)", async () => {
    const saved = { ...RISK, settings: { ...RISK.settings, high_threshold: 75 }, updated_at: "2026-11-15T03:00:00+00:00",
      last_change: { by: "admin", at: "2026-11-15T03:00:00+00:00", action: "RISK_SETTINGS_UPDATED" }, changed: true,
      propagation: { file: "ok", redis: "ok" } };
    mockApi(settingsRoutes([{ method: "PUT", path: "/api/v1/risk/settings", body: saved }]), ADMIN);
    renderApp(<App />, "/settings?tab=scoring");
    const high = await screen.findByTestId("scoring-high_threshold");
    await userEvent.clear(high);
    await userEvent.type(high, "75");
    await userEvent.click(screen.getByTestId("scoring-review"));
    await userEvent.click(await screen.findByTestId("confirm-scoring-confirm"));
    expect(await screen.findByTestId("save-status")).toHaveTextContent("Saved. Active configuration: MEDIUM ≥ 40, HIGH ≥ 75");
    expect(screen.getByTestId("scoring-high_threshold")).toHaveValue(75);                // remounted with the server's values
    expect(screen.getByTestId("scoring-last-change")).toHaveTextContent("by admin");
    expect(screen.getByTestId("scoring-review")).toBeDisabled();                         // nothing left to save
  });

  it("drift: sends only changed values with the loaded version", async () => {
    const calls = mockApi(settingsRoutes([
      { method: "PUT", path: "/api/v1/settings/drift", body: { drift: { ...DRIFT.value, min_rows: 150 }, changed: true } },
    ]), ADMIN);
    renderApp(<App />, "/settings?tab=drift");
    const min = await screen.findByTestId("drift-min_rows");
    await userEvent.clear(min);
    await userEvent.type(min, "5000");
    expect(screen.getByText("must not exceed the window")).toBeInTheDocument();
    await userEvent.clear(min);
    await userEvent.type(min, "150");
    await userEvent.click(screen.getByTestId("drift-review"));
    await userEvent.click(await screen.findByTestId("confirm-drift-confirm"));
    expect(await screen.findByTestId("save-status")).toHaveTextContent("Saved drift settings (min_rows 150)");
    expect(calls.find((c) => c.method === "PUT")!.body).toEqual({ value: { min_rows: 150 }, expected_updated_at: "2026-11-14T18:00:00+00:00" });
  });

  it("drift: two consecutive saves against a stateful backend both confirm success (updated_at changes each time)", async () => {
    let doc: Omit<typeof DRIFT, "last_change"> & { last_change: unknown } = DRIFT;
    let version = 0;
    const calls = mockApi(settingsRoutes([
      { path: "/api/v1/settings/drift", get body() { return doc; } },
      { method: "PUT", path: "/api/v1/settings/drift", get body() {
        const put = calls.filter((c) => c.method === "PUT").at(-1)!.body as { value: Record<string, number> };
        version += 1;
        doc = { ...doc, value: { ...doc.value, ...put.value }, updated_at: `2026-11-15T0${version}:00:00+00:00`,
          last_change: { by: "admin", at: `2026-11-15T0${version}:00:00+00:00`, action: "SETTING_UPDATED" } };
        return { drift: doc.value, changed: true, updated_at: doc.updated_at };
      } },
    ]), ADMIN);
    renderApp(<App />, "/settings?tab=drift");
    for (const v of [150, 200]) {
      const min = await screen.findByTestId("drift-min_rows");
      await userEvent.clear(min);
      await userEvent.type(min, String(v));
      await userEvent.click(screen.getByTestId("drift-review"));
      await userEvent.click(await screen.findByTestId("confirm-drift-confirm"));
      await waitFor(() => expect(screen.getByTestId("save-status")).toHaveTextContent(`Saved drift settings (min_rows ${v})`));
      await waitFor(() => expect(screen.getByTestId("drift-min_rows")).toHaveValue(v));
      await new Promise((r) => setTimeout(r, 50));
      expect(screen.getByTestId("save-status")).toHaveTextContent(`min_rows ${v}`);
    }
    expect(calls.filter((c) => c.method === "PUT").map((c) => c.body)).toEqual([
      { value: { min_rows: 150 }, expected_updated_at: "2026-11-14T18:00:00+00:00" },
      { value: { min_rows: 200 }, expected_updated_at: "2026-11-15T01:00:00+00:00" },
    ]);
  });

  it("audit trail shows changes, rejections with their reason, and denied attempts", async () => {
    const calls = mockApi(settingsRoutes(), ADMIN);
    renderApp(<App />, "/settings?tab=audit");
    const table = await screen.findByTestId("audit-table");
    expect(within(table).getByTestId("audit-7")).toHaveTextContent('{"count": 6}');
    expect(within(table).getByTestId("audit-8")).toHaveTextContent("require 0 < medium_threshold");
    expect(within(table).getByTestId("audit-9")).toHaveTextContent("denied: insufficient role");
    expect(calls.some((c) => c.url.includes("/audit?entity_type=risk_rule%2Csystem_setting&page=1&size=25"))).toBe(true);
  });

  it("account tab shows role, accounts read-only and the permission matrix", async () => {
    mockApi(settingsRoutes(), ADMIN);
    renderApp(<App />, "/settings?tab=account");
    expect(await screen.findByTestId("account-role")).toHaveTextContent("ADMIN");
    expect(screen.getByTestId("permissions-table")).toHaveTextContent("Change risk rules, risk scoring and drift settings");
    expect(await screen.findByTestId("users-table")).toHaveTextContent("analyst");
    expect(screen.queryByRole("button", { name: /create|delete|reset password/i })).toBeNull();
  });
});

describe("settings helpers", () => {
  it("reads only the exp claim of a JWT", () => {
    const b64 = (o: object) => btoa(JSON.stringify(o)).replace(/=+$/, "").replace(/\+/g, "-").replace(/\//g, "_");
    const tok = `${b64({ alg: "HS256" })}.${b64({ sub: "1", exp: 1794690708 })}.sig`;
    expect(tokenExpiry(tok)?.toISOString()).toBe(new Date(1794690708 * 1000).toISOString());
    expect(tokenExpiry("t")).toBeNull();
    expect(tokenExpiry(null)).toBeNull();
  });
  it("mirrors the backend scoring rules", () => {
    const ok = { medium_threshold: "40", high_threshold: "70", model_weight: "0.9", anomaly_weight: "0.3", anomaly_min_percentile: "0.97" };
    expect(Object.values(validateScoring(ok)).every((e) => !e)).toBe(true);
    expect(validateScoring({ ...ok, anomaly_min_percentile: "1" }).anomaly_min_percentile).toBe("must be ≥ 0 and < 1");
    expect(validateScoring({ ...ok, model_weight: "" }).model_weight).toBe("must be a number");
    expect(validateScoring({ ...ok, medium_threshold: "100" }).medium_threshold).toBe("must be between 1 and 99");
  });
});
