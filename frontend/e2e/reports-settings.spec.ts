/**
 * Phase 11C: Reports + Settings against a REAL running stack (nothing mocked).
 * - Reports: every report type available to the analyst is generated through the UI; what the page
 *   shows is cross-checked against the stored report (API) and, where possible, against an independent
 *   endpoint; CSV and PDF are downloaded through the browser and inspected; server filters; empty period;
 *   persistence (reload by id, history).
 * - Settings: the analyst sees the API's configuration read-only and the API itself rejects changes; the
 *   admin sees client and server validation, a cancelled rule edit that changes nothing, and a confirmed,
 *   audited drift-setting change that is restored afterwards (drift settings never affect risk scoring).
 * - 390 px: both pages, every settings tab and an open confirmation dialog fit the viewport.
 */
import fs from "node:fs";
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { apiGet as apiGetAs, apiSend } from "./api";

const USER = process.env.E2E_USERNAME ?? "e2e_analyst";
const PASS = process.env.E2E_PASSWORD ?? "";
const ADMIN = process.env.E2E_ADMIN_USERNAME ?? "admin";
const ADMIN_PASS = process.env.E2E_ADMIN_PASSWORD ?? "";
const PROFILE = process.env.E2E_PROFILE ?? "local";
const SHOT = (n: string) => `e2e-results/screenshots/${PROFILE}-${n}.png`;

async function login(page: Page, user: string, pass: string) {
  await page.goto("/login");
  await page.getByLabel("Username").fill(user);
  await page.getByLabel("Password").fill(pass);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Intelligence Overview" })).toBeVisible();
}
const get = <T,>(request: APIRequestContext, path: string, admin = false) =>
  admin ? apiGetAs<T>(request, ADMIN, ADMIN_PASS, path) : apiGetAs<T>(request, USER, PASS, path);

async function noHorizontalOverflow(page: Page, what: string) {
  const { sw, cw } = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth }));
  expect(sw, `${what}: page must not scroll horizontally (scrollWidth ${sw}, viewport ${cw})`).toBeLessThanOrEqual(cw + 1);
}

async function generate(page: Page, type: string, preset = "7d") {
  await page.getByTestId("report-type").selectOption(type);
  await page.getByTestId("report-preset").selectOption(preset);
  const resp = page.waitForResponse((r) => r.url().endsWith("/api/v1/reports") && r.request().method() === "POST");
  await page.getByTestId("report-generate").click();
  const r = await resp;
  expect(r.status(), `generate ${type}`).toBe(201);
  const body = await r.json();
  expect(body.status, `${type}: ${body.error}`).toBe("COMPLETED");
  await expect(page.getByTestId("report-uid")).toHaveText(body.report_uid);
  return body;
}

const intText = (v: number) => Math.round(v).toLocaleString("en-US");

test(`${PROFILE}: Reports — every available type from backend data; real CSV/PDF downloads; server filters; persistence`, async ({ page, request }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page, USER, PASS);
  await page.getByRole("link", { name: "Reports", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Reports", exact: true })).toBeVisible();
  const cat = await get<any>(request, "/reports/types");
  const available = cat.types.filter((t: any) => t.available).map((t: any) => t.key);
  expect(available).not.toContain("system_activity");                                        // admin only
  await expect(page.getByTestId("report-type").locator('option[value="system_activity"]')).toHaveJSProperty("disabled", true);

  const made: Record<string, any> = {};
  for (const type of available) {
    const body = await generate(page, type);
    made[type] = body;
    const stored = await get<any>(request, `/reports/generated/${body.report_uid}`);
    await expect(page.getByTestId("report-period")).toHaveText(stored.content.period.label);
    for (const m of stored.content.summary) {                                                 // page = stored backend values
      const tile = page.getByTestId(`metric-${m.key}`);
      if (m.value === null) await expect(tile).toContainText("n/a");
      else if (m.format === "int") await expect(tile).toContainText(intText(m.value));
    }
  }
  await page.screenshot({ path: SHOT("17-reports"), fullPage: true });

  // independent cross-checks against other endpoints (traffic may still arrive: allow only small growth)
  const fa = made.fraud_activity.content;
  const scored = fa.summary.find((m: any) => m.key === "transactions_scored").value;
  const merch = await get<any>(request, "/analytics/merchants?minutes=10080&limit=1");
  const apiScored = merch.categories.reduce((a: number, c: any) => a + c.count, 0);
  expect(Math.abs(apiScored - scored), `fraud report ${scored} vs analytics ${apiScored}`).toBeLessThanOrEqual(50);
  const alerts = made.alert_summary.content.summary.find((m: any) => m.key === "alerts").value;
  const alertApi = await get<any>(request, `/alerts?size=1&since=${encodeURIComponent(made.alert_summary.content.period.start)}`);
  expect(alertApi.total).toBeGreaterThanOrEqual(alerts);
  expect(alertApi.total).toBeLessThan(alerts + 50);

  // downloads through the browser: real files from the server
  await page.goto(`/reports?report=${made.alert_summary.report_uid}`);
  await expect(page.getByTestId("report-uid")).toHaveText(made.alert_summary.report_uid);   // persisted: reload by id
  const [pdf] = await Promise.all([page.waitForEvent("download"), page.getByTestId("export-pdf").click()]);
  expect(pdf.suggestedFilename()).toMatch(/^fraudstream-alert-summary-\d{8}-\d{8}-[0-9a-f]{8}\.pdf$/);
  const pdfBytes = fs.readFileSync((await pdf.path())!);
  expect(pdfBytes.subarray(0, 5).toString()).toBe("%PDF-");
  expect(pdfBytes.includes("/FontFile2")).toBe(true);
  await expect(page.getByTestId("export-status")).toContainText("Downloaded fraudstream-alert-summary-");
  const [csv] = await Promise.all([page.waitForEvent("download"), page.getByTestId("export-csv").click()]);
  const csvText = fs.readFileSync((await csv.path())!, "utf8");
  expect(csvText.charCodeAt(0)).toBe(0xfeff);                                                // UTF-8 BOM
  expect(csvText).toContain(`Report ID,${made.alert_summary.report_uid}`);
  expect(csvText).toContain("Section,Alert list");
  const [sec] = await Promise.all([page.waitForEvent("download"), page.getByTestId("section-csv-by_status").click()]);
  expect(sec.suggestedFilename()).toMatch(/-by_status-[0-9a-f]{8}\.csv$/);
  expect(fs.readFileSync((await sec.path())!, "utf8").split(/\r?\n/)[0]).toBe("﻿Status,Alerts,Share");

  // server-side filter: the request carries it and every listed alert matches
  await page.goto("/reports");
  await page.getByTestId("report-type").selectOption("alert_summary");
  await page.getByTestId("report-preset").selectOption("7d");
  await page.getByTestId("report-filter-risk_level").selectOption("HIGH");
  const highResp = page.waitForResponse((r) => r.url().endsWith("/api/v1/reports") && r.request().method() === "POST");
  await page.getByTestId("report-generate").click();
  const hr = await highResp;
  expect(hr.request().postDataJSON().risk_level).toBe("HIGH");
  const high = await hr.json();
  await expect(page.getByTestId("report-uid")).toHaveText(high.report_uid);
  await expect(page.getByTestId("report-filters")).toContainText("Risk level: HIGH");
  const rows = page.getByTestId("section-alerts").locator("tbody tr");
  const n = await rows.count();
  for (let i = 0; i < n; i++) await expect(rows.nth(i)).toContainText("HIGH");

  // empty period: zeros and "n/a", never invented values; exports still work
  await page.getByTestId("report-type").selectOption("fraud_activity");
  await page.getByTestId("report-preset").selectOption("custom");
  await page.getByTestId("report-from").fill("2001-01-01");
  await page.getByTestId("report-to").fill("2001-01-07");
  const resp = page.waitForResponse((r) => r.url().endsWith("/api/v1/reports") && r.request().method() === "POST");
  await page.getByTestId("report-generate").click();
  const empty = await (await resp).json();
  await expect(page.getByTestId("report-uid")).toHaveText(empty.report_uid);
  await expect(page.getByTestId("metric-transactions_scored")).toContainText("0");
  await expect(page.getByTestId("metric-flag_rate")).toContainText("n/a");
  await expect(page.getByTestId("section-empty-daily")).toBeVisible();
  const [emptyPdf] = await Promise.all([page.waitForEvent("download"), page.getByTestId("export-pdf").click()]);
  expect(fs.readFileSync((await emptyPdf.path())!).subarray(0, 5).toString()).toBe("%PDF-");

  // history lists them (server-side type filter)
  await page.getByTestId("history-type").selectOption("alert_summary");
  await expect(page.getByTestId(`history-${made.alert_summary.report_uid}`)).toBeVisible();
  await expect(page.getByTestId(`history-${made.fraud_activity.report_uid}`)).toHaveCount(0);
});

test(`${PROFILE}: Settings — analyst sees the API configuration read-only and the API rejects changes`, async ({ page, request }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page, USER, PASS);
  await page.getByRole("link", { name: "Settings", exact: true }).click();
  await expect(page.getByTestId("readonly-notice")).toBeVisible();
  const rules = await get<any[]>(request, "/risk/rules");
  await expect(page.getByTestId("rules-list").locator("section[data-testid^=rule-]")).toHaveCount(rules.length);
  for (const r of rules) {
    await expect(page.getByTestId(`rule-state-${r.code}`)).toHaveText(r.enabled ? "ENABLED" : "DISABLED");
    await expect(page.getByTestId(`rule-weight-${r.code}`)).toHaveText(r.weight.toFixed(2));
    for (const [k, v] of Object.entries(r.params)) await expect(page.getByTestId(`rule-param-${r.code}-${k}`)).toHaveText(String(v));
  }
  await expect(page.locator("[data-testid^=rule-edit-]")).toHaveCount(0);
  await page.screenshot({ path: SHOT("18-settings-rules"), fullPage: true });
  const rs = await get<any>(request, "/risk/settings");
  await page.getByTestId("tab-scoring").click();
  for (const k of ["medium_threshold", "high_threshold", "model_weight", "anomaly_weight", "anomaly_min_percentile"]) {
    await expect(page.getByTestId(`scoring-${k}`)).toHaveValue(String(rs.settings[k]));
    await expect(page.getByTestId(`scoring-${k}`)).toBeDisabled();
  }
  // the backend (not only the UI) refuses an analyst's change
  const denied = await apiSend(request, USER, PASS, "PUT", "/risk/settings", rs.settings);
  expect(denied.status()).toBe(403);
  expect((await get<any>(request, "/risk/settings")).settings).toEqual(rs.settings);
  await page.getByTestId("tab-audit").click();
  await expect(page.getByTestId("audit-restricted")).toBeVisible();
  await page.getByTestId("tab-account").click();
  await expect(page.getByTestId("account-role")).toHaveText("ANALYST");
  await expect(page.getByTestId("session-expiry")).not.toHaveText("—");
  await page.getByTestId("tab-system").click();
  await expect(page.getByTestId("system-profile")).toHaveText(PROFILE);
  await expect(page.getByTestId("sys-db")).toHaveText("UP");
  await expect(page.getByTestId("sys-kafka")).toHaveText(PROFILE === "local" ? "NOT USED" : "UP");
  await page.screenshot({ path: SHOT("19-settings-system"), fullPage: true });
});

test(`${PROFILE}: Settings — admin: validation keeps the config; cancelled edit changes nothing; confirmed drift change is audited and restored`, async ({ page, request }) => {
  test.skip(!ADMIN_PASS, "E2E_ADMIN_PASSWORD not set");
  const before = await get<any>(request, "/risk/settings", true);
  const rulesBefore = await get<any[]>(request, "/risk/rules", true);
  await login(page, ADMIN, ADMIN_PASS);
  await page.goto("/settings?tab=scoring");
  const high = page.getByTestId("scoring-high_threshold");
  await high.fill(String(before.settings.medium_threshold - 1));
  await expect(page.getByText("must be > MEDIUM threshold and ≤ 100")).toBeVisible();
  await expect(page.getByTestId("scoring-review")).toBeDisabled();
  // server-side validation (bypassing the UI): rejected and audited, previous config kept
  const bad = await apiSend(request, ADMIN, ADMIN_PASS, "PUT", "/risk/settings",
    { ...before.settings, high_threshold: before.settings.medium_threshold - 1 });
  expect(bad.status()).toBe(422);
  expect((await get<any>(request, "/risk/settings", true)).settings).toEqual(before.settings);

  // rule edit opened, reviewed, then cancelled: nothing saved
  await page.goto("/settings?tab=rules");
  const rule = rulesBefore.find((r) => Object.keys(r.params).length > 0)!;
  const [pk, pv] = Object.entries(rule.params)[0] as [string, number];
  await page.getByTestId(`rule-edit-${rule.code}`).click();
  await page.getByTestId(`rule-param-input-${pk}`).fill(String(pv + 1));
  await page.getByTestId("rule-review").click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText(`params.${pk}: ${pv} → ${pv + 1}`);
  await expect(page.getByTestId("confirm-rule-cancel")).toBeFocused();
  await page.screenshot({ path: SHOT("20-settings-rule-confirm"), fullPage: false });
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  const after = (await get<any[]>(request, "/risk/rules", true)).find((r) => r.code === rule.code);
  expect(after.params).toEqual(rule.params);
  expect(after.updated_at).toBe(rule.updated_at);

  // confirmed drift change (drift checks only, never scoring) -> saved, shown, audited, then restored
  const drift = await get<any>(request, "/settings/drift", true);
  const orig = drift.value.min_rows as number;
  const next = orig < drift.value.window_rows ? orig + 1 : orig - 1;
  const setMinRows = async (v: number) => {
    await page.goto("/settings?tab=drift");
    await page.getByTestId("drift-min_rows").fill(String(v));
    await page.getByTestId("drift-review").click();
    await expect(page.getByRole("dialog")).toContainText(`min_rows: `);
    const put = page.waitForResponse((r) => r.url().endsWith("/api/v1/settings/drift") && r.request().method() === "PUT");
    await page.getByTestId("confirm-drift-confirm").click();
    expect((await put).status()).toBe(200);
    await expect(page.getByTestId("save-status")).toContainText(`Saved drift settings (min_rows ${v})`);
    await expect(page.getByTestId("drift-min_rows")).toHaveValue(String(v));
  };
  await setMinRows(next);
  expect((await get<any>(request, "/settings/drift", true)).value.min_rows).toBe(next);
  await page.getByTestId("tab-audit").click();
  const top = page.getByTestId("audit-table").locator("tbody tr").first();
  await expect(top).toContainText("SETTING_UPDATED");
  await expect(top).toContainText(ADMIN);
  await expect(top).toContainText(`"min_rows": ${next}`);
  await page.screenshot({ path: SHOT("21-settings-audit"), fullPage: true });
  await setMinRows(orig);
  expect((await get<any>(request, "/settings/drift", true)).value.min_rows).toBe(orig);
  expect((await get<any>(request, "/risk/settings", true)).settings).toEqual(before.settings);   // scoring untouched throughout
});

test(`${PROFILE}: Reports and Settings fit a 390 px phone screen (incl. an open dialog)`, async ({ page }) => {
  test.skip(!ADMIN_PASS, "E2E_ADMIN_PASSWORD not set");
  await page.setViewportSize({ width: 390, height: 844 });
  await login(page, ADMIN, ADMIN_PASS);
  await page.goto("/reports");
  await expect(page.getByTestId("report-generate")).toBeVisible();
  await noHorizontalOverflow(page, "/reports (empty)");
  await generate(page, "alert_summary");
  await page.waitForTimeout(800);
  await noHorizontalOverflow(page, "/reports (alert summary preview)");
  await page.screenshot({ path: SHOT("22-reports-mobile"), fullPage: true });
  await generate(page, "system_activity", "24h");                                           // widest table (audit values)
  await page.waitForTimeout(800);
  await noHorizontalOverflow(page, "/reports (system activity preview)");
  for (const tab of ["rules", "scoring", "drift", "account", "system", "audit"]) {
    await page.goto(`/settings?tab=${tab}`);
    await expect(page.getByRole("tab", { selected: true })).toBeVisible();
    await page.waitForTimeout(1200);
    await noHorizontalOverflow(page, `/settings?tab=${tab}`);
    if (tab === "rules") await page.screenshot({ path: SHOT("23-settings-mobile"), fullPage: true });
  }
  await page.goto("/settings?tab=rules");
  const first = page.locator("[data-testid^=rule-edit-]").first();
  await first.click();
  await page.getByTestId("rule-enabled").click();
  await page.getByTestId("rule-review").click();
  const box = await page.getByRole("dialog").boundingBox();
  expect(box && box.x >= 0 && box.x + box.width <= 390 && box.y >= 0 && box.y + box.height <= 844, `dialog ${JSON.stringify(box)}`).toBeTruthy();
  await noHorizontalOverflow(page, "rule confirmation dialog");
  await page.screenshot({ path: SHOT("24-dialog-mobile"), fullPage: false });
  await page.getByTestId("confirm-rule-cancel").click();                                   // nothing saved
});
