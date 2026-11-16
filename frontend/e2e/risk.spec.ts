/**
 * Phase 11B: Risk Analytics + Fraud Intelligence Map against a REAL running stack (nothing mocked).
 * Values in the browser are cross-checked against the API (same account, fetched separately).
 * The 7-day period is used for cross-checks so the test does not depend on traffic in the last hour.
 */
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { apiGet as apiGetAs } from "./api";

const USER = process.env.E2E_USERNAME ?? "e2e_analyst";
const PASS = process.env.E2E_PASSWORD ?? "";
const PROFILE = process.env.E2E_PROFILE ?? "local";
const SHOT = (n: string) => `e2e-results/screenshots/${PROFILE}-${n}.png`;
const num = (s: string) => Number(s.replace(/[^\d.]/g, ""));

async function login(page: Page) {
  await page.goto("/login");
  await page.getByLabel("Username").fill(USER);
  await page.getByLabel("Password").fill(PASS);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Intelligence Overview" })).toBeVisible();
}


const apiGet = <T,>(request: APIRequestContext, path: string) => apiGetAs<T>(request, USER, PASS, path);

async function noHorizontalOverflow(page: Page, what: string) {
  const { sw, cw } = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth }));
  expect(sw, `${what}: page must not scroll horizontally (scrollWidth ${sw}, viewport ${cw})`).toBeLessThanOrEqual(cw + 1);
}

test(`${PROFILE}: Risk Analytics shows backend aggregates; filters go to the server and survive reload`, async ({ page, request }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page);
  await page.getByRole("link", { name: "Risk Analytics", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Risk Analytics" })).toBeVisible();
  await expect(page.getByTestId("period-label")).toHaveText("Last 24 hours");
  await page.getByTestId("period-filter").selectOption("10080");
  await expect(page).toHaveURL(/period=10080/);
  await expect(page.getByTestId("period-label")).toHaveText("Last 7 days");

  const [m, cm] = await Promise.all([
    apiGet<any>(request, "/analytics/merchants?minutes=10080&limit=15"), apiGet<any>(request, "/analytics/confusion?minutes=10080")]);
  const apiCount = m.categories.reduce((a: number, c: any) => a + c.count, 0);
  test.skip(apiCount === 0, "no scored transactions in the last 7 days on this stack");
  // stack may still receive traffic: the page value must be >= the API value read before it, and close to it
  await expect.poll(async () => num(await page.getByTestId("sum-analysed").locator(".num").innerText())).toBeGreaterThanOrEqual(apiCount);
  expect(num(await page.getByTestId("sum-analysed").locator(".num").innerText())).toBeLessThan(apiCount + 500);
  expect(num(await page.getByTestId("cm-tp").innerText())).toBeGreaterThanOrEqual(cm.risk_engine_vs_simulator.tp);
  await expect(page.getByTestId("synthetic-note")).toContainText("SYNTHETIC");
  await expect(page.getByTestId("patterns-table").locator("tbody tr").first()).toBeVisible();
  await expect(page.getByTestId("pattern-chart").locator(".recharts-bar-rectangle path").first()).toBeVisible();   // a drawn (non-zero) bar
  await expect(page.getByTestId("hourly-chart").locator(".recharts-bar-rectangle path").first()).toBeVisible();   // a drawn (non-zero) bar
  await expect(page.getByTestId("category-chart").locator(".recharts-bar-rectangle path").first()).toBeVisible();   // a drawn (non-zero) bar
  await page.screenshot({ path: SHOT("13-risk-analytics"), fullPage: true });

  // merchant category filter (server-side) — every ranked merchant belongs to the category
  const cat = [...m.categories].sort((a: any, b: any) => b.count - a.count)[0].category as string;
  const req = page.waitForRequest((r) => r.url().includes(`category=${cat}`));
  await page.getByTestId("category-filter").selectOption(cat);
  await req;
  await expect(page).toHaveURL(new RegExp(`category=${cat}`));
  const merchantRows = page.getByTestId("merchant-table").locator("tbody tr");
  await expect(merchantRows.first()).toBeVisible();
  for (const row of await merchantRows.all()) await expect(row.locator("td").nth(2)).toHaveText(cat.replace(/_/g, " "));

  // velocity window (server-side, own window)
  const vreq = page.waitForRequest((r) => r.url().includes("/analytics/velocity?minutes=1440"));
  await page.getByTestId("velocity-window").selectOption("1440");
  await vreq;
  await expect(page.getByTestId("velocity-window-note").or(page.getByText(/No transactions processed/))).toBeVisible();

  // page refresh keeps every filter (state is in the URL)
  await page.reload();
  await expect(page.getByTestId("period-label")).toHaveText("Last 7 days");
  await expect(page.getByTestId("category-filter")).toHaveValue(cat);
  await expect(page.getByTestId("velocity-window")).toHaveValue("1440");
  await page.getByTestId("risk-refresh").click();
  await expect(page.getByTestId("sum-analysed")).toBeVisible();
});

test(`${PROFILE}: Fraud Intelligence Map plots backend city aggregates with honest labelling`, async ({ page, request }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page);
  await page.getByRole("link", { name: "Fraud Intelligence Map", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Fraud Intelligence Map" })).toBeVisible();
  await expect(page.getByTestId("geo-disclaimer")).toContainText("SIMULATED GEOGRAPHY");
  await page.getByTestId("map-period").selectOption("10080");
  const geo = await apiGet<any>(request, "/analytics/geo?minutes=10080&min_risk=0");
  test.skip(!geo.available, "no located transactions in the last 7 days on this stack");
  const apiTotal = geo.points.reduce((a: number, p: any) => a + p.count, 0);
  await expect.poll(async () => num(await page.getByTestId("geo-total").locator(".num").innerText())).toBeGreaterThanOrEqual(apiTotal);
  await expect(page.getByTestId("geo-map").locator("g[data-testid^=geo-point-]")).toHaveCount(geo.points.length);
  await expect(page.getByTestId("geo-legend")).toContainText("Flagged share");

  // tooltip + selection show the backend's values for that city
  const top = [...geo.points].sort((a: any, b: any) => b.count - a.count)[0];
  // EVERY city must be hoverable at its centre even where bubbles overlap (the hit layer picks the nearest centre)
  for (const p of geo.points) {
    const box = await page.getByTestId(`geo-point-${p.city}`).locator("circle").boundingBox();
    expect(box, p.city).not.toBeNull();
    await page.mouse.move(box!.x + box!.width / 2, box!.y + box!.height / 2);
    await expect(page.getByTestId("geo-tooltip"), `hover ${p.city}`).toContainText(`${p.city}, ${p.country}`);
  }
  await expect(page.getByTestId("geo-tooltip")).toContainText("simulated");
  const tb = (await page.getByTestId(`geo-point-${top.city}`).locator("circle").boundingBox())!;
  await page.mouse.click(tb.x + tb.width / 2, tb.y + tb.height / 2);
  await expect(page.getByTestId("geo-detail")).toContainText(`${top.count.toLocaleString("en-US")}`);
  await page.getByTestId("geo-table").getByRole("button", { name: top.city, exact: true }).click();
  await expect(page.getByTestId("geo-detail")).toContainText("simulated city centroid");
  await page.screenshot({ path: SHOT("14-fraud-map"), fullPage: true });

  // risk level is a server filter: HIGH-only totals match min_risk = configured high threshold
  const st = await apiGet<any>(request, "/risk/settings");
  const req = page.waitForRequest((r) => r.url().includes(`min_risk=${st.settings.high_threshold}`));
  await page.getByTestId("map-risk").selectOption("high");
  await req;
  const geoHigh = await apiGet<any>(request, `/analytics/geo?minutes=10080&min_risk=${st.settings.high_threshold}`);
  if (geoHigh.available) {
    const hi = geoHigh.points.reduce((a: number, p: any) => a + p.count, 0);
    await expect.poll(async () => num(await page.getByTestId("geo-total").locator(".num").innerText())).toBeGreaterThanOrEqual(hi);
    expect(num(await page.getByTestId("geo-total").locator(".num").innerText())).toBeLessThan(apiTotal);
  } else {
    await expect(page.getByText(/No located transactions/)).toBeVisible();
  }

  // country is a view filter; it survives reload with the other filters
  await page.getByTestId("map-risk").selectOption("all");
  await page.getByTestId("map-country").selectOption(top.country);
  await expect(page).toHaveURL(new RegExp(`country=${top.country}`));
  await page.reload();
  await expect(page.getByTestId("map-country")).toHaveValue(top.country);
  await expect(page.getByTestId("map-period")).toHaveValue("10080");
  const shownCities = geo.points.filter((p: any) => p.country === top.country).length;
  await expect(page.getByTestId("geo-map").locator("g[data-testid^=geo-point-]")).toHaveCount(shownCities);
});

test(`${PROFILE}: every dashboard page fits a 390 px phone screen`, async ({ page }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await page.setViewportSize({ width: 390, height: 844 });
  await login(page);
  const pages: [string, string][] = [
    ["/risk?period=10080", "sum-analysed"], ["/map?period=10080", "geo-disclaimer"], ["/", "stat-total"],
    ["/streaming", "health-database"], ["/models", "models-table"], ["/models?tab=compare", "compare-table"],
    ["/live", "stream-state"], ["/investigations", "alerts-table"], ["/simulation", "sim-status"],
  ];
  for (const [path, testId] of pages) {
    await page.goto(path);
    await expect(page.getByTestId(testId).first()).toBeVisible();
    await page.waitForTimeout(1500);   // let charts and late data render before measuring
    await noHorizontalOverflow(page, path);
  }
  await page.goto("/risk?period=10080");
  await expect(page.getByTestId("sum-analysed")).toBeVisible();
  await page.waitForTimeout(1500);
  await page.screenshot({ path: SHOT("15-risk-mobile"), fullPage: true });
  await page.goto("/map?period=10080");
  await expect(page.getByTestId("geo-disclaimer")).toBeVisible();
  await page.waitForTimeout(1500);
  await page.screenshot({ path: SHOT("16-map-mobile"), fullPage: true });
});
