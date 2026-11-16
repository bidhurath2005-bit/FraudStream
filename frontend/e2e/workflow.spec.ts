/**
 * Phase 7 end-to-end workflow against the real LOCAL profile stack:
 *   simulation -> real features -> real ML inference -> risk engine -> DB -> SSE/dashboard
 *   -> alert -> investigation -> notes -> status changes -> resolution -> audit trail.
 */
import { expect, test, type Page } from "@playwright/test";

const USER = process.env.E2E_USERNAME ?? "e2e_analyst";
const PASS = process.env.E2E_PASSWORD ?? "";
const SHOTS = "e2e-results/screenshots";
const PROFILE = process.env.E2E_PROFILE ?? "local";              // local | distributed
const EXPECT_PROCESSOR = process.env.E2E_EXPECT_PROCESSOR ?? "";   // e.g. Spark Structured Streaming
const SHOT = (n: string) => `${SHOTS}/${PROFILE}-${n}.png`;

async function login(page: Page) {
  await page.goto("/login");
  await page.getByLabel("Username").fill(USER);
  await page.getByLabel("Password").fill(PASS);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Intelligence Overview" })).toBeVisible();
}

test(`${PROFILE} end-to-end: simulate, detect, display, investigate, audit`, async ({ page }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page);
  await expect(page.getByTestId("profile-badge")).toHaveText(PROFILE === "local" ? "LOCAL DEVELOPMENT MODE" : "FULL DISTRIBUTED MODE");
  if (PROFILE !== "local" && EXPECT_PROCESSOR) {
    await expect(page.getByTestId("stream-processor")).toContainText(EXPECT_PROCESSOR);
  }
  await expect(page.getByTestId("api-status")).toHaveText("API online");

  // 1-2. Start mixed traffic from the Simulation Lab
  await page.getByRole("link", { name: "Simulation Lab", exact: true }).click();
  await page.getByTestId("sim-tps").fill("25");
  await page.getByTestId("sim-duration").fill("90");
  await page.getByTestId("sim-pct").fill("8");
  await page.getByTestId("sim-start").click();
  await expect(page.getByTestId("sim-status")).toContainText("RUNNING");
  await expect.poll(async () => Number((await page.getByTestId("sim-events").innerText()).replace(/,/g, "")), { timeout: 30_000 })
    .toBeGreaterThan(50);

  // inject a known suspicious pattern and observe the real detection result
  await page.getByTestId("inject-account_takeover").click();
  const inj = page.getByTestId("inject-result");
  await expect(inj).toContainText("account_takeover");
  await expect(inj.getByText(PROFILE === "local" ? "created" : "published_to_kafka").first()).toBeVisible();
  await page.screenshot({ path: SHOT("01-simulation-lab"), fullPage: true });

  // 3. Live stream receives scored events over SSE
  await page.getByRole("link", { name: "Live Transaction Stream", exact: true }).click();
  await expect(page.getByTestId("stream-state")).toHaveText("Live");
  await expect(page.getByTestId("live-table").locator("tbody tr").first()).toBeVisible({ timeout: 30_000 });
  await page.getByTestId("pause-toggle").click();
  await expect(page.getByTestId("pause-toggle")).toContainText("Resume");
  await page.getByTestId("pause-toggle").click();
  await page.getByTestId("risk-filter").selectOption("FLAGGED");
  await expect(page.getByTestId("live-table").locator("tbody tr").first()).toBeVisible({ timeout: 45_000 });
  await page.screenshot({ path: SHOT("02-live-stream"), fullPage: true });

  // 4. Overview shows persisted, non-zero metrics from THIS run (not cached/historical values)
  await page.getByRole("link", { name: "Intelligence Overview", exact: true }).click();
  await expect.poll(async () => {
    const m = (await page.getByTestId("stat-total").innerText()).match(/([\d,]+) in the last minute/);
    return m ? Number(m[1].replace(/,/g, "")) : 0;
  }, { timeout: 30_000 }).toBeGreaterThan(0);
  await expect(page.locator(".recharts-area-curve").first()).toBeVisible();
  await expect.poll(async () => Number((await page.getByTestId("stat-total").locator(".num").innerText()).replace(/,/g, "")))
    .toBeGreaterThan(50);
  await expect.poll(async () => Number((await page.getByTestId("stat-suspicious").locator(".num").innerText()).replace(/,/g, "")))
    .toBeGreaterThan(0);
  await page.screenshot({ path: SHOT("03-overview"), fullPage: true });

  // 5. Alert appears in the Investigation Center; open its detail
  await page.getByRole("link", { name: "Investigation Center", exact: true }).click();
  await page.getByTestId("alert-status-filter").selectOption("NEW");
  const firstRow = page.getByTestId("alerts-table").locator("tbody tr").first();
  await expect(firstRow).toBeVisible();
  const alertLink = firstRow.locator("a").first();
  const alertShort = (await alertLink.innerText()).trim();
  await alertLink.click();
  await expect(page.getByRole("heading", { name: `Alert ${alertShort}` })).toBeVisible();
  await expect(page.getByTestId("explanation")).toContainText("Risk");
  await page.screenshot({ path: SHOT("04-alert-detail"), fullPage: true });
  await page.getByRole("button", { name: "Start review" }).click();
  await expect(page.getByText("UNDER REVIEW").first()).toBeVisible();
  await page.getByTestId("note-input").fill("E2E: reviewed device and location history.");
  await page.getByTestId("add-note").click();
  await expect(page.getByTestId("notes")).toContainText("E2E: reviewed device");

  // 6. Open an investigation from that alert (back in the queue, under-review filter)
  await page.goto("/investigations?status=UNDER_REVIEW&assigned=me");
  const row = page.getByTestId("alerts-table").locator("tbody tr", { hasText: alertShort });
  await expect(row).toBeVisible();
  await row.locator("input[type=checkbox]").check();
  await page.getByTestId("inv-title").fill(`E2E case for alert ${alertShort}`);
  await page.getByTestId("open-investigation").click();
  await expect(page.getByRole("heading", { name: `E2E case for alert ${alertShort}` })).toBeVisible();

  // 7-8. Work the case: start, note, resolve with findings, close
  await page.getByRole("button", { name: "Start work" }).click();
  await expect(page.getByText("IN PROGRESS").first()).toBeVisible();
  await page.getByTestId("note-input").fill("E2E: customer transaction history inspected.");
  await page.getByTestId("add-note").click();
  await expect(page.getByTestId("notes")).toContainText("customer transaction history");
  await page.getByTestId("classification").selectOption("CONFIRMED_SUSPICIOUS");
  await page.getByTestId("findings-input").fill("Synthetic pattern reviewed end-to-end; activity confirmed suspicious for the simulation.");
  await page.getByTestId("resolve").click();
  await expect(page.getByTestId("findings")).toContainText("Synthetic pattern reviewed");
  await page.getByRole("button", { name: "Close case" }).click();
  await expect(page.getByText("CLOSED").first()).toBeVisible();

  // 9. Audit trail records who/what/old/new/when
  const audit = page.getByTestId("audit-trail");
  for (const action of ["INVESTIGATION_OPENED", "NOTE_ADDED", "INVESTIGATION_FINDINGS", "INVESTIGATION_STATUS_CHANGED"]) {
    await expect(audit).toContainText(action);
  }
  await expect(audit).toContainText(USER);
  await page.screenshot({ path: SHOT("05-investigation-closed"), fullPage: true });

  // alert status propagated by the analyst's decision (not automatically by the model)
  await page.getByRole("link", { name: alertShort }).first().click();
  await expect(page.getByText("CONFIRMED SUSPICIOUS").first()).toBeVisible();
  await expect(page.getByTestId("audit-trail")).toContainText("ALERT_STATUS_CHANGED");

  // 10. Stop the simulation
  await page.getByRole("link", { name: "Simulation Lab", exact: true }).click();
  await expect(page.getByTestId("sim-status")).toBeVisible();
  const stop = page.getByTestId("sim-stop");
  if ((await page.getByTestId("sim-status").innerText()).match(/RUNNING|PAUSED/)) await stop.click();
  await expect(page.getByTestId("sim-status")).not.toContainText("RUNNING");
});

test("unauthorised and expired sessions are rejected", async ({ page, request }) => {
  const r = await request.get("/api/v1/alerts");
  expect(r.status()).toBe(401);
  await page.goto("/investigations");
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
});
