/**
 * Phase 11A: Streaming Infrastructure + Model Intelligence pages against a REAL running stack.
 * Values shown in the browser are cross-checked against the backend API (fetched separately
 * with the same account) — nothing is mocked.
 *   E2E_PROFILE=local|distributed, E2E_ARTIFACTS=read-only|writable (container stacks mount read-only)
 */
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { apiGet as apiGetAs } from "./api";

const USER = process.env.E2E_USERNAME ?? "e2e_analyst";
const PASS = process.env.E2E_PASSWORD ?? "";
const PROFILE = process.env.E2E_PROFILE ?? "local";
const ARTIFACTS = process.env.E2E_ARTIFACTS ?? "";
const SHOT = (n: string) => `e2e-results/screenshots/${PROFILE}-${n}.png`;

async function login(page: Page) {
  await page.goto("/login");
  await page.getByLabel("Username").fill(USER);
  await page.getByLabel("Password").fill(PASS);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Intelligence Overview" })).toBeVisible();
}


const apiGet = <T,>(request: APIRequestContext, path: string) => apiGetAs<T>(request, USER, PASS, path);

async function noHorizontalOverflow(page: Page) {
  const { sw, cw } = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth }));
  expect(sw, "page must not scroll horizontally").toBeLessThanOrEqual(cw + 1);
}

test(`${PROFILE}: Streaming Infrastructure page shows live backend status`, async ({ page, request }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page);
  await page.getByRole("link", { name: "Streaming Infrastructure", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Streaming Infrastructure" })).toBeVisible();
  await expect(page.getByTestId("health-database")).toHaveText("UP");
  const infra = await apiGet<any>(request, "/infrastructure/status");
  expect(infra.profile).toBe(PROFILE);
  await expect(page.getByTestId("stat-dlq-db")).toContainText(String(infra.dead_letters_db));
  if (PROFILE === "distributed") {
    await expect(page.getByTestId("health-kafka")).toHaveText("UP");
    await expect(page.getByTestId("health-spark")).toHaveText("RUNNING");
    await expect(page.getByTestId("health-consumer")).toHaveText("UP");
    await expect(page.getByTestId("active-processor")).toHaveText("Spark Structured Streaming");
    const topics = page.getByTestId("kafka-topics");
    for (const t of infra.kafka.topics) await expect(topics).toContainText(t.name);
    await expect(page.getByTestId("consumer-groups")).toContainText("fraudstream-persistence");
    const queries = page.getByTestId("spark-queries");
    await expect(queries.locator("tbody tr")).toHaveCount(infra.spark.queries.length);
    // filter + expand (checkpoint details come from the running Spark job)
    await page.getByTestId("query-filter").selectOption("window");
    await expect(queries.getByText("score", { exact: true })).toHaveCount(0);
    await page.getByTestId("query-filter").selectOption("all");
    await page.getByLabel("Details for score").click();
    await expect(page.getByTestId("query-detail-score")).toContainText(`${infra.spark.checkpoint_root}/score`);
    await expect(page.getByTestId("query-detail-score")).toContainText("last committed batch");
    await expect.poll(async () => (await page.getByTestId("stat-dlq-kafka").innerText()).replace(/\D/g, "")).toBe(String(infra.kafka.dlq_messages));
  } else {
    await expect(page.getByText(/Kafka and Spark are not used/)).toBeVisible();
    await expect(page.getByTestId("health-kafka")).toHaveText("NOT USED");
    await expect(page.getByTestId("kafka-topics")).toHaveCount(0);
  }
  // manual refresh and auto-refresh selector
  await page.getByTestId("refresh-interval").selectOption("0");
  const req = page.waitForRequest((r) => r.url().includes("/infrastructure/status"));
  await page.getByTestId("refresh-now").click();
  await req;
  await page.getByTestId("refresh-interval").selectOption("5000");
  await page.screenshot({ path: SHOT("06-streaming"), fullPage: true });
});

test(`${PROFILE}: Model Intelligence page shows registry, comparison, drift and artifacts`, async ({ page, request }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await login(page);
  const [active, models, artifacts] = await Promise.all([
    apiGet<any>(request, "/models/active"), apiGet<any[]>(request, "/models"), apiGet<any>(request, "/models/artifacts")]);
  await page.getByRole("link", { name: "Model Intelligence", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Model Intelligence" })).toBeVisible();
  const sup = active.active.supervised;
  await expect(page.getByTestId("active-supervised")).toContainText(sup.model_name);
  await expect(page.getByTestId("active-supervised")).toContainText(sup.metrics.test.pr_auc.toFixed(4));
  await expect(page.getByTestId("models-table").locator("tbody tr")).toHaveCount(models.length);
  await expect(page.getByTestId("artifacts-mode")).toContainText(active.artifacts_writable ? "writable" : "READ-ONLY");
  if (ARTIFACTS) expect(active.artifacts_writable).toBe(ARTIFACTS === "writable");

  // model detail: confusion matrix values are the stored test-split values
  await page.getByTestId(`model-row-${sup.model_name}`).click();
  const detail = page.getByTestId("model-detail");
  await expect(detail.getByTestId("confusion-matrix")).toContainText(sup.metrics.test.confusion_matrix.tp.toLocaleString("en-US"));
  await expect(page.getByTestId("live-inference")).toBeVisible();
  await page.screenshot({ path: SHOT("07-models-registry"), fullPage: true });

  // comparison from stored experiment results; toggling a model changes the table
  await page.getByTestId("tab-compare").click();
  await expect(page).toHaveURL(/tab=compare/);
  const cmp = page.getByTestId("compare-table");
  await expect(cmp.locator("tbody tr")).toHaveCount(models.length);
  await page.getByTestId("compare-toggle-isolation_forest").uncheck();
  await expect(cmp.locator("tbody tr")).toHaveCount(models.length - 1);
  await page.getByTestId("compare-split").selectOption("unseen_customer_test");
  await expect(page.getByTestId("compare-chart").locator(".recharts-bar-rectangle").first()).toBeVisible();
  await page.screenshot({ path: SHOT("08-models-compare"), fullPage: true });

  // drift: run a check on the live predictions (stored as a monitoring result) and display it
  await page.getByTestId("tab-drift").click();
  await page.getByTestId("run-drift").click();
  await expect(page.getByTestId("run-drift")).toHaveText("Run drift check now", { timeout: 60_000 });
  const insufficient = page.getByTestId("drift-insufficient");
  if (await insufficient.isVisible()) {
    await expect(insufficient).toContainText("required");
  } else {
    const report = page.getByTestId("drift-report");
    await expect(report).toBeVisible();
    const hist = await apiGet<any[]>(request, "/drift/history?limit=1");
    await expect(report).toContainText(hist[0].status);
    await expect(page.getByTestId("drift-history").locator("tbody tr").first()).toBeVisible();
    await page.getByTestId("drift-feature-filter").selectOption("drifted");
  }
  await page.screenshot({ path: SHOT("09-models-drift"), fullPage: true });

  // experiments, datasets, artifacts
  await page.getByTestId("tab-artifacts").click();
  await expect(page.getByTestId("experiment-detail")).toBeVisible();
  await expect(page.getByTestId("artifacts-table").locator("tbody tr")).toHaveCount(artifacts.files.length);
  await expect(page.getByTestId("registry-consistency")).toHaveText("registry.json matches the database");
  await page.getByTestId("artifact-filter").fill("metadata.json");
  await expect(page.getByTestId("artifacts-table").locator("tbody tr")).toHaveCount(
    artifacts.files.filter((f: { path: string }) => f.path.includes("metadata.json")).length);
  await page.screenshot({ path: SHOT("10-models-artifacts"), fullPage: true });
});

test(`${PROFILE}: new pages are usable at phone width`, async ({ page }) => {
  test.skip(!PASS, "E2E_PASSWORD not set");
  await page.setViewportSize({ width: 390, height: 844 });
  await login(page);
  await page.getByLabel("Open menu").click();
  await page.getByRole("link", { name: "Streaming Infrastructure", exact: true }).click();
  await expect(page.getByTestId("health-database")).toBeVisible();
  await noHorizontalOverflow(page);
  await page.screenshot({ path: SHOT("11-streaming-mobile"), fullPage: true });
  await page.goto("/models?tab=compare");
  await expect(page.getByTestId("compare-table")).toBeVisible();
  await noHorizontalOverflow(page);
  await page.goto("/models");
  await expect(page.getByTestId("models-table")).toBeVisible();
  await noHorizontalOverflow(page);
  await page.screenshot({ path: SHOT("12-models-mobile"), fullPage: true });
});
