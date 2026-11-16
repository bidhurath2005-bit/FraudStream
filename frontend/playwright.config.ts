import { defineConfig, devices } from "@playwright/test";

/**
 * End-to-end tests run against a REAL running stack (backend + frontend); nothing is mocked.
 * Start it first (docker compose ... --profile distributed|local up -d), then run
 * `python scripts/run_e2e.py --env-file <env file> --profile distributed|local`, which passes the accounts from that
 * env file as E2E_USERNAME / E2E_PASSWORD / E2E_ADMIN_USERNAME / E2E_ADMIN_PASSWORD (see CONTRIBUTING.md section 5).
 */
export default defineConfig({
  testDir: "./e2e",
  timeout: 180_000,
  expect: { timeout: 20_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [["list"], ["json", { outputFile: "e2e-results/results.json" }]],
  outputDir: "e2e-results/artifacts",
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://127.0.0.1:5173",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    viewport: { width: 1440, height: 900 },
  },
  projects: [{
    name: "chromium",
    use: {
      ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 },
      // Optional: use an already-installed Chromium/Chrome binary (e.g. when the headless-shell download is unavailable)
      launchOptions: process.env.E2E_CHROMIUM_PATH ? { executablePath: process.env.E2E_CHROMIUM_PATH } : {},
    },
  }],
});
