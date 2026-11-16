import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import { App } from "../App";
import { mockApi, renderApp } from "./utils";

// Phase 12: with Redis unreachable the API cannot read processor heartbeats and now reports
// "UNKNOWN (Redis unavailable: ...)" instead of a false "NOT RUNNING". The header must not show that as healthy.
const status = (stream_processor: string) => ({
  path: "/api/v1/system/status",
  body: { profile: "distributed", profile_description: "FULL DISTRIBUTED MODE", stream_processor, model: {} },
});

afterEach(() => vi.unstubAllGlobals());

describe("stream processor badge", () => {
  it.each([
    ["Spark Structured Streaming", false],
    ["NOT RUNNING (no processor heartbeat)", true],
    ["UNKNOWN (Redis unavailable: processor heartbeats cannot be read)", true],
  ])("%s -> warning colour %s", async (proc, warn) => {
    mockApi([status(proc)]);
    renderApp(<App />, "/");
    const badge = await screen.findByTestId("stream-processor", {}, { timeout: 5000 });
    expect(badge).toHaveTextContent(`processor: ${proc}`);
    expect(badge.className.includes("text-coral-400")).toBe(warn);
  });
});
