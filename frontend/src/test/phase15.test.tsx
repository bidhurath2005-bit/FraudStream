import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import { App } from "../App";
import { safeRedirect } from "../pages/Login";
import { mockApi, renderApp } from "./utils";

// Phase 15: the post-login redirect only follows same-origin absolute paths.
describe("safeRedirect", () => {
  it.each([
    ["/", "/"],
    ["/investigations?tab=alerts&q=C-1", "/investigations?tab=alerts&q=C-1"],
    ["/alerts/abc#x", "/alerts/abc#x"],
    ["//evil.example/x", "/"],
    ["/\\evil.example", "/"],
    ["\\\\evil.example", "/"],
    ["https://evil.example/", "/"],
    ["javascript:alert(1)", "/"],
    ["/ok\u0000", "/"],
    ["/ok\n//evil", "/"],
    ["", "/"],
    [undefined, "/"],
    [42, "/"],
  ])("%j -> %j", (input, expected) => {
    expect(safeRedirect(input)).toBe(expected);
  });

  it("an authenticated user on /login is sent to the dashboard", async () => {
    mockApi([]);
    renderApp(<App />, "/login");
    expect((await screen.findAllByRole("navigation", { name: "Main" }, { timeout: 5000 })).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "Sign in" })).toBeNull();
  });
});
