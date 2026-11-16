import type { ReactNode } from "react";
import { render } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { vi } from "vitest";
import { AuthProvider } from "../services/auth";
import { setAccessToken } from "../services/api";

export type Route = { method?: string; path: string | RegExp; status?: number; body?: unknown; stream?: string[]; delay?: number; headers?: Record<string, string> };

export const ANALYST = { id: 2, username: "analyst", full_name: "", role: "ANALYST", is_active: true, created_at: "", last_login_at: null };

/** Installs a fetch mock that answers by method+path and records every call. */
export function mockApi(routes: Route[], user: object | null = ANALYST) {
  const calls: { method: string; url: string; body?: unknown }[] = [];
  const all: Route[] = [
    ...routes,
    { method: "POST", path: "/api/v1/auth/refresh", status: user ? 200 : 401, body: user ? { access_token: "t", user } : { detail: "no" } },
    { path: "/api/v1/health", body: { status: "ok", model_loaded: true } },
    { path: "/api/v1/system/status", body: { profile: "local", profile_description: "LOCAL DEVELOPMENT MODE", model: {} } },
  ];
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = (init?.method ?? "GET").toUpperCase();
    calls.push({ method, url, body: init?.body ? JSON.parse(String(init.body)) : undefined });
    const pathOnly = url.split("?")[0];
    const r = all.find((x) => (x.method ?? "GET") === method && (typeof x.path === "string" ? x.path === pathOnly : x.path.test(url)));
    if (!r) return new Response(JSON.stringify({ detail: `unmocked ${method} ${url}` }), { status: 404 });
    if (r.delay) await new Promise((res) => setTimeout(res, r.delay));   // simulate a slow response
    if (r.stream) {
      const enc = new TextEncoder();
      const chunks = r.stream;
      const body = new ReadableStream({ start(c) { chunks.forEach((s) => c.enqueue(enc.encode(s))); } });
      return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
    }
    return new Response(r.status === 204 ? null : JSON.stringify(r.body ?? {}), { status: r.status ?? 200, headers: r.headers });
  });
  vi.stubGlobal("fetch", fn);
  setAccessToken(user ? "t" : null);
  return calls;
}

export function renderApp(ui: ReactNode, route = "/") {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[route]}>
        <AuthProvider>{ui}</AuthProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
