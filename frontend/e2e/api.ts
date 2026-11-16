/**
 * Backend access for E2E cross-checks. Logs in ONCE per user per worker and reuses the access token: the
 * API allows 10 logins per minute per client (security control, deliberately not relaxed for tests); the
 * earlier per-call login exhausted it, the login answered 429 and the next GET went out with
 * "Bearer undefined" (seen as a confusing 401 in Phase 11B). A failed login now fails loudly.
 * Phase 11C: the cache is keyed by user so the admin and the analyst account can both be used.
 */
import { expect, type APIRequestContext } from "@playwright/test";

const tokens = new Map<string, { value: string; at: number }>();
const TOKEN_MAX_AGE_MS = 5 * 60_000;     // well inside the access-token lifetime

export async function apiToken(request: APIRequestContext, user: string, pass: string): Promise<string> {
  const t = tokens.get(user);
  if (t && Date.now() - t.at < TOKEN_MAX_AGE_MS) return t.value;
  const lr = await request.post("/api/v1/auth/login", { data: { username: user, password: pass } });
  const body = await lr.json().catch(() => ({}));
  expect(lr.ok(), `E2E API login failed: HTTP ${lr.status()} ${JSON.stringify(body).slice(0, 200)}`).toBeTruthy();
  expect(typeof body.access_token, "login response has no access_token").toBe("string");
  tokens.set(user, { value: body.access_token, at: Date.now() });
  return body.access_token;
}

export async function apiGet<T>(request: APIRequestContext, user: string, pass: string, path: string): Promise<T> {
  const t = await apiToken(request, user, pass);
  const r = await request.get(`/api/v1${path}`, { headers: { Authorization: `Bearer ${t}` } });
  expect(r.ok(), `${path}: HTTP ${r.status()}`).toBeTruthy();
  return r.json() as Promise<T>;
}

/** Raw authenticated request (status not asserted) — used to check that the API itself rejects a call. */
export async function apiSend(request: APIRequestContext, user: string, pass: string, method: "PUT" | "PATCH" | "POST",
  path: string, data: unknown) {
  const t = await apiToken(request, user, pass);
  return request.fetch(`/api/v1${path}`, { method, data, headers: { Authorization: `Bearer ${t}` } });
}
