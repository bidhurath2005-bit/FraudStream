/**
 * API client. The access token lives in memory only; the refresh token is an httpOnly
 * cookie scoped to /api/v1/auth. On 401 the client tries one silent refresh, then
 * signals session expiry so the UI can return to the login screen.
 */
export const API_BASE = "/api/v1";

export class ApiError extends Error {
  constructor(public status: number, message: string, public body?: unknown) {
    super(message);
  }
}

let accessToken: string | null = null;
let onSessionExpired: (() => void) | null = null;
let refreshing: Promise<boolean> | null = null;

export function setAccessToken(token: string | null) {
  accessToken = token;
}
export function getAccessToken() {
  return accessToken;
}
export function setSessionExpiredHandler(fn: (() => void) | null) {
  onSessionExpired = fn;
}

export async function refreshSession(): Promise<{ access_token: string; user: unknown } | null> {
  const r = await fetch(`${API_BASE}/auth/refresh`, { method: "POST", credentials: "same-origin" });
  if (!r.ok) return null;
  const body = await r.json();
  accessToken = body.access_token;
  return body;
}

async function tryRefresh(): Promise<boolean> {
  if (!refreshing) refreshing = refreshSession().then((b) => !!b).finally(() => (refreshing = null));
  return refreshing;
}

type Opts = { method?: string; body?: unknown; form?: FormData; raw?: boolean; signal?: AbortSignal };

export async function api<T = unknown>(path: string, opts: Opts = {}, retried = false): Promise<T> {
  const headers: Record<string, string> = {};
  if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, {
      method: opts.method ?? (opts.body !== undefined || opts.form ? "POST" : "GET"),
      headers,
      body: opts.form ?? (opts.body !== undefined ? JSON.stringify(opts.body) : undefined),
      credentials: "same-origin",
      signal: opts.signal,
    });
  } catch (e) {
    throw new ApiError(0, "Cannot reach the FraudStream API");
  }
  if (res.status === 401 && !retried && !path.startsWith("/auth/")) {
    if (await tryRefresh()) return api<T>(path, opts, true);
    accessToken = null;
    onSessionExpired?.();
  }
  if (!res.ok) {
    let detail: string = res.statusText;
    let body: unknown;
    try {
      body = await res.json();
      const d = (body as { detail?: unknown }).detail;
      detail = typeof d === "string" ? d : JSON.stringify(d ?? body);
    } catch {
      /* non-JSON error */
    }
    throw new ApiError(res.status, detail, body);
  }
  if (opts.raw) return res as unknown as T;
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

/** Filename from Content-Disposition, reduced to a safe character set (never a path). */
export function dispositionFilename(header: string | null, fallback: string): string {
  const m = header?.match(/filename\*?=(?:UTF-8'')?"?([^";]+)"?/i);
  const raw = m ? decodeURIComponent(m[1]) : fallback;
  const base = raw.split(/[\\/]/).pop() ?? fallback;
  const safe = base.replace(/[^A-Za-z0-9._-]/g, "_").replace(/^\.+/, "");
  return safe || fallback;
}

/**
 * Downloads a file from the API. The body is streamed into a Blob by the browser (no parsing on the
 * main thread), so large exports do not freeze the page. Rejects with ApiError on HTTP errors, a network
 * failure or an empty body, so callers can show the reason.
 */
export async function download(path: string, filename: string): Promise<{ filename: string; bytes: number }> {
  const res = await api<Response>(path, { raw: true });
  let blob: Blob;
  try {
    blob = await res.blob();
  } catch {
    throw new ApiError(0, "The download was interrupted");
  }
  if (blob.size === 0) throw new ApiError(res.status, "The server returned an empty file");
  const name = dispositionFilename(res.headers.get("content-disposition"), filename);
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.rel = "noopener";
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  a.remove();
  // revoking immediately can cancel the download in some browsers
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
  return { filename: name, bytes: blob.size };
}

export function qs(params: Record<string, string | number | boolean | null | undefined>) {
  const p = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") p.set(k, String(v));
  });
  const s = p.toString();
  return s ? `?${s}` : "";
}
