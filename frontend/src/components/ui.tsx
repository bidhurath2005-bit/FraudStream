import { useEffect, useId, useRef, type ReactNode } from "react";
import { AlertTriangle, ChevronLeft, ChevronRight, Inbox, Loader2 } from "lucide-react";
import type { AlertStatus, RiskCategory } from "../types/api";

export function cx(...c: (string | false | null | undefined)[]) {
  return c.filter(Boolean).join(" ");
}

export function Panel({ title, actions, children, className, bodyClass }: {
  title?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string; bodyClass?: string;
}) {
  return (
    <section className={cx("panel flex flex-col min-w-0", className)}>
      {(title || actions) && (
        <header className="flex items-center justify-between gap-3 px-4 pt-3 pb-2">
          {title && <h2 className="panel-title">{title}</h2>}
          {actions && <div className="flex items-center gap-2">{actions}</div>}
        </header>
      )}
      <div className={cx("px-4 pb-4 min-w-0", !title && !actions && "pt-4", bodyClass)}>{children}</div>
    </section>
  );
}

export function PageHeader({ title, subtitle, actions }: { title: string; subtitle?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-3 mb-5">
      <div>
        <h1 className="text-xl font-semibold tracking-tight text-mist-100">{title}</h1>
        {subtitle && <p className="text-sm text-mist-300 mt-1">{subtitle}</p>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </div>
  );
}

const toneClass = {
  cyan: "text-stream-400", amber: "text-amber-400", coral: "text-coral-400", emerald: "text-emerald-400", mist: "text-mist-100",
};

export function StatTile({ label, value, hint, tone = "mist", testId, compact = false }: {
  label: string; value: ReactNode; hint?: ReactNode; tone?: keyof typeof toneClass; testId?: string; compact?: boolean;
}) {
  return (
    <div className={cx("panel min-w-0", compact ? "px-3 py-2" : "px-4 py-3")} data-testid={testId}>
      <div className="panel-title truncate">{label}</div>
      <div className={cx("num font-semibold mt-1 truncate", compact ? "text-base" : "text-2xl", toneClass[tone])}>{value}</div>
      {hint && <div className="text-xs text-mist-400 mt-1 truncate">{hint}</div>}
    </div>
  );
}

const riskStyles: Record<RiskCategory, string> = {
  LOW: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  MEDIUM: "bg-amber-500/10 text-amber-400 border-amber-500/30",
  HIGH: "bg-coral-500/15 text-coral-400 border-coral-500/40",
};

export function RiskBadge({ category, score }: { category: RiskCategory; score?: number }) {
  return (
    <span className={cx("inline-flex items-center gap-1.5 rounded border px-1.5 py-0.5 text-[11px] font-semibold", riskStyles[category])}>
      {category}
      {score !== undefined && <span className="num font-normal opacity-80">{score.toFixed(1)}</span>}
    </span>
  );
}

const statusStyles: Record<string, string> = {
  NEW: "text-stream-400 border-stream-500/40 bg-stream-500/10",
  UNDER_REVIEW: "text-amber-400 border-amber-500/40 bg-amber-500/10",
  CONFIRMED_SUSPICIOUS: "text-coral-400 border-coral-500/40 bg-coral-500/10",
  DISMISSED: "text-mist-300 border-ink-500 bg-ink-800",
  CLOSED: "text-mist-400 border-ink-600 bg-ink-850",
  OPEN: "text-stream-400 border-stream-500/40 bg-stream-500/10",
  IN_PROGRESS: "text-amber-400 border-amber-500/40 bg-amber-500/10",
  RESOLVED: "text-emerald-400 border-emerald-500/40 bg-emerald-500/10",
};

export function StatusBadge({ status }: { status: AlertStatus | string }) {
  return (
    <span className={cx("inline-flex rounded border px-1.5 py-0.5 text-[11px] font-medium", statusStyles[status] ?? statusStyles.CLOSED)}>
      {status.replace(/_/g, " ")}
    </span>
  );
}

export function Loading({ label = "Loading" }: { label?: string }) {
  return (
    <div role="status" className="flex items-center gap-2 text-sm text-mist-300 py-8 justify-center">
      <Loader2 className="h-4 w-4 animate-spin" /> {label}…
    </div>
  );
}

export function ErrorState({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div role="alert" className="flex flex-col items-center gap-2 py-8 text-center">
      <AlertTriangle className="h-5 w-5 text-coral-400" />
      <p className="text-sm text-coral-400">{msg}</p>
      {onRetry && <button className="btn btn-secondary" onClick={onRetry}>Retry</button>}
    </div>
  );
}

export function EmptyState({ title, hint }: { title: string; hint?: ReactNode }) {
  return (
    <div className="flex flex-col items-center gap-2 py-10 text-center">
      <Inbox className="h-5 w-5 text-mist-400" />
      <p className="text-sm text-mist-200">{title}</p>
      {hint && <p className="text-xs text-mist-400 max-w-md">{hint}</p>}
    </div>
  );
}

export function QueryState({ q, empty, children }: {
  q: { isLoading: boolean; isError: boolean; error: unknown; refetch: () => unknown };
  empty?: boolean | ReactNode;
  children: ReactNode;
}) {
  if (q.isLoading) return <Loading />;
  if (q.isError) return <ErrorState error={q.error} onRetry={() => q.refetch()} />;
  if (empty) return typeof empty === "boolean" ? <EmptyState title="Nothing to show yet" /> : <>{empty}</>;
  return <>{children}</>;
}

export function Pagination({ page, size, total, onPage }: { page: number; size: number; total: number; onPage: (p: number) => void }) {
  const pages = Math.max(1, Math.ceil(total / size));
  return (
    <div className="flex items-center justify-between gap-3 pt-3 text-xs text-mist-300">
      <span className="num">{total.toLocaleString()} total · page {page} of {pages}</span>
      <div className="flex gap-1">
        <button className="btn btn-secondary px-2 py-1" aria-label="Previous page" disabled={page <= 1} onClick={() => onPage(page - 1)}>
          <ChevronLeft className="h-4 w-4" />
        </button>
        <button className="btn btn-secondary px-2 py-1" aria-label="Next page" disabled={page >= pages} onClick={() => onPage(page + 1)}>
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}

export function Select({ label, value, onChange, options, testId }: {
  label: string; value: string; onChange: (v: string) => void; options: { value: string; label: string }[]; testId?: string;
}) {
  return (
    <label className="flex flex-col gap-1 text-xs text-mist-300">
      {label}
      <select className="input py-1.5" value={value} onChange={(e) => onChange(e.target.value)} data-testid={testId}>
        {options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
    </label>
  );
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-1 text-xs text-mist-300">
      <span>{label}</span>
      {children}
    </div>
  );
}

export function KV({ k, v }: { k: string; v: ReactNode }) {
  return (
    <div className="flex justify-between gap-4 py-1.5 border-b border-ink-800 text-sm">
      <span className="text-mist-400">{k}</span>
      <span className="text-mist-100 text-right min-w-0 break-all">{v}</span>
    </div>
  );
}

export const fmt = {
  money: (n: number | null | undefined, c = "USD") =>
    n == null ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: c, maximumFractionDigits: 2 }).format(n),
  num: (n: number | null | undefined, d = 0) => (n == null ? "—" : n.toLocaleString("en-US", { maximumFractionDigits: d, minimumFractionDigits: d })),
  pct: (n: number | null | undefined, d = 1) => (n == null ? "—" : `${(n * 100).toFixed(d)}%`),
  time: (s: string | null | undefined) => (s ? new Date(s).toLocaleTimeString("en-GB", { hour12: false }) : "—"),
  dt: (s: string | null | undefined) => (s ? new Date(s).toLocaleString("en-GB", { hour12: false }) : "—"),
  ms: (n: number | null | undefined) => (n == null ? "—" : n < 10 ? `${n.toFixed(1)} ms` : `${Math.round(n)} ms`),
};

/** Health state of a component. The raw backend state is always shown as text (never colour alone). */
const HEALTH_GOOD = new Set(["UP", "RUNNING", "STABLE", "ACTIVE", "OK"]);
const HEALTH_WARN = new Set(["STALE", "MODERATE", "EMPTY", "NOT_REPORTING", "NO_HEARTBEAT", "UNKNOWN", "PREPARING_REBALANCING", "COMPLETING_REBALANCING", "INSUFFICIENT_DATA", "CANDIDATE"]);
const HEALTH_NEUTRAL = new Set(["NOT_USED", "NOT_CONFIGURED", "RETIRED", "DEAD"]);
export function healthTone(state: string | null | undefined): "good" | "warn" | "bad" | "neutral" {
  const s = (state ?? "UNKNOWN").toUpperCase();
  if (HEALTH_GOOD.has(s)) return "good";
  if (HEALTH_NEUTRAL.has(s)) return "neutral";
  if (HEALTH_WARN.has(s)) return "warn";
  return "bad";
}
const healthCls = {
  good: "text-emerald-400 border-emerald-500/40 bg-emerald-500/10",
  warn: "text-amber-400 border-amber-500/40 bg-amber-500/10",
  bad: "text-coral-400 border-coral-500/40 bg-coral-500/10",
  neutral: "text-mist-300 border-ink-500 bg-ink-800",
};
const healthDot = { good: "bg-emerald-400", warn: "bg-amber-400", bad: "bg-coral-400", neutral: "bg-mist-400" };
export function HealthPill({ state, testId }: { state: string | null | undefined; testId?: string }) {
  const t = healthTone(state);
  return (
    <span data-testid={testId} className={cx("inline-flex items-center gap-1.5 rounded border px-1.5 py-0.5 text-[11px] font-semibold whitespace-nowrap", healthCls[t])}>
      <span aria-hidden className={cx("h-1.5 w-1.5 rounded-full", healthDot[t])} />
      {(state ?? "UNKNOWN").replace(/_/g, " ")}
    </span>
  );
}

export function Tabs<T extends string>({ tabs, value, onChange, label }: {
  tabs: { id: T; label: string }[]; value: T; onChange: (id: T) => void; label: string;
}) {
  return (
    <div role="tablist" aria-label={label} className="flex gap-1 overflow-x-auto border-b border-ink-700 mb-4">
      {tabs.map((t) => (
        <button key={t.id} role="tab" aria-selected={value === t.id} data-testid={`tab-${t.id}`} onClick={() => onChange(t.id)}
          className={cx("px-3 py-2 text-sm whitespace-nowrap border-b-2 -mb-px transition-colors",
            value === t.id ? "border-stream-500 text-stream-400" : "border-transparent text-mist-300 hover:text-mist-100")}>
          {t.label}
        </button>
      ))}
    </div>
  );
}

/** "Not available" marker with the backend's reason (unavailable metrics are never shown as 0). */
export function Unavailable({ reason }: { reason?: string | null }) {
  // `relative` makes this span the containing block of the absolutely positioned sr-only text; otherwise
  // that text escapes a table's overflow-x-auto wrapper and widens the whole page on phones (measured: 1053 px at 390 px).
  return <span className="relative text-mist-400" title={reason ?? "not reported"}>n/a{reason ? <span className="sr-only"> ({reason})</span> : null}</span>;
}

export const fmtBytes = (n: number) => (n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(2)} MB`);
export const fmtAge = (s: number | null | undefined) => (s == null ? "—" : s < 90 ? `${s.toFixed(1)} s ago` : s < 5400 ? `${Math.round(s / 60)} min ago` : `${(s / 3600).toFixed(1)} h ago`);

/**
 * Accessible confirmation dialog: role=dialog + aria-modal, labelled by its title, focus moves into it
 * and is trapped while open, Escape / backdrop cancel (unless busy), focus returns to the opener.
 * Width is capped to the viewport and the body scrolls, so it fits a 390 px phone.
 */
export function ConfirmDialog({ open, title, children, confirmLabel = "Confirm", onConfirm, onCancel, busy = false,
  tone = "primary", testId, confirmDisabled = false }: {
  open: boolean; title: string; children: ReactNode; confirmLabel?: string; onConfirm: () => void; onCancel: () => void;
  busy?: boolean; tone?: "primary" | "danger"; testId?: string; confirmDisabled?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const busyRef = useRef(busy);
  busyRef.current = busy;
  const cancelRef = useRef(onCancel);
  cancelRef.current = onCancel;
  useEffect(() => {
    if (!open) return;
    const opener = document.activeElement as HTMLElement | null;
    const focusables = () => Array.from(ref.current?.querySelectorAll<HTMLElement>(
      "button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])") ?? []);
    (ref.current?.querySelector<HTMLElement>("[data-autofocus]") ?? focusables()[0])?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busyRef.current) { e.preventDefault(); cancelRef.current(); }
      if (e.key === "Tab") {
        const f = focusables();
        if (!f.length) return;
        const first = f[0], last = f[f.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
      }
    };
    document.addEventListener("keydown", onKey);
    return () => { document.removeEventListener("keydown", onKey); opener?.focus?.(); };
  }, [open]);
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-ink-950/80 p-4"
      onMouseDown={(e) => { if (e.target === e.currentTarget && !busy) onCancel(); }}>
      <div ref={ref} role="dialog" aria-modal="true" aria-labelledby={titleId} data-testid={testId}
        className="panel w-full max-w-lg max-h-[calc(100vh-2rem)] overflow-y-auto p-4 min-w-0">
        <h2 id={titleId} className="text-base font-semibold text-mist-100 mb-3">{title}</h2>
        <div className="text-sm text-mist-200 min-w-0">{children}</div>
        <div className="flex flex-wrap justify-end gap-2 mt-4">
          <button className="btn btn-secondary" onClick={onCancel} disabled={busy} data-autofocus data-testid={testId ? `${testId}-cancel` : undefined}>Cancel</button>
          <button className={cx("btn", tone === "danger" ? "btn-danger" : "btn-primary")} onClick={onConfirm}
            disabled={busy || confirmDisabled} data-testid={testId ? `${testId}-confirm` : undefined}>
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : null}{confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

/** Inline feedback line (success / error / info) announced to screen readers. */
export function Notice({ tone, children, testId }: { tone: "ok" | "error" | "info"; children: ReactNode; testId?: string }) {
  return (
    <div role={tone === "error" ? "alert" : "status"} data-testid={testId}
      className={cx("rounded-md border px-3 py-2 text-sm break-words",
        tone === "ok" && "border-emerald-500/40 bg-emerald-500/10 text-emerald-400",
        tone === "error" && "border-coral-500/40 bg-coral-500/10 text-coral-400",
        tone === "info" && "border-ink-600 bg-ink-850 text-mist-200")}>
      {children}
    </div>
  );
}
