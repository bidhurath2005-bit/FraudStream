import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Activity, BarChart3, BrainCircuit, FileText, FlaskConical, Globe2, LayoutDashboard, LogOut, Menu, Server, Settings, ShieldAlert, X } from "lucide-react";
import { api } from "../services/api";
import { useAuth } from "../services/auth";
import { Logo } from "../components/Logo";
import { cx } from "../components/ui";
import type { SystemStatus } from "../types/api";

/** Navigation only lists pages that are implemented (no dead links). */
export const NAV = [
  { to: "/", label: "Intelligence Overview", icon: LayoutDashboard, end: true },
  { to: "/live", label: "Live Transaction Stream", icon: Activity },
  { to: "/investigations", label: "Investigation Center", icon: ShieldAlert },
  { to: "/risk", label: "Risk Analytics", icon: BarChart3 },
  { to: "/map", label: "Fraud Intelligence Map", icon: Globe2 },
  { to: "/streaming", label: "Streaming Infrastructure", icon: Server },
  { to: "/models", label: "Model Intelligence", icon: BrainCircuit },
  { to: "/simulation", label: "Simulation Lab", icon: FlaskConical },
  { to: "/reports", label: "Reports", icon: FileText },
  { to: "/settings", label: "Settings", icon: Settings },
];

function ConnectivityBadge() {
  const health = useQuery({
    queryKey: ["health"],
    queryFn: () => api<{ status: string; model_loaded: boolean }>("/health"),
    refetchInterval: 10000,
    retry: false,
  });
  const ok = health.isSuccess;
  const label = health.isLoading ? "checking" : ok ? (health.data.status === "ok" ? "API online" : `API ${health.data.status}`) : "API offline";
  return (
    <span data-testid="api-status" className={cx("inline-flex items-center gap-1.5 text-xs",
      ok && health.data.status === "ok" ? "text-emerald-400" : ok ? "text-amber-400" : "text-coral-400")}>
      <span className={cx("h-2 w-2 rounded-full", ok && health.data.status === "ok" ? "bg-emerald-400" : ok ? "bg-amber-400" : "bg-coral-400")} />
      {label}
    </span>
  );
}

function ProfileBadge() {
  const st = useQuery({ queryKey: ["system-status"], queryFn: () => api<SystemStatus>("/system/status"), refetchInterval: 30000 });
  if (!st.data) return null;
  const local = st.data.profile === "local";
  const proc = st.data.stream_processor;
  const procOk = !!proc && !proc.startsWith("NOT RUNNING") && !proc.startsWith("UNKNOWN");
  return (
    <span className="flex items-center gap-2 min-w-0">
      <span title={st.data.profile_description} data-testid="profile-badge"
        className={cx("rounded border px-2 py-0.5 text-[11px] font-semibold tracking-wide shrink-0",
          local ? "border-amber-500/40 text-amber-400 bg-amber-500/10" : "border-stream-500/40 text-stream-400 bg-stream-500/10")}>
        {local ? "LOCAL DEVELOPMENT MODE" : "FULL DISTRIBUTED MODE"}
      </span>
      {!local && (
        <span data-testid="stream-processor" title="Stream processor detected from live heartbeats"
          className={cx("hidden md:inline truncate text-[11px]", procOk ? "text-mist-300" : "text-coral-400")}>
          processor: {proc ?? "unknown"}
        </span>
      )}
    </span>
  );
}

export function AppLayout() {
  const { user, logout } = useAuth();
  const [open, setOpen] = useState(false);
  const nav = (
    <nav className="flex flex-col gap-0.5" aria-label="Main">
      {NAV.map(({ to, label, icon: Icon, end }) => (
        <NavLink key={to} to={to} end={end} onClick={() => setOpen(false)}
          className={({ isActive }) => cx("flex items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors",
            isActive ? "bg-ink-800 text-stream-400 border-l-2 border-stream-500" : "text-mist-300 hover:text-mist-100 hover:bg-ink-850")}>
          <Icon className="h-4 w-4 shrink-0" /> {label}
        </NavLink>
      ))}
    </nav>
  );
  return (
    <div className="min-h-full flex bg-[linear-gradient(to_right,rgba(11,15,25,0.6)_0,rgba(11,15,25,0.6)_16rem,transparent_16rem)] max-lg:bg-none">
      <aside className="hidden lg:flex w-64 shrink-0 flex-col gap-6 border-r border-ink-800 bg-ink-900/60 px-4 py-5 sticky top-0 h-screen">
        <Logo />
        {nav}
        <div className="mt-auto text-[11px] leading-relaxed text-mist-400">
          Educational platform. All transactions are <span className="text-amber-400">synthetic</span>; risk scores are model
          estimates, not determinations of fraud.
        </div>
      </aside>
      {open && (
        <div className="fixed inset-0 z-40 bg-ink-950/80 lg:hidden" onClick={() => setOpen(false)}>
          <aside className="w-72 h-full bg-ink-900 border-r border-ink-700 p-4 flex flex-col gap-6" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between"><Logo />
              <button className="btn btn-ghost p-1" aria-label="Close menu" onClick={() => setOpen(false)}><X className="h-5 w-5" /></button>
            </div>
            {nav}
          </aside>
        </div>
      )}
      <div className="flex-1 min-w-0 flex flex-col">
        <header className="sticky top-0 z-30 flex items-center gap-3 border-b border-ink-800 bg-ink-950/90 backdrop-blur px-4 lg:px-6 h-14">
          <button className="btn btn-ghost p-1 lg:hidden" aria-label="Open menu" onClick={() => setOpen(true)}><Menu className="h-5 w-5" /></button>
          <ProfileBadge />
          <div className="ml-auto flex items-center gap-4">
            <ConnectivityBadge />
            <span className="hidden sm:inline text-sm text-mist-300">
              {user?.username} <span className="text-mist-400 text-xs">· {user?.role}</span>
            </span>
            <button className="btn btn-ghost px-2 py-1" onClick={() => void logout()} aria-label="Sign out"><LogOut className="h-4 w-4" /></button>
          </div>
        </header>
        <main className="flex-1 min-w-0 px-4 lg:px-6 py-6"><Outlet /></main>
      </div>
    </div>
  );
}
