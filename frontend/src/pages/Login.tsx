import { useState } from "react";
import { useForm } from "react-hook-form";
import { Navigate, useLocation } from "react-router-dom";
import { ApiError } from "../services/api";
import { useAuth } from "../services/auth";
import { LogoMark } from "../components/Logo";

type Form = { username: string; password: string };

/** Post-login target: only a same-origin absolute path. A pathname such as "//host/x" or "/\host" would otherwise
 *  become a protocol-relative (off-site) redirect (react-router GHSA-2j2x-hqr9-3h42 / GHSA-wrjc-x8rr-h8h6). */
export function safeRedirect(from: unknown): string {
  if (typeof from !== "string" || !from.startsWith("/") || from.startsWith("//")) return "/";
  if (/[\\\u0000-\u001f\u007f]/.test(from)) return "/";
  return from;
}

export function LoginPage() {
  const { user, login, expired } = useAuth();
  const loc = useLocation();
  const [error, setError] = useState<string | null>(null);
  const { register, handleSubmit, formState } = useForm<Form>();
  if (user) return <Navigate to={safeRedirect((loc.state as { from?: unknown } | null)?.from)} replace />;

  const onSubmit = async (f: Form) => {
    setError(null);
    try {
      await login(f.username, f.password);
    } catch (e) {
      setError(e instanceof ApiError && e.status === 429 ? "Too many attempts — wait a minute." :
        e instanceof ApiError && e.status === 0 ? e.message : "Invalid username or password.");
    }
  };

  return (
    <div className="min-h-full flex items-center justify-center px-4 py-10">
      <div className="w-full max-w-sm">
        <div className="flex flex-col items-center gap-3 mb-8">
          <LogoMark size={48} />
          <div className="text-center">
            <div className="text-lg font-semibold tracking-[0.2em]">FRAUDSTREAM</div>
            <div className="text-[11px] uppercase tracking-[0.14em] text-mist-400">Financial Intelligence &amp; Risk Operations Center</div>
          </div>
        </div>
        <form onSubmit={handleSubmit(onSubmit)} className="panel p-6 flex flex-col gap-4" noValidate>
          {expired && <p className="text-xs text-amber-400" role="status">Your session expired. Please sign in again.</p>}
          <label className="flex flex-col gap-1 text-xs text-mist-300">Username
            <input className="input" autoComplete="username" {...register("username", { required: "Username is required" })} />
            {formState.errors.username && <span className="text-coral-400">{formState.errors.username.message}</span>}
          </label>
          <label className="flex flex-col gap-1 text-xs text-mist-300">Password
            <input className="input" type="password" autoComplete="current-password" {...register("password", { required: "Password is required" })} />
            {formState.errors.password && <span className="text-coral-400">{formState.errors.password.message}</span>}
          </label>
          {error && <p role="alert" className="text-sm text-coral-400">{error}</p>}
          <button className="btn btn-primary" type="submit" disabled={formState.isSubmitting}>
            {formState.isSubmitting ? "Signing in…" : "Sign in"}
          </button>
          <p className="text-[11px] text-mist-400 leading-relaxed">
            Development accounts are created locally with random passwords by <code className="num">scripts/dev_setup.ps1</code>
            (see <code className="num">.dev-credentials.txt</code>). No default credentials exist.
          </p>
        </form>
      </div>
    </div>
  );
}
