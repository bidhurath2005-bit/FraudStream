import { useState } from "react";
import { useForm } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Pause, Play, RotateCcw, Square, Zap } from "lucide-react";
import { api } from "../services/api";
import { EmptyState, KV, PageHeader, Panel, QueryState, RiskBadge, StatusBadge, fmt } from "../components/ui";
import type { RiskCategory } from "../types/api";

type SimForm = { mode: string; tps: number; duration_seconds: number; customer_count: number; suspicious_pct: number; avg_amount: number; seed: number };
type SimStatus = {
  status: string; run_uid?: string; config?: SimForm; events_generated?: number; events_suspicious_injected?: number;
  errors?: number; error?: string | null; last_error?: string; sink?: string; profile?: string; model_ready: boolean;
  pipeline?: Record<string, number>; kafka?: { delivered: number; failed: number; last_error: string | null };
};
type Run = { id: number; run_uid: string; status: string; started_at: string; ended_at: string | null; events_generated: number; events_suspicious: number; config: SimForm; profile: string; error: string | null };
type Injected = { scenario: string; events: { event_id: string; transaction_id: string; status: string; risk_score?: number; risk_category?: RiskCategory; alert_uid?: string }[] };

const SCENARIOS: [string, string][] = [
  ["large_amount", "Unusually large payment"], ["velocity_burst", "Rapid transaction burst"], ["failed_attempts", "Repeated failed attempts"],
  ["account_takeover", "New device + new location"], ["impossible_travel", "Impossible travel"], ["odd_hour", "Night-time spending"],
  ["suspicious_merchant", "High-risk merchant"],
];

export function SimulationLabPage() {
  const qc = useQueryClient();
  const status = useQuery({ queryKey: ["sim-status"], queryFn: () => api<SimStatus>("/simulation/status"), refetchInterval: 1500 });
  const runs = useQuery({ queryKey: ["sim-runs"], queryFn: () => api<Run[]>("/simulation/runs?limit=10"), refetchInterval: 5000 });
  const [err, setErr] = useState<string | null>(null);
  const [injected, setInjected] = useState<Injected | null>(null);
  const { register, handleSubmit, formState } = useForm<SimForm>({
    defaultValues: { mode: "mixed", tps: 10, duration_seconds: 300, customer_count: 20000, suspicious_pct: 3, avg_amount: 60, seed: 42 },
  });
  const refresh = () => { qc.invalidateQueries({ queryKey: ["sim-status"] }); qc.invalidateQueries({ queryKey: ["sim-runs"] }); };
  const opts = { onSuccess: () => { setErr(null); refresh(); }, onError: (e: Error) => setErr(e.message) };
  const start = useMutation({ mutationFn: (f: SimForm) => api("/simulation/start", { body: f }), ...opts });
  const control = useMutation({ mutationFn: (a: string) => api(`/simulation/${a}`, { method: "POST" }), ...opts });
  const replay = useMutation({ mutationFn: (id: number) => api(`/simulation/runs/${id}/replay`, { method: "POST" }), ...opts });
  const inject = useMutation({
    mutationFn: (scenario: string) => api<Injected>("/simulation/inject", { body: { scenario } }),
    onSuccess: (r) => { setErr(null); setInjected(r); }, onError: (e: Error) => setErr(e.message),
  });
  const s = status.data;
  const running = s?.status === "RUNNING" || s?.status === "PAUSED";
  const num = { valueAsNumber: true } as const;

  return (
    <div>
      <PageHeader title="Simulation Lab" subtitle="Generates SYNTHETIC transactions only. BLOCK_SIMULATION decisions affect this simulation, never real payments." />
      {err && <div role="alert" className="panel border-coral-500/40 px-4 py-2 mb-4 text-sm text-coral-400">{err}</div>}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel title="Traffic generator" className="xl:col-span-2">
          <form onSubmit={handleSubmit((f) => start.mutate(f))} className="grid grid-cols-2 md:grid-cols-4 gap-3" data-testid="sim-form">
            <label className="flex flex-col gap-1 text-xs text-mist-300">Mode
              <select className="input" {...register("mode")} data-testid="sim-mode">
                <option value="mixed">Mixed traffic</option><option value="normal">Normal only</option>
                <option value="fraud">Fraud-pattern heavy</option><option value="high_volume">High volume</option>
              </select></label>
            <label className="flex flex-col gap-1 text-xs text-mist-300">Transactions / s
              <input className="input" type="number" step="0.1" {...register("tps", { ...num, required: true, min: 0.1, max: 2000 })} data-testid="sim-tps" /></label>
            <label className="flex flex-col gap-1 text-xs text-mist-300">Duration (s)
              <input className="input" type="number" {...register("duration_seconds", { ...num, required: true, min: 1, max: 86400 })} data-testid="sim-duration" /></label>
            <label className="flex flex-col gap-1 text-xs text-mist-300">Customers
              <input className="input" type="number" {...register("customer_count", { ...num, required: true, min: 10, max: 500000 })} /></label>
            <label className="flex flex-col gap-1 text-xs text-mist-300">Suspicious %
              <input className="input" type="number" step="0.5" {...register("suspicious_pct", { ...num, required: true, min: 0, max: 100 })} data-testid="sim-pct" /></label>
            <label className="flex flex-col gap-1 text-xs text-mist-300">Average amount
              <input className="input" type="number" {...register("avg_amount", { ...num, required: true, min: 1 })} /></label>
            <label className="flex flex-col gap-1 text-xs text-mist-300">Random seed
              <input className="input" type="number" {...register("seed", { ...num, required: true, min: 0 })} /></label>
            <div className="flex items-end">
              <button className="btn btn-primary w-full" type="submit" disabled={running || start.isPending} data-testid="sim-start"><Play className="h-4 w-4" /> Start</button>
            </div>
            {Object.keys(formState.errors).length > 0 && <p className="col-span-full text-xs text-coral-400">Check the highlighted values (ranges: TPS 0.1–2000, customers ≥ 10, % 0–100).</p>}
          </form>
          <p className="text-[11px] text-mist-400 mt-3">
            The training data used ~1 event per customer per 2000 s. Very different rates (e.g. few customers at high TPS) change behavioural
            feature distributions — the drift monitor will report that as data drift.
          </p>
        </Panel>
        <Panel title="Run status">
          <QueryState q={status}>
            {s && (
              <div data-testid="sim-status">
                <div className="flex items-center gap-2 mb-2"><StatusBadge status={s.status} />
                  {!s.model_ready && <span className="text-xs text-coral-400">no model — rules only</span>}</div>
                <KV k="Events sent" v={<span className="num" data-testid="sim-events">{fmt.num(s.events_generated ?? 0)}</span>} />
                <KV k="Injected suspicious" v={<span className="num">{fmt.num(s.events_suspicious_injected ?? 0)}</span>} />
                <KV k="Sink" v={s.sink ?? "—"} />
                {s.pipeline && <KV k="Pipeline (dup / late / err)" v={<span className="num">{s.pipeline.duplicates} / {s.pipeline.late} / {s.pipeline.errors}</span>} />}
                {s.kafka && <KV k="Kafka delivered / failed" v={<span className="num">{s.kafka.delivered} / {s.kafka.failed}</span>} />}
                {(s.error || s.last_error) && <p className="text-xs text-coral-400 mt-2">{s.error ?? s.last_error}</p>}
                <div className="flex gap-2 mt-3">
                  {s.status === "RUNNING" && <button className="btn btn-secondary" onClick={() => control.mutate("pause")} data-testid="sim-pause"><Pause className="h-4 w-4" /> Pause</button>}
                  {s.status === "PAUSED" && <button className="btn btn-secondary" onClick={() => control.mutate("resume")} data-testid="sim-resume"><Play className="h-4 w-4" /> Resume</button>}
                  {running && <button className="btn btn-danger" onClick={() => control.mutate("stop")} data-testid="sim-stop"><Square className="h-4 w-4" /> Stop</button>}
                </div>
              </div>
            )}
          </QueryState>
        </Panel>
        <Panel title="Inject a suspicious scenario" className="xl:col-span-2">
          <div className="flex flex-wrap gap-2">
            {SCENARIOS.map(([k, l]) => (
              <button key={k} className="btn btn-secondary" disabled={inject.isPending} onClick={() => inject.mutate(k)} data-testid={`inject-${k}`}>
                <Zap className="h-4 w-4 text-amber-400" /> {l}
              </button>
            ))}
          </div>
          {injected && (
            <div className="mt-3 overflow-x-auto" data-testid="inject-result">
              <div className="text-xs text-mist-300 mb-1">Scenario <b>{injected.scenario}</b> · {injected.events.length} synthetic events · detection result:</div>
              <table className="w-full"><thead><tr><th className="th">Transaction</th><th className="th">Pipeline</th><th className="th">Risk</th><th className="th">Alert</th></tr></thead>
                <tbody>{injected.events.map((e) => (
                  <tr key={e.event_id}><td className="td num text-xs">{e.transaction_id}</td><td className="td text-xs">{e.status}</td>
                    <td className="td">{e.risk_category ? <RiskBadge category={e.risk_category} score={e.risk_score} /> : "—"}</td>
                    <td className="td">{e.alert_uid ? <a className="text-stream-400 text-xs hover:underline" href={`/alerts/${e.alert_uid}`}>open</a> : "—"}</td></tr>))}</tbody></table>
            </div>
          )}
        </Panel>
        <Panel title="Recent runs">
          <QueryState q={runs} empty={!runs.data?.length && <EmptyState title="No runs yet" />}>
            <ul className="space-y-2">
              {runs.data?.map((r) => (
                <li key={r.id} className="rounded-md border border-ink-700 px-3 py-2 text-xs">
                  <div className="flex items-center justify-between gap-2"><StatusBadge status={r.status} /><span className="num text-mist-400">{fmt.dt(r.started_at)}</span></div>
                  <div className="mt-1 text-mist-300">{r.config.mode} · {r.config.tps}/s · seed {r.config.seed} · <span className="num">{fmt.num(r.events_generated)}</span> events</div>
                  {r.error && <div className="text-coral-400 mt-1">{r.error}</div>}
                  <button className="btn btn-ghost px-1 py-0.5 mt-1 text-xs" disabled={running || replay.isPending} onClick={() => replay.mutate(r.id)}>
                    <RotateCcw className="h-3 w-3" /> Replay pattern</button>
                </li>
              ))}
            </ul>
          </QueryState>
        </Panel>
      </div>
    </div>
  );
}
