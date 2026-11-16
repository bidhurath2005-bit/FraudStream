import { Fragment, useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, ChevronRight, RefreshCw } from "lucide-react";
import { api } from "../services/api";
import {
  EmptyState, HealthPill, KV, PageHeader, Panel, QueryState, Select, StatTile, Unavailable,
  cx, fmt, fmtAge,
} from "../components/ui";
import { TimeSeries, VolumeChart } from "../charts/Charts";
import type { InfrastructureStatus, Overview, SparkQuery } from "../types/api";

type Timeline = { bucket_seconds: number; points: { t: string; total: number; medium: number; high: number; avg_latency_ms: number | null }[] };
type Sample = { at: number; label: string; input: number | null; processed: number | null };

const INTERVALS = [
  { value: "5000", label: "every 5 s" }, { value: "10000", label: "every 10 s" },
  { value: "30000", label: "every 30 s" }, { value: "0", label: "paused" },
];
const QUERY_FILTERS = [
  { value: "all", label: "All queries" }, { value: "score", label: "Scoring" }, { value: "dedup", label: "Validation / dedup" },
  { value: "window", label: "Windows" }, { value: "problem", label: "Inactive / errors" },
];
const MAX_SAMPLES = 120;

function matchesFilter(q: SparkQuery, f: string) {
  if (f === "score") return q.name === "score";
  if (f === "dedup") return q.name === "validated-dedup";
  if (f === "window") return q.name.startsWith("window-");
  if (f === "problem") return !q.active || !!q.exception;
  return true;
}

const rate = (n: number | null | undefined) => (n == null ? "—" : n < 10 ? n.toFixed(2) : Math.round(n).toLocaleString());

export function StreamingPage() {
  const [interval, setIntervalMs] = useState("5000");
  const [queryFilter, setQueryFilter] = useState("all");
  const [expanded, setExpanded] = useState<string | null>(null);
  const refetchInterval = Number(interval) || false;
  const infra = useQuery({ queryKey: ["infrastructure"], queryFn: () => api<InfrastructureStatus>("/infrastructure/status"), refetchInterval });
  const ov = useQuery({ queryKey: ["overview"], queryFn: () => api<Overview>("/analytics/overview"), refetchInterval });
  const tl = useQuery({ queryKey: ["timeline", "streaming"], queryFn: () => api<Timeline>("/analytics/timeline?minutes=30&bucket_seconds=30"), refetchInterval: refetchInterval && Math.max(10000, refetchInterval) });

  // Spark's score-query rates sampled from each status response, while this page is open (client-side only).
  const [samples, setSamples] = useState<Sample[]>([]);
  const lastReport = useRef<number | null>(null);
  useEffect(() => {
    const sp = infra.data?.spark;
    if (!sp?.reported_at || sp.reported_at === lastReport.current) return;
    lastReport.current = sp.reported_at;
    const score = sp.queries?.find((q) => q.name === "score");
    const at = sp.reported_at * 1000;
    setSamples((s) => [...s, {
      at, label: new Date(at).toLocaleTimeString("en-GB", { hour12: false }),
      input: score?.input_rows_per_second ?? null, processed: score?.processed_rows_per_second ?? null,
    }].slice(-MAX_SAMPLES));
  }, [infra.data]);

  const d = infra.data;
  const distributed = d?.profile === "distributed";
  return (
    <div>
      <PageHeader title="Streaming Infrastructure"
        subtitle={d ? <>Profile <b className="text-mist-100">{d.profile.toUpperCase()}</b> · checked {fmt.time(d.checked_at)} · every value read from the running system</> : "Reading live infrastructure status"}
        actions={<>
          <Select label="Auto-refresh" value={interval} onChange={setIntervalMs} options={INTERVALS} testId="refresh-interval" />
          <button className="btn btn-secondary self-end" data-testid="refresh-now" onClick={() => { void infra.refetch(); void ov.refetch(); void tl.refetch(); }}
            disabled={infra.isFetching}>
            <RefreshCw className={cx("h-4 w-4", infra.isFetching && "animate-spin")} /> Refresh
          </button>
        </>} />
      <QueryState q={{ ...infra, isError: infra.isError && !d }}>
        {d && (
          <>
            {infra.isError && (
              <div role="alert" data-testid="stale-banner" className="panel border-coral-500/40 px-4 py-3 mb-4 text-sm text-coral-400">
                Last refresh failed ({infra.error instanceof Error ? infra.error.message : String(infra.error)}) — showing the status checked at {fmt.time(d.checked_at)}.
              </div>
            )}
            <ComponentHealth d={d} />
            {distributed ? <Distributed d={d} ov={ov.data} tl={tl} samples={samples} queryFilter={queryFilter}
              setQueryFilter={setQueryFilter} expanded={expanded} setExpanded={setExpanded} /> : <Local d={d} ov={ov.data} tl={tl} />}
            <DeadLetters d={d} />
          </>
        )}
      </QueryState>
    </div>
  );
}

function ComponentHealth({ d }: { d: InfrastructureStatus }) {
  const sp = d.spark;
  const items: { label: string; state: string; detail: string; testId: string }[] = [
    { label: "PostgreSQL / DB", state: d.database.status, testId: "health-database",
      detail: d.database.status === "UP" ? `${d.database.dialect} · ${fmt.ms(d.database.latency_ms)}` : d.database.error ?? "" },
    { label: "Redis", state: d.redis.status, testId: "health-redis", detail: d.redis.note ?? "" },
    { label: "Kafka", state: d.kafka.status, testId: "health-kafka",
      detail: d.kafka.status === "UP" ? `${d.kafka.brokers} broker${d.kafka.brokers === 1 ? "" : "s"} · ${d.kafka.topics?.filter((t) => t.exists).length ?? 0} topics` : d.kafka.note ?? d.kafka.error ?? "" },
    { label: "Spark Structured Streaming", state: sp.status, testId: "health-spark",
      detail: sp.note ?? (sp.status === "RUNNING" || sp.status === "STALE" || sp.status === "STOPPED" || sp.status === "FAILED"
        ? `${sp.master ?? ""} · reported ${fmtAge(sp.age_seconds)}` : sp.unavailable_reason ?? "") },
  ];
  if (d.profile === "distributed") {
    items.push(
      { label: "Persistence consumer", state: d.consumer?.status ?? "UNKNOWN", testId: "health-consumer",
        detail: d.consumer?.status === "UP" || d.consumer?.status === "STALE" ? `heartbeat ${fmtAge(d.consumer.age_seconds)}` : d.consumer?.unavailable_reason ?? "" },
      { label: "Fallback python processor", state: d.processor?.status === "NO_HEARTBEAT" ? "NOT_USED" : d.processor?.status ?? "UNKNOWN", testId: "health-processor",
        detail: d.processor?.status === "NO_HEARTBEAT" ? "not running (expected while Spark runs)" : `heartbeat ${fmtAge(d.processor?.age_seconds)}` },
    );
  }
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6 gap-3 mb-4">
      {items.map((i) => (
        <div key={i.label} className="panel px-4 py-3 min-w-0">
          <div className="flex items-center justify-between gap-2">
            <span className="panel-title truncate">{i.label}</span>
            <HealthPill state={i.state} testId={i.testId} />
          </div>
          <div className="text-xs text-mist-400 mt-2 truncate" title={i.detail}>{i.detail || "—"}</div>
        </div>
      ))}
      {d.profile === "distributed" && (
        <div className="panel px-4 py-3 min-w-0 sm:col-span-2 xl:col-span-3 2xl:col-span-6 text-xs text-mist-300">
          Active stream processor (from live heartbeats): <b data-testid="active-processor" className={cx(d.stream_processor?.startsWith("NOT RUNNING") ? "text-coral-400" : "text-mist-100")}>{d.stream_processor ?? "unknown"}</b>
        </div>
      )}
    </div>
  );
}

function Distributed({ d, ov, tl, samples, queryFilter, setQueryFilter, expanded, setExpanded }: {
  d: InfrastructureStatus; ov?: Overview; tl: { data?: Timeline; isLoading: boolean; isError: boolean; error: unknown; refetch: () => unknown };
  samples: Sample[]; queryFilter: string; setQueryFilter: (v: string) => void; expanded: string | null; setExpanded: (v: string | null) => void;
}) {
  const sp = d.spark;
  const queries = sp.queries ?? [];
  const score = queries.find((q) => q.name === "score");
  const persistence = d.kafka.consumer_groups?.find((g) => g.group === "fraudstream-persistence");
  const shown = queries.filter((q) => matchesFilter(q, queryFilter));
  const sparkReporting = !!sp.queries;
  return (
    <>
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 mb-4">
        <StatTile testId="stat-spark-input" label="Spark input (score)" tone="cyan"
          value={sparkReporting ? <>{rate(score?.input_rows_per_second)}<span className="text-sm text-mist-400"> rows/s</span></> : <Unavailable reason={sp.unavailable_reason} />}
          hint={sparkReporting ? `batch ${score?.batch_id ?? "—"} · ${fmt.num(score?.num_input_rows)} rows` : sp.unavailable_reason} />
        <StatTile label="Spark processed (score)"
          value={sparkReporting ? <>{rate(score?.processed_rows_per_second)}<span className="text-sm text-mist-400"> rows/s</span></> : <Unavailable reason={sp.unavailable_reason} />}
          hint={score ? `trigger ${fmt.ms(score.trigger_ms)}` : "last micro-batch"} />
        <StatTile label="Persisted throughput" value={ov ? `${ov.tps_last_minute.toFixed(2)}/s` : "—"} hint="PostgreSQL, 60 s window" />
        <StatTile testId="stat-e2e-p95" label="End-to-end p95" value={fmt.ms(ov?.detection_latency_ms.p95)}
          hint={ov ? `p50 ${fmt.ms(ov.detection_latency_ms.p50)} · n=${fmt.num(ov.detection_latency_ms.samples)} · 15 min` : "ingest → persisted decision"} />
        <StatTile testId="stat-spark-lag" label="Spark lag (raw topic)" tone={score?.lag_vs_raw_topic ? "amber" : "emerald"}
          value={score?.lag_vs_raw_topic != null ? fmt.num(score.lag_vs_raw_topic) : <Unavailable reason="Spark not reporting source offsets" />}
          hint="topic end offsets − last processed offsets" />
        <StatTile testId="stat-persist-lag" label="Persistence lag" tone={persistence?.total_lag ? "amber" : "emerald"}
          value={persistence?.total_lag != null ? fmt.num(persistence.total_lag) : <Unavailable reason={persistence?.error ?? "consumer group not found"} />}
          hint={persistence ? `${persistence.state} · ${persistence.members ?? "?"} member(s)` : "consumer group"} />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4 mb-4">
        <Panel title="Persisted transactions · last 30 min (30 s buckets)">
          <QueryState q={tl} empty={!tl.data?.points.length && <EmptyState title="No transactions persisted in the last 30 minutes" hint="Start traffic from the Simulation Lab." />}>
            {tl.data && <VolumeChart points={tl.data.points} height={200} />}
          </QueryState>
        </Panel>
        <Panel title="Spark score query rows/s · sampled by this page">
          {samples.length < 2
            ? <EmptyState title={sparkReporting ? "Collecting samples…" : "Spark is not reporting progress"}
              hint={sparkReporting ? "One sample per new Spark progress report (~5 s). Samples exist only while this page is open." : sp.unavailable_reason} />
            : <TimeSeries points={samples} height={200} series={[
              { key: "input", name: "input rows/s", color: "#3987e5" }, { key: "processed", name: "processed rows/s", color: "#d95926" }]} />}
          <p className="text-[11px] text-mist-400 mt-1">{samples.length} sample(s) · not persisted; history is shown above from PostgreSQL.</p>
        </Panel>
      </div>

      <Panel className="mb-4" title={`Spark streaming queries${sp.spark_version ? ` · Spark ${sp.spark_version} · ${sp.master}` : ""}`}
        actions={<Select label="" value={queryFilter} onChange={setQueryFilter} options={QUERY_FILTERS} testId="query-filter" />}>
        {!sparkReporting ? <EmptyState title={`Spark: ${sp.status.replace(/_/g, " ")}`} hint={sp.unavailable_reason} /> : (
          <>
            <div className="text-xs text-mist-400 mb-2 flex flex-wrap gap-x-4 gap-y-1">
              <span>application <span className="num text-mist-300">{sp.app_id}</span></span>
              <span>status <HealthPill state={sp.status} /></span>
              <span>last report {fmtAge(sp.age_seconds)}</span>
              <span>checkpoints <span className="num text-mist-300">{sp.checkpoint_root ?? "not reported by this Spark job version"}</span></span>
            </div>
            {shown.length === 0 ? <EmptyState title="No query matches this filter" /> : (
              <div className="overflow-x-auto">
                <table className="w-full" data-testid="spark-queries">
                  <thead><tr>
                    <th className="th" /><th className="th">Query</th><th className="th">State</th><th className="th">Batch</th><th className="th">Rows</th>
                    <th className="th">In rows/s</th><th className="th">Proc rows/s</th><th className="th">Trigger</th><th className="th">State rows</th>
                    <th className="th">Late dropped</th><th className="th">Lag</th><th className="th">Checkpoint</th>
                  </tr></thead>
                  <tbody>
                    {shown.map((q) => {
                      const open = expanded === q.name;
                      const cp = q.checkpoint;
                      return (
                        <Fragment key={q.name}>
                          <tr className="hover:bg-ink-850 cursor-pointer" onClick={() => setExpanded(open ? null : q.name)} data-testid={`query-row-${q.name}`}>
                            <td className="td"><button className="btn-ghost rounded p-0.5" aria-expanded={open} aria-label={`Details for ${q.name}`}>
                              {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}</button></td>
                            <td className="td num text-mist-100">{q.name}</td>
                            <td className="td"><HealthPill state={q.exception ? "FAILED" : q.active ? "ACTIVE" : "INACTIVE"} /></td>
                            <td className="td num">{q.batch_id ?? "—"}</td>
                            <td className="td num">{fmt.num(q.num_input_rows)}</td>
                            <td className="td num">{rate(q.input_rows_per_second)}</td>
                            <td className="td num">{rate(q.processed_rows_per_second)}</td>
                            <td className="td num">{fmt.ms(q.trigger_ms)}</td>
                            <td className="td num">{fmt.num(q.state_rows)}</td>
                            <td className={cx("td num", q.rows_dropped_by_watermark > 0 && "text-amber-400")}>{fmt.num(q.rows_dropped_by_watermark)}</td>
                            <td className="td num">{q.lag_vs_raw_topic != null ? fmt.num(q.lag_vs_raw_topic) : "—"}</td>
                            <td className="td num text-mist-300">
                              {!cp ? <Unavailable reason="not reported by this Spark job version" />
                                : !cp.exists ? <span className="text-coral-400">missing</span>
                                  : <>#{cp.last_committed_batch ?? "—"} · {fmtAge(cp.last_commit_age_seconds)}</>}
                            </td>
                          </tr>
                          {open && (
                            <tr><td className="td bg-ink-850" colSpan={12}>
                              <QueryDetail q={q} />
                            </td></tr>
                          )}
                        </Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
            <p className="text-[11px] text-mist-400 mt-2">Rates and durations are from each query's last micro-batch (Spark <code>lastProgress</code>), published every 5 s. Click a row for the duration breakdown and checkpoint details.</p>
          </>
        )}
      </Panel>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4 mb-4">
        <Panel title="Kafka topics">
          {d.kafka.status !== "UP" ? <EmptyState title={`Kafka ${d.kafka.status}`} hint={d.kafka.error} /> : (
            <div className="overflow-x-auto">
              <table className="w-full" data-testid="kafka-topics">
                <thead><tr><th className="th">Topic</th><th className="th">Partitions</th><th className="th">End offset</th><th className="th">Retained</th><th className="th">Msgs/s</th></tr></thead>
                <tbody>
                  {d.kafka.topics?.map((t) => (
                    <tr key={t.name} className="hover:bg-ink-850">
                      <td className="td" title={t.description}><div className="num text-mist-100">{t.name}</div><div className="text-[11px] text-mist-400 whitespace-normal">{t.description}</div></td>
                      {t.exists ? <>
                        <td className="td num">{t.partitions}</td>
                        <td className="td num">{fmt.num(t.end_offset_total)}</td>
                        <td className="td num">{fmt.num(t.messages_retained)}</td>
                        <td className="td num">{t.rate_per_sec_since_last_check == null
                          ? <Unavailable reason="needs two consecutive status checks" /> : t.rate_per_sec_since_last_check.toFixed(2)}</td>
                      </> : <td className="td text-coral-400" colSpan={4}>topic does not exist</td>}
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="text-[11px] text-mist-400 mt-2">Msgs/s = change in end offsets between this API's two most recent status checks.</p>
            </div>
          )}
        </Panel>
        <Panel title="Consumer groups & lag">
          {d.kafka.status !== "UP" ? <EmptyState title={`Kafka ${d.kafka.status}`} /> : (
            <div className="space-y-3" data-testid="consumer-groups">
              {d.kafka.consumer_groups?.map((g) => (
                <div key={g.group} className="rounded-lg border border-ink-700 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="num text-sm text-mist-100">{g.group}</span>
                    {g.error ? <HealthPill state="ERROR" /> : <HealthPill state={g.state} />}
                  </div>
                  {g.error ? <p className="text-xs text-coral-400 mt-1">{g.error}</p> : (
                    <>
                      <div className="text-xs text-mist-400 mt-1">{g.members ?? "?"} member(s) · {g.committed_partitions} committed partition(s) · total lag <b className="num text-mist-100">{fmt.num(g.total_lag)}</b></div>
                      {!g.active && <p className="text-[11px] text-mist-400 mt-1">Inactive group — its lag is not counted as pipeline lag
                        {g.group === "fraudstream-processor" ? " (this is the fallback python processor's group; Spark tracks offsets in its checkpoint)." : "."}</p>}
                      <div className="mt-2 flex flex-wrap gap-2">
                        {Object.entries(g.lag_by_topic ?? {}).map(([t, lag]) => (
                          <span key={t} className="rounded bg-ink-850 border border-ink-700 px-2 py-0.5 text-[11px] num">{t}: {fmt.num(lag)}</span>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              ))}
            </div>
          )}
        </Panel>
      </div>

      {d.consumer?.stats && (
        <Panel className="mb-4" title="Persistence consumer counters (since process start)">
          <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-7 gap-3">
            {Object.entries(d.consumer.stats).map(([k, v]) => <StatTile compact key={k} label={k.replace(/_/g, " ")} value={fmt.num(v)} />)}
          </div>
          {d.consumer.last_error && <p className="text-xs text-coral-400 mt-2">last error: {d.consumer.last_error}</p>}
        </Panel>
      )}
    </>
  );
}

function QueryDetail({ q }: { q: SparkQuery }) {
  const durations = Object.entries(q.duration_ms ?? {}).sort((a, b) => b[1] - a[1]);
  const cp = q.checkpoint;
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 whitespace-normal py-1" data-testid={`query-detail-${q.name}`}>
      <div>
        <div className="panel-title mb-1">Last micro-batch durations</div>
        {durations.length ? durations.map(([k, v]) => <KV key={k} k={k} v={fmt.ms(v)} />) : <p className="text-xs text-mist-400">none reported</p>}
      </div>
      <div>
        <div className="panel-title mb-1">Query</div>
        <KV k="status" v={q.status ?? "—"} />
        <KV k="event-time watermark" v={q.event_time_watermark ? fmt.dt(q.event_time_watermark) : "— (none)"} />
        <KV k="batch timestamp" v={fmt.dt(q.timestamp)} />
        <KV k="run id" v={<span className="num text-xs">{q.run_id}</span>} />
        {q.exception && <p className="text-xs text-coral-400 mt-2 break-all">{q.exception}</p>}
      </div>
      <div>
        <div className="panel-title mb-1">Checkpoint</div>
        {!cp ? <p className="text-xs text-mist-400">Not reported by this Spark job version.</p> : (
          <>
            <KV k="location" v={<span className="num text-xs">{cp.location}</span>} />
            <KV k="exists" v={cp.exists ? "yes" : "NO"} />
            {cp.exists && <>
              <KV k="last committed batch" v={cp.last_committed_batch ?? "—"} />
              <KV k="last planned batch" v={cp.last_planned_batch ?? "—"} />
              <KV k="uncommitted (in flight)" v={cp.uncommitted_batches ?? "—"} />
              <KV k="last commit" v={cp.last_commit_at ? `${fmt.dt(new Date(cp.last_commit_at * 1000).toISOString())} (${fmtAge(cp.last_commit_age_seconds)})` : "—"} />
              <KV k="state store" v={cp.has_state ? "present" : "none (stateless)"} />
            </>}
          </>
        )}
      </div>
    </div>
  );
}

function Local({ d, ov, tl }: { d: InfrastructureStatus; ov?: Overview; tl: { data?: Timeline; isLoading: boolean; isError: boolean; error: unknown; refetch: () => unknown } }) {
  const p = d.pipeline;
  return (
    <>
      <div role="note" className="panel border-amber-500/40 px-4 py-3 mb-4 text-sm text-amber-400">
        LOCAL DEVELOPMENT profile: Kafka and Spark are not used. Events are processed by the in-process pipeline
        ({p?.mode ?? "simplified replacement for Kafka + Spark"}); Kafka topics, consumer lag, Spark queries and checkpoints do not exist here.
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        <StatTile label="Persisted throughput" value={ov ? `${ov.tps_last_minute.toFixed(2)}/s` : "—"} hint="60 s window" tone="cyan" />
        <StatTile label="End-to-end p95" value={fmt.ms(ov?.detection_latency_ms.p95)} hint={ov ? `p50 ${fmt.ms(ov.detection_latency_ms.p50)} · 15 min` : ""} />
        <StatTile label="Feature-store customers" value={fmt.num(p?.feature_store_customers)} hint="in-memory behavioural state" />
        <StatTile label="Warm-started events" value={fmt.num(p?.warm_started_events)} hint="replayed from DB at startup" />
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 mb-4">
        <Panel title="Persisted transactions · last 30 min" className="xl:col-span-2">
          <QueryState q={tl} empty={!tl.data?.points.length && <EmptyState title="No transactions in the last 30 minutes" />}>
            {tl.data && <VolumeChart points={tl.data.points} height={200} />}
          </QueryState>
        </Panel>
        <Panel title="In-process pipeline counters">
          {p && Object.keys(p.counters).length ? Object.entries(p.counters).map(([k, v]) => <KV key={k} k={k.replace(/_/g, " ")} v={<span className="num">{fmt.num(v)}</span>} />)
            : <EmptyState title="No events processed by this API process yet" />}
        </Panel>
      </div>
    </>
  );
}

function DeadLetters({ d }: { d: InfrastructureStatus }) {
  return (
    <Panel title="Dead-letter queue">
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-3">
        <StatTile testId="stat-dlq-kafka" compact label="fraud.dlq topic (retained)"
          value={d.kafka.dlq_messages != null ? fmt.num(d.kafka.dlq_messages) : <Unavailable reason={d.kafka.note ?? "Kafka not reachable"} />}
          tone={d.kafka.dlq_messages ? "amber" : "mist"} />
        <StatTile testId="stat-dlq-db" compact label="Dead letters in database" value={fmt.num(d.dead_letters_db)} tone={d.dead_letters_db ? "amber" : "mist"} />
      </div>
      {d.recent_dead_letters.length === 0 ? <EmptyState title="No dead letters recorded" hint="Malformed, late or unprocessable events appear here with their reason." /> : (
        <div className="overflow-x-auto">
          <table className="w-full" data-testid="dead-letters">
            <thead><tr><th className="th">Created</th><th className="th">Source</th><th className="th">Reason</th><th className="th">Event id</th></tr></thead>
            <tbody>
              {d.recent_dead_letters.map((x) => (
                <tr key={x.id}><td className="td num text-mist-300">{fmt.dt(x.created_at)}</td><td className="td">{x.source}</td>
                  <td className="td whitespace-normal">{x.reason}</td><td className="td num text-mist-300">{x.event_id ?? "—"}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

