import { useEffect, useRef, useState } from "react";
import { openStream } from "../services/sse";
import type { LiveTransaction } from "../types/api";

export type StreamState = "connecting" | "open" | "closed" | "error" | "unavailable";

/** Subscribes to /stream/transactions. When paused, incoming events are counted but not shown. */
export function useLiveStream(max = 200, paused = false) {
  const [rows, setRows] = useState<LiveTransaction[]>([]);
  const [state, setState] = useState<StreamState>("connecting");
  const [detail, setDetail] = useState<string | undefined>();
  const [missed, setMissed] = useState(0);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  useEffect(() => {
    if (!paused) setMissed(0);
  }, [paused]);

  useEffect(() => {
    const close = openStream(
      "/stream/transactions",
      (m) => {
        const d = m.data as Record<string, unknown>;
        if (m.event === "status") {
          if (d.live === "unavailable") {
            setState("unavailable");
            setDetail(String(d.reason ?? ""));
          }
          return;
        }
        if (m.event !== "transaction") return;
        if (pausedRef.current) {
          setMissed((n) => n + 1);
          return;
        }
        setRows((prev) => [d as unknown as LiveTransaction, ...prev].slice(0, max));
      },
      (s, det) => {
        setState((cur) => (cur === "unavailable" && s !== "open" ? cur : s));
        setDetail(det);
      },
    );
    return close;
  }, [max]);

  return { rows, state, detail, missed, clear: () => setRows([]) };
}
