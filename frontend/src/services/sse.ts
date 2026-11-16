/**
 * Minimal SSE client over fetch() so the bearer token travels in a header (EventSource
 * cannot set headers and query-string tokens leak into logs).
 */
import { API_BASE, getAccessToken, refreshSession } from "./api";

export type SSEMessage = { event: string; data: unknown };

export function parseSSEChunk(buffer: string): { messages: SSEMessage[]; rest: string } {
  const messages: SSEMessage[] = [];
  const blocks = buffer.split(/\r?\n\r?\n/);
  const rest = blocks.pop() ?? "";
  for (const block of blocks) {
    let event = "message";
    const data: string[] = [];
    for (const line of block.split(/\r?\n/)) {
      if (line.startsWith(":")) continue;
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
    }
    if (data.length) {
      try {
        messages.push({ event, data: JSON.parse(data.join("\n")) });
      } catch {
        messages.push({ event, data: data.join("\n") });
      }
    }
  }
  return { messages, rest };
}

export function openStream(
  path: string,
  onMessage: (m: SSEMessage) => void,
  onState: (s: "connecting" | "open" | "closed" | "error", detail?: string) => void,
): () => void {
  const ctrl = new AbortController();
  let stopped = false;
  let attempt = 0;

  const run = async () => {
    while (!stopped) {
      onState("connecting");
      try {
        let res = await fetch(`${API_BASE}${path}`, {
          headers: { Authorization: `Bearer ${getAccessToken() ?? ""}`, Accept: "text/event-stream" },
          signal: ctrl.signal,
        });
        if (res.status === 401 && (await refreshSession())) {
          res = await fetch(`${API_BASE}${path}`, {
            headers: { Authorization: `Bearer ${getAccessToken() ?? ""}` },
            signal: ctrl.signal,
          });
        }
        if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
        onState("open");
        attempt = 0;
        const reader = res.body.getReader();
        const dec = new TextDecoder();
        let buf = "";
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          const { messages, rest } = parseSSEChunk(buf);
          buf = rest;
          messages.forEach(onMessage);
        }
        onState("closed");
      } catch (e) {
        if (stopped) return;
        onState("error", e instanceof Error ? e.message : String(e));
      }
      attempt += 1;
      await new Promise((r) => setTimeout(r, Math.min(15000, 1000 * 2 ** Math.min(attempt, 4))));
    }
  };
  void run();
  return () => {
    stopped = true;
    ctrl.abort();
  };
}
