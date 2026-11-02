// Benchmark-only storage worker: the real storage session of the source tree under test, plus
// per-operation timings (lock wait, transaction body, commit) and error counts by SQLite code.
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { parentPort, workerData } from "node:worker_threads";

type Session = import("../src/storage/session.ts").StorageSession;
type Cuts = import("../src/storage/session.ts").TransactionCuts;

const { src } = workerData as { src: string };
const { createStorageSession } = (await import(
  pathToFileURL(join(src, "storage/session.ts")).href
)) as typeof import("../src/storage/session.ts");
const { serializeInternalError } = (await import(
  pathToFileURL(join(src, "errors.ts")).href
)) as typeof import("../src/errors.ts");

if (!parentPort) throw new Error("diag worker needs a parentPort");
const port = parentPort;

type OpStats = { calls: number; ms: number[]; errors: Record<string, number> };
const ops = new Map<string, OpStats>();
const txn = new Map<string, { lockWaitMs: number[]; bodyMs: number[]; commitMs: number[] }>();
const op = (name: string) => {
  let s = ops.get(name);
  if (!s) ops.set(name, (s = { calls: 0, ms: [], errors: {} }));
  return s;
};

function timed(name: string): Cuts {
  let t0 = 0;
  let t1 = 0;
  let t2 = 0;
  return {
    beforeBegin: () => void (t0 = performance.now()),
    afterBegin: () => void (t1 = performance.now()),
    beforeCommit: () => void (t2 = performance.now()),
    afterCommit: () => {
      let t = txn.get(name);
      if (!t) txn.set(name, (t = { lockWaitMs: [], bodyMs: [], commitMs: [] }));
      t.lockWaitMs.push(t1 - t0);
      t.bodyMs.push(t2 - t1);
      t.commitMs.push(performance.now() - t2);
    },
  };
}

const session: Session = createStorageSession(
  (event, jobId, generation) => port.postMessage({ type: "event", event, jobId, generation }),
  () => Date.now(),
  {
    enqueue: timed("enqueue"),
    claim: timed("claim"),
    settleSuccess: timed("settleSuccess"),
    settleFailure: timed("settleFailure"),
  },
);

function summarize(samples: number[]) {
  const s = [...samples].sort((a, b) => a - b);
  const at = (p: number) => s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] ?? 0;
  const r = (x: number) => Math.round(x * 1000) / 1000;
  return { n: s.length, p50: r(at(50)), p95: r(at(95)), p99: r(at(99)), max: r(s.at(-1) ?? 0) };
}

port.on("message", (msg: { id: string; op: string; payload: unknown }) => {
  if (msg.op === "diag.stats") {
    const out: Record<string, unknown> = {};
    for (const [name, s] of ops) {
      out[name] = { calls: s.calls, latencyMs: summarize(s.ms), errors: s.errors };
    }
    const tx: Record<string, unknown> = {};
    for (const [name, t] of txn) {
      tx[name] = {
        lockWaitMs: summarize(t.lockWaitMs),
        bodyMs: summarize(t.bodyMs),
        commitMs: summarize(t.commitMs),
      };
    }
    port.postMessage({ id: msg.id, ok: true, result: { ops: out, transactions: tx } });
    return;
  }
  const s = op(msg.op);
  s.calls += 1;
  const t = performance.now();
  try {
    const result = session.handle(msg);
    s.ms.push(performance.now() - t);
    port.postMessage({ id: msg.id, ok: true, result });
  } catch (err) {
    s.ms.push(performance.now() - t);
    const raw = err as { errcode?: unknown; errstr?: unknown };
    const classified = session.classify(err) as { code?: string };
    const key = `${classified.code ?? "?"}${raw.errcode !== undefined ? `/sqlite:${String(raw.errcode)}` : ""}`;
    s.errors[key] = (s.errors[key] ?? 0) + 1;
    port.postMessage({ id: msg.id, ok: false, error: serializeInternalError(classified) });
  }
});
port.on("close", () => session.dispose());
