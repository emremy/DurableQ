import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parentPort } from "node:worker_threads";
import {
  DurableQBusyError,
  DurableQStorageError,
  DurableQStorageWorkerError,
  DurableQValidationError,
  serializeInternalError,
} from "../../src/errors.ts";
import { createStorageSession } from "../../src/storage/session.ts";
import type {
  StorageFailure,
  StorageRequest,
  StorageResponse,
  StorageSuccess,
} from "../../src/storage/protocol.ts";
import { isStorageRequest } from "../../src/storage/protocol.ts";
import type { FaultSpec } from "./fault-spec.ts";

if (!parentPort) {
  throw new DurableQStorageWorkerError("storage worker started without parentPort");
}

const port = parentPort;
let frozenNow: number | null = null;
const faults = new Map<string, FaultSpec>();
// Migration runs during init, before any RPC can configure faults.
if (process.env.DURABLEQ_TEST_MIGRATE_FAULT) {
  const spec = JSON.parse(process.env.DURABLEQ_TEST_MIGRATE_FAULT) as FaultSpec;
  faults.set(spec.point, spec);
}

function now(): number {
  return frozenNow ?? Date.now();
}

function applyFault(point: string): void {
  const spec = faults.get(point);
  if (!spec || spec.action === "none") return;
  if (spec.gateDir) {
    mkdirSync(spec.gateDir, { recursive: true });
    writeFileSync(join(spec.gateDir, "paused"), point);
  }
  if (spec.action === "busy") {
    if (spec.times !== undefined) {
      if (spec.times <= 0) return;
      spec.times -= 1;
    }
    throw new DurableQBusyError("SQLITE_BUSY (injected)");
  }
  if (spec.action === "throw") {
    throw new DurableQStorageError(`injected fault: ${point}`);
  }
  if (spec.action === "exit") {
    process.exit(2);
  }
  if (spec.action === "hang") {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  }
  if (spec.action === "pause") {
    if (!spec.gateDir) {
      throw new DurableQStorageError("pause requires gateDir");
    }
    const resume = join(spec.gateDir, "resume");
    while (!existsSync(resume)) {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
    }
  }
}

function cutsFor(prefix: string) {
  return {
    beforeBegin: () => applyFault(`${prefix}:before-begin`),
    afterBegin: () => applyFault(`${prefix}:after-begin`),
    beforeCommit: () => applyFault(`${prefix}:before-commit`),
    afterCommit: () => applyFault(`${prefix}:after-commit`),
  };
}

const session = createStorageSession(
  (event, jobId, generation) => {
    port.postMessage({ type: "event", event, jobId, generation });
  },
  now,
  {
    enqueue: cutsFor("enqueue"),
    claim: cutsFor("claim"),
    settleSuccess: cutsFor("settleSuccess"),
    settleFailure: cutsFor("settleFailure"),
    progress: cutsFor("progress"),
    retention: cutsFor("retention"),
    migrate: cutsFor("migrate"),
  },
);

function handleTestOp(req: StorageRequest): unknown {
  switch (req.op) {
    case "test.configureFault": {
      const p = req.payload as FaultSpec;
      if (p.action === "none") faults.delete(p.point);
      else faults.set(p.point, p);
      return { ok: true };
    }
    case "test.setNow": {
      const p = req.payload as { nowMs: number | null };
      frozenNow = p.nowMs;
      session.setNowFn(now);
      return { nowMs: now() };
    }
    case "test.getNow": {
      return { nowMs: now() };
    }
    case "test.tickLeases": {
      return { recovered: session.recoverNow() };
    }
    default:
      throw new DurableQValidationError(`unknown storage op: ${String(req.op)}`);
  }
}

port.on("message", (msg: unknown) => {
  if (!isStorageRequest(msg)) {
    const res: StorageResponse = {
      id: "unknown",
      ok: false,
      error: serializeInternalError(new DurableQStorageError("malformed storage request")),
    };
    port.postMessage(res);
    return;
  }
  try {
    const result = msg.op.startsWith("test.") ? handleTestOp(msg) : session.handle(msg);
    const res: StorageSuccess = { id: msg.id, ok: true, result };
    port.postMessage(res);
  } catch (err) {
    const classified = session.classify(err);
    const res: StorageFailure = {
      id: msg.id,
      ok: false,
      error: serializeInternalError(classified),
    };
    port.postMessage(res);
  }
});

port.on("close", () => {
  session.dispose();
});
