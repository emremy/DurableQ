import { parentPort } from "node:worker_threads";
import type { DatabaseSync } from "node:sqlite";
import {
  DurableQBusyError,
  DurableQClosedError,
  DurableQError,
  DurableQStorageError,
  DurableQStorageWorkerError,
  DurableQValidationError,
  isSqliteBusy,
  serializeInternalError,
} from "../errors.ts";
import { openDatabase, transact } from "./database.ts";
import { migrate } from "./migrations.ts";
import { LeaseTracker } from "./lease-tracker.ts";
import {
  cancelQueued,
  claimJob,
  cleanJobs,
  countJobs,
  enqueueJob,
  getJob,
  heartbeatJob,
  manualRetry,
  nextWakeHint,
  recoverExpiredLocked,
  settleFailure,
  settleSuccess,
} from "./ops.ts";
import type {
  ClaimPayload,
  EnqueuePayload,
  FaultSpec,
  HeartbeatPayload,
  SettleFailurePayload,
  SettleSuccessPayload,
  StorageFailure,
  StorageRequest,
  StorageResponse,
  StorageSuccess,
  TrackPayload,
  WorkerEventMessage,
} from "./protocol.ts";
import { isStorageRequest } from "./protocol.ts";
import type { JobStatus } from "../types.ts";

if (!parentPort) {
  throw new DurableQStorageWorkerError("storage worker started without parentPort");
}

const port = parentPort;
let db: DatabaseSync | undefined;
let tracker: LeaseTracker | undefined;
let closed = false;
let frozenNow: number | null = null;
let testHooks = false;
const faults = new Map<string, "throw" | "exit" | "hang">();

function now(): number {
  return frozenNow ?? Date.now();
}

function applyFault(point: string): void {
  if (!testHooks) return;
  const action = faults.get(point);
  if (action === "throw") {
    throw new DurableQStorageError(`injected fault: ${point}`);
  }
  if (action === "exit") {
    process.exit(2);
  }
  if (action === "hang") {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  }
}

function emitEvent(event: WorkerEventMessage["event"], jobId: string, generation: number): void {
  const msg: WorkerEventMessage = { type: "event", event, jobId, generation };
  port.postMessage(msg);
}

function requireDb(): DatabaseSync {
  if (!db) throw new DurableQStorageError("storage worker is not initialized");
  return db;
}

function classify(err: unknown): unknown {
  if (err instanceof DurableQError) return err;
  if (isSqliteBusy(err)) {
    return new DurableQBusyError("SQLITE_BUSY", { cause: err });
  }
  return new DurableQStorageError(err instanceof Error ? err.message : String(err), { cause: err });
}

function handle(req: StorageRequest): unknown {
  if (closed && req.op !== "close") {
    throw new DurableQClosedError("storage worker is closed");
  }

  switch (req.op) {
    case "init": {
      const p = req.payload as { path: string; busyTimeoutMs: number; enableTestHooks?: boolean };
      if (!p?.path) throw new DurableQValidationError("init requires path");
      testHooks = p.enableTestHooks === true || process.env.DURABLEQ_ENABLE_TEST_HOOKS === "1";
      db = openDatabase(p.path, p.busyTimeoutMs);
      migrate(db);
      tracker = new LeaseTracker(
        db,
        now,
        (nowMs, id, workerId, generation, leaseDurationMs) =>
          heartbeatJob(db!, {
            nowMs,
            id,
            workerId,
            generation,
            leaseDurationMs,
          }),
        (jobId, generation, reason) => {
          emitEvent(reason === "timeout" ? "attemptTimeout" : "leaseLost", jobId, generation);
        },
      );
      tracker.start();
      return { ok: true };
    }
    case "enqueue": {
      const p = req.payload as EnqueuePayload;
      applyFault("enqueue:before-begin");
      const result = transact(requireDb(), () => {
        applyFault("enqueue:after-begin");
        const out = enqueueJob(requireDb(), p);
        applyFault("enqueue:before-commit");
        return out;
      });
      applyFault("enqueue:after-commit");
      return result;
    }
    case "get": {
      const p = req.payload as { id: string };
      return getJob(requireDb(), p.id);
    }
    case "count": {
      const p = req.payload as { queue: string; status?: JobStatus };
      return countJobs(requireDb(), p.queue, p.status);
    }
    case "claim": {
      const p = req.payload as ClaimPayload;
      applyFault("claim:before-begin");
      const result = transact(requireDb(), () => {
        applyFault("claim:after-begin");
        const out = claimJob(requireDb(), p);
        applyFault("claim:before-commit");
        return out;
      });
      applyFault("claim:after-commit");
      return result;
    }
    case "heartbeat": {
      const p = req.payload as HeartbeatPayload;
      return transact(requireDb(), () => heartbeatJob(requireDb(), p));
    }
    case "settleSuccess": {
      const p = req.payload as SettleSuccessPayload;
      applyFault("settleSuccess:before-begin");
      const result = transact(requireDb(), () => {
        applyFault("settleSuccess:after-begin");
        const out = settleSuccess(requireDb(), p);
        applyFault("settleSuccess:before-commit");
        return out;
      });
      applyFault("settleSuccess:after-commit");
      tracker?.untrack(p.id, p.generation);
      return result;
    }
    case "settleFailure": {
      const p = req.payload as SettleFailurePayload;
      applyFault("settleFailure:before-begin");
      const result = transact(requireDb(), () => {
        applyFault("settleFailure:after-begin");
        const out = settleFailure(requireDb(), p);
        applyFault("settleFailure:before-commit");
        return out;
      });
      applyFault("settleFailure:after-commit");
      tracker?.untrack(p.id, p.generation);
      return result;
    }
    case "manualRetry": {
      const p = req.payload as { nowMs: number; id: string; queue: string };
      return transact(requireDb(), () => manualRetry(requireDb(), p));
    }
    case "cancelQueued": {
      const p = req.payload as { nowMs: number; id: string; queue: string };
      return transact(requireDb(), () => cancelQueued(requireDb(), p));
    }
    case "clean": {
      const p = req.payload as {
        queue: string;
        status: "completed" | "failed" | "cancelled";
        before: number;
      };
      return transact(requireDb(), () => cleanJobs(requireDb(), p));
    }
    case "recoverExpired": {
      const p = req.payload as { nowMs: number };
      return transact(requireDb(), () => recoverExpiredLocked(requireDb(), p.nowMs));
    }
    case "startLeaseTracking": {
      const p = req.payload as TrackPayload;
      tracker?.track(p);
      return { ok: true };
    }
    case "stopLeaseTracking": {
      const p = req.payload as { jobId: string; generation?: number };
      tracker?.untrack(p.jobId, p.generation);
      return { ok: true };
    }
    case "nextWakeHint": {
      const p = req.payload as { queue: string; names: string[]; nowMs: number };
      return nextWakeHint(requireDb(), p.queue, p.names, p.nowMs);
    }
    case "close": {
      closed = true;
      tracker?.stop();
      if (db) {
        db.close();
        db = undefined;
      }
      return { ok: true };
    }
    case "test.configureFault": {
      if (!testHooks) throw new DurableQValidationError("test hooks disabled");
      const p = req.payload as FaultSpec;
      if (p.action === "none") faults.delete(p.point);
      else faults.set(p.point, p.action);
      return { ok: true };
    }
    case "test.setNow": {
      if (!testHooks) throw new DurableQValidationError("test hooks disabled");
      const p = req.payload as { nowMs: number | null };
      frozenNow = p.nowMs;
      tracker?.setNowFn(now);
      return { nowMs: now() };
    }
    case "test.getNow": {
      if (!testHooks) throw new DurableQValidationError("test hooks disabled");
      return { nowMs: now() };
    }
    case "test.tickLeases": {
      if (!testHooks) throw new DurableQValidationError("test hooks disabled");
      const n = transact(requireDb(), () => recoverExpiredLocked(requireDb(), now()));
      tracker?.tick();
      return { recovered: n };
    }
    default:
      throw new DurableQValidationError(
        `unknown storage op: ${String((req as StorageRequest).op)}`,
      );
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
    const result = handle(msg);
    const res: StorageSuccess = { id: msg.id, ok: true, result };
    port.postMessage(res);
  } catch (err) {
    const classified = classify(err);
    const res: StorageFailure = {
      id: msg.id,
      ok: false,
      error: serializeInternalError(classified),
    };
    port.postMessage(res);
  }
});

port.on("close", () => {
  closed = true;
  tracker?.stop();
  try {
    db?.close();
  } catch {
    // already closed
  }
});
