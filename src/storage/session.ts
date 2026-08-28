import type { DatabaseSync } from "node:sqlite";
import {
  DurableQBusyError,
  DurableQClosedError,
  DurableQError,
  DurableQStorageError,
  DurableQValidationError,
  isSqliteBusy,
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
  HeartbeatPayload,
  SettleFailurePayload,
  SettleSuccessPayload,
  StorageRequest,
  TrackPayload,
  WorkerEventMessage,
} from "./protocol.ts";
import type { JobStatus } from "../types.ts";

export type TransactionCuts = {
  beforeBegin?(): void;
  afterBegin?(): void;
  beforeCommit?(): void;
  afterCommit?(): void;
};

export type StorageCuts = {
  enqueue?: TransactionCuts;
  claim?: TransactionCuts;
  settleSuccess?: TransactionCuts;
  settleFailure?: TransactionCuts;
};

export type StorageSession = {
  handle(req: StorageRequest): unknown;
  classify(err: unknown): unknown;
  dispose(): void;
  setNowFn(fn: () => number): void;
  recoverNow(): number;
};

export function createStorageSession(
  emitEvent: (event: WorkerEventMessage["event"], jobId: string, generation: number) => void,
  nowFn: () => number,
  cuts: StorageCuts = {},
): StorageSession {
  let db: DatabaseSync | undefined;
  let tracker: LeaseTracker | undefined;
  let closed = false;
  let currentNow = nowFn;

  function requireDb(): DatabaseSync {
    if (!db) throw new DurableQStorageError("storage worker is not initialized");
    return db;
  }

  function withCuts<T>(spec: TransactionCuts | undefined, fn: () => T): T {
    spec?.beforeBegin?.();
    const result = transact(requireDb(), () => {
      spec?.afterBegin?.();
      const out = fn();
      spec?.beforeCommit?.();
      return out;
    });
    spec?.afterCommit?.();
    return result;
  }

  function classify(err: unknown): unknown {
    if (err instanceof DurableQError) return err;
    if (isSqliteBusy(err)) {
      return new DurableQBusyError("SQLITE_BUSY", { cause: err });
    }
    return new DurableQStorageError(err instanceof Error ? err.message : String(err), {
      cause: err,
    });
  }

  function handle(req: StorageRequest): unknown {
    if (closed && req.op !== "close") {
      throw new DurableQClosedError("storage worker is closed");
    }

    switch (req.op) {
      case "init": {
        const p = req.payload as { path: string; busyTimeoutMs: number };
        if (!p?.path) throw new DurableQValidationError("init requires path");
        db = openDatabase(p.path, p.busyTimeoutMs);
        migrate(db);
        tracker = new LeaseTracker(
          db,
          () => currentNow(),
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
        return withCuts(cuts.enqueue, () => enqueueJob(requireDb(), p));
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
        return withCuts(cuts.claim, () => claimJob(requireDb(), p));
      }
      case "heartbeat": {
        const p = req.payload as HeartbeatPayload;
        return transact(requireDb(), () => heartbeatJob(requireDb(), p));
      }
      case "settleSuccess": {
        const p = req.payload as SettleSuccessPayload;
        const result = withCuts(cuts.settleSuccess, () => settleSuccess(requireDb(), p));
        tracker?.untrack(p.id, p.generation);
        return result;
      }
      case "settleFailure": {
        const p = req.payload as SettleFailurePayload;
        const result = withCuts(cuts.settleFailure, () => settleFailure(requireDb(), p));
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
      default:
        throw new DurableQValidationError(`unknown storage op: ${String(req.op)}`);
    }
  }

  function dispose(): void {
    closed = true;
    tracker?.stop();
    try {
      db?.close();
    } catch {
      // already closed
    }
  }

  function setNowFn(fn: () => number): void {
    currentNow = fn;
    tracker?.setNowFn(fn);
  }

  function recoverNow(): number {
    const n = transact(requireDb(), () => recoverExpiredLocked(requireDb(), currentNow()));
    tracker?.tick();
    return n;
  }

  return { handle, classify, dispose, setNowFn, recoverNow };
}
