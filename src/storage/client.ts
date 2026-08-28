import { Worker } from "node:worker_threads";
import { EventEmitter } from "node:events";
import {
  DurableQClosedError,
  DurableQStorageWorkerError,
  deserializeInternalError,
} from "../errors.ts";
import { newRpcId } from "../ids.ts";
import type { StorageRequest, StorageResponse, WorkerEventMessage } from "./protocol.ts";

type Pending = {
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
};

export class StorageClient extends EventEmitter {
  private worker: Worker;
  private pending = new Map<string, Pending>();
  private closed = false;

  constructor(worker: Worker) {
    super();
    this.worker = worker;
    worker.on("message", (msg: unknown) => this.onMessage(msg));
    worker.on("error", (err) =>
      this.failAll(new DurableQStorageWorkerError("storage worker error", { cause: err })),
    );
    worker.on("exit", (code) => {
      if (this.closed && this.pending.size === 0) return;
      this.failAll(new DurableQStorageWorkerError(`storage worker exited (${code})`));
    });
  }

  private onMessage(msg: unknown): void {
    if (msg && typeof msg === "object" && (msg as WorkerEventMessage).type === "event") {
      const ev = msg as WorkerEventMessage;
      this.emit(ev.event, { jobId: ev.jobId, generation: ev.generation });
      return;
    }
    const res = msg as StorageResponse;
    if (!res || typeof res.id !== "string") return;
    const pending = this.pending.get(res.id);
    if (!pending) return;
    this.pending.delete(res.id);
    if (res.ok) pending.resolve(res.result);
    else pending.reject(deserializeInternalError(res.error));
  }

  private failAll(err: Error): void {
    this.closed = true;
    const pending = [...this.pending.values()];
    this.pending.clear();
    for (const p of pending) p.reject(err);
  }

  async request<T>(op: string, payload: unknown): Promise<T> {
    if (this.closed) throw new DurableQClosedError("storage client is closed");
    const id = newRpcId();
    const req: StorageRequest = { id, op, payload };
    return await new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: (v) => resolve(v as T),
        reject,
      });
      try {
        this.worker.postMessage(req);
      } catch (err) {
        this.pending.delete(id);
        reject(err);
      }
    });
  }

  async close(): Promise<void> {
    if (this.closed && this.pending.size === 0) return;
    try {
      await this.request("close", {});
    } catch {
      // worker may already be gone
    }
    this.closed = true;
    this.failAll(new DurableQClosedError("storage client closed"));
    await this.worker.terminate();
  }
}

const SPAWN_OVERRIDE = Symbol.for("durableq.spawnStorageWorker");

function workerUrl(): URL {
  const ext = import.meta.url.endsWith(".ts") ? ".ts" : ".js";
  return new URL(`./storage-worker${ext}`, import.meta.url);
}

export function spawnStorageWorker(): Worker {
  const override = (globalThis as Record<symbol, unknown>)[SPAWN_OVERRIDE];
  if (typeof override === "function") {
    return (override as () => Worker)();
  }
  return new Worker(workerUrl(), {
    env: process.env,
    execArgv: [],
  });
}
