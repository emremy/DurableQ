import { EventEmitter } from "node:events";
import { encodeJson } from "./codec/json.ts";
import { serializeStoredError } from "./codec/error.ts";
import {
  RETENTION_INTERVAL_MS,
  SETTLE_BUSY_ATTEMPTS,
  SETTLE_BUSY_BASE_DELAY_MS,
  SETTLE_BUSY_MAX_DELAY_MS,
} from "./constants.ts";
import {
  DurableQBusyError,
  DurableQClosedError,
  DurableQSerializationError,
  NonRetryableError,
} from "./errors.ts";
import { newWorkerId } from "./ids.ts";
import { jobRowToView } from "./job.ts";
import type { Queue } from "./queue.ts";
import type { StorageClient } from "./storage/client.ts";
import { acquireRuntime, type RuntimeHandle } from "./storage/runtime-registry.ts";
import type { JobRow } from "./storage/protocol.ts";
import { assertJobNameList, assertSafeInteger, normalizeWorkerOptions } from "./validation.ts";
import type {
  JobContext,
  JobHandler,
  JobMap,
  JobView,
  JsonValue,
  NormalizedWorkerOptions,
  WorkerOptions,
} from "./types.ts";

type Handlers<Jobs extends JobMap> = {
  [K in keyof Jobs & string]: JobHandler<Jobs[K]["input"], Jobs[K]["output"]>;
};

type ActiveAttempt = {
  jobId: string;
  generation: number;
  abort: AbortController;
  done: Promise<void>;
};

export class Worker<Jobs extends JobMap = JobMap> extends EventEmitter {
  readonly workerId: string;
  readonly options: NormalizedWorkerOptions;
  readonly queue: Queue<Jobs>;
  #handlers: Map<string, JobHandler<JsonValue, JsonValue>>;
  #names: string[];
  #running = false;
  #stopping = false;
  #closed = false;
  #loop: Promise<void> | undefined;
  #wake: (() => void) | undefined;
  #active = new Map<string, ActiveAttempt>();
  #storageHandle: RuntimeHandle | undefined;
  #nextRetentionAt = 0;

  constructor(queue: Queue<Jobs>, handlers: Handlers<Jobs>, options?: WorkerOptions) {
    super();
    this.queue = queue;
    this.workerId = newWorkerId();
    this.options = normalizeWorkerOptions(options, queue.busyTimeoutMs);
    const names = Object.keys(handlers);
    assertJobNameList(names);
    this.#names = names;
    this.#handlers = new Map();
    for (const name of names) {
      this.#handlers.set(name, handlers[name] as JobHandler<JsonValue, JsonValue>);
    }
  }

  async start(): Promise<void> {
    if (this.#closed) throw new DurableQClosedError("worker is closed");
    if (this.#running) return;
    this.#storageHandle = await acquireRuntime({
      path: this.queue.options.path,
      busyTimeoutMs: this.queue.busyTimeoutMs,
    });
    this.#running = true;
    this.#stopping = false;
    this.queue.on("enqueued", () => this.#kick());
    const client = this.#storageHandle.client;
    client.on("attemptTimeout", (info: { jobId: string; generation: number }) => {
      const active = this.#active.get(info.jobId);
      if (active && active.generation === info.generation) {
        active.abort.abort();
        this.#emit("leaseLost", {
          jobId: info.jobId,
          generation: info.generation,
          reason: "timeout",
        });
      }
    });
    client.on("leaseLost", (info: { jobId: string; generation: number }) => {
      const active = this.#active.get(info.jobId);
      if (active && active.generation === info.generation) {
        active.abort.abort();
        this.#emit("leaseLost", {
          jobId: info.jobId,
          generation: info.generation,
          reason: "heartbeat",
        });
      }
    });
    this.#loop = this.#runLoop();
  }

  async close(opts?: { timeoutMs?: number }): Promise<void> {
    if (this.#closed) return;
    this.#stopping = true;
    this.#kick();
    const timeoutMs = opts?.timeoutMs ?? this.options.defaultAttemptTimeoutMs;
    const deadline = this.queue.clock.now() + timeoutMs;
    const wait = this.#waitActive(deadline);
    const timedOut = await wait;
    if (timedOut) {
      for (const a of this.#active.values()) a.abort.abort();
      const graceDeadline = Date.now() + 250;
      while (this.#active.size > 0 && Date.now() < graceDeadline) {
        await Promise.race([
          Promise.all([...this.#active.values()].map((a) => a.done)),
          this.#sleep(20),
        ]);
      }
    }
    this.#running = false;
    this.#closed = true;
    this.#kick();
    await this.#loop?.catch(() => undefined);
    const client = this.#storageHandle?.client;
    for (const a of this.#active.values()) {
      await client
        ?.request("stopLeaseTracking", { jobId: a.jobId, generation: a.generation })
        .catch(() => undefined);
    }
    if (this.#storageHandle) {
      await this.#storageHandle.unref().catch(() => undefined);
      this.#storageHandle = undefined;
    }
  }

  #kick(): void {
    this.#wake?.();
  }

  #sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const t = setTimeout(resolve, ms);
      this.#wake = () => {
        clearTimeout(t);
        resolve();
      };
    });
  }

  async #waitActive(deadline: number): Promise<boolean> {
    while (this.#active.size > 0) {
      if (this.queue.clock.now() >= deadline) return true;
      await Promise.race([
        Promise.all([...this.#active.values()].map((a) => a.done)),
        this.#sleep(20),
      ]);
    }
    return false;
  }

  #client(): StorageClient {
    const handle = this.#storageHandle;
    if (!handle) throw new DurableQClosedError("worker storage is not available");
    return handle.client;
  }

  async #runLoop(): Promise<void> {
    const client = this.#client();
    while (this.#running && !this.#closed) {
      try {
        if (this.#stopping) {
          await this.#sleep(this.options.pollIntervalMs);
          continue;
        }
        await this.#maybeApplyRetention(client);
        while (!this.#stopping && this.#active.size < this.options.concurrency) {
          const row = await client.request<JobRow | null>("claim", {
            nowMs: this.queue.clock.now(),
            queue: this.queue.name,
            workerId: this.workerId,
            names: this.#names,
            leaseDurationMs: this.options.leaseDurationMs,
          });
          if (!row) break;
          this.#dispatch(row);
        }
      } catch (err) {
        this.#emit("error", err);
        await this.#sleep(this.options.pollIntervalMs);
        continue;
      }
      await this.#sleep(this.options.pollIntervalMs);
    }
  }

  async #maybeApplyRetention(client: StorageClient): Promise<void> {
    const rules = this.queue.options.retention;
    const nowMs = this.queue.clock.now();
    if (rules.length === 0 || nowMs < this.#nextRetentionAt) return;
    // Local throttle; the database slot coordinates processes sharing the queue.
    this.#nextRetentionAt = nowMs + RETENTION_INTERVAL_MS;
    try {
      const deleted = await client.request<number>("applyRetention", {
        nowMs,
        queue: this.queue.name,
        rules,
      });
      if (deleted > 0) this.#emit("retention", { deleted });
    } catch (err) {
      this.#emit("error", err);
    }
  }

  #dispatch(row: JobRow): void {
    const abort = new AbortController();
    const job = jobRowToView(row);
    const attemptTimeout = row.attempt_timeout_ms ?? this.options.defaultAttemptTimeoutMs;
    const started = this.queue.clock.now();
    const attempt: ActiveAttempt = {
      jobId: row.id,
      generation: row.lease_generation,
      abort,
      done: Promise.resolve(),
    };
    attempt.done = this.#runOne(row, job, abort, started, attemptTimeout);
    this.#active.set(row.id, attempt);
    void attempt.done.finally(() => {
      this.#active.delete(row.id);
      this.#kick();
    });
  }

  async #runOne(
    row: JobRow,
    job: JobView,
    abort: AbortController,
    started: number,
    attemptTimeout: number,
  ): Promise<void> {
    const client = this.#client();
    const ctx: JobContext = {
      signal: abort.signal,
      attempt: row.attempt_count,
      maxAttempts: row.max_attempts,
      workerId: this.workerId,
      updateProgress: async (progress: number) => {
        assertSafeInteger(progress, "progress", 0, 100);
        await client.request("updateProgress", {
          nowMs: this.queue.clock.now(),
          id: row.id,
          workerId: this.workerId,
          generation: row.lease_generation,
          progress,
        });
      },
    };
    this.#emit("active", job);
    await client.request("startLeaseTracking", {
      jobId: row.id,
      workerId: this.workerId,
      generation: row.lease_generation,
      leaseDurationMs: this.options.leaseDurationMs,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
      attemptDeadlineMs: started + attemptTimeout,
    });
    const handler = this.#handlers.get(row.name);
    if (!handler) {
      await this.#fail(row, new Error(`no handler for ${row.name}`), true);
      return;
    }
    try {
      let output: JsonValue;
      try {
        output = await handler(job, ctx);
      } catch (err) {
        if (abort.signal.aborted) {
          this.#emit("leaseLost", { jobId: row.id, generation: row.lease_generation });
          return;
        }
        await this.#fail(row, err, err instanceof NonRetryableError);
        return;
      }
      if (abort.signal.aborted) {
        this.#emit("leaseLost", { jobId: row.id, generation: row.lease_generation });
        return;
      }
      let resultJson: string;
      try {
        resultJson = encodeJson(output, this.queue.options.maxResultBytes);
      } catch (err) {
        const ser =
          err instanceof DurableQSerializationError
            ? err
            : new DurableQSerializationError("handler result is not JSON-safe", { cause: err });
        await this.#fail(row, ser, true);
        return;
      }
      try {
        await this.#settle("settleSuccess", () => ({
          nowMs: this.queue.clock.now(),
          id: row.id,
          workerId: this.workerId,
          generation: row.lease_generation,
          resultJson,
        }));
      } catch (err) {
        // A storage failure is not a handler failure: the job stays active and is recovered
        // (and re-run, at-least-once) when its lease expires.
        this.#emit("error", err);
        return;
      }
      this.#emit("completed", { ...job, status: "completed" as const });
    } finally {
      await client
        .request("stopLeaseTracking", { jobId: row.id, generation: row.lease_generation })
        .catch(() => undefined);
    }
  }

  /**
   * Settles with bounded, jittered retries on SQLITE_BUSY. A BUSY write transaction never
   * committed (it failed at BEGIN IMMEDIATE or was rolled back), and lease fencing rejects a
   * retry that is no longer valid, so retrying cannot double-settle.
   */
  async #settle<T>(op: "settleSuccess" | "settleFailure", payload: () => object): Promise<T> {
    const client = this.#client();
    for (let attempt = 1; ; attempt++) {
      try {
        return await client.request<T>(op, payload());
      } catch (err) {
        if (!(err instanceof DurableQBusyError) || attempt >= SETTLE_BUSY_ATTEMPTS) throw err;
        const capMs = Math.min(SETTLE_BUSY_MAX_DELAY_MS, SETTLE_BUSY_BASE_DELAY_MS * 2 ** attempt);
        await new Promise((r) => setTimeout(r, Math.random() * capMs));
      }
    }
  }

  async #fail(row: JobRow, err: unknown, nonRetryable: boolean): Promise<void> {
    try {
      const error = serializeStoredError(err);
      const settled = await this.#settle<JobRow>("settleFailure", () => ({
        nowMs: this.queue.clock.now(),
        id: row.id,
        workerId: this.workerId,
        generation: row.lease_generation,
        error,
        nonRetryable,
      }));
      if (settled.status === "queued") this.#emit("retrying", jobRowToView(settled));
      else this.#emit("failed", jobRowToView(settled));
    } catch (settleErr) {
      this.#emit("error", settleErr);
    }
  }

  #emit(event: string, payload?: unknown): void {
    try {
      this.emit(event, payload);
    } catch (err) {
      try {
        this.emit("error", err);
      } catch {
        // listener failures must not corrupt job state
      }
    }
  }
}
