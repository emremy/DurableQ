import { EventEmitter } from "node:events";
import type { Clock } from "./clock.ts";
import { systemClock } from "./clock.ts";
import { encodeJson } from "./codec/json.ts";
import { DurableQClosedError, DurableQValidationError } from "./errors.ts";
import { jobRowToView } from "./job.ts";
import { acquireRuntime, type RuntimeHandle } from "./storage/runtime-registry.ts";
import type { JobRow } from "./storage/protocol.ts";
import { assertName, normalizeAddOptions, normalizeQueueOptions } from "./validation.ts";
import type {
  AddOptions,
  AddResult,
  CancelResult,
  CleanOptions,
  CountFilter,
  JobMap,
  JobView,
  NormalizedQueueOptions,
  QueueOptions,
  RetryFailureReason,
  RetryResult,
} from "./types.ts";

export class Queue<Jobs extends JobMap = JobMap> extends EventEmitter {
  readonly name: string;
  readonly options: NormalizedQueueOptions;
  readonly clock: Clock;
  #runtime: RuntimeHandle | undefined;
  #opening: Promise<RuntimeHandle> | undefined;
  #closed = false;

  constructor(name: string, options: QueueOptions, clock: Clock = systemClock) {
    super();
    assertName(name, "queue name");
    this.name = name;
    this.options = normalizeQueueOptions(options);
    this.clock = clock;
  }

  get path(): string {
    return this.#runtime?.path ?? this.options.path;
  }

  get busyTimeoutMs(): number {
    return this.options.busyTimeoutMs;
  }

  async #ensure(): Promise<RuntimeHandle> {
    if (this.#closed) throw new DurableQClosedError("queue is closed");
    if (this.#runtime) return this.#runtime;
    if (!this.#opening) {
      this.#opening = acquireRuntime({
        path: this.options.path,
        busyTimeoutMs: this.options.busyTimeoutMs,
      });
    }
    this.#runtime = await this.#opening;
    return this.#runtime;
  }

  async add<K extends keyof Jobs & string>(
    name: K,
    data: Jobs[K]["input"],
    options?: AddOptions,
  ): Promise<AddResult<K, Jobs[K]["input"]>> {
    assertName(name, "job name");
    const opts = normalizeAddOptions(options);
    const payloadJson = encodeJson(data, this.options.maxPayloadBytes);
    const rt = await this.#ensure();
    const result = await rt.client.request<{ row: JobRow; deduplicated: boolean }>("enqueue", {
      nowMs: this.clock.now(),
      queue: this.name,
      name,
      payloadJson,
      maxAttempts: opts.attempts,
      delayMs: opts.delayMs,
      priority: opts.priority,
      dedupeKey: opts.dedupeKey,
      attemptTimeoutMs: opts.attemptTimeoutMs,
      backoff: opts.backoff,
    });
    this.emit("enqueued", result.row.id);
    return {
      job: jobRowToView(result.row) as JobView<K, Jobs[K]["input"]>,
      deduplicated: result.deduplicated,
    };
  }

  async get(jobId: string): Promise<JobView | null> {
    if (typeof jobId !== "string" || jobId.length === 0) {
      throw new DurableQValidationError("job id is required");
    }
    const rt = await this.#ensure();
    const row = await rt.client.request<JobRow | null>("get", { id: jobId });
    if (!row || row.queue !== this.name) return null;
    return jobRowToView(row);
  }

  async count(filter?: CountFilter): Promise<number> {
    const rt = await this.#ensure();
    return rt.client.request<number>("count", { queue: this.name, status: filter?.status });
  }

  async cancel(jobId: string): Promise<CancelResult> {
    const rt = await this.#ensure();
    return rt.client.request<CancelResult>("cancelQueued", {
      nowMs: this.clock.now(),
      id: jobId,
      queue: this.name,
    });
  }

  async retry(jobId: string): Promise<RetryResult> {
    const rt = await this.#ensure();
    const out = await rt.client.request<JobRow | { reason: RetryFailureReason }>("manualRetry", {
      nowMs: this.clock.now(),
      id: jobId,
      queue: this.name,
    });
    if ("reason" in out) {
      return { retried: false, reason: out.reason };
    }
    return { retried: true, job: jobRowToView(out) };
  }

  async clean(options: CleanOptions): Promise<{ deleted: number }> {
    if (
      !options ||
      (options.status !== "completed" &&
        options.status !== "failed" &&
        options.status !== "cancelled")
    ) {
      throw new DurableQValidationError("clean status must be completed, failed, or cancelled");
    }
    if (!Number.isSafeInteger(options.before)) {
      throw new DurableQValidationError("clean.before must be a safe integer timestamp");
    }
    const rt = await this.#ensure();
    const deleted = await rt.client.request<number>("clean", {
      queue: this.name,
      status: options.status,
      before: options.before,
    });
    return { deleted };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const rt = this.#runtime;
    this.#runtime = undefined;
    this.#opening = undefined;
    if (rt) await rt.unref();
  }
}
