import { EventEmitter } from "node:events";
import type { Clock } from "./clock.ts";
import { systemClock } from "./clock.ts";
import { decodeJson, encodeJson } from "./codec/json.ts";
import {
  DEFAULT_LIST_LIMIT,
  JOB_STATUSES,
  MAX_BATCH_SIZE,
  MAX_LIST_LIMIT,
  MAX_SCHEDULE_INTERVAL_MS,
  MIN_SCHEDULE_INTERVAL_MS,
} from "./constants.ts";
import { DurableQClosedError, DurableQValidationError } from "./errors.ts";
import { jobRowToView } from "./job.ts";
import { acquireRuntime, type RuntimeHandle } from "./storage/runtime-registry.ts";
import {
  backoffFromColumns,
  type EnqueuePayload,
  type JobRow,
  type ScheduleRow,
} from "./storage/protocol.ts";
import {
  assertName,
  assertSafeInteger,
  normalizeAddOptions,
  normalizeQueueOptions,
} from "./validation.ts";
import type {
  AddManyItem,
  AddOptions,
  AddResult,
  CancelResult,
  CleanOptions,
  CountFilter,
  JobMap,
  JobView,
  ListOptions,
  ListResult,
  NormalizedQueueOptions,
  QueueOptions,
  QueueStats,
  RetryFailureReason,
  RetryResult,
  ScheduleOptions,
  ScheduleResult,
  ScheduleView,
} from "./types.ts";

function scheduleRowToView(row: ScheduleRow): ScheduleView {
  return {
    id: row.id,
    queue: row.queue,
    name: row.name,
    data: decodeJson(row.payload_json),
    everyMs: row.every_ms,
    nextRunAt: row.next_run_at,
    attempts: row.max_attempts,
    priority: row.priority,
    attemptTimeoutMs: row.attempt_timeout_ms,
    backoff: backoffFromColumns(row.backoff_type, row.backoff_delay_ms, row.backoff_max_delay_ms),
    lastJobId: row.last_job_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

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

  #enqueuePayload(name: string, data: unknown, options: AddOptions | undefined, nowMs: number) {
    assertName(name, "job name");
    const opts = normalizeAddOptions(options);
    const payload: EnqueuePayload = {
      nowMs,
      queue: this.name,
      name,
      payloadJson: encodeJson(data, this.options.maxPayloadBytes),
      maxAttempts: opts.attempts,
      delayMs: opts.delayMs,
      priority: opts.priority,
      dedupeKey: opts.dedupeKey,
      attemptTimeoutMs: opts.attemptTimeoutMs,
      backoff: opts.backoff,
    };
    return payload;
  }

  async add<K extends keyof Jobs & string>(
    name: K,
    data: Jobs[K]["input"],
    options?: AddOptions,
  ): Promise<AddResult<K, Jobs[K]["input"]>> {
    const payload = this.#enqueuePayload(name, data, options, this.clock.now());
    const rt = await this.#ensure();
    const result = await rt.client.request<{ row: JobRow; deduplicated: boolean }>(
      "enqueue",
      payload,
    );
    this.emit("enqueued", result.row.id);
    return {
      job: jobRowToView(result.row) as JobView<K, Jobs[K]["input"]>,
      deduplicated: result.deduplicated,
    };
  }

  /**
   * Enqueues up to 1000 jobs in one transaction: either every job is committed or none is.
   * Results are in input order; deduplication applies per item exactly as for add(), including
   * against earlier items in the same batch.
   */
  async addMany(items: readonly AddManyItem<Jobs>[]): Promise<AddResult<keyof Jobs & string>[]> {
    if (!Array.isArray(items)) throw new DurableQValidationError("addMany expects an array");
    if (items.length > MAX_BATCH_SIZE) {
      throw new DurableQValidationError(`addMany accepts at most ${MAX_BATCH_SIZE} jobs`);
    }
    if (this.#closed) throw new DurableQClosedError("queue is closed");
    if (items.length === 0) return [];
    const nowMs = this.clock.now();
    const payloads = items.map((item, i) => {
      if (!item || typeof item !== "object") {
        throw new DurableQValidationError(`addMany item ${i} must be an object`);
      }
      return this.#enqueuePayload(item.name, item.data, item.options, nowMs);
    });
    const rt = await this.#ensure();
    const results = await rt.client.request<{ row: JobRow; deduplicated: boolean }[]>(
      "enqueueMany",
      { items: payloads },
    );
    for (const r of results) this.emit("enqueued", r.row.id);
    return results.map((r) => ({
      job: jobRowToView(r.row) as JobView<keyof Jobs & string>,
      deduplicated: r.deduplicated,
    }));
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

  /** Read-only snapshot of job counts for this queue. */
  async stats(): Promise<QueueStats> {
    const rt = await this.#ensure();
    return rt.client.request<QueueStats>("stats", { queue: this.name, nowMs: this.clock.now() });
  }

  /** Lists jobs newest-updated first, with cursor pagination. */
  async list(options: ListOptions = {}): Promise<ListResult> {
    if (!options || typeof options !== "object") {
      throw new DurableQValidationError("list options must be an object");
    }
    const { status, name, cursor } = options;
    const limit = options.limit ?? DEFAULT_LIST_LIMIT;
    if (status !== undefined && !JOB_STATUSES.includes(status)) {
      throw new DurableQValidationError(`unknown job status: ${String(status)}`);
    }
    if (name !== undefined) assertName(name, "job name");
    assertSafeInteger(limit, "limit", 1, MAX_LIST_LIMIT);
    let parsed: { updatedAt: number; rowid: number } | null = null;
    if (cursor !== undefined) {
      const m = typeof cursor === "string" ? /^(\d{1,15})\.(\d{1,15})$/.exec(cursor) : null;
      if (!m) throw new DurableQValidationError("invalid list cursor");
      parsed = { updatedAt: Number(m[1]), rowid: Number(m[2]) };
    }
    const rt = await this.#ensure();
    const out = await rt.client.request<{
      rows: JobRow[];
      next: { updatedAt: number; rowid: number } | null;
    }>("list", { queue: this.name, status, name, limit, cursor: parsed });
    return {
      jobs: out.rows.map(jobRowToView),
      nextCursor: out.next ? `${out.next.updatedAt}.${out.next.rowid}` : null,
    };
  }

  /** Persistently pauses claiming for this queue in every process. Running jobs continue. */
  async pause(): Promise<void> {
    const rt = await this.#ensure();
    await rt.client.request("setPaused", {
      nowMs: this.clock.now(),
      queue: this.name,
      paused: true,
    });
  }

  async resume(): Promise<void> {
    const rt = await this.#ensure();
    await rt.client.request("setPaused", {
      nowMs: this.clock.now(),
      queue: this.name,
      paused: false,
    });
  }

  async isPaused(): Promise<boolean> {
    const rt = await this.#ensure();
    return rt.client.request<boolean>("isPaused", { queue: this.name });
  }

  /**
   * Creates or updates a durable fixed-interval schedule. The first occurrence is due
   * everyMs after creation. Re-declaring an identical schedule is a no-op ("unchanged");
   * a different definition for the same id replaces it ("updated").
   */
  async schedule<K extends keyof Jobs & string>(
    name: K,
    options: ScheduleOptions<Jobs[K]["input"]>,
  ): Promise<ScheduleResult<K, Jobs[K]["input"]>> {
    assertName(name, "job name");
    if (!options || typeof options !== "object") {
      throw new DurableQValidationError("schedule options are required");
    }
    assertName(options.id, "schedule id");
    assertSafeInteger(
      options.everyMs,
      "everyMs",
      MIN_SCHEDULE_INTERVAL_MS,
      MAX_SCHEDULE_INTERVAL_MS,
    );
    const opts = normalizeAddOptions({
      ...(options.attempts !== undefined ? { attempts: options.attempts } : {}),
      ...(options.priority !== undefined ? { priority: options.priority } : {}),
      ...(options.attemptTimeoutMs !== undefined
        ? { attemptTimeoutMs: options.attemptTimeoutMs }
        : {}),
      ...(options.backoff !== undefined ? { backoff: options.backoff } : {}),
    });
    const payloadJson = encodeJson(options.data, this.options.maxPayloadBytes);
    const rt = await this.#ensure();
    const out = await rt.client.request<{
      row: ScheduleRow;
      outcome: ScheduleResult["outcome"];
    }>("upsertSchedule", {
      nowMs: this.clock.now(),
      queue: this.name,
      id: options.id,
      name,
      payloadJson,
      everyMs: options.everyMs,
      maxAttempts: opts.attempts,
      priority: opts.priority,
      attemptTimeoutMs: opts.attemptTimeoutMs,
      backoff: opts.backoff,
    });
    return {
      schedule: scheduleRowToView(out.row) as ScheduleView<K, Jobs[K]["input"]>,
      outcome: out.outcome,
    };
  }

  async getSchedule(id: string): Promise<ScheduleView | null> {
    assertName(id, "schedule id");
    const rt = await this.#ensure();
    const row = await rt.client.request<ScheduleRow | null>("getSchedule", {
      queue: this.name,
      id,
    });
    return row ? scheduleRowToView(row) : null;
  }

  async listSchedules(): Promise<ScheduleView[]> {
    const rt = await this.#ensure();
    const rows = await rt.client.request<ScheduleRow[]>("listSchedules", { queue: this.name });
    return rows.map(scheduleRowToView);
  }

  /** Stops future occurrences. Jobs already created by the schedule are left untouched. */
  async removeSchedule(id: string): Promise<{ removed: boolean }> {
    assertName(id, "schedule id");
    const rt = await this.#ensure();
    const removed = await rt.client.request<boolean>("removeSchedule", { queue: this.name, id });
    return { removed };
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
