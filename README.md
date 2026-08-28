# DurableQ

**Durable background jobs for Node.js, powered by built-in SQLite. No Redis. No external services. Zero runtime dependencies.**

DurableQ is a deliberately small, single-machine job queue for Node.js. It uses the built-in `node:sqlite` module, keeps synchronous SQLite work off the application event loop with a dedicated storage worker thread, and provides crash-safe, at-least-once background job execution without requiring Redis, PostgreSQL, or another queue service.

> Requires **Node.js >= 24.15.0**.

---

## Why DurableQ?

Most production queue systems are designed for distributed infrastructure.

DurableQ is intentionally narrower.

Use it when you have:

- one server, VPS, desktop application, or single host;
- durable background work that must survive process restarts;
- one or more Node.js processes on that host;
- no desire to operate Redis or another external service;
- a workload where correctness matters more than extreme queue throughput.

DurableQ is built around a few strict constraints:

- **Node.js only**
- **built-in `node:sqlite` only**
- **single-machine only**
- **zero runtime npm dependencies**
- **dedicated storage worker thread**
- **at-least-once execution**
- **lease fencing for stale workers**
- **SQLite WAL + `synchronous=FULL` by default**

It is not intended to become a multi-database or distributed queue abstraction.

---

## Install

```bash
npm install durableq
```

Use a **dedicated database file** for DurableQ.

```ts
const queue = new Queue("app", {
  path: "./data/durableq.db",
});
```

Do not point DurableQ at your application's primary SQLite database.

---

## Quick start

```ts
import { Queue, Worker } from "durableq";

type Jobs = {
  "email.send": {
    input: {
      to: string;
      subject: string;
    };
    output: {
      messageId: string;
    };
  };
};

const queue = new Queue<Jobs>("app", {
  path: "./data/durableq.db",
});

await queue.add("email.send", {
  to: "foo@example.com",
  subject: "Welcome",
});

const worker = new Worker(queue, {
  "email.send": async (job, ctx) => {
    const response = await sendEmail(job.data, {
      signal: ctx.signal,
      idempotencyKey: job.id,
    });

    return {
      messageId: response.id,
    };
  },
});

await worker.start();
```

Graceful shutdown:

```ts
const shutdown = async () => {
  await worker.close({
    timeoutMs: 30_000,
  });

  await queue.close();
};

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
```

---

## Typed jobs

DurableQ can infer the input and output type for every named job.

```ts
import {
  Queue,
  Worker,
  NonRetryableError,
} from "durableq";

type Jobs = {
  "email.send": {
    input: {
      to: string;
      subject: string;
    };
    output: {
      messageId: string;
    };
  };

  "report.generate": {
    input: {
      userId: number;
    };
    output: {
      path: string;
    };
  };
};

const queue = new Queue<Jobs>("app", {
  path: "./data/durableq.db",
});

const result = await queue.add(
  "email.send",
  {
    to: "foo@example.com",
    subject: "Welcome",
  },
  {
    attempts: 5,
    delayMs: 5_000,
    priority: 10,
    dedupeKey: "welcome:user:123",
    attemptTimeoutMs: 60_000,
    backoff: {
      type: "exponential",
      delayMs: 1_000,
      maxDelayMs: 60_000,
    },
  },
);

console.log(result.job.id);
console.log(result.deduplicated);

const worker = new Worker(
  queue,
  {
    "email.send": async (job, ctx) => {
      const response = await sendEmail(job.data, {
        signal: ctx.signal,
        idempotencyKey: job.id,
      });

      return {
        messageId: response.id,
      };
    },

    "report.generate": async (job, ctx) => {
      try {
        return await generateReport(
          job.data.userId,
          ctx.signal,
        );
      } catch (error) {
        if (isPermanentReportError(error)) {
          throw new NonRetryableError(
            "The report cannot be generated.",
          );
        }

        throw error;
      }
    },
  },
  {
    concurrency: 5,
    pollIntervalMs: 100,
    leaseDurationMs: 30_000,
    heartbeatIntervalMs: 10_000,
    defaultAttemptTimeoutMs: 300_000,
  },
);

await worker.start();
```

TypeScript rejects payloads that do not match the selected job name.

```ts
await queue.add("email.send", {
  userId: 123,
  // Type error
});
```

---

## Delivery semantics

DurableQ provides **at-least-once execution**.

It does **not** provide exactly-once execution.

A handler can run more than once when a process dies after performing an external side effect but before DurableQ commits the completion acknowledgement.

For example:

```text
handler sends email
        │
        ▼
email provider accepts it
        │
        ▼
process is killed
        │
        ▼
completion is never committed
        │
        ▼
lease expires
        │
        ▼
job may run again
```

If duplicate business effects matter, use idempotency at the destination.

```ts
await paymentProvider.capture({
  amount: 5000,
  idempotencyKey: job.id,
});
```

DurableQ's queue semantics and your application's business idempotency solve different problems.

---

## Enqueue durability

When this resolves:

```ts
await queue.add("email.send", data);
```

the insert transaction has been committed.

There is still an unavoidable ambiguity window:

```text
SQLite COMMIT succeeds
        │
        ▼
process dies before Promise result is observed
```

The caller cannot know whether the enqueue succeeded.

Use the same `dedupeKey` when retrying an indeterminate enqueue.

```ts
await queue.add(
  "email.send",
  data,
  {
    dedupeKey: "welcome:user:123",
  },
);
```

---

## Job lifecycle

A normal job moves through the following states:

```text
                   claim
        ┌──────────────────────────┐
        │                          ▼
    ┌────────┐                ┌────────┐
    │ queued │                │ active │
    └───┬────┘                └───┬────┘
        │                          │
        │ cancel                   ├──────── success ───────► completed
        ▼                          │
   cancelled                       ├──────── retryable failure
                                   │                │
                                   │                ▼
                                   │             queued
                                   │
                                   └──────── attempts exhausted
                                                    │
                                                    ▼
                                                  failed
```

An active job can also lose its lease:

```text
active
  │
  │ lease expires
  ▼
recovery
  │
  ├── attempts remain ──► queued
  │
  └── exhausted ────────► failed
```

Delayed jobs do not use a separate `delayed` state. They remain `queued` with an `available_at` timestamp in the future.

---

## Leases and fencing

Workers do not permanently own jobs.

An active job is protected by a time-limited lease containing:

- the worker identity;
- a lease generation;
- an expiry timestamp.

Every lease-authoritative mutation must still match the current owner and generation and must occur before the lease expires.

Conceptually:

```sql
WHERE id = ?
  AND status = 'active'
  AND lease_owner = ?
  AND lease_generation = ?
  AND lease_expires_at > ?
```

Once:

```text
lease_expires_at <= now
```

the previous worker has **zero settlement authority**, even if recovery has not yet incremented the generation.

This prevents a stale worker from completing, failing, retrying, or heartbeating a job after losing authority.

Lease heartbeats are performed from DurableQ's storage worker thread rather than relying solely on the application's main event loop.

The worker configuration must satisfy:

```text
leaseDurationMs > heartbeatIntervalMs + busyTimeoutMs
```

The defaults include safety margin, but extreme OS stalls or storage contention can still cause a lease to be lost. Handlers must therefore remain safe under at-least-once execution.

---

## Retries and backoff

A job can specify its maximum number of attempts.

```ts
await queue.add(
  "email.send",
  data,
  {
    attempts: 5,
  },
);
```

Use fixed backoff:

```ts
await queue.add(
  "email.send",
  data,
  {
    attempts: 5,
    backoff: {
      type: "fixed",
      delayMs: 5_000,
    },
  },
);
```

Or exponential backoff:

```ts
await queue.add(
  "email.send",
  data,
  {
    attempts: 5,
    backoff: {
      type: "exponential",
      delayMs: 1_000,
      maxDelayMs: 60_000,
    },
  },
);
```

Throw `NonRetryableError` for permanent failures:

```ts
throw new NonRetryableError(
  "The destination rejected this request permanently.",
);
```

This skips the remaining automatic retries.

`attempt_count` never decreases, including after a manual retry.

---

## Deduplication

A `dedupeKey` is unique per:

```text
queue + job name + dedupe key
```

Example:

```ts
const first = await queue.add(
  "email.send",
  data,
  {
    dedupeKey: "welcome:user:123",
  },
);

const second = await queue.add(
  "email.send",
  data,
  {
    dedupeKey: "welcome:user:123",
  },
);

console.log(first.deduplicated);
// false

console.log(second.deduplicated);
// true
```

The first insert wins. A later duplicate does not overwrite the existing payload.

The key remains reserved for the **lifetime of the job row**, including when the job is:

- queued;
- active;
- completed;
- failed;
- cancelled.

Reuse becomes possible only after the owning row is removed with `clean()`.

This behavior is intentional because it protects retries after an indeterminate enqueue response.

---

## Priority and ordering

Among jobs that are currently eligible for execution, DurableQ orders claims by:

1. priority descending;
2. `available_at` ascending;
3. `created_at` ascending;
4. `id` ascending.

Example:

```ts
await queue.add(
  "email.send",
  data,
  {
    priority: 100,
  },
);
```

Strict priority can starve lower-priority jobs. DurableQ does not currently implement priority aging or fairness scheduling.

---

## Timeouts and AbortSignal

Each handler receives an `AbortSignal`.

```ts
"report.generate": async (job, ctx) => {
  return generateReport(
    job.data.userId,
    ctx.signal,
  );
}
```

A per-job timeout can be configured:

```ts
await queue.add(
  "report.generate",
  {
    userId: 123,
  },
  {
    attemptTimeoutMs: 60_000,
  },
);
```

Or a worker-level default:

```ts
new Worker(queue, handlers, {
  defaultAttemptTimeoutMs: 300_000,
});
```

Handlers are ordinary JavaScript. DurableQ cannot forcefully terminate an arbitrary handler.

If a handler ignores `ctx.signal`:

- DurableQ can stop renewing its authority;
- the current lease can expire;
- another worker can later retry the job;
- the stale handler is not allowed to settle the job after it loses lease authority.

This can cause overlapping execution, which is another reason handlers must tolerate at-least-once behavior.

---

## Graceful shutdown

```ts
await worker.close({
  timeoutMs: 30_000,
});
```

Shutdown follows this model:

1. stop claiming new jobs;
2. continue managing currently active work during the grace period;
3. signal handlers when shutdown requires cancellation;
4. wait up to `timeoutMs`;
5. return after the grace period even if user code ignores `AbortSignal`;
6. never fake-complete unfinished work.

A handler that eventually returns after it has lost lease authority cannot complete the job.

After workers are closed:

```ts
await queue.close();
```

---

## Crash recovery

Committed queued jobs survive process termination, including `SIGKILL`.

Active jobs use leases. If a process dies:

```text
worker claims job
       │
       ▼
job becomes active
       │
       ▼
process is killed
       │
       ▼
heartbeats stop
       │
       ▼
lease expires
       │
       ▼
job becomes eligible for recovery
```

If attempts remain, the job can return to `queued`.

If attempts are exhausted, it becomes `failed`.

DurableQ's automated crash suite tests **process-level** failure behavior. It does not attempt to emulate every possible kernel, filesystem, storage-controller, or hardware failure.

---

## SQLite durability

DurableQ uses SQLite in WAL mode with:

```text
journal_mode = WAL
synchronous = FULL
foreign_keys = ON
```

The configured values are read back during initialization rather than assumed.

SQLite documents `synchronous=FULL` with WAL as durable when the operating system, filesystem, and storage hardware correctly honor synchronization requests.

That does not mean DurableQ can promise:

> "a committed job can never be lost under any power or hardware failure."

No such claim is made.

---

## Dedicated storage worker thread

The built-in `node:sqlite` `DatabaseSync` API is synchronous.

DurableQ therefore confines queue database access to a dedicated Node.js worker thread.

Conceptually:

```text
Application main thread
        │
        ├── Queue
        ├── Worker
        │
        ▼
   StorageClient
        │
        │ typed RPC / MessagePort
        ▼
Storage worker thread
        │
        ▼
   DatabaseSync
        │
        ▼
 durableq.db
```

DurableQ performs its queue database operations from a dedicated
storage worker thread rather than calling `DatabaseSync` from the
application's main thread.

It does not make CPU-bound user handlers non-blocking. CPU-heavy handlers should use your own worker threads or processes.

---

## Multiple processes

Multiple Node.js processes on the **same machine** may share the same DurableQ database file.

DurableQ uses SQLite transactions, job leases, and generation fencing to coordinate ownership.

Example:

```text
Host
├── api process
├── worker process A
├── worker process B
└── ./data/durableq.db
```

This is supported.

This is not:

```text
Host A ─┐
        ├── network filesystem ── durableq.db
Host B ─┘
```

Do not use DurableQ across multiple hosts.

---

## Database files

SQLite WAL mode can create sidecar files next to the main database:

```text
durableq.db
durableq.db-wal
durableq.db-shm
```

Treat the **database directory** as application state.

Do not manually move, copy, replace, or manipulate individual SQLite files while DurableQ is running.

For backups, use a SQLite-aware backup procedure rather than copying only the main `.db` file while the queue is active.

---

## Containers

If DurableQ runs inside a container, the database must live on persistent **local** storage if jobs need to survive container replacement.

An ephemeral container filesystem is not durable across container recreation.

At the same time, do not solve that by placing the database on NFS, EFS, or another network filesystem. DurableQ is designed for a local SQLite database on one host.

---

## Queue operations

### `new Queue(name, options)`

Creates or opens a queue.

```ts
const queue = new Queue<Jobs>("app", {
  path: "./data/durableq.db",
});
```

### `queue.add(name, data, options?)`

Adds a job.

```ts
const result = await queue.add(
  "email.send",
  {
    to: "foo@example.com",
    subject: "Welcome",
  },
);
```

The result contains the job and whether the request was deduplicated.

```ts
result.job;
result.deduplicated;
```

### `queue.get(id)`

Reads a job by ID.

```ts
const job = await queue.get(jobId);
```

### `queue.count(...)`

Counts jobs according to the supported queue filters.

```ts
const count = await queue.count();
```

### `queue.cancel(id)`

Cancels a `queued` job.

```ts
const result = await queue.cancel(jobId);
```

An active job is not interrupted:

```ts
{
  cancelled: false,
  reason: "active"
}
```

### `queue.retry(id)`

Manually retries a `failed` job.

```ts
await queue.retry(jobId);
```

Manual retry does not reset `attempt_count` and does not release the existing dedupe key.

### `queue.clean(options)`

Deletes retained jobs matching the requested status/time boundary.

```ts
await queue.clean({
  status: "completed",
  before: Date.now() - 7 * 24 * 60 * 60 * 1000,
});
```

Removing a job also releases its dedupe key.

### `queue.close()`

Releases the queue's storage resources.

```ts
await queue.close();
```

---

## Worker API

A `Worker` instance is one-shot. After `worker.close()` completes,
create a new `Worker` instance if processing needs to be started again.

### `new Worker(queue, handlers, options?)`

```ts
const worker = new Worker(
  queue,
  {
    "email.send": async (job, ctx) => {
      // ...
    },
  },
  {
    concurrency: 5,
    pollIntervalMs: 100,
    leaseDurationMs: 30_000,
    heartbeatIntervalMs: 10_000,
  },
);
```

A worker only claims job names for which it has a handler.

### `worker.start()`

Starts processing jobs.

```ts
await worker.start();
```

### `worker.close(options?)`

Stops claiming new jobs and begins graceful shutdown.

```ts
await worker.close({
  timeoutMs: 30_000,
});
```

---

## Worker events

Worker events are operational signals, not part of DurableQ's durability protocol.

Depending on the public API version, a worker can emit lifecycle events such as:

```text
active
completed
retrying
failed
leaseLost
error
```

Do not use an in-process event listener as the only durable record of a business operation.

The SQLite job state is authoritative.

---

## Retention

Completed, failed, and cancelled jobs remain in the database until explicitly cleaned.

This is intentional.

It provides:

- inspection after execution;
- durable result/error history;
- persistent deduplication;
- predictable retention behavior.

Applications should define an explicit retention policy.

Example:

```ts
await queue.clean({
  status: "completed",
  before: Date.now() - 30 * 24 * 60 * 60 * 1000,
});
```

---

## Payload and result constraints

DurableQ persists job data as JSON.

Payloads and handler results must therefore be JSON-compatible.

Do not enqueue values such as:

- `undefined`;
- `bigint`;
- functions;
- symbols;
- cyclic objects;
- `NaN`;
- `Infinity`;
- `Date`;
- `Map`;
- `Set`;
- class instances;
- buffers as arbitrary object values.

Convert application-specific values into explicit JSON representations first.

Example:

```ts
await queue.add("report.generate", {
  createdAt: new Date().toISOString(),
});
```

DurableQ also applies storage limits to serialized payloads, results, and errors rather than allowing unbounded rows.

---

## CPU-bound handlers

DurableQ moves its own synchronous SQLite work away from the application event loop.

It cannot make this code non-blocking:

```ts
"report.generate": async () => {
  while (true) {
    // CPU-bound infinite loop
  }
}
```

For heavy CPU work, use your own worker threads or child processes.

DurableQ is a durable job coordinator, not a JavaScript sandbox.

---

## Security

DurableQ:

- performs no telemetry;
- makes no outbound network requests;
- uses bound SQL parameters for user-controlled values;
- disables SQLite extension loading;
- validates persisted data before storage.

See [SECURITY.md](SECURITY.md) for the security policy and vulnerability reporting process.

---

## Do not use DurableQ if

DurableQ is the wrong tool when:

- workers need to share one queue across multiple hosts;
- the database must live on NFS, EFS, SMB, or another network filesystem;
- you require exactly-once handler execution;
- you need distributed rate limiting;
- you need workflow/DAG orchestration;
- you need parent/child jobs;
- you need globally distributed workers;
- your queue write volume exceeds what a single SQLite writer can reasonably sustain;
- you need Redis/PostgreSQL/MySQL-backed queue storage;
- you expect the queue itself to terminate uncooperative JavaScript handlers.

Use a distributed queue or workflow system for those workloads.

---

## Scope of v0.1

DurableQ v0.1 intentionally focuses on:

- named jobs;
- typed inputs and outputs;
- delayed execution;
- priority;
- concurrency;
- retries;
- fixed and exponential backoff;
- attempt timeouts;
- durable deduplication;
- leases;
- lease generation fencing;
- crash recovery;
- graceful shutdown;
- job retention and cleanup;
- multiple Node.js processes on one host.

Out of scope for v0.1:

- cron scheduling;
- dashboards;
- Redis;
- PostgreSQL;
- MySQL;
- storage adapters;
- distributed multi-host execution;
- workflows/DAGs;
- parent/child jobs;
- progress tracking;
- plugins;
- OpenTelemetry integration;
- exactly-once execution.

---

## Reliability philosophy

DurableQ favors correctness and explicit failure semantics over hiding edge cases.

The implementation and test suite are designed around failures such as:

- process termination during enqueue;
- commit-before-response ambiguity;
- worker termination during execution;
- lease expiry;
- stale worker settlement;
- concurrent claims;
- concurrent deduplicated enqueue;
- SQLite lock contention;
- storage worker failure;
- migration races;
- graceful-shutdown timeout;
- handler overlap after lease loss.

A green happy-path test suite is not considered sufficient evidence for a durable queue.

---

## Node.js support

Minimum supported version:

```text
Node.js >= 24.15.0
```

DurableQ relies on the built-in `node:sqlite` implementation available in supported Node.js releases.

The package is ESM-only.

```ts
import { Queue, Worker } from "durableq";
```

---

## License

MIT
