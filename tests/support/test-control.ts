import { ensureTestWorker } from "./install-test-worker.ts";
import type { Queue } from "../../src/queue.ts";
import { acquireRuntime } from "../../src/storage/runtime-registry.ts";
import type { StorageClient } from "../../src/storage/client.ts";
import type { FaultSpec } from "./fault-spec.ts";

export type { FaultSpec };

async function withClient<T>(queue: Queue, fn: (client: StorageClient) => Promise<T>): Promise<T> {
  await queue.count();
  const rt = await acquireRuntime({
    path: queue.options.path,
    busyTimeoutMs: queue.busyTimeoutMs,
  });
  try {
    return await fn(rt.client);
  } finally {
    await rt.unref();
  }
}

export async function configureFault(queue: Queue, spec: FaultSpec): Promise<void> {
  ensureTestWorker();
  await withClient(queue, (client) => client.request("test.configureFault", spec));
}

export async function setStorageNow(queue: Queue, nowMs: number | null): Promise<number> {
  ensureTestWorker();
  const out = await withClient(queue, (client) =>
    client.request<{ nowMs: number }>("test.setNow", { nowMs }),
  );
  return out.nowMs;
}

export async function tickLeases(queue: Queue): Promise<number> {
  ensureTestWorker();
  const out = await withClient(queue, (client) =>
    client.request<{ recovered: number }>("test.tickLeases", {}),
  );
  return out.recovered;
}

export async function rawRequest<T>(queue: Queue, op: string, payload: unknown): Promise<T> {
  return withClient(queue, (client) => client.request<T>(op, payload));
}
