import type { Queue } from "../../src/queue.ts";
import type { FaultSpec, StorageOp } from "../../src/storage/protocol.ts";

export async function configureFault(queue: Queue, spec: FaultSpec): Promise<void> {
  const client = await queue.storage();
  await client.request("test.configureFault", spec);
}

export async function setStorageNow(queue: Queue, nowMs: number | null): Promise<number> {
  const client = await queue.storage();
  const out = await client.request<{ nowMs: number }>("test.setNow", { nowMs });
  return out.nowMs;
}

export async function tickLeases(queue: Queue): Promise<number> {
  const client = await queue.storage();
  const out = await client.request<{ recovered: number }>("test.tickLeases", {});
  return out.recovered;
}

export async function rawRequest<T>(queue: Queue, op: StorageOp, payload: unknown): Promise<T> {
  const client = await queue.storage();
  return client.request<T>(op, payload);
}
