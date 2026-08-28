import { closeSync, mkdirSync, openSync, realpathSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { StorageClient, spawnStorageWorker } from "./client.ts";

export type RuntimeHandle = {
  client: StorageClient;
  path: string;
  busyTimeoutMs: number;
  ref(): void;
  unref(): Promise<void>;
};

type Entry = {
  client: StorageClient;
  path: string;
  busyTimeoutMs: number;
  refs: number;
  ready: Promise<void>;
};

const registry = new Map<string, Entry>();

export function canonicalPath(filePath: string): string {
  const abs = resolve(filePath);
  mkdirSync(dirname(abs), { recursive: true });
  const fd = openSync(abs, "a");
  closeSync(fd);
  return realpathSync(abs);
}

export async function acquireRuntime(opts: {
  path: string;
  busyTimeoutMs: number;
  enableTestHooks?: boolean;
}): Promise<RuntimeHandle> {
  const key = canonicalPath(opts.path);
  const existing = registry.get(key);
  if (existing) {
    existing.refs += 1;
    try {
      await existing.ready;
    } catch (err) {
      existing.refs -= 1;
      if (existing.refs <= 0) {
        registry.delete(key);
        await existing.client.close().catch(() => undefined);
      }
      throw err;
    }
    return handleFrom(existing, key);
  }

  const worker = spawnStorageWorker();
  const client = new StorageClient(worker);
  const created: Entry = {
    client,
    path: key,
    busyTimeoutMs: opts.busyTimeoutMs,
    refs: 1,
    ready: client
      .request("init", {
        path: key,
        busyTimeoutMs: opts.busyTimeoutMs,
        enableTestHooks:
          opts.enableTestHooks === true || process.env.DURABLEQ_ENABLE_TEST_HOOKS === "1",
      })
      .then(() => undefined),
  };
  registry.set(key, created);
  try {
    await created.ready;
  } catch (err) {
    created.refs -= 1;
    if (created.refs <= 0) {
      registry.delete(key);
      await created.client.close().catch(() => undefined);
    }
    throw err;
  }
  return handleFrom(created, key);
}

function handleFrom(entry: Entry, key: string): RuntimeHandle {
  return {
    client: entry.client,
    path: entry.path,
    busyTimeoutMs: entry.busyTimeoutMs,
    ref() {
      entry.refs += 1;
    },
    async unref() {
      entry.refs -= 1;
      if (entry.refs <= 0) {
        registry.delete(key);
        await entry.client.close();
      }
    },
  };
}

export function registrySize(): number {
  return registry.size;
}
