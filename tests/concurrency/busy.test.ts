import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("C-004 busy timeout surfaces classified error", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 });
    const locker = new DatabaseSync(tmp.path, { timeout: 1 });
    locker.exec("BEGIN IMMEDIATE");
    const start = Date.now();
    await assert.rejects(() => q.add("n", { v: 2 }));
    locker.exec("COMMIT");
    locker.close();
    assert.ok(Date.now() - start >= 40);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});
