import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");

test("public exports do not include test hooks", () => {
  const src = readFileSync(join(root, "src/index.ts"), "utf8");
  assert.equal(src.includes("test.configureFault"), false);
  assert.equal(src.includes("fault-hooks"), false);
});

test("npm pack dry-run succeeds", () => {
  const pack = spawnSync("npm", ["pack", "--dry-run", "--ignore-scripts"], {
    cwd: root,
    encoding: "utf8",
  });
  assert.equal(pack.status, 0, pack.stderr);
});
