import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");

test("tarball installs in a fresh project", () => {
  const build = spawnSync("npm", ["run", "build"], { cwd: root, encoding: "utf8" });
  assert.equal(build.status, 0, build.stderr + build.stdout);
  const pack = spawnSync("npm", ["pack", "--ignore-scripts"], { cwd: root, encoding: "utf8" });
  assert.equal(pack.status, 0, pack.stderr);
  const tarball = pack.stdout.trim().split("\n").at(-1);
  assert.ok(tarball && tarball.endsWith(".tgz"));
  const dir = mkdtempSync(join(tmpdir(), "durableq-consumer-"));
  try {
    writeFileSync(
      join(dir, "package.json"),
      JSON.stringify({ name: "consumer", type: "module", private: true }),
    );
    const install = spawnSync("npm", ["install", join(root, tarball)], {
      cwd: dir,
      encoding: "utf8",
    });
    assert.equal(install.status, 0, install.stderr);
    const run = spawnSync(
      process.execPath,
      ["-e", "import { Queue } from 'durableq'; console.log(typeof Queue);"],
      { cwd: dir, encoding: "utf8" },
    );
    assert.equal(run.status, 0, run.stderr);
    assert.match(run.stdout, /function/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
