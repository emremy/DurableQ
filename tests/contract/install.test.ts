import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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

    const dts = readFileSync(join(dir, "node_modules/durableq/dist/queue.d.ts"), "utf8");
    assert.equal(dts.includes("storage("), false, "queue.d.ts must not expose storage()");
    assert.equal(dts.includes("StorageClient"), false);

    const indexDts = readFileSync(join(dir, "node_modules/durableq/dist/index.d.ts"), "utf8");
    assert.equal(indexDts.includes("StorageClient"), false);
    assert.equal(indexDts.includes("storage()"), false);

    const api = spawnSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `
        import { Queue } from 'durableq';
        if (typeof Queue.prototype.storage !== 'undefined') {
          throw new Error('Queue.prototype.storage is public');
        }
        try {
          await import('durableq/storage');
          throw new Error('durableq/storage imported');
        } catch (err) {
          const code = err && typeof err === 'object' && 'code' in err ? String(err.code) : '';
          if (code !== 'ERR_PACKAGE_PATH_NOT_EXPORTED') throw err;
        }
        try {
          await import('durableq/dist/storage/client.js');
          throw new Error('deep storage client imported');
        } catch (err) {
          const code = err && typeof err === 'object' && 'code' in err ? String(err.code) : '';
          if (code !== 'ERR_PACKAGE_PATH_NOT_EXPORTED') throw err;
        }
        console.log('api-locked');
        `,
      ],
      { cwd: dir, encoding: "utf8" },
    );
    assert.equal(api.status, 0, api.stderr + api.stdout);
    assert.match(api.stdout, /api-locked/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
