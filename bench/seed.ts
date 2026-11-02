import { DatabaseSync } from "node:sqlite";

export const PAYLOAD = { to: "user@example.com", subject: "Welcome", body: "x".repeat(450) };

/**
 * Bulk-loads rows straight into SQLite, so every version under test starts from an identical
 * database regardless of which enqueue APIs it has.
 */
export function seed(path: string, count: number, status: string, at: number): void {
  const db = new DatabaseSync(path, { timeout: 5_000 });
  const terminal = `${status}_at`;
  const col = ["completed", "failed", "cancelled"].includes(status) ? `, ${terminal}` : "";
  const val = col ? `, ${at}` : "";
  const lease = status === "active" ? ", lease_owner, lease_expires_at, attempt_count" : "";
  const leaseVal = status === "active" ? `, 'w', ${at + 1e12}, 1` : "";
  db.exec(`
    WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < ${count})
    INSERT INTO durableq_jobs (id, queue, name, payload_json, status, available_at, max_attempts,
      created_at, updated_at${col}${lease})
    SELECT lower(hex(randomblob(16))), 'bench', 'job', '${JSON.stringify(PAYLOAD)}', '${status}',
      ${at}, 3, ${at}, ${at}${val}${leaseVal}
    FROM c;`);
  db.close();
}
