// Version 1 baseline (v0.1.0). Never edit: schema changes go in MIGRATIONS.
export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS _durableq_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT;

CREATE TABLE durableq_jobs (
  id                  TEXT PRIMARY KEY,
  queue               TEXT NOT NULL,
  name                TEXT NOT NULL,
  payload_json        TEXT NOT NULL,

  status              TEXT NOT NULL
    CHECK (status IN ('queued', 'active', 'completed', 'failed', 'cancelled')),

  priority            INTEGER NOT NULL DEFAULT 0,
  available_at        INTEGER NOT NULL,

  attempt_count       INTEGER NOT NULL DEFAULT 0
    CHECK (attempt_count >= 0),
  max_attempts        INTEGER NOT NULL
    CHECK (max_attempts >= 1),

  backoff_type        TEXT NOT NULL DEFAULT 'none'
    CHECK (backoff_type IN ('none', 'fixed', 'exponential')),
  backoff_delay_ms    INTEGER NOT NULL DEFAULT 0
    CHECK (backoff_delay_ms >= 0),
  backoff_max_delay_ms INTEGER,

  attempt_timeout_ms  INTEGER,

  lease_owner         TEXT,
  lease_generation    INTEGER NOT NULL DEFAULT 0
    CHECK (lease_generation >= 0),
  lease_expires_at    INTEGER,

  dedupe_key          TEXT,

  result_json         TEXT,
  error_json          TEXT,

  created_at          INTEGER NOT NULL,
  updated_at          INTEGER NOT NULL,
  started_at          INTEGER,
  completed_at        INTEGER,
  failed_at           INTEGER,
  cancelled_at        INTEGER,

  CHECK (attempt_count <= max_attempts),
  CHECK (
    (status = 'active' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (status <> 'active' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  )
) STRICT;

CREATE TABLE durableq_attempts (
  job_id            TEXT NOT NULL,
  attempt            INTEGER NOT NULL,
  lease_generation  INTEGER NOT NULL,
  worker_id          TEXT NOT NULL,
  started_at         INTEGER NOT NULL,
  finished_at        INTEGER,
  outcome            TEXT
    CHECK (outcome IS NULL OR outcome IN (
      'completed',
      'failed_retrying',
      'failed_terminal',
      'lease_expired',
      'non_retryable'
    )),
  error_json         TEXT,

  PRIMARY KEY (job_id, lease_generation),
  FOREIGN KEY (job_id) REFERENCES durableq_jobs(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX durableq_jobs_claim_idx
ON durableq_jobs (
  queue,
  status,
  available_at,
  priority DESC,
  created_at,
  id
);

CREATE INDEX durableq_jobs_lease_idx
ON durableq_jobs (
  status,
  lease_expires_at
);

CREATE UNIQUE INDEX durableq_jobs_dedupe_idx
ON durableq_jobs (queue, name, dedupe_key)
WHERE dedupe_key IS NOT NULL;
`;

// MIGRATIONS[i] upgrades schema version i + 1 to i + 2. Append only.
export const MIGRATIONS: readonly string[] = [
  `
ALTER TABLE durableq_jobs ADD COLUMN progress INTEGER NOT NULL DEFAULT 0
  CHECK (progress BETWEEN 0 AND 100);

-- The v1 claim index led with available_at, so every claim sorted all due jobs inside its
-- write transaction (lock hold time grew with the backlog). This order matches the claim's
-- ORDER BY, so a claim reads the first eligible entry instead.
DROP INDEX durableq_jobs_claim_idx;
CREATE INDEX durableq_jobs_claim_idx
ON durableq_jobs (queue, status, priority DESC, available_at, created_at, id);

CREATE INDEX durableq_jobs_status_updated_idx
ON durableq_jobs (queue, status, updated_at);

CREATE INDEX durableq_jobs_name_status_updated_idx
ON durableq_jobs (queue, name, status, updated_at);

CREATE TABLE durableq_queues (
  queue              TEXT PRIMARY KEY,
  paused             INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
  retention_next_at  INTEGER NOT NULL DEFAULT 0,
  updated_at         INTEGER NOT NULL
) STRICT;

CREATE TABLE durableq_schedules (
  queue                TEXT NOT NULL,
  id                   TEXT NOT NULL,
  name                 TEXT NOT NULL,
  payload_json         TEXT NOT NULL,
  every_ms             INTEGER NOT NULL CHECK (every_ms >= 1),
  next_run_at          INTEGER NOT NULL,
  max_attempts         INTEGER NOT NULL CHECK (max_attempts >= 1),
  priority             INTEGER NOT NULL,
  attempt_timeout_ms   INTEGER,
  backoff_type         TEXT NOT NULL CHECK (backoff_type IN ('none', 'fixed', 'exponential')),
  backoff_delay_ms     INTEGER NOT NULL CHECK (backoff_delay_ms >= 0),
  backoff_max_delay_ms INTEGER,
  last_job_id          TEXT,
  created_at           INTEGER NOT NULL,
  updated_at           INTEGER NOT NULL,
  PRIMARY KEY (queue, id)
) STRICT;

CREATE INDEX durableq_schedules_due_idx
ON durableq_schedules (queue, next_run_at);
`,
];
