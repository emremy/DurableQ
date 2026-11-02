export const SCHEMA_VERSION = 2;

export const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
export const DEFAULT_MAX_PAYLOAD_BYTES = 1024 * 1024;
export const DEFAULT_MAX_RESULT_BYTES = 1024 * 1024;
export const MAX_STORED_ERROR_BYTES = 64 * 1024;
export const DEFAULT_MAX_ATTEMPTS = 3;
export const DEFAULT_PRIORITY = 0;
export const DEFAULT_CONCURRENCY = 1;
export const DEFAULT_POLL_INTERVAL_MS = 100;
export const DEFAULT_LEASE_DURATION_MS = 30_000;
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 10_000;
export const DEFAULT_ATTEMPT_TIMEOUT_MS = 300_000;
export const MAX_HANDLERS_PER_WORKER = 64;
export const MAX_NAME_LENGTH = 128;
export const MAX_DEDUPE_KEY_LENGTH = 256;
export const MIN_PRIORITY = -1_000_000;
export const MAX_PRIORITY = 1_000_000;
export const MIN_CONCURRENCY = 1;
export const MAX_CONCURRENCY = 256;
export const RECOVERY_BATCH_SIZE = 50;
export const LEASE_TICK_MS = 50;
export const MAX_BATCH_SIZE = 1_000;
export const MIN_SCHEDULE_INTERVAL_MS = 1_000;
export const MAX_SCHEDULE_INTERVAL_MS = 366 * 24 * 60 * 60 * 1000;
export const SCHEDULE_BATCH_SIZE = 50;
export const DEFAULT_LIST_LIMIT = 50;
export const MAX_LIST_LIMIT = 1_000;
export const RETENTION_INTERVAL_MS = 60_000;
export const RETENTION_BATCH_SIZE = 500;
export const RETENTION_MAX_BATCHES = 20;
export const SETTLE_BUSY_ATTEMPTS = 5;
export const SETTLE_BUSY_BASE_DELAY_MS = 25;
export const SETTLE_BUSY_MAX_DELAY_MS = 1_000;

export const JOB_STATUSES = ["queued", "active", "completed", "failed", "cancelled"] as const;

export const BACKOFF_TYPES = ["none", "fixed", "exponential"] as const;
