export class DurableQError extends Error {
  readonly code: string;
  override readonly cause?: unknown;

  constructor(message: string, options?: { code?: string; cause?: unknown }) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "DurableQError";
    this.code = options?.code ?? "DURABLEQ";
    this.cause = options?.cause;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class DurableQValidationError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "VALIDATION", cause: options?.cause });
    this.name = "DurableQValidationError";
  }
}

export class DurableQSerializationError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "SERIALIZATION", cause: options?.cause });
    this.name = "DurableQSerializationError";
  }
}

export class DurableQStorageError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown; code?: string }) {
    super(message, { code: options?.code ?? "STORAGE", cause: options?.cause });
    this.name = "DurableQStorageError";
  }
}

export class DurableQBusyError extends DurableQStorageError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "BUSY", cause: options?.cause });
    this.name = "DurableQBusyError";
  }
}

export class DurableQMigrationError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "MIGRATION", cause: options?.cause });
    this.name = "DurableQMigrationError";
  }
}

export class DurableQSchemaVersionError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "SCHEMA_VERSION", cause: options?.cause });
    this.name = "DurableQSchemaVersionError";
  }
}

export class DurableQStorageWorkerError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "STORAGE_WORKER", cause: options?.cause });
    this.name = "DurableQStorageWorkerError";
  }
}

export class DurableQStaleLeaseError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "STALE_LEASE", cause: options?.cause });
    this.name = "DurableQStaleLeaseError";
  }
}

export class DurableQClosedError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "CLOSED", cause: options?.cause });
    this.name = "DurableQClosedError";
  }
}

export class NonRetryableError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "NON_RETRYABLE", cause: options?.cause });
    this.name = "NonRetryableError";
  }
}

export class LeaseExpiredError extends DurableQError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { code: "LEASE_EXPIRED", cause: options?.cause });
    this.name = "LeaseExpiredError";
  }
}

export type SerializedInternalError = {
  name: string;
  message: string;
  stack?: string;
  code: string;
};

export function serializeInternalError(err: unknown): SerializedInternalError {
  if (err instanceof DurableQError) {
    const out: SerializedInternalError = {
      name: err.name,
      message: err.message,
      code: err.code,
    };
    if (err.stack) out.stack = err.stack;
    return out;
  }
  if (err instanceof Error) {
    const out: SerializedInternalError = {
      name: err.name,
      message: err.message,
      code: "STORAGE",
    };
    if (err.stack) out.stack = err.stack;
    return out;
  }
  return { name: "Error", message: String(err), code: "STORAGE" };
}

export function deserializeInternalError(payload: SerializedInternalError): DurableQError {
  const map: Record<string, typeof DurableQError> = {
    DurableQValidationError,
    DurableQSerializationError,
    DurableQStorageError,
    DurableQBusyError,
    DurableQMigrationError,
    DurableQSchemaVersionError,
    DurableQStorageWorkerError,
    DurableQStaleLeaseError,
    DurableQClosedError,
    NonRetryableError,
    LeaseExpiredError,
    DurableQError,
  };
  const Ctor = map[payload.name] ?? DurableQError;
  const err = new Ctor(payload.message);
  if (payload.stack) err.stack = payload.stack;
  return err;
}

export function isSqliteBusy(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const rec = err as { code?: unknown; errcode?: unknown; message?: unknown };
  if (rec.code === "ERR_SQLITE_BUSY" || rec.code === 5 || rec.errcode === 5) return true;
  const msg = String(rec.message ?? "");
  return msg.includes("SQLITE_BUSY") || msg.includes("database is locked");
}
