import { parentPort } from "node:worker_threads";
import {
  DurableQStorageError,
  DurableQStorageWorkerError,
  serializeInternalError,
} from "../errors.ts";
import { createStorageSession } from "./session.ts";
import type { StorageFailure, StorageResponse, StorageSuccess } from "./protocol.ts";
import { isStorageRequest } from "./protocol.ts";

if (!parentPort) {
  throw new DurableQStorageWorkerError("storage worker started without parentPort");
}

const port = parentPort;
const session = createStorageSession(
  (event, jobId, generation) => {
    port.postMessage({ type: "event", event, jobId, generation });
  },
  () => Date.now(),
);

port.on("message", (msg: unknown) => {
  if (!isStorageRequest(msg)) {
    const res: StorageResponse = {
      id: "unknown",
      ok: false,
      error: serializeInternalError(new DurableQStorageError("malformed storage request")),
    };
    port.postMessage(res);
    return;
  }
  try {
    const result = session.handle(msg);
    const res: StorageSuccess = { id: msg.id, ok: true, result };
    port.postMessage(res);
  } catch (err) {
    const classified = session.classify(err);
    const res: StorageFailure = {
      id: msg.id,
      ok: false,
      error: serializeInternalError(classified),
    };
    port.postMessage(res);
  }
});

port.on("close", () => {
  session.dispose();
});
