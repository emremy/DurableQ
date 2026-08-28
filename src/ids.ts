import { hostname } from "node:os";
import { randomUUID } from "node:crypto";

export function newJobId(): string {
  return randomUUID();
}

export function newWorkerId(): string {
  return `${hostname()}:${process.pid}:${randomUUID()}`;
}

export function newRpcId(): string {
  return randomUUID();
}
