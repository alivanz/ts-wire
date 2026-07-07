/**
 * The two failure modes, kept structurally distinct (DESIGN.md §4.4):
 *
 *  - {@link SseConnectionError} — a TRANSPORT problem (connection drop, non-2xx,
 *    wrong content-type). Drives reconnection. Retriable ones auto-reconnect and
 *    the event iterator keeps yielding; fatal ones make the iterator throw.
 *  - {@link SseValidationError} — a DATA-plane problem (a frame failed its event
 *    schema). Never reconnects; default is skip-and-continue.
 *
 * A contract's own typed `error` event does NOT live here — it flows through the
 * normal data channel like any other event (and cannot even be named `error`; see
 * the reserved-name guard).
 */
import type { StandardSchemaV1 } from "@standard-schema/spec";

export type SseConnectionErrorKind = "http" | "network" | "parse" | "content-type";

export interface SseConnectionErrorInit {
  kind: SseConnectionErrorKind;
  /** true => the transport will reconnect; false => fatal, the stream is over. */
  retriable: boolean;
  message?: string;
  /** HTTP status, when `kind === "http"`. */
  status?: number;
  /** Parsed `Retry-After` / server `retry:` hint, when present. */
  retryAfterMs?: number;
  /** The offending `Content-Type`, when `kind === "content-type"`. */
  received?: string | null;
  cause?: unknown;
}

/** A transport-level connection failure. `kind` discriminates the cause. */
export class SseConnectionError extends Error {
  readonly kind: SseConnectionErrorKind;
  readonly retriable: boolean;
  readonly status?: number;
  readonly retryAfterMs?: number;
  readonly received?: string | null;

  constructor(init: SseConnectionErrorInit) {
    super(
      init.message ?? `${init.kind} connection error`,
      init.cause !== undefined ? { cause: init.cause } : undefined,
    );
    this.name = "SseConnectionError";
    this.kind = init.kind;
    this.retriable = init.retriable;
    this.status = init.status;
    this.retryAfterMs = init.retryAfterMs;
    this.received = init.received;
  }
}

/** A data-plane failure: an incoming frame did not satisfy its event schema. */
export interface SseValidationError {
  readonly event: string;
  readonly issues: ReadonlyArray<StandardSchemaV1.Issue>;
  /** The raw `data` string that failed to decode. */
  readonly raw: string;
  readonly lastEventId?: string;
}
