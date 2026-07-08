/**
 * Server-facing types. One contract = one SSE endpoint, so a handler is a single
 * `fn(ctx)` — the ctx carries the validated `query`, the inbound `lastEventId`, a
 * disconnect `signal`, the typed `emit`, and a runtime `init(...)` for stream setup.
 */
import type { EventsMap, InferIn, InferOut, StandardSchemaV1 } from "../../core/schema.js";
import type { SseDef } from "../contract.js";

/** The single frame-writing seam every output (Response / Node / ...) implements. */
export interface FrameSink {
  /** Write one frame. Resolves on FLUSH, not enqueue — this IS the backpressure story. */
  write(frame: Uint8Array): Promise<void>;
  close(): Promise<void>;
  /** True once the client disconnected / the stream was cancelled. */
  readonly aborted: boolean;
  /** Fires on disconnect; thread into handler loops for cleanup. */
  readonly signal: AbortSignal;
}

export interface EmitOpts {
  /** SSE `id:` — surfaces as `lastEventId` on the client and drives resume. */
  id?: string;
}

/** Emit controls that ride alongside the per-event methods. */
export interface EmitControls {
  /** Write a one-off `retry:` line (reconnection hint, ms). */
  retry(ms: number): Promise<void>;
  /** Write a `:comment` line (ignored by EventSource; used for keep-alives). */
  comment(text: string): Promise<void>;
  /** Send the `ts-sse-eos` sentinel and end the stream (explicit early terminal). */
  close(): Promise<void>;
}

/**
 * The typed emitter, derived from the contract's `events` map. Each method takes the
 * schema's INPUT type (the decoder invariant: the wire carries INPUT JSON), validates
 * it as a fail-fast pre-check, and resolves on flush (`await emit.x(...)` = backpressure).
 */
export type Emit<E extends EventsMap> = {
  [K in keyof E]: (data: InferIn<E[K]>, opts?: EmitOpts) => Promise<void>;
} & EmitControls;

/** Runtime stream setup, called inside the handler (not at server creation). */
export interface InitOptions {
  /** Start an idle-gated heartbeat: a `:keep-alive` comment every `heartbeat` ms. */
  heartbeat?: number;
  /** Write the initial `retry:` reconnection hint (ms). */
  retry?: number;
}

/** The decoded query type for a contract, from its optional `query` schema. */
export type QueryOutput<D extends SseDef> = D extends {
  query: infer Q extends StandardSchemaV1;
}
  ? InferOut<Q>
  : Record<string, never>;

/** The handler context. Uniform across every output (Response, Node, ...). */
export interface SseContext<D extends SseDef> {
  /** Validated + coerced query (server-side) from the contract's `query` schema. */
  readonly query: QueryOutput<D>;
  /** The inbound `Last-Event-ID` header, if the browser is resuming. */
  readonly lastEventId: string | undefined;
  /** Aborts when the client disconnects; pass to your loops/`fetch` for cleanup. */
  readonly signal: AbortSignal;
  /** Typed emitter for the contract's events (+ retry/comment/close controls). */
  readonly emit: Emit<D["events"]>;
  /** Runtime stream setup — heartbeat timer + initial `retry:`. Call once, up top. */
  init(options: InitOptions): void;
}

/** A single SSE handler. Return normally ⇒ EOS (terminal); throw ⇒ reconnect. */
export type SseHandler<D extends SseDef> = (ctx: SseContext<D>) => void | Promise<void>;

/** Loose runtime ctx base (query typed structurally, re-attached at the boundary). */
export interface RunBase {
  query: unknown;
  lastEventId: string | undefined;
}
