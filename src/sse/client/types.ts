/**
 * Client-facing types + the internal Transport seam.
 *
 * One contract = one SSE endpoint, so `initClient(contract, opts)` returns a single
 * {@link SseEndpoint} (not a map of routes). Everything the client exposes derives
 * from the contract's `events` map (the decoded OUTPUT side) and its optional `query`
 * schema (the INPUT side, typed on `subscribe`).
 */
import type { EventsMap, InferIn, InferOut, StandardSchemaV1 } from "../../core/schema.js";
import type { SseDef } from "../contract.js";
import type { RawFrame } from "../wire.js";
import type { SseConnectionError, SseValidationError } from "./errors.js";

// ── Public surface ───────────────────────────────────────────────────────────

/** Metadata delivered alongside every decoded event. */
export interface EventMeta {
  readonly id?: string;
  readonly lastEventId?: string;
  readonly retry?: number;
}

/** The decoded discriminated union of a contract's whole event catalog. */
export type ClientEvent<E extends EventsMap> = {
  [K in keyof E]: { readonly event: K; readonly data: InferOut<E[K]> } & EventMeta;
}[keyof E];

export type Unsubscribe = () => void;
export type SseClientState = "connecting" | "open" | "reconnecting" | "closed";

/**
 * A live subscription. It is BOTH an async-iterable of the decoded event union AND a
 * target for typed `.on(name, cb)` handlers — use whichever fits.
 */
export interface SseSubscription<E extends EventsMap> extends AsyncIterable<ClientEvent<E>> {
  on<K extends keyof E>(name: K, cb: (data: InferOut<E[K]>, meta: EventMeta) => void): Unsubscribe;
  onOpen(cb: () => void): Unsubscribe;
  /** Graceful terminal end (server sent the EOS sentinel). */
  onClose(cb: () => void): Unsubscribe;
  /** Connection trouble. The browser owns reconnection; fatal → iterator throws. */
  onConnectionError(cb: (err: SseConnectionError) => void): Unsubscribe;
  /** Data-plane decode failures. Never reconnects. */
  onValidationError(cb: (err: SseValidationError) => void): Unsubscribe;
  readonly state: SseClientState;
  /** Permanent stop: closes the EventSource and resolves the iterator as done. */
  close(): void;
}

// ── initClient options + per-subscribe args ──────────────────────────────────

export type ValidationMode = "throw" | "skip" | "emit";

/** Minimal slice of native EventSource we depend on (injectable for tests). */
export interface MessageEventLike {
  readonly data: string;
  readonly lastEventId: string;
  readonly type: string;
}
export interface EventSourceLike {
  addEventListener(type: string, listener: (ev: MessageEventLike) => void): void;
  close(): void;
  readonly readyState: number; // 0 CONNECTING, 1 OPEN, 2 CLOSED
}
export type EventSourceCtor = new (
  url: string,
  init?: { withCredentials?: boolean },
) => EventSourceLike;

export interface InitClientOptions {
  /** The full endpoint URL (the contract has no path). Query is appended per-subscribe. */
  url: string;
  /** Send cookies/credentials (`new EventSource(url, { withCredentials })`). */
  withCredentials?: boolean;
  /** Re-validate every incoming frame against its event schema. Default: true. */
  validateEvents?: boolean;
  /** What to do on a validation failure. Default: "skip". */
  onValidationError?: ValidationMode;
  /** EventSource implementation (defaults to the global; injectable for tests/SSR). */
  EventSource?: EventSourceCtor;
}

/** Per-subscribe args. `query` is typed from the contract's optional `query` schema. */
export type SubscribeArgs<D extends SseDef> = { signal?: AbortSignal } & (D extends {
  query: infer Q extends StandardSchemaV1;
}
  ? { query: InferIn<Q> }
  : { query?: never });

/** Make the args object required only when the contract declares a `query` schema. */
export type SubscribeArgsRest<D extends SseDef> = D extends { query: StandardSchemaV1 }
  ? [args: SubscribeArgs<D>]
  : [args?: SubscribeArgs<D>];

/** The client for one contract: a single `.subscribe()`. */
export interface SseEndpoint<D extends SseDef> {
  subscribe(...args: SubscribeArgsRest<D>): SseSubscription<D["events"]>;
}

/** Loose runtime shape of subscribe args (the typed surface is {@link SubscribeArgs}). */
export interface AnySubscribeArgs {
  query?: Record<string, unknown>;
  signal?: AbortSignal;
}

// ── Internal Transport seam (implemented by the EventSource transport) ─────────

export interface TransportConfig {
  /** Fully-resolved URL including the query string. */
  url: string;
  /** Known contract event names — EventSource must `addEventListener` for each. */
  eventNames: readonly string[];
  withCredentials?: boolean;
  EventSourceImpl?: EventSourceCtor;
  /** Caller abort (from SubscribeArgs.signal); merged with `close()`. */
  signal?: AbortSignal;
}

export interface TransportHandlers {
  onOpen(): void;
  onFrame(frame: RawFrame): void;
  /** Connection error. retriable ⇒ the browser is auto-retrying; fatal ⇒ it gave up. */
  onError(err: SseConnectionError): void;
  /** Clean terminal end of the stream (the EOS sentinel). */
  onClose(): void;
}

export interface Transport {
  start(): void;
  /** Permanent stop; idempotent. */
  close(): void;
}

export type TransportFactory = (
  config: TransportConfig,
  handlers: TransportHandlers,
) => Transport;
