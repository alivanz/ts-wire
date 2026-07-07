/**
 * Client-facing types + the internal Transport seam.
 *
 * The public surface (`SseSubscription`) is derived from a route's `events` map:
 * `.on()` / the async-iterator both project the OUTPUT (decoded) side. The internal
 * {@link Transport} seam is what `fetch-transport.ts` and `eventsource-transport.ts`
 * each implement, and what `client.ts` orchestrates on top of.
 */
import type { EventsMap, InferOut } from "../core/schema.js";
import type { HttpMethod, SseDef } from "../core/contract.js";
import type { RawFrame } from "../core/wire.js";
import type { SseConnectionError, SseValidationError } from "./errors.js";

// ── Public surface ───────────────────────────────────────────────────────────

/** Metadata delivered alongside every decoded event. */
export interface EventMeta {
  readonly id?: string;
  readonly lastEventId?: string;
  readonly retry?: number;
}

/** The decoded discriminated union of a route's whole event catalog. */
export type ClientEvent<E extends EventsMap> = {
  [K in keyof E]: { readonly event: K; readonly data: InferOut<E[K]> } & EventMeta;
}[keyof E];

export type Unsubscribe = () => void;
export type SseClientState = "connecting" | "open" | "reconnecting" | "closed";

/**
 * A live subscription. It is BOTH an async-iterable of the decoded event union AND
 * a target for typed `.on(name, cb)` handlers — use whichever fits.
 */
export interface SseSubscription<E extends EventsMap> extends AsyncIterable<ClientEvent<E>> {
  /** Typed per-event handler. Returns an unsubscribe fn. */
  on<K extends keyof E>(name: K, cb: (data: InferOut<E[K]>, meta: EventMeta) => void): Unsubscribe;
  onOpen(cb: () => void): Unsubscribe;
  /** Graceful terminal end (server sent the EOS sentinel or closed cleanly). */
  onClose(cb: () => void): Unsubscribe;
  /** The ONLY reconnect-driving channel. Retriable → auto-reconnect; fatal → iterator throws. */
  onConnectionError(cb: (err: SseConnectionError) => void): Unsubscribe;
  /** Data-plane decode failures. Never reconnects. */
  onValidationError(cb: (err: SseValidationError) => void): Unsubscribe;
  readonly state: SseClientState;
  /** Permanent stop: cancels reconnection and resolves the iterator as done. */
  close(): void;
}

// ── initClient options + per-subscribe args ──────────────────────────────────

export type HeaderValue = string | (() => string | Promise<string>);
export type ValidationMode = "throw" | "skip" | "emit";
export type TransportKind = "auto" | "eventsource" | "fetch";

/** A `fetch`-compatible function (injectable for tests). */
export type FetchLike = typeof fetch;

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

export interface ReconnectPolicy {
  /** Max reconnect attempts before giving up (fatal). Use `Infinity` for unlimited. */
  retries: number;
  /** Delay before attempt N (1-based), given the server's `retry:` hint if any. */
  backoffMs(attempt: number, serverRetryMs: number | undefined): number;
}

export interface InitClientOptions {
  baseUrl: string;
  transport?: TransportKind;
  baseHeaders?: Record<string, HeaderValue>;
  withCredentials?: boolean;
  /** Re-validate every incoming frame against its event schema. Default: true. */
  validateEvents?: boolean;
  /** What to do on a validation failure. Default: "skip". */
  onValidationError?: ValidationMode;
  /** Reconnection policy, or `false` to disable reconnection entirely. */
  reconnect?: Partial<ReconnectPolicy> | false;
  fetch?: FetchLike;
  EventSource?: EventSourceCtor;
}

export interface SubscribeArgs {
  params?: Record<string, string | number>;
  query?: Record<string, string | number | boolean | undefined>;
  headers?: Record<string, HeaderValue>;
  /** Request body for POST routes (JSON-serialized; forces the fetch transport). */
  body?: unknown;
  /** Seed the `Last-Event-ID` to resume from (forces the fetch transport). */
  resumeFrom?: string;
  signal?: AbortSignal;
}

/** The client: one entry per route, each exposing `.subscribe()`. */
export type SseClient<C extends Record<string, SseDef>> = {
  [K in keyof C]: {
    subscribe(args?: SubscribeArgs): SseSubscription<C[K]["events"]>;
  };
};

// ── Internal Transport seam (implemented by both transports) ──────────────────

export interface TransportConfig {
  /** Fully-resolved URL including query string. */
  url: string;
  method: HttpMethod;
  /** Resolved headers (all HeaderValue functions already awaited). */
  headers: Record<string, string>;
  /** Serialized request body, if any. */
  body?: string;
  /** Known contract event names — EventSource must `addEventListener` for each. */
  eventNames: readonly string[];
  /** Seed for the first connection's `Last-Event-ID`. */
  resumeFrom?: string;
  withCredentials?: boolean;
  reconnect: ReconnectPolicy | false;
  fetchImpl: FetchLike;
  EventSourceImpl?: EventSourceCtor;
  /** Caller abort (from SubscribeArgs.signal); merged with `close()`. */
  signal?: AbortSignal;
}

export interface TransportHandlers {
  onOpen(): void;
  onFrame(frame: RawFrame): void;
  /** retriable ⇒ the transport auto-reconnects and keeps running; fatal ⇒ it has stopped. */
  onError(err: SseConnectionError): void;
  /** Clean terminal end of the stream (no reconnect). */
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
