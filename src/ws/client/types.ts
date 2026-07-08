/**
 * Client-facing types + the injectable {@link WebSocketLike} seam.
 *
 * A WS contract is bidirectional, so the client both SENDS the client-schema INPUT
 * ({@link ClientInput}) and RECEIVES the server-schema OUTPUT ({@link ServerOutput}).
 * `connectWs` never statically imports a concrete WebSocket: the platform socket is
 * injected through {@link WebSocketCtor} (defaulting to `globalThis.WebSocket`), which
 * keeps `client.ts` unit-testable against a fake socket.
 */
import type { ClientInput, ServerOutput, WsDef } from "../contract.js";
import type { WsValidationError } from "../errors.js";

// ── Injectable WebSocket seam ──────────────────────────────────────────────────

/** The minimal slice of a `MessageEvent` we read (data may arrive as text or binary). */
export interface MessageEventLike {
  readonly data: string | ArrayBuffer;
}

/** The minimal slice of a `CloseEvent` we read. */
export interface CloseEventLike {
  readonly code: number;
  readonly reason: string;
}

/** The minimal slice of a native `WebSocket` `connectWs` depends on. */
export interface WebSocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  readonly bufferedAmount: number;
  /** 0 CONNECTING, 1 OPEN, 2 CLOSING, 3 CLOSED. */
  readonly readyState: number;
  addEventListener(type: "open", cb: () => void): void;
  addEventListener(type: "message", cb: (ev: MessageEventLike) => void): void;
  addEventListener(type: "close", cb: (ev: CloseEventLike) => void): void;
  addEventListener(type: "error", cb: (ev: unknown) => void): void;
}

/** A WebSocket constructor (native `WebSocket` conforms; tests inject a fake). */
export type WebSocketCtor = new (url: string, protocols?: string | string[]) => WebSocketLike;

// ── Public surface ─────────────────────────────────────────────────────────────

/** What to do with a frame that fails to decode. */
export type ValidationMode = "throw" | "skip" | "emit";
export type Unsubscribe = () => void;
export type WsClientState = "connecting" | "open" | "reconnecting" | "closed";

/** Reconnection strategy: how many attempts, and the backoff before each. */
export interface ReconnectPolicy {
  retries: number;
  backoffMs(attempt: number): number;
}

export interface ConnectWsOptions {
  /** The full WebSocket URL (`ws://` / `wss://`). */
  url: string;
  protocols?: string | string[];
  /** WebSocket implementation (defaults to `globalThis.WebSocket`; injectable for tests/SSR). */
  WebSocket?: WebSocketCtor;
  /** Re-validate every incoming frame against the server schema. Default: true. */
  validateMessages?: boolean;
  /** What to do on a decode failure. Default: "skip". */
  onValidationError?: ValidationMode;
  /** Reconnection policy, or `false` to disable. Default: infinite capped-exp backoff. */
  reconnect?: Partial<ReconnectPolicy> | false;
  /** `bufferedAmount` backpressure threshold in bytes. Default: 1_048_576 (1 MiB). */
  sendBufferHwm?: number;
}

/**
 * A live WS connection. It is BOTH an async-iterable of the decoded server OUTPUT union
 * AND a target for `.onMessage`/lifecycle handlers — use whichever fits.
 */
export interface WsClient<D extends WsDef> extends AsyncIterable<ServerOutput<D>> {
  /** Serialize + send a client message. Rejects if the message fails its schema. */
  send(msg: ClientInput<D>): Promise<void>;
  onMessage(cb: (msg: ServerOutput<D>) => void): Unsubscribe;
  onOpen(cb: () => void): Unsubscribe;
  onClose(cb: (code: number, reason: string) => void): Unsubscribe;
  onError(cb: (err: unknown) => void): Unsubscribe;
  /** Data-plane decode failures (fired only when `onValidationError` is "emit"). */
  onValidationError(cb: (err: WsValidationError) => void): Unsubscribe;
  readonly state: WsClientState;
  /** Permanent stop: closes the socket, cancels any reconnect, completes the iterator. */
  close(code?: number, reason?: string): void;
}
