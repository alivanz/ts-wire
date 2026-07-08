/**
 * `connectWs` — the transport-agnostic ORCHESTRATOR for ONE WebSocket connection.
 *
 * A WS contract is a single {@link WsDef} (`{ client?, server? }`), so `connectWs`
 * returns a single {@link WsClient}: it `send`s the client-schema INPUT and yields the
 * server-schema OUTPUT. The socket itself is injected through `options.WebSocket`
 * (defaulting to `globalThis.WebSocket`), so this file never statically imports the
 * network layer and stays unit-testable against a fake socket.
 *
 * The decoder invariant lives in {@link decodeMessage}: the wire carries a schema's
 * INPUT JSON, so `send` serializes the INPUT (never the transformed OUTPUT) and the
 * receive path re-runs the server schema INPUT→OUTPUT before handing the consumer the
 * OUTPUT value.
 */
import { decodeMessage, encodeMessage } from "../codec.js";
import type { ClientInput, ServerOutput, WsDef } from "../contract.js";
import type { WsValidationError } from "../errors.js";
import type {
  ConnectWsOptions,
  ReconnectPolicy,
  Unsubscribe,
  WebSocketCtor,
  WebSocketLike,
  WsClient,
  WsClientState,
} from "./types.js";

/** Default backpressure high-water mark: pause `send` above 1 MiB of buffered data. */
const DEFAULT_SEND_BUFFER_HWM = 1_048_576;

/** Resolve the reconnect option to a concrete policy, or `false` when disabled. */
function resolveReconnect(reconnect: ConnectWsOptions["reconnect"]): ReconnectPolicy | false {
  if (reconnect === false) return false;
  const defaults: ReconnectPolicy = {
    retries: Number.POSITIVE_INFINITY,
    // Capped exponential backoff: 1s, 1s, 2s, 4s, … clamped at 30s.
    backoffMs: (n) => Math.min(30_000, 1_000 * 2 ** Math.max(0, n - 1)),
  };
  if (reconnect === undefined) return defaults;
  return {
    retries: reconnect.retries ?? defaults.retries,
    backoffMs: reconnect.backoffMs ?? defaults.backoffMs,
  };
}

export function connectWs<D extends WsDef>(
  contract: D,
  options: ConnectWsOptions,
): WsClient<D> {
  // ── Resolve the socket constructor (fail fast on a misconfiguration) ─────────
  const resolved: WebSocketCtor | undefined =
    options.WebSocket ?? (globalThis as { WebSocket?: WebSocketCtor }).WebSocket;
  if (!resolved) {
    throw new Error(
      "ts-wire/ws: no WebSocket implementation — pass options.WebSocket " +
        "or run where globalThis.WebSocket exists.",
    );
  }
  // Non-optional binding so the `connect()` closure below sees a narrowed type.
  const Ctor: WebSocketCtor = resolved;

  const reconnectPolicy = resolveReconnect(options.reconnect);
  const sendBufferHwm = options.sendBufferHwm ?? DEFAULT_SEND_BUFFER_HWM;

  let state: WsClientState = "connecting";
  let ws: WebSocketLike | undefined;
  let userClosed = false;
  let attempt = 0; // reconnect attempt counter; reset to 0 on every open
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  /** Strings queued while not open; flushed in order on the next open. */
  const outgoing: string[] = [];

  // ── Listener registries ──────────────────────────────────────────────────────
  const messageListeners = new Set<(msg: ServerOutput<D>) => void>();
  const openListeners = new Set<() => void>();
  const closeListeners = new Set<(code: number, reason: string) => void>();
  const errorListeners = new Set<(err: unknown) => void>();
  const validationListeners = new Set<(err: WsValidationError) => void>();

  function register<T>(set: Set<T>, cb: T): Unsubscribe {
    set.add(cb);
    return () => {
      set.delete(cb);
    };
  }

  // ── Async push/pull buffer (same shape as the SSE client) ──────────────────────
  // A value pushed before a pull is buffered in `queue`; a pull before a push parks in
  // the single `pending` slot (for-await consumes serially, so one slot suffices).
  const queue: ServerOutput<D>[] = [];
  let pending:
    | {
        resolve: (result: IteratorResult<ServerOutput<D>>) => void;
        reject: (reason: unknown) => void;
      }
    | undefined;
  let ended = false; // graceful done (close() / non-reconnecting close)
  let failed = false; // fatal — `failure` is the reject reason (onValidationError "throw")
  let failure: unknown;

  /** Deliver a decoded message to a waiting pull, else buffer it. No-op once terminal. */
  function push(msg: ServerOutput<D>): void {
    if (ended || failed) return;
    if (pending) {
      const { resolve } = pending;
      pending = undefined;
      resolve({ value: msg, done: false });
    } else {
      queue.push(msg);
    }
  }

  /** Graceful end: drain what is buffered, then complete the iterator as done. */
  function finish(): void {
    if (ended || failed) return;
    ended = true;
    if (pending) {
      const { resolve } = pending;
      pending = undefined;
      resolve({ value: undefined, done: true });
    }
  }

  /** Fatal end: reject a waiting pull now, and any later one, with `err`. */
  function fail(err: unknown): void {
    if (ended || failed) return;
    failed = true;
    failure = err;
    if (pending) {
      const { reject } = pending;
      pending = undefined;
      reject(err);
    }
  }

  function next(): Promise<IteratorResult<ServerOutput<D>>> {
    // Buffered values win, so a graceful end still yields everything already queued.
    const buffered = queue.shift();
    if (buffered !== undefined) return Promise.resolve({ value: buffered, done: false });
    if (failed) return Promise.reject(failure);
    if (ended) return Promise.resolve({ value: undefined, done: true });
    return new Promise((resolve, reject) => {
      pending = { resolve, reject };
    });
  }

  // ── Receive path (the decoder invariant) ───────────────────────────────────────

  /** Route a decode failure by the configured mode. */
  function reportValidation(err: WsValidationError): void {
    const mode = options.onValidationError ?? "skip";
    if (mode === "throw") {
      fail(err); // the async-iterator rejects with the error
    } else if (mode === "emit") {
      for (const cb of validationListeners) cb(err);
    }
    // "skip" (default) → silently drop the frame.
  }

  /** Fan a decoded OUTPUT value out to `.onMessage` handlers + the async-iterator queue. */
  function deliver(msg: ServerOutput<D>): void {
    for (const cb of messageListeners) cb(msg);
    push(msg);
  }

  function handleMessage(data: string | ArrayBuffer): void {
    const text = typeof data === "string" ? data : new TextDecoder().decode(data);

    if (options.validateMessages === false) {
      // Validation disabled: JSON.parse only, no schema round-trip.
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch (e) {
        reportValidation({
          issues: [{ message: e instanceof Error ? e.message : "invalid JSON" }],
          raw: text,
        });
        return;
      }
      deliver(parsed as ServerOutput<D>);
      return;
    }

    const result = decodeMessage<ServerOutput<D>>(contract.server, text);
    if (result.ok) {
      deliver(result.value);
    } else {
      reportValidation({ issues: result.issues, raw: text });
    }
  }

  // ── Close / reconnect ──────────────────────────────────────────────────────────

  function handleClose(code: number, reason: string): void {
    for (const cb of closeListeners) cb(code, reason);

    // Terminal: the user asked to stop, reconnection is disabled, or retries exhausted.
    if (userClosed || reconnectPolicy === false || attempt >= reconnectPolicy.retries) {
      state = "closed";
      finish();
      return;
    }

    // Otherwise schedule a (cancellable) reconnect after the policy's backoff.
    state = "reconnecting";
    const delay = reconnectPolicy.backoffMs(attempt);
    attempt += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;
      connect();
    }, delay);
  }

  // ── Connect loop ───────────────────────────────────────────────────────────────

  function connect(): void {
    const socket = new Ctor(options.url, options.protocols);
    ws = socket;

    socket.addEventListener("open", () => {
      state = "open";
      attempt = 0; // a successful open resets the backoff sequence
      // Flush everything queued while we were disconnected, in order.
      const flush = outgoing.splice(0, outgoing.length);
      for (const str of flush) socket.send(str);
      for (const cb of openListeners) cb();
    });

    socket.addEventListener("message", (ev) => {
      handleMessage(ev.data);
    });

    socket.addEventListener("close", (ev) => {
      handleClose(ev.code, ev.reason);
    });

    socket.addEventListener("error", (ev) => {
      // Surface the error but do NOT tear down — a `close` event always follows.
      for (const cb of errorListeners) cb(ev);
    });
  }

  connect();

  // ── Send (serialize INPUT, apply backpressure, or queue while disconnected) ─────

  const backpressureDelay = (): Promise<void> =>
    new Promise((resolve) => setTimeout(resolve, 4));

  async function send(msg: ClientInput<D>): Promise<void> {
    // Validate + serialize the INPUT. A bad message is a programmer error → reject.
    const str = encodeMessage(contract.client, msg);

    if (state === "open" && ws) {
      // Backpressure: wait for the socket's buffer to drain below the high-water mark.
      while (ws.bufferedAmount > sendBufferHwm) {
        await backpressureDelay();
      }
      ws.send(str);
      return;
    }

    // Not open: buffer for the next open. Resolves immediately.
    outgoing.push(str);
  }

  // ── Caller-driven permanent stop ────────────────────────────────────────────────

  function close(code?: number, reason?: string): void {
    if (userClosed) return;
    userClosed = true;
    if (reconnectTimer !== undefined) {
      clearTimeout(reconnectTimer);
      reconnectTimer = undefined;
    }
    state = "closed";
    finish();
    ws?.close(code, reason);
  }

  // ── Public client surface ────────────────────────────────────────────────────────

  const client: WsClient<D> = {
    send,
    onMessage: (cb) => register(messageListeners, cb),
    onOpen: (cb) => register(openListeners, cb),
    onClose: (cb) => register(closeListeners, cb),
    onError: (cb) => register(errorListeners, cb),
    onValidationError: (cb) => register(validationListeners, cb),
    get state() {
      return state;
    },
    close,
    [Symbol.asyncIterator]() {
      return {
        next,
        // `break`/`return` out of a for-await tears the connection down.
        return: async () => {
          close();
          return { value: undefined, done: true } as IteratorResult<ServerOutput<D>>;
        },
      };
    },
  };

  return client;
}
