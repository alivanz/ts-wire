/**
 * A {@link Transport} backed by the browser's native `EventSource`.
 *
 * Native EventSource is deliberately limited, and those limits shape this transport:
 *
 *   - it is GET-only: no request headers, no body, no resume seed;
 *   - it OWNS its reconnect loop — on a drop it reconnects on its own, re-sending the
 *     `Last-Event-ID` it tracked internally. So this transport implements NO reconnect
 *     logic of its own: the browser is in charge.
 *   - it cannot wildcard-listen, so we must `addEventListener` once per known event
 *     name (from {@link TransportConfig.eventNames}) plus the default `"message"`.
 *
 * A caller-supplied {@link TransportConfig.signal} tears the source down: when it aborts
 * (or is already aborted at `start()`) we invoke `close()`, so an external `AbortSignal`
 * ends the subscription just like an explicit `close()` does.
 */
import type {
  EventSourceLike,
  MessageEventLike,
  TransportFactory,
} from "./types.js";
import type { RawFrame } from "../wire.js";
import { TS_SSE_EOS } from "../wire.js";
import { SseConnectionError } from "./errors.js";

/** `EventSourceLike.readyState` values: 0 CONNECTING, 1 OPEN, 2 CLOSED. */
const CLOSED = 2;

export const eventSourceTransport: TransportFactory = (config, handlers) => {
  // The live source, once constructed. Undefined before `start()` and when `start()`
  // bailed out (no implementation available).
  let es: EventSourceLike | undefined;
  // Set the instant we permanently stop. Every listener callback checks it first so a
  // native event that fires after teardown is silently dropped — no handler runs late.
  let closed = false;
  // The caller's abort signal, remembered so `close()` can detach its listener.
  let abortSignal: AbortSignal | undefined;

  /** Caller aborted (via `config.signal`) — tear the source down like an explicit close. */
  function onAbort(): void {
    close();
  }

  /** Translate one native message event into a {@link RawFrame}, or terminate on EOS. */
  function onData(ev: MessageEventLike): void {
    if (closed) return;

    // The reserved terminal sentinel. It can NEVER be a contract event name (the
    // reserved-name guard forbids it, so it is never in `eventNames`), but a server may
    // still emit it on the wire, hence the explicit listener that routes it here. Treat
    // it as a clean, graceful end: tear the source down and report onClose exactly once.
    if (ev.type === TS_SSE_EOS) {
      close();
      handlers.onClose();
      return;
    }

    // `lastEventId` is the empty string until the stream sends its first `id:`. Normalize
    // that to `undefined` so `RawFrame.id` is absent rather than an empty string.
    const frame: RawFrame = { event: ev.type, data: ev.data };
    if (ev.lastEventId) frame.id = ev.lastEventId;
    handlers.onFrame(frame);
  }

  function start(): void {
    // The client only routes plain-GET subscriptions here, so a missing implementation
    // is a CONFIG error, not a runtime connection fault. Report it as fatal (there is
    // nothing to retry) and stop — we never construct a source.
    const EventSourceImpl = config.EventSourceImpl;
    if (EventSourceImpl === undefined) {
      handlers.onError(
        new SseConnectionError({
          kind: "network",
          retriable: false,
          message: "No EventSource implementation available",
        }),
      );
      return;
    }

    const source = new EventSourceImpl(config.url, { withCredentials: config.withCredentials });
    es = source;

    // Lifecycle: native "open" fires on the first successful connect AND after every
    // successful auto-reconnect. It is not a data event.
    source.addEventListener("open", () => {
      if (!closed) handlers.onOpen();
    });

    // Register one DATA listener per known event name plus the default "message".
    // Dedupe so a contract that explicitly names "message" doesn't get a doubled listener.
    // Reserved names (`error`/`open`/`message`/EOS) can't be contract event names, so a
    // contract event can never collide with these lifecycle/terminal listeners.
    const dataNames = new Set<string>([...config.eventNames, "message"]);
    for (const name of dataNames) source.addEventListener(name, onData);

    // EOS is never in `eventNames` (reserved), so add its listener explicitly — see onData.
    source.addEventListener(TS_SSE_EOS, onData);

    // Native "error" is OPAQUE: EventSource surfaces no HTTP status or reason, only its
    // readyState. CLOSED (2) => the browser has permanently given up => fatal. Anything
    // else (CONNECTING (0)) => the browser is already auto-retrying => retriable. Either
    // way this is a connection-plane signal, never a clean end, so we never call onClose.
    source.addEventListener("error", () => {
      if (closed) return;
      const fatal = source.readyState === CLOSED;
      handlers.onError(
        new SseConnectionError({
          kind: "network",
          retriable: !fatal,
          message: fatal
            ? "EventSource connection closed"
            : "EventSource connection error (reconnecting)",
        }),
      );
    });

    // Wire the caller's abort: an already-aborted signal tears down immediately; otherwise
    // an `abort` later routes to `close()`. `close()` detaches this listener.
    const signal = config.signal;
    if (signal !== undefined) {
      abortSignal = signal;
      if (signal.aborted) {
        close();
        return;
      }
      signal.addEventListener("abort", onAbort);
    }
  }

  function close(): void {
    // Idempotent: the flag guards a double `es.close()` and, together with the per-listener
    // checks, ensures no handler fires after teardown.
    if (closed) return;
    closed = true;
    abortSignal?.removeEventListener("abort", onAbort);
    es?.close();
  }

  return { start, close };
};
