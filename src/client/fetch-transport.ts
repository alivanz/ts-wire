/**
 * The `fetch` + `ReadableStream` {@link Transport} (DESIGN.md §4.4).
 *
 * This is the richer of the two transports: unlike native `EventSource`, `fetch`
 * lets us read the response STATUS and headers at open time, send a request body,
 * and set a `Last-Event-ID` resume header — so we do the full "open-time
 * classification" here, and only this transport backs POST / resumable routes.
 *
 * The shape is a single self-driving reconnect loop:
 *
 *   start() ─▶ run() ┌─▶ connectOnce()  (one fetch → classify → stream)
 *                    │      ├─ clean EOS sentinel ⇒ onClose, stop
 *                    │      ├─ fatal open failure  ⇒ onError(fatal), stop
 *                    │      └─ retriable drop      ⇒ throw ─┐
 *                    └───────── backoff + retry ◀───────────┘  (until retries run out)
 *
 * `close()` aborts an internal controller (merged into every fetch `signal`) and
 * cancels any pending backoff timer, so the loop unwinds without further callbacks.
 */
import { TS_SSE_EOS } from "../core/wire.js";
import { parseSseStream } from "../core/parse.js";
import { SseConnectionError } from "./errors.js";
import type { TransportConfig, TransportFactory, TransportHandlers } from "./types.js";

export const fetchTransport: TransportFactory = (
  config: TransportConfig,
  handlers: TransportHandlers,
) => {
  // ── loop state (all mutated across reconnect attempts) ───────────────────────
  //
  // The resume anchor: seeded from `resumeFrom`, then advanced by every framed `id:`
  // so a reconnect re-opens with the right `Last-Event-ID`.
  let lastEventId: string | undefined = config.resumeFrom;
  // Reconnect attempt counter. Reset to 0 on every successful open so a long-lived
  // stream that later drops starts its backoff schedule fresh (not mid-ramp).
  let attempt = 0;

  // `close()` aborts this; it is merged into every fetch signal so a close (or a
  // caller abort) tears down the in-flight request and its body stream.
  const internalController = new AbortController();
  let closed = false;
  let started = false;

  // The pending backoff timer + its resolver, tracked so `close()` can cancel the
  // wait without leaking the timer or hanging the awaiting loop forever.
  let delayTimer: ReturnType<typeof setTimeout> | undefined;
  let delayResolve: (() => void) | undefined;

  // ── helpers ──────────────────────────────────────────────────────────────────

  /** Merge the caller's abort signal (if any) with our internal close signal. */
  function connectSignal(): AbortSignal {
    return config.signal
      ? AbortSignal.any([config.signal, internalController.signal])
      : internalController.signal;
  }

  /** Fire one connect request with the resume header + SSE `Accept` applied. */
  function openRequest(): Promise<Response> {
    // Start from the resolved contract headers, then ADD ours. Setting `Accept`
    // last means it wins over any inherited value; the resume header is conditional.
    const headers: Record<string, string> = { ...config.headers, Accept: "text/event-stream" };
    if (lastEventId !== undefined) headers["Last-Event-ID"] = lastEventId;

    return config.fetchImpl(config.url, {
      method: config.method,
      headers,
      body: config.body,
      signal: connectSignal(),
      credentials: config.withCredentials ? "include" : "same-origin",
    });
  }

  /** A `setTimeout` we can cancel from `close()`; resolves early if cancelled. */
  function delay(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      delayResolve = resolve;
      delayTimer = setTimeout(() => {
        delayTimer = undefined;
        delayResolve = undefined;
        resolve();
      }, ms);
    });
  }

  /** Cancel a pending backoff wait (called by `close()`), resolving it immediately. */
  function cancelDelay(): void {
    if (delayTimer !== undefined) {
      clearTimeout(delayTimer);
      delayTimer = undefined;
    }
    if (delayResolve !== undefined) {
      const resolve = delayResolve;
      delayResolve = undefined;
      resolve();
    }
  }

  // ── open-time classification (DESIGN.md §4.4) ────────────────────────────────
  //
  // Runs one fetch, classifies the response, and — if the stream opens cleanly —
  // pumps frames until EOS or a drop. Throws an {@link SseConnectionError} for every
  // failure mode; `run()` decides fatal-vs-retriable from `err.retriable`.

  async function connectOnce(): Promise<void> {
    const res = await openRequest();

    // 1) Non-2xx: an HTTP-level failure. 429 + 5xx are transient (honor `Retry-After`);
    //    everything else (204, other 4xx) is fatal — reconnecting can't fix it.
    if (!res.ok) {
      await res.body?.cancel().catch(() => {}); // don't leak the error body
      const retriable = res.status === 429 || res.status >= 500;
      throw new SseConnectionError({
        kind: "http",
        retriable,
        status: res.status,
        retryAfterMs: parseRetryAfter(res.headers.get("retry-after")),
        message: `SSE open failed: HTTP ${res.status}`,
      });
    }

    // 2) 2xx but not an event stream: the server answered something else entirely
    //    (an HTML error page, JSON, …). Fatal — the body is not parseable as SSE.
    const contentType = res.headers.get("content-type");
    if (!isEventStream(contentType)) {
      await res.body?.cancel().catch(() => {});
      throw new SseConnectionError({
        kind: "content-type",
        retriable: false,
        received: contentType,
        message: `expected text/event-stream, received ${contentType ?? "(none)"}`,
      });
    }

    // 3) 2xx event stream with no body to read: nothing to parse. Fatal.
    if (res.body === null) {
      throw new SseConnectionError({
        kind: "network",
        retriable: false,
        message: "response has no body",
      });
    }

    // 4) Open! Announce it and reset backoff so a later drop restarts the schedule.
    handlers.onOpen();
    attempt = 0;

    // ── stream ──
    for await (const frame of parseSseStream(res.body)) {
      // Guard against a frame arriving in the microtask gap after `close()`.
      if (closed) return;

      // Advance the resume anchor on every id so a reconnect resumes correctly.
      if (frame.id !== undefined) lastEventId = frame.id;

      if (frame.event === TS_SSE_EOS) {
        // Graceful terminal. Choice (per spec): forward the EOS frame THEN onClose,
        // so a client that wants to observe the sentinel can, but the stream still
        // ends cleanly with NO reconnect.
        handlers.onFrame(frame);
        handlers.onClose();
        return;
      }

      handlers.onFrame(frame);
    }

    // The generator completed WITHOUT an EOS sentinel: the server (or a proxy) dropped
    // the connection. That is a retriable drop — reconnect and resume from lastEventId.
    throw new SseConnectionError({
      kind: "network",
      retriable: true,
      message: "stream ended without EOS sentinel",
    });
  }

  // ── the reconnect loop ───────────────────────────────────────────────────────

  async function run(): Promise<void> {
    while (!closed) {
      try {
        await connectOnce();
        return; // connectOnce returned normally ⇒ clean EOS terminal; stop.
      } catch (err) {
        // `close()` (or a caller abort) won the race: unwind silently, no callbacks.
        if (closed || isAbortError(err)) return;

        const connErr = toConnectionError(err);

        if (!connErr.retriable) {
          // Fatal open failure (204 / 4xx≠429 / wrong content-type / null body):
          // report once and stop — reconnecting would be pointless.
          handlers.onError(connErr);
          return;
        }

        // Retriable drop: surface it (the client keeps its iterator alive)…
        handlers.onError(connErr);

        // …then decide whether we may try again.
        if (config.reconnect === false || attempt >= config.reconnect.retries) {
          // Reconnection disabled or attempts exhausted ⇒ escalate to a FATAL error
          // so the client's iterator throws and the subscription closes.
          handlers.onError(toFatal(connErr));
          return;
        }

        // Wait out the backoff for the upcoming (1-based) attempt, then loop.
        attempt += 1;
        await delay(config.reconnect.backoffMs(attempt, connErr.retryAfterMs));
        if (closed) return; // close() may have fired during the wait.
      }
    }
  }

  // ── public Transport surface ─────────────────────────────────────────────────

  return {
    start(): void {
      if (started || closed) return; // idempotent; a no-op after close.
      started = true;
      void run();
    },
    close(): void {
      if (closed) return; // idempotent.
      closed = true;
      cancelDelay(); // don't leak the backoff timer.
      internalController.abort(); // tear down any in-flight fetch + body stream.
    },
  };
};

// ── module-local pure helpers ──────────────────────────────────────────────────

/** A `Content-Type` counts as an event stream iff it starts with `text/event-stream`. */
function isEventStream(contentType: string | null): boolean {
  return contentType !== null && contentType.toLowerCase().startsWith("text/event-stream");
}

/**
 * Parse a `Retry-After` header into milliseconds. Supports both forms the spec allows:
 * a delay in seconds (`120`) and an HTTP-date (`Wed, 21 Oct 2026 07:28:00 GMT`).
 */
function parseRetryAfter(value: string | null): number | undefined {
  if (value === null) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000);
  const dateMs = Date.parse(value);
  if (!Number.isNaN(dateMs)) return Math.max(0, dateMs - Date.now());
  return undefined;
}

/** Was this thrown because the fetch/stream was aborted (by `close()` or the caller)? */
function isAbortError(err: unknown): boolean {
  return err instanceof Error && err.name === "AbortError";
}

/** Normalize any thrown value into an {@link SseConnectionError}. */
function toConnectionError(err: unknown): SseConnectionError {
  if (err instanceof SseConnectionError) return err;
  // An unexpected throw from the fetch or the parser (DNS failure, socket reset, …):
  // treat it as a retriable network drop so the loop gets a chance to recover.
  return new SseConnectionError({
    kind: "network",
    retriable: true,
    message: err instanceof Error ? err.message : "network error",
    cause: err,
  });
}

/** Re-wrap an exhausted retriable error as FATAL (so the client iterator throws). */
function toFatal(err: SseConnectionError): SseConnectionError {
  return new SseConnectionError({
    kind: err.kind,
    retriable: false,
    status: err.status,
    retryAfterMs: err.retryAfterMs,
    received: err.received,
    message: `reconnection exhausted: ${err.message}`,
    cause: err,
  });
}
