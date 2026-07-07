import { describe, it, expect, vi } from "vitest";
import { fetchTransport } from "../src/client/fetch-transport.js";
import type { FetchLike, TransportConfig, TransportHandlers } from "../src/client/types.js";
import { SseConnectionError } from "../src/client/errors.js";
import { TS_SSE_EOS } from "../src/core/wire.js";

// ── test harness ────────────────────────────────────────────────────────────

/** Yield to the macrotask queue once (drives the transport's setTimeout(0) backoff). */
const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

/** Poll `cond` across macrotasks until true, or fail after a bounded number of ticks. */
async function waitFor(cond: () => boolean, label = "condition"): Promise<void> {
  for (let i = 0; i < 500; i += 1) {
    if (cond()) return;
    await tick();
  }
  throw new Error(`waitFor timed out: ${label}`);
}

/** A hand-driven `ReadableStream<Uint8Array>` we can push SSE text into on demand. */
function makeStream(): {
  stream: ReadableStream<Uint8Array>;
  push(text: string): void;
  close(): void;
} {
  const encoder = new TextEncoder();
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
    },
  });
  return {
    stream,
    push: (text) => controller.enqueue(encoder.encode(text)),
    close: () => controller.close(),
  };
}

/** A 200 `text/event-stream` response wrapping the given body. */
function sseResponse(body: ReadableStream<Uint8Array>): Response {
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function makeHandlers(): {
  onOpen: ReturnType<typeof vi.fn>;
  onFrame: ReturnType<typeof vi.fn>;
  onError: ReturnType<typeof vi.fn>;
  onClose: ReturnType<typeof vi.fn>;
} {
  return {
    onOpen: vi.fn(),
    onFrame: vi.fn(),
    onError: vi.fn(),
    onClose: vi.fn(),
  };
}

function makeConfig(
  fetchImpl: FetchLike,
  overrides: Partial<TransportConfig> = {},
): TransportConfig {
  return {
    url: "https://example.test/sse",
    method: "GET",
    headers: {},
    eventNames: [],
    reconnect: { retries: 3, backoffMs: () => 0 },
    fetchImpl,
    ...overrides,
  };
}

/** Read the `init` recorded for the Nth (0-based) fetch call. */
function initAt(fetchImpl: ReturnType<typeof vi.fn>, n: number): RequestInit {
  return fetchImpl.mock.calls[n]![1] as RequestInit;
}
function headersAt(fetchImpl: ReturnType<typeof vi.fn>, n: number): Record<string, string> {
  return initAt(fetchImpl, n).headers as Record<string, string>;
}

// A vi.fn typed loosely enough to hand to `fetchImpl`.
const asFetch = (fn: ReturnType<typeof vi.fn>): FetchLike => fn as unknown as FetchLike;

// ── tests ───────────────────────────────────────────────────────────────────

describe("fetchTransport", () => {
  it("happy path: streams two frames ⇒ onOpen once, onFrame twice", async () => {
    const s = makeStream();
    const fetchImpl = vi.fn(async () => sseResponse(s.stream));
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    s.push('event: chat\ndata: {"a":1}\n\n');
    s.push('event: chat\ndata: {"a":2}\n\n');

    await waitFor(() => handlers.onFrame.mock.calls.length >= 2, "two frames");
    transport.close();
    s.close();

    expect(handlers.onOpen).toHaveBeenCalledTimes(1);
    expect(handlers.onFrame).toHaveBeenCalledTimes(2);
    expect(handlers.onFrame.mock.calls[0]![0]).toEqual({ event: "chat", data: '{"a":1}' });
    expect(handlers.onFrame.mock.calls[1]![0]).toEqual({ event: "chat", data: '{"a":2}' });
    expect(handlers.onError).not.toHaveBeenCalled();
  });

  it("Last-Event-ID: after an id:5 frame drops, the reconnect resends it", async () => {
    const first = makeStream();
    const second = makeStream();
    let call = 0;
    const fetchImpl = vi.fn(async () => {
      call += 1;
      return sseResponse(call === 1 ? first.stream : second.stream);
    });
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    first.push("id: 5\ndata: hi\n\n");
    await waitFor(() => handlers.onFrame.mock.calls.length >= 1, "first frame");
    first.close(); // drop without EOS ⇒ retriable ⇒ reconnect

    await waitFor(() => fetchImpl.mock.calls.length >= 2, "second fetch");

    // First open carried no resume header; the reconnect resumes from id 5.
    expect(headersAt(fetchImpl, 0)["Last-Event-ID"]).toBeUndefined();
    expect(headersAt(fetchImpl, 0)["Accept"]).toBe("text/event-stream");
    expect(headersAt(fetchImpl, 1)["Last-Event-ID"]).toBe("5");

    transport.close();
    second.close();
  });

  it("resumeFrom seeds the very first Last-Event-ID", async () => {
    const s = makeStream();
    const fetchImpl = vi.fn(async () => sseResponse(s.stream));
    const handlers = makeHandlers();
    const transport = fetchTransport(
      makeConfig(asFetch(fetchImpl), { resumeFrom: "42" }),
      handlers as TransportHandlers,
    );

    transport.start();
    await waitFor(() => fetchImpl.mock.calls.length >= 1, "first fetch");
    expect(headersAt(fetchImpl, 0)["Last-Event-ID"]).toBe("42");

    transport.close();
    s.close();
  });

  it("EOS: a ts-sse-eos frame ⇒ onClose, and no reconnect", async () => {
    const s = makeStream();
    const fetchImpl = vi.fn(async () => sseResponse(s.stream));
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    s.push("event: chat\ndata: 1\n\n");
    s.push(`event: ${TS_SSE_EOS}\ndata: {}\n\n`);

    await waitFor(() => handlers.onClose.mock.calls.length >= 1, "onClose");
    // Give a potential (buggy) reconnect a chance to fire, then assert it did not.
    await tick();
    await tick();

    expect(handlers.onClose).toHaveBeenCalledTimes(1);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(handlers.onError).not.toHaveBeenCalled();

    transport.close();
  });

  it("fatal HTTP: 404 ⇒ fatal http onError, no reconnect", async () => {
    const fetchImpl = vi.fn(async () => new Response(null, { status: 404 }));
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    await waitFor(() => handlers.onError.mock.calls.length >= 1, "onError");
    await tick();

    const err = handlers.onError.mock.calls[0]![0] as SseConnectionError;
    expect(err).toBeInstanceOf(SseConnectionError);
    expect(err.retriable).toBe(false);
    expect(err.kind).toBe("http");
    expect(err.status).toBe(404);
    expect(handlers.onError).toHaveBeenCalledTimes(1);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(handlers.onOpen).not.toHaveBeenCalled();

    transport.close();
  });

  it("retriable HTTP: 503 ⇒ retriable onError (with Retry-After), then reconnect", async () => {
    const s = makeStream();
    let call = 0;
    const fetchImpl = vi.fn(async () => {
      call += 1;
      if (call === 1) {
        return new Response(null, { status: 503, headers: { "retry-after": "2" } });
      }
      return sseResponse(s.stream);
    });
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    await waitFor(() => handlers.onError.mock.calls.length >= 1, "first onError");

    const err = handlers.onError.mock.calls[0]![0] as SseConnectionError;
    expect(err.retriable).toBe(true);
    expect(err.kind).toBe("http");
    expect(err.status).toBe(503);
    expect(err.retryAfterMs).toBe(2000); // "2" seconds → ms

    // The reconnect happens and opens successfully.
    await waitFor(() => handlers.onOpen.mock.calls.length >= 1, "reconnect opens");
    expect(fetchImpl.mock.calls.length).toBeGreaterThanOrEqual(2);

    transport.close();
    s.close();
  });

  it("wrong content-type: 200 text/plain ⇒ fatal content-type error", async () => {
    const fetchImpl = vi.fn(
      async () =>
        new Response("not an event stream", {
          status: 200,
          headers: { "content-type": "text/plain" },
        }),
    );
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    await waitFor(() => handlers.onError.mock.calls.length >= 1, "onError");
    await tick();

    const err = handlers.onError.mock.calls[0]![0] as SseConnectionError;
    expect(err.kind).toBe("content-type");
    expect(err.retriable).toBe(false);
    expect(err.received).toBe("text/plain");
    expect(handlers.onError).toHaveBeenCalledTimes(1);
    expect(handlers.onOpen).not.toHaveBeenCalled();
    expect(fetchImpl).toHaveBeenCalledTimes(1);

    transport.close();
  });

  it("close() mid-stream: no further onFrame, and the in-flight fetch is aborted", async () => {
    const s = makeStream();
    const fetchImpl = vi.fn(async () => sseResponse(s.stream));
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.start();
    s.push("event: chat\ndata: 1\n\n");
    await waitFor(() => handlers.onFrame.mock.calls.length >= 1, "first frame");

    const signal = initAt(fetchImpl, 0).signal as AbortSignal;
    expect(signal.aborted).toBe(false);

    transport.close();
    s.push("event: chat\ndata: 2\n\n"); // arrives after close ⇒ must be ignored
    await tick();
    await tick();

    expect(handlers.onFrame).toHaveBeenCalledTimes(1);
    expect(signal.aborted).toBe(true);
    expect(handlers.onClose).not.toHaveBeenCalled();
    // No s.close() here: the early return inside the stream loop already cancelled
    // res.body (the async-iterator's default cancel-on-return), tearing the stream down.
  });

  it("retries exhausted ⇒ a final FATAL onError after the retriable ones", async () => {
    const fetchImpl = vi.fn(async () => new Response(null, { status: 503 }));
    const handlers = makeHandlers();
    const transport = fetchTransport(
      makeConfig(asFetch(fetchImpl), { reconnect: { retries: 2, backoffMs: () => 0 } }),
      handlers as TransportHandlers,
    );

    transport.start();
    await waitFor(
      () => handlers.onError.mock.calls.some((c) => (c[0] as SseConnectionError).retriable === false),
      "fatal onError",
    );
    await tick();

    const errors = handlers.onError.mock.calls.map((c) => c[0] as SseConnectionError);
    const fatal = errors[errors.length - 1]!;
    expect(fatal.retriable).toBe(false);
    expect(fatal.kind).toBe("http");
    expect(fatal.status).toBe(503);
    // initial attempt + 2 retries = 3 fetches; the first 2 failures were retriable.
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(errors.filter((e) => e.retriable).length).toBe(3);

    transport.close();
  });

  it("close() is idempotent and start() after close is a no-op", async () => {
    const s = makeStream();
    const fetchImpl = vi.fn(async () => sseResponse(s.stream));
    const handlers = makeHandlers();
    const transport = fetchTransport(makeConfig(asFetch(fetchImpl)), handlers as TransportHandlers);

    transport.close();
    transport.close(); // second close must not throw
    transport.start(); // no-op after close
    await tick();

    expect(fetchImpl).not.toHaveBeenCalled();
    expect(handlers.onOpen).not.toHaveBeenCalled();
  });
});
