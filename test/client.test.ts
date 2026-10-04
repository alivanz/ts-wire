import { describe, it, expect, vi } from "vitest";
import { z } from "zod";
import { defineSse } from "../src/sse/contract.js";
import { initClient, type ClientDeps } from "../src/sse/client/client.js";
import type {
  EventSourceCtor,
  InitClientOptions,
  Transport,
  TransportConfig,
  TransportFactory,
  TransportHandlers,
} from "../src/sse/client/types.js";
import { SseConnectionError } from "../src/sse/client/errors.js";

// ── Fake transport ──────────────────────────────────────────────────────────
// Captures the `config` and `handlers` each factory call receives and lets the
// test drive `handlers.onOpen()/onFrame()/onError()/onClose()` by hand. No real
// EventSource is ever touched. Because `subscribe()` starts the transport
// SYNCHRONOUSLY, the captured call is available immediately after `subscribe`.

interface FakeCall {
  config: TransportConfig;
  handlers: TransportHandlers;
  start: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
}

function makeFakeTransport(): { transport: TransportFactory; calls: FakeCall[] } {
  const calls: FakeCall[] = [];
  const transport: TransportFactory = (config, handlers): Transport => {
    const start = vi.fn();
    const close = vi.fn();
    calls.push({ config, handlers, start, close });
    return { start, close };
  };
  return { transport, calls };
}

function deps(transport: TransportFactory): ClientDeps {
  return { transport };
}

/** Grab the single captured transport call (started synchronously by `subscribe`). */
function only(calls: FakeCall[]): FakeCall {
  const call = calls[0];
  if (!call) throw new Error("transport was never started");
  return call;
}

/** Flush pending microtasks so a would-be settle of a Promise can be observed. */
const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

const options = (extra?: Partial<InitClientOptions>): InitClientOptions => ({
  url: "https://api.example.com/stream",
  ...extra,
});

// A single-endpoint chat contract reused across most tests.
const chatContract = defineSse({
  events: { chat: z.object({ text: z.string() }) },
});

// ── Decoding ──────────────────────────────────────────────────────────────

describe("initClient — decoding", () => {
  it("delivers a decoded event to .on and to the async iterator with meta", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const seen: Array<{ data: { text: string }; lastEventId?: string }> = [];
    sub.on("chat", (data, meta) => seen.push({ data, lastEventId: meta.lastEventId }));

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next(); // pull BEFORE any push → must wait

    const call = only(fake.calls);
    expect(call.start).toHaveBeenCalledTimes(1);
    call.handlers.onFrame({ event: "chat", data: '{"text":"hi"}', id: "3" });

    // `.on` fired synchronously with the decoded value + meta.
    expect(seen).toEqual([{ data: { text: "hi" }, lastEventId: "3" }]);

    // The iterator yields the same decoded event.
    const result = await pull;
    expect(result.done).toBe(false);
    expect(result.value).toEqual({
      event: "chat",
      data: { text: "hi" },
      id: "3",
      lastEventId: "3",
      retry: undefined,
    });
  });

  it("decodes INPUT→OUTPUT through a non-round-tripping transform", async () => {
    // `n` maps a wire STRING to its length — proves the client runs the schema in the
    // decode direction rather than blind-casting the raw JSON.
    const contract = defineSse({
      events: { n: z.string().transform((str) => str.length) },
    });
    const fake = makeFakeTransport();
    const endpoint = initClient(contract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const pull = sub[Symbol.asyncIterator]().next();

    const call = only(fake.calls);
    call.handlers.onFrame({ event: "n", data: '"hello"' }); // wire carries the INPUT string

    const result = await pull;
    expect(result.value).toMatchObject({ event: "n", data: 5 }); // OUTPUT is its length
  });
});

// ── Query typing + URL building ─────────────────────────────────────────────

describe("initClient — query + URL", () => {
  it("validates the query and appends it to options.url", () => {
    const contract = defineSse({
      query: z.object({ since: z.coerce.number().optional() }),
      events: { chat: z.object({ text: z.string() }) },
    });
    const fake = makeFakeTransport();
    const endpoint = initClient(
      contract,
      { url: "https://x.test/rooms/42/stream" },
      { transport: fake.transport },
    );
    endpoint.subscribe({ query: { since: 100 } });

    expect(only(fake.calls).config.url).toBe("https://x.test/rooms/42/stream?since=100");
  });

  it("throws from subscribe when the query is invalid", () => {
    const contract = defineSse({
      query: z.object({ since: z.number() }),
      events: { chat: z.object({ text: z.string() }) },
    });
    const fake = makeFakeTransport();
    const endpoint = initClient(contract, options(), deps(fake.transport));

    expect(() => endpoint.subscribe({ query: { since: "bad" } as any })).toThrow();
    expect(fake.calls).toHaveLength(0); // never reached the transport
  });
});

// ── Validation ──────────────────────────────────────────────────────────────

describe("initClient — validation", () => {
  it("mode 'emit': a bad frame reaches onValidationError but is NOT yielded", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(
      chatContract,
      options({ onValidationError: "emit" }),
      deps(fake.transport),
    );
    const sub = endpoint.subscribe();

    const errors: Array<{ event: string; raw: string; lastEventId?: string }> = [];
    sub.onValidationError((err) => errors.push(err));

    const pull = sub[Symbol.asyncIterator]().next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    only(fake.calls).handlers.onFrame({ event: "chat", data: '{"text":123}', id: "9" });
    await tick();

    expect(errors).toHaveLength(1);
    expect(errors[0]?.event).toBe("chat");
    expect(errors[0]?.raw).toBe('{"text":123}');
    expect(errors[0]?.lastEventId).toBe("9");
    expect(settled).toBe(false); // the iterator never saw the bad frame
  });

  it("mode 'skip' (default): a bad frame is silently dropped", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const errors: unknown[] = [];
    sub.onValidationError((err) => errors.push(err));

    const pull = sub[Symbol.asyncIterator]().next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    only(fake.calls).handlers.onFrame({ event: "chat", data: '{"text":123}', id: "9" });
    await tick();

    expect(errors).toHaveLength(0); // dropped, no listener notified
    expect(settled).toBe(false);
  });
});

// ── Error + lifecycle channels ──────────────────────────────────────────────

describe("initClient — error & lifecycle channels", () => {
  it("an error raised synchronously during start reaches listeners registered after subscribe", async () => {
    const fatal = new SseConnectionError({ kind: "network", retriable: false, message: "boom" });
    const transport: TransportFactory = (_config, handlers) => ({
      start: () => handlers.onError(fatal),
      close: vi.fn(),
    });
    const sub = initClient(chatContract, options(), deps(transport)).subscribe();

    const connErrors: SseConnectionError[] = [];
    sub.onConnectionError((err) => connErrors.push(err));
    const pull = sub[Symbol.asyncIterator]().next();

    await expect(pull).rejects.toBe(fatal);
    expect(connErrors).toEqual([fatal]);
    expect(sub.state).toBe("closed");
  });

  it("a synchronous start error is dropped if the caller closes first", async () => {
    const fatal = new SseConnectionError({ kind: "network", retriable: false, message: "boom" });
    const transport: TransportFactory = (_config, handlers) => ({
      start: () => handlers.onError(fatal),
      close: vi.fn(),
    });
    const sub = initClient(chatContract, options(), deps(transport)).subscribe();
    const connErrors: SseConnectionError[] = [];
    sub.onConnectionError((err) => connErrors.push(err));
    sub.close();
    await tick();

    expect(connErrors).toEqual([]);
  });

  it("a fatal onError rejects the iterator and closes the state", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const pull = sub[Symbol.asyncIterator]().next();

    const fatal = new SseConnectionError({ kind: "http", retriable: false, status: 404 });
    only(fake.calls).handlers.onError(fatal);

    await expect(pull).rejects.toBe(fatal);
    expect(sub.state).toBe("closed");
  });

  it("a retriable onError fires onConnectionError, sets 'reconnecting', keeps the iterator pending", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const connErrors: SseConnectionError[] = [];
    sub.onConnectionError((err) => connErrors.push(err));

    const pull = sub[Symbol.asyncIterator]().next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    const retriable = new SseConnectionError({ kind: "network", retriable: true });
    only(fake.calls).handlers.onError(retriable);
    await tick();

    expect(connErrors).toEqual([retriable]);
    expect(sub.state).toBe("reconnecting");
    expect(settled).toBe(false); // still yielding — the transport is reconnecting
  });

  it("onClose completes the iterator as done and fires onClose listeners", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const onClose = vi.fn();
    sub.onClose(onClose);

    const pull = sub[Symbol.asyncIterator]().next();

    only(fake.calls).handlers.onClose();

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });
    expect(sub.state).toBe("closed");
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("EOS sentinel completes the iterator and fires onClose", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const onClose = vi.fn();
    sub.onClose(onClose);

    const pull = sub[Symbol.asyncIterator]().next();

    only(fake.calls).handlers.onFrame({ event: "ts-sse-eos", data: "{}" });

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(sub.state).toBe("closed");
  });

  it("close() stops the transport and completes the iterator", async () => {
    const fake = makeFakeTransport();
    const endpoint = initClient(chatContract, options(), deps(fake.transport));
    const sub = endpoint.subscribe();

    const pull = sub[Symbol.asyncIterator]().next();

    const call = only(fake.calls);
    sub.close();

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });
    expect(call.close).toHaveBeenCalledTimes(1);
    expect(sub.state).toBe("closed");
  });
});

// ── Wiring guard ────────────────────────────────────────────────────────────

describe("initClient — wiring", () => {
  it("throws a clear error when the transport is not injected", () => {
    const endpoint = initClient(chatContract, options()); // no deps
    expect(() => endpoint.subscribe()).toThrow(/transport not wired/);
  });

  it("defaults to the global EventSource when none is passed", () => {
    class GlobalEventSource {}
    vi.stubGlobal("EventSource", GlobalEventSource);
    try {
      const fake = makeFakeTransport();
      initClient(chatContract, options(), deps(fake.transport)).subscribe();
      expect(only(fake.calls).config.EventSourceImpl).toBe(GlobalEventSource);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("an injected EventSource wins over the global", () => {
    class GlobalEventSource {}
    class Injected {}
    vi.stubGlobal("EventSource", GlobalEventSource);
    try {
      const fake = makeFakeTransport();
      initClient(
        chatContract,
        options({ EventSource: Injected as unknown as EventSourceCtor }),
        deps(fake.transport),
      ).subscribe();
      expect(only(fake.calls).config.EventSourceImpl).toBe(Injected);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
