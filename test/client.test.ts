import { describe, it, expect, vi } from "vitest";
import { z } from "zod";
import { c } from "../src/core/contract.js";
import { initClient, selectTransport, type ClientDeps } from "../src/client/client.js";
import type {
  EventSourceCtor,
  InitClientOptions,
  Transport,
  TransportConfig,
  TransportFactory,
  TransportHandlers,
} from "../src/client/types.js";
import { SseConnectionError } from "../src/client/errors.js";

// ── Fake transport ──────────────────────────────────────────────────────────
// Captures the `config` and `handlers` each factory call receives and lets the
// test drive `handlers.onOpen()/onFrame()/onError()/onClose()` by hand. No real
// fetch/EventSource is ever touched.

interface FakeCall {
  config: TransportConfig;
  handlers: TransportHandlers;
  started: boolean;
  closed: boolean;
}

function makeFakeTransport(): { factory: TransportFactory; calls: FakeCall[] } {
  const calls: FakeCall[] = [];
  const factory: TransportFactory = (config, handlers): Transport => {
    const call: FakeCall = { config, handlers, started: false, closed: false };
    calls.push(call);
    return {
      start() {
        call.started = true;
      },
      close() {
        call.closed = true;
      },
    };
  };
  return { factory, calls };
}

/** Wire the same fake to both slots (the routing itself is unit-tested separately). */
function deps(factory: TransportFactory): ClientDeps {
  return { transports: { eventsource: factory, fetch: factory } };
}

/** Flush pending microtasks/timers so the async transport-start IIFE has run. */
const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

const BASE = "https://api.example.com";
const baseOptions = (extra?: Partial<InitClientOptions>): InitClientOptions => ({
  baseUrl: BASE,
  ...extra,
});

// A minimal, do-nothing EventSource impl — `selectTransport` only checks presence.
class FakeEventSource {
  readyState = 0;
  constructor(_url: string) {}
  addEventListener(): void {}
  close(): void {}
}
const ES = FakeEventSource as unknown as EventSourceCtor;

// A single-route chat contract reused across most tests.
const chatContract = c.router({
  room: c.sse({
    method: "GET",
    path: "/rooms/:id/stream",
    events: { chat: z.object({ text: z.string() }) },
  }),
});

/** Subscribe and return the fake's captured call once the transport has started. */
async function ready(calls: FakeCall[]): Promise<FakeCall> {
  await tick();
  const call = calls[0];
  if (!call) throw new Error("transport was never started");
  return call;
}

// ── Decoding ──────────────────────────────────────────────────────────────

describe("initClient — decoding", () => {
  it("delivers a decoded event to .on and to the async iterator with meta", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const seen: Array<{ data: { text: string }; lastEventId?: string }> = [];
    sub.on("chat", (data, meta) => seen.push({ data, lastEventId: meta.lastEventId }));

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next(); // pull BEFORE any push → must wait

    const call = await ready(fake.calls);
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

  it("buffers a value pushed before the pull arrives", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const call = await ready(fake.calls);
    // Push BEFORE anyone pulls — the value must be buffered.
    call.handlers.onFrame({ event: "chat", data: '{"text":"buffered"}', id: "7" });

    const result = await sub[Symbol.asyncIterator]().next();
    expect(result.value).toMatchObject({ event: "chat", data: { text: "buffered" } });
  });

  it("decodes INPUT→OUTPUT through a non-round-tripping transform", async () => {
    // `n` maps a wire STRING to its length — proves the client runs the schema in the
    // decode direction rather than blind-casting the raw JSON.
    const contract = c.router({
      s: c.sse({
        method: "GET",
        path: "/s",
        events: { n: z.string().transform((str) => str.length) },
      }),
    });
    const fake = makeFakeTransport();
    const client = initClient(contract, baseOptions(), deps(fake.factory));
    const sub = client.s.subscribe();

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();

    const call = await ready(fake.calls);
    call.handlers.onFrame({ event: "n", data: '"hello"' }); // wire carries the INPUT string

    const result = await pull;
    expect(result.value).toMatchObject({ event: "n", data: 5 }); // OUTPUT is its length
  });
});

// ── Validation ──────────────────────────────────────────────────────────────

describe("initClient — validation", () => {
  it("mode 'emit': a bad frame reaches onValidationError but is NOT yielded", async () => {
    const fake = makeFakeTransport();
    const client = initClient(
      chatContract,
      baseOptions({ onValidationError: "emit" }),
      deps(fake.factory),
    );
    const sub = client.room.subscribe({ params: { id: "1" } });

    const errors: Array<{ event: string; raw: string; lastEventId?: string }> = [];
    sub.onValidationError((err) => errors.push(err));

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    const call = await ready(fake.calls);
    call.handlers.onFrame({ event: "chat", data: '{"text":123}', id: "9" }); // text must be string
    await tick();

    expect(errors).toHaveLength(1);
    expect(errors[0]?.event).toBe("chat");
    expect(errors[0]?.raw).toBe('{"text":123}');
    expect(errors[0]?.lastEventId).toBe("9");
    expect(settled).toBe(false); // the iterator never saw the bad frame
  });

  it("mode 'skip' (default): a bad frame is silently dropped", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const errors: unknown[] = [];
    sub.onValidationError((err) => errors.push(err));

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    const call = await ready(fake.calls);
    call.handlers.onFrame({ event: "chat", data: '{"text":123}', id: "9" });
    await tick();

    expect(errors).toHaveLength(0); // dropped, no listener notified
    expect(settled).toBe(false);
  });
});

// ── URL building ──────────────────────────────────────────────────────────

describe("initClient — request building", () => {
  it("substitutes path params and appends the query string", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    client.room.subscribe({ params: { id: "42" }, query: { since: 100 } });

    const call = await ready(fake.calls);
    expect(call.config.url).toBe("https://api.example.com/rooms/42/stream?since=100");
  });
});

// ── Transport selection ─────────────────────────────────────────────────────

describe("selectTransport", () => {
  it("auto: GET with no headers/body/resume and an EventSource impl → eventsource", () => {
    expect(selectTransport("GET", undefined, baseOptions({ EventSource: ES }), false)).toBe(
      "eventsource",
    );
  });

  it("auto: a POST route → fetch", () => {
    expect(selectTransport("POST", undefined, baseOptions({ EventSource: ES }), false)).toBe(
      "fetch",
    );
  });

  it("auto: a GET that resumes → fetch", () => {
    expect(
      selectTransport("GET", { resumeFrom: "10" }, baseOptions({ EventSource: ES }), false),
    ).toBe("fetch");
  });

  it("auto: custom headers force fetch (EventSource cannot set them)", () => {
    expect(selectTransport("GET", undefined, baseOptions({ EventSource: ES }), true)).toBe("fetch");
  });

  it("auto: no EventSource impl → fetch", () => {
    expect(selectTransport("GET", undefined, baseOptions(), false)).toBe("fetch");
  });

  it("explicit transport:'fetch' overrides an otherwise-eventsource request", () => {
    expect(
      selectTransport("GET", undefined, baseOptions({ EventSource: ES, transport: "fetch" }), false),
    ).toBe("fetch");
  });
});

// ── Error + lifecycle channels ──────────────────────────────────────────────

describe("initClient — error & lifecycle channels", () => {
  it("a fatal onError rejects the iterator and closes the state", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();

    const call = await ready(fake.calls);
    const fatal = new SseConnectionError({ kind: "http", retriable: false, status: 404 });
    call.handlers.onError(fatal);

    await expect(pull).rejects.toBe(fatal);
    expect(sub.state).toBe("closed");
  });

  it("a retriable onError fires onConnectionError, sets 'reconnecting', keeps the iterator pending", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const connErrors: SseConnectionError[] = [];
    sub.onConnectionError((err) => connErrors.push(err));

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    const call = await ready(fake.calls);
    const retriable = new SseConnectionError({ kind: "network", retriable: true });
    call.handlers.onError(retriable);
    await tick();

    expect(connErrors).toEqual([retriable]);
    expect(sub.state).toBe("reconnecting");
    expect(settled).toBe(false); // still yielding — the transport is reconnecting
  });

  it("onClose completes the iterator as done and fires onClose listeners", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const onClose = vi.fn();
    sub.onClose(onClose);

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();

    const call = await ready(fake.calls);
    call.handlers.onClose();

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });
    expect(sub.state).toBe("closed");
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("close() stops the transport and completes the iterator", async () => {
    const fake = makeFakeTransport();
    const client = initClient(chatContract, baseOptions(), deps(fake.factory));
    const sub = client.room.subscribe({ params: { id: "1" } });

    const iterator = sub[Symbol.asyncIterator]();
    const pull = iterator.next();

    const call = await ready(fake.calls);
    sub.close();

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });
    expect(call.closed).toBe(true);
    expect(sub.state).toBe("closed");
  });
});

// ── Wiring guard ────────────────────────────────────────────────────────────

describe("initClient — wiring", () => {
  it("throws a clear error when transports are not injected", () => {
    const client = initClient(chatContract, baseOptions()); // no deps
    expect(() => client.room.subscribe({ params: { id: "1" } })).toThrow(/transports not wired/);
  });
});
