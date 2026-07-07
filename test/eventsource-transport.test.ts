import { describe, it, expect, vi } from "vitest";
import { eventSourceTransport } from "../src/client/eventsource-transport.js";
import type {
  EventSourceCtor,
  EventSourceLike,
  MessageEventLike,
  TransportConfig,
} from "../src/client/types.js";
import { SseConnectionError } from "../src/client/errors.js";
import type { RawFrame } from "../src/core/wire.js";

/**
 * A fully in-memory EventSource. It records the constructor args + every listener so a
 * test can drive lifecycle/data/error events by hand and assert what the transport did.
 * `readyState` is mutable so a test can simulate CONNECTING (0) vs CLOSED (2) at error time.
 */
class FakeEventSource implements EventSourceLike {
  /** Every instance ever constructed, newest last — the test grabs `instances[0]`. */
  static instances: FakeEventSource[] = [];

  readonly url: string;
  readonly init: { withCredentials?: boolean } | undefined;
  readyState = 0; // CONNECTING
  closeCount = 0;
  private readonly listeners = new Map<string, Array<(ev: MessageEventLike) => void>>();

  constructor(url: string, init?: { withCredentials?: boolean }) {
    this.url = url;
    this.init = init;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (ev: MessageEventLike) => void): void {
    const arr = this.listeners.get(type) ?? [];
    arr.push(listener);
    this.listeners.set(type, arr);
  }

  close(): void {
    this.readyState = 2; // CLOSED
    this.closeCount += 1;
  }

  // ---- test helpers ----

  hasListener(type: string): boolean {
    return (this.listeners.get(type)?.length ?? 0) > 0;
  }

  /** Fire every listener registered for `type` with a synthetic message event. */
  emit(type: string, opts: { data?: string; lastEventId?: string } = {}): void {
    const ev: MessageEventLike = {
      type,
      data: opts.data ?? "",
      lastEventId: opts.lastEventId ?? "",
    };
    for (const listener of this.listeners.get(type) ?? []) listener(ev);
  }

  /** Fire the opaque native "error" event (its listener ignores the event object). */
  emitError(): void {
    this.emit("error");
  }
}

function makeConfig(overrides: Partial<TransportConfig> = {}): TransportConfig {
  return {
    url: "https://example.test/sse?room=1",
    method: "GET",
    headers: {},
    eventNames: ["chat", "presence"],
    withCredentials: true,
    reconnect: false,
    fetchImpl: fetch,
    EventSourceImpl: FakeEventSource as unknown as EventSourceCtor,
    ...overrides,
  };
}

function makeHandlers() {
  return {
    onOpen: vi.fn<() => void>(),
    onFrame: vi.fn<(frame: RawFrame) => void>(),
    onError: vi.fn<(err: SseConnectionError) => void>(),
    onClose: vi.fn<() => void>(),
  };
}

/** Reset the instance registry, build config + spy handlers, and start the transport. */
function boot(overrides: Partial<TransportConfig> = {}) {
  FakeEventSource.instances.length = 0;
  const config = makeConfig(overrides);
  const handlers = makeHandlers();
  const transport = eventSourceTransport(config, handlers);
  transport.start();
  return { config, handlers, transport };
}

describe("eventSourceTransport — construction", () => {
  it("builds one EventSource with the resolved url + withCredentials", () => {
    const { config } = boot();
    expect(FakeEventSource.instances).toHaveLength(1);
    const es = FakeEventSource.instances[0]!;
    expect(es.url).toBe(config.url);
    expect(es.init).toEqual({ withCredentials: true });
  });

  it("registers a listener for each event name plus open, message, error, and ts-sse-eos", () => {
    boot({ eventNames: ["chat", "presence"] });
    const es = FakeEventSource.instances[0]!;
    for (const name of ["chat", "presence", "message", "open", "error", "ts-sse-eos"]) {
      expect(es.hasListener(name)).toBe(true);
    }
  });
});

describe("eventSourceTransport — data plane", () => {
  it("fires onOpen when the source emits \"open\"", () => {
    const { handlers } = boot();
    FakeEventSource.instances[0]!.emit("open");
    expect(handlers.onOpen).toHaveBeenCalledTimes(1);
  });

  it("maps a named event to a RawFrame carrying event, data, and id", () => {
    const { handlers } = boot();
    FakeEventSource.instances[0]!.emit("chat", { data: '{"t":"hi"}', lastEventId: "9" });
    expect(handlers.onFrame).toHaveBeenCalledTimes(1);
    expect(handlers.onFrame).toHaveBeenCalledWith({ event: "chat", data: '{"t":"hi"}', id: "9" });
  });

  it("omits frame.id (undefined, not \"\") when lastEventId is empty", () => {
    const { handlers } = boot();
    FakeEventSource.instances[0]!.emit("message", { data: "hello", lastEventId: "" });
    expect(handlers.onFrame).toHaveBeenCalledTimes(1);
    const frame = handlers.onFrame.mock.calls[0]![0];
    expect(frame).toEqual({ event: "message", data: "hello" });
    expect("id" in frame).toBe(false);
  });
});

describe("eventSourceTransport — terminal EOS", () => {
  it("treats ts-sse-eos as a clean close, closes the source, and drops later frames", () => {
    const { handlers } = boot();
    const es = FakeEventSource.instances[0]!;

    es.emit("ts-sse-eos", { data: "" });

    expect(handlers.onClose).toHaveBeenCalledTimes(1);
    expect(es.closeCount).toBe(1);
    expect(es.readyState).toBe(2);

    // Anything arriving after the terminal sentinel is ignored.
    es.emit("chat", { data: "late" });
    expect(handlers.onFrame).not.toHaveBeenCalled();
  });
});

describe("eventSourceTransport — opaque native errors", () => {
  it("reports a RETRIABLE onError while CONNECTING (readyState 0)", () => {
    const { handlers } = boot();
    const es = FakeEventSource.instances[0]!;
    es.readyState = 0; // CONNECTING — the browser will auto-retry
    es.emitError();

    expect(handlers.onError).toHaveBeenCalledTimes(1);
    const err = handlers.onError.mock.calls[0]![0];
    expect(err).toBeInstanceOf(SseConnectionError);
    expect(err.kind).toBe("network");
    expect(err.retriable).toBe(true);
    // A native error is never a clean end.
    expect(handlers.onClose).not.toHaveBeenCalled();
  });

  it("reports a FATAL onError while CLOSED (readyState 2)", () => {
    const { handlers } = boot();
    const es = FakeEventSource.instances[0]!;
    es.readyState = 2; // CLOSED — the browser has given up
    es.emitError();

    expect(handlers.onError).toHaveBeenCalledTimes(1);
    const err = handlers.onError.mock.calls[0]![0];
    expect(err).toBeInstanceOf(SseConnectionError);
    expect(err.kind).toBe("network");
    expect(err.retriable).toBe(false);
    expect(handlers.onClose).not.toHaveBeenCalled();
  });
});

describe("eventSourceTransport — missing implementation", () => {
  it("emits a fatal onError and constructs nothing when EventSourceImpl is undefined", () => {
    const { handlers } = boot({ EventSourceImpl: undefined });
    expect(FakeEventSource.instances).toHaveLength(0);
    expect(handlers.onError).toHaveBeenCalledTimes(1);
    const err = handlers.onError.mock.calls[0]![0];
    expect(err).toBeInstanceOf(SseConnectionError);
    expect(err.kind).toBe("network");
    expect(err.retriable).toBe(false);
    expect(err.message).toMatch(/No EventSource/);
  });
});

describe("eventSourceTransport — close()", () => {
  it("ignores every late listener callback after close(), and is idempotent", () => {
    const { handlers, transport } = boot();
    const es = FakeEventSource.instances[0]!;

    transport.close();
    expect(es.closeCount).toBe(1);

    // Native events racing in after teardown must reach no handler.
    es.emit("open");
    es.emit("chat", { data: "x" });
    es.emitError();
    es.emit("ts-sse-eos");
    expect(handlers.onOpen).not.toHaveBeenCalled();
    expect(handlers.onFrame).not.toHaveBeenCalled();
    expect(handlers.onError).not.toHaveBeenCalled();
    expect(handlers.onClose).not.toHaveBeenCalled();

    // A second close() is a no-op — the source is not closed twice.
    transport.close();
    expect(es.closeCount).toBe(1);
  });
});
