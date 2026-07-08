import { describe, it, expect, vi } from "vitest";
import { z } from "zod";
import { connectWs } from "../src/ws/client/client.js";
import { defineWs } from "../src/ws/contract.js";
import type {
  CloseEventLike,
  ConnectWsOptions,
  MessageEventLike,
  WebSocketLike,
} from "../src/ws/client/types.js";

// ── Fake WebSocket ──────────────────────────────────────────────────────────
// Records url/protocols and captured sends, exposes mutable readyState/bufferedAmount,
// stores listeners, and lets the test drive open/message/close/error by hand. Each
// constructed instance is pushed to a static array so a test can grab the latest — the
// reconnect path constructs a NEW instance, which is exactly what we assert on.

class FakeWebSocket implements WebSocketLike {
  static instances: FakeWebSocket[] = [];
  static last(): FakeWebSocket {
    const inst = FakeWebSocket.instances[FakeWebSocket.instances.length - 1];
    if (!inst) throw new Error("no FakeWebSocket has been constructed");
    return inst;
  }
  static reset(): void {
    FakeWebSocket.instances = [];
  }

  readonly url: string;
  readonly protocols?: string | string[];
  readyState = 0; // CONNECTING
  bufferedAmount = 0;
  readonly sent: string[] = [];
  readonly closeCalls: Array<{ code?: number; reason?: string }> = [];

  private readonly listeners: {
    open: Array<() => void>;
    message: Array<(ev: MessageEventLike) => void>;
    close: Array<(ev: CloseEventLike) => void>;
    error: Array<(ev: unknown) => void>;
  } = { open: [], message: [], close: [], error: [] };

  constructor(url: string, protocols?: string | string[]) {
    this.url = url;
    this.protocols = protocols;
    FakeWebSocket.instances.push(this);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    this.closeCalls.push({ code, reason });
    this.readyState = 3; // CLOSED
    this.emitClose(code ?? 1000, reason ?? "");
  }

  addEventListener(type: "open", cb: () => void): void;
  addEventListener(type: "message", cb: (ev: MessageEventLike) => void): void;
  addEventListener(type: "close", cb: (ev: CloseEventLike) => void): void;
  addEventListener(type: "error", cb: (ev: unknown) => void): void;
  addEventListener(type: string, cb: (ev: never) => void): void {
    (this.listeners[type as keyof FakeWebSocket["listeners"]] as Array<unknown>).push(cb);
  }

  // ── Test drivers ──────────────────────────────────────────────────────────
  emitOpen(): void {
    this.readyState = 1; // OPEN
    for (const cb of this.listeners.open) cb();
  }
  emitMessage(data: string | ArrayBuffer): void {
    for (const cb of this.listeners.message) cb({ data });
  }
  emitClose(code: number, reason: string): void {
    for (const cb of this.listeners.close) cb({ code, reason });
  }
  emitError(): void {
    for (const cb of this.listeners.error) cb(new Error("ws error"));
  }
}

/** Flush pending macrotasks so a scheduled reconnect timer (backoff 0) can run. */
const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

// A `client` schema whose transform makes INPUT ≠ OUTPUT — proves the wire carries the
// INPUT (lowercase) rather than the transformed OUTPUT (uppercase).
const contract = defineWs({
  client: z.object({ text: z.string().transform((s) => s.toUpperCase()) }),
  server: z.object({ kind: z.literal("chat"), text: z.string() }),
});

function connect(extra?: Partial<ConnectWsOptions>) {
  FakeWebSocket.reset();
  const client = connectWs(contract, {
    url: "wss://x.test/socket",
    WebSocket: FakeWebSocket,
    reconnect: { retries: Number.POSITIVE_INFINITY, backoffMs: () => 0 },
    ...extra,
  });
  return { client, socket: FakeWebSocket.last() };
}

// ── Wiring ──────────────────────────────────────────────────────────────────

describe("connectWs — wiring", () => {
  it("throws a clear error when no WebSocket implementation is available", () => {
    const globals = globalThis as { WebSocket?: unknown };
    const had = "WebSocket" in globals;
    const prev = globals.WebSocket;
    delete globals.WebSocket;
    try {
      expect(() => connectWs(contract, { url: "wss://x.test/socket" })).toThrow(
        /no WebSocket implementation/,
      );
    } finally {
      if (had) globals.WebSocket = prev;
    }
  });

  it("passes url + protocols to the injected constructor", () => {
    FakeWebSocket.reset();
    connectWs(contract, {
      url: "wss://x.test/socket",
      protocols: ["chat", "v2"],
      WebSocket: FakeWebSocket,
      reconnect: false,
    });
    const socket = FakeWebSocket.last();
    expect(socket.url).toBe("wss://x.test/socket");
    expect(socket.protocols).toEqual(["chat", "v2"]);
  });
});

// ── Open ──────────────────────────────────────────────────────────────────────

describe("connectWs — open", () => {
  it("open fires onOpen and moves state connecting → open", () => {
    const { client, socket } = connect();
    const onOpen = vi.fn();
    client.onOpen(onOpen);

    expect(client.state).toBe("connecting");
    socket.emitOpen();

    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(client.state).toBe("open");
  });
});

// ── Send ────────────────────────────────────────────────────────────────────

describe("connectWs — send", () => {
  it("captures the encoded INPUT JSON after open (wire carries INPUT, not OUTPUT)", async () => {
    const { client, socket } = connect();
    socket.emitOpen();

    await client.send({ text: "hi" });

    // The transform uppercases the OUTPUT, but the wire must carry the INPUT "hi".
    expect(socket.sent).toEqual(['{"text":"hi"}']);
  });

  it("rejects when the message fails its client schema", async () => {
    const { client, socket } = connect();
    socket.emitOpen();

    await expect(client.send({ text: 123 as unknown as string })).rejects.toThrow();
    expect(socket.sent).toEqual([]);
  });

  it("queues sends while disconnected and flushes them on the next open", async () => {
    const { client, socket } = connect();

    expect(client.state).toBe("connecting");
    await client.send({ text: "queued" });
    expect(socket.sent).toEqual([]); // nothing sent while not open

    socket.emitOpen();
    expect(socket.sent).toEqual(['{"text":"queued"}']); // flushed on open
  });
});

// ── Receive ─────────────────────────────────────────────────────────────────

describe("connectWs — receive", () => {
  it("delivers the decoded OUTPUT to onMessage and the async iterator", async () => {
    const { client, socket } = connect();
    socket.emitOpen();

    const seen: unknown[] = [];
    client.onMessage((m) => seen.push(m));

    const pull = client[Symbol.asyncIterator]().next();
    socket.emitMessage(JSON.stringify({ kind: "chat", text: "hi" }));

    expect(seen).toEqual([{ kind: "chat", text: "hi" }]);

    const result = await pull;
    expect(result).toEqual({ value: { kind: "chat", text: "hi" }, done: false });
  });

  it("runs the server schema INPUT→OUTPUT (a non-round-tripping transform decodes)", async () => {
    FakeWebSocket.reset();
    const nContract = defineWs({
      server: z.object({ text: z.string().transform((s) => s.length) }),
    });
    const client = connectWs(nContract, {
      url: "wss://x.test/socket",
      WebSocket: FakeWebSocket,
      reconnect: false,
    });
    const socket = FakeWebSocket.last();
    socket.emitOpen();

    const seen: unknown[] = [];
    client.onMessage((m) => seen.push(m));

    socket.emitMessage(JSON.stringify({ text: "hello" })); // wire carries the INPUT string
    expect(seen).toEqual([{ text: 5 }]); // OUTPUT is its length
  });

  it("decodes a binary (ArrayBuffer) frame via TextDecoder", async () => {
    const { client, socket } = connect();
    socket.emitOpen();

    const seen: unknown[] = [];
    client.onMessage((m) => seen.push(m));

    const bytes = new TextEncoder().encode(JSON.stringify({ kind: "chat", text: "bin" }));
    socket.emitMessage(bytes.buffer);

    expect(seen).toEqual([{ kind: "chat", text: "bin" }]);
  });
});

// ── Validation ──────────────────────────────────────────────────────────────

describe("connectWs — validation", () => {
  it("mode 'emit': a bad frame reaches onValidationError but is NOT yielded", async () => {
    const { client, socket } = connect({ onValidationError: "emit" });
    socket.emitOpen();

    const errors: Array<{ raw: string }> = [];
    client.onValidationError((err) => errors.push(err));

    const pull = client[Symbol.asyncIterator]().next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    socket.emitMessage("not json");
    await tick();

    expect(errors).toHaveLength(1);
    expect(errors[0]?.raw).toBe("not json");
    expect(settled).toBe(false); // the iterator never saw the bad frame
  });

  it("mode 'skip' (default): a bad frame is silently dropped", async () => {
    const { client, socket } = connect();
    socket.emitOpen();

    const errors: unknown[] = [];
    client.onValidationError((err) => errors.push(err));

    const pull = client[Symbol.asyncIterator]().next();
    let settled = false;
    void pull.then(
      () => (settled = true),
      () => (settled = true),
    );

    socket.emitMessage(JSON.stringify({ kind: "chat", text: 123 })); // text must be string
    await tick();

    expect(errors).toHaveLength(0); // dropped, no listener notified
    expect(settled).toBe(false);
  });

  it("mode 'throw': a bad frame rejects the async iterator", async () => {
    const { client, socket } = connect({ onValidationError: "throw" });
    socket.emitOpen();

    const pull = client[Symbol.asyncIterator]().next();
    socket.emitMessage("not json");

    await expect(pull).rejects.toMatchObject({ raw: "not json" });
  });
});

// ── Reconnect ───────────────────────────────────────────────────────────────

describe("connectWs — reconnect", () => {
  it("an unexpected close fires onClose, enters 'reconnecting', and opens a new socket", async () => {
    const { client, socket } = connect();
    socket.emitOpen();
    expect(client.state).toBe("open");

    const onClose = vi.fn();
    client.onClose(onClose);

    socket.emitClose(1006, "drop");

    expect(onClose).toHaveBeenCalledWith(1006, "drop");
    expect(client.state).toBe("reconnecting");
    expect(FakeWebSocket.instances).toHaveLength(1); // not yet reconnected

    await tick(); // backoff 0 + a macrotask
    expect(FakeWebSocket.instances).toHaveLength(2); // a NEW socket was constructed
  });

  it("reconnect: false completes the iterator on close instead of retrying", async () => {
    FakeWebSocket.reset();
    const client = connectWs(contract, {
      url: "wss://x.test/socket",
      WebSocket: FakeWebSocket,
      reconnect: false,
    });
    const socket = FakeWebSocket.last();
    socket.emitOpen();

    const pull = client[Symbol.asyncIterator]().next();
    socket.emitClose(1006, "drop");

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });
    expect(client.state).toBe("closed");
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1); // no reconnect
  });
});

// ── Close ───────────────────────────────────────────────────────────────────

describe("connectWs — close", () => {
  it("close() closes the socket, completes the iterator, and does not reconnect", async () => {
    const { client, socket } = connect();
    socket.emitOpen();

    const pull = client[Symbol.asyncIterator]().next();

    client.close(1000, "bye");

    expect(socket.closeCalls).toHaveLength(1);
    expect(socket.readyState).toBe(3);
    expect(client.state).toBe("closed");

    const result = await pull;
    expect(result).toEqual({ value: undefined, done: true });

    // A subsequent close event must NOT trigger a reconnect.
    socket.emitClose(1006, "late");
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
  });
});
