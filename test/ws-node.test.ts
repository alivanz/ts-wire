import { describe, it, expect } from "vitest";
import { z } from "zod";
import { wsSocket } from "../src/ws/node/index.js";
import type { NodeSocket } from "../src/ws/node/index.js";
import { defineWs } from "../src/ws/contract.js";

/** The example bidirectional contract: server sends `message`, client sends `send`. */
const chat = defineWs({
  client: z.object({ type: z.literal("send"), text: z.string() }),
  server: z.object({ type: z.literal("message"), text: z.string() }),
});

/**
 * A fake node `ws` socket. Captures every string it is asked to send, then invokes the
 * flush callback — with `undefined` (success) or, when constructed with an error, that
 * error (a failed flush / backpressure abort).
 */
class FakeSocket implements NodeSocket {
  readonly sent: string[] = [];
  constructor(private readonly err?: Error) {}
  send(data: string, cb?: (err?: Error) => void): void {
    this.sent.push(data);
    if (cb) cb(this.err);
  }
}

const utf8 = new TextEncoder();

describe("wsSocket.send", () => {
  it("resolves once the socket flushes and captures the encoded frame", async () => {
    const fake = new FakeSocket();
    const sock = wsSocket(chat, fake);

    await expect(sock.send({ type: "message", text: "hi" })).resolves.toBeUndefined();
    expect(fake.sent).toEqual(['{"type":"message","text":"hi"}']);
  });

  it("rejects when the flush callback yields an error (backpressure/abort)", async () => {
    const boom = new Error("flush failed");
    const fake = new FakeSocket(boom);
    const sock = wsSocket(chat, fake);

    await expect(sock.send({ type: "message", text: "hi" })).rejects.toBe(boom);
  });

  it("rejects when the outgoing message fails validation, never touching the socket", async () => {
    const fake = new FakeSocket();
    const sock = wsSocket(chat, fake);

    // `text` must be a string — force an invalid payload past the type wall.
    await expect(sock.send({ type: "message", text: 123 } as never)).rejects.toThrow(
      /invalid outgoing message/,
    );
    expect(fake.sent).toEqual([]);
  });

  it("puts the schema INPUT on the wire, not the transformed OUTPUT (decoder invariant)", async () => {
    const contract = defineWs({
      server: z.object({ n: z.string().transform((s) => s.length) }),
    });
    const fake = new FakeSocket();
    const sock = wsSocket(contract, fake);

    await sock.send({ n: "hello" });

    expect(fake.sent).toEqual(['{"n":"hello"}']); // INPUT "hello", not OUTPUT 5
    expect(fake.sent[0]).not.toBe('{"n":5}');
  });
});

describe("wsSocket.decode", () => {
  const sock = wsSocket(chat, new FakeSocket());
  const ok = { ok: true as const, value: { type: "send", text: "hi" } };

  it("decodes a plain string frame", () => {
    expect(sock.decode('{"type":"send","text":"hi"}')).toEqual(ok);
  });

  it("decodes a Uint8Array (Buffer) frame", () => {
    const bytes = utf8.encode('{"type":"send","text":"hi"}');
    expect(sock.decode(bytes)).toEqual(ok);
  });

  it("decodes an ArrayBuffer frame", () => {
    const bytes = utf8.encode('{"type":"send","text":"hi"}');
    const ab = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(ab).set(bytes);
    expect(sock.decode(ab)).toEqual(ok);
  });

  it("joins a fragmented ArrayBufferView[] frame", () => {
    const parts = [utf8.encode('{"type":"send",'), utf8.encode('"text":"hi"}')];
    expect(sock.decode(parts)).toEqual(ok);
  });

  it("returns ok:false for invalid JSON", () => {
    const res = sock.decode("not json {");
    expect(res.ok).toBe(false);
  });

  it("returns ok:false for a schema-failing frame", () => {
    const res = sock.decode('{"type":"nope","text":"hi"}');
    expect(res.ok).toBe(false);
  });
});
