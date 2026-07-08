import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineWs } from "../src/ws/contract.js";
import { wsSocket } from "../src/ws/cf/index.js";
import type { CfSocket } from "../src/ws/cf/index.js";

/** A fake CF/standard WebSocket that captures the last string it was told to send. */
class FakeCfSocket implements CfSocket {
  last: string | undefined;
  send(data: string): void {
    this.last = data;
  }
}

const chat = defineWs({
  client: z.discriminatedUnion("type", [z.object({ type: z.literal("send"), text: z.string() })]),
  server: z.discriminatedUnion("type", [
    z.object({ type: z.literal("message"), text: z.string() }),
  ]),
});

describe("wsSocket (ws/cf)", () => {
  it("send() validates + encodes the server INPUT and ws.send()s the JSON", async () => {
    const ws = new FakeCfSocket();
    const socket = wsSocket(chat, ws);

    await expect(socket.send({ type: "message", text: "hi" })).resolves.toBeUndefined();
    expect(ws.last).toBe('{"type":"message","text":"hi"}');
  });

  it("send() rejects when the outgoing message is invalid", async () => {
    const ws = new FakeCfSocket();
    const socket = wsSocket(chat, ws);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await expect(socket.send({ type: "message", text: 123 } as any)).rejects.toThrow();
    expect(ws.last).toBeUndefined(); // never made it to the wire
  });

  it("decode() parses + validates a client frame from a string", () => {
    const ws = new FakeCfSocket();
    const socket = wsSocket(chat, ws);

    const res = socket.decode('{"type":"send","text":"hi"}');
    expect(res).toEqual({ ok: true, value: { type: "send", text: "hi" } });
  });

  it("decode() returns ok:false on invalid JSON", () => {
    const socket = wsSocket(chat, new FakeCfSocket());
    const res = socket.decode("{not json");
    expect(res.ok).toBe(false);
  });

  it("decode() returns ok:false on a schema-failing message", () => {
    const socket = wsSocket(chat, new FakeCfSocket());
    const res = socket.decode('{"type":"nope"}');
    expect(res.ok).toBe(false);
  });

  it("decode() accepts an ArrayBuffer and yields the same value as the string form", () => {
    const socket = wsSocket(chat, new FakeCfSocket());
    const text = '{"type":"send","text":"hi"}';
    const buf = new TextEncoder().encode(text).buffer;

    const fromBuffer = socket.decode(buf);
    const fromString = socket.decode(text);
    expect(fromBuffer).toEqual(fromString);
    expect(fromBuffer).toEqual({ ok: true, value: { type: "send", text: "hi" } });
  });

  it("decoder invariant: the wire carries the INPUT, not the transformed output", async () => {
    const transformed = defineWs({
      server: z.object({ n: z.string().transform((s) => s.length) }),
    });
    const ws = new FakeCfSocket();
    const socket = wsSocket(transformed, ws);

    await socket.send({ n: "hello" });
    expect(ws.last).toBe('{"n":"hello"}'); // the INPUT string, NOT {"n":5}
  });
});
