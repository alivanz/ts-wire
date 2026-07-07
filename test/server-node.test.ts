import { describe, it, expect } from "vitest";
import { EventEmitter } from "node:events";
import { z } from "zod";
import { defineSse } from "../src/core/contract.js";
import { parseSseText } from "../src/core/parse.js";
import { toNodeHandler } from "../src/server/node.js";

const concat = (arr: Uint8Array[]): Uint8Array => {
  const out = new Uint8Array(arr.reduce((n, a) => n + a.length, 0));
  let o = 0;
  for (const a of arr) {
    out.set(a, o);
    o += a.length;
  }
  return out;
};

/** Minimal stand-in for a Node `ServerResponse` (only what NodeSink/toNodeHandler use). */
class FakeRes extends EventEmitter {
  statusCode = 0;
  headers: Record<string, string> = {};
  chunks: Uint8Array[] = [];
  ended = false;
  writeHead(status: number, headers?: Record<string, string>): this {
    this.statusCode = status;
    this.headers = headers ?? {};
    return this;
  }
  write(chunk: Uint8Array): boolean {
    this.chunks.push(chunk);
    return true; // never backpressures in the fake
  }
  end(chunk?: Uint8Array | string): this {
    if (chunk !== undefined) {
      this.chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
    }
    this.ended = true;
    this.emit("close");
    return this;
  }
  text(): string {
    return new TextDecoder().decode(concat(this.chunks));
  }
}

const fakeReq = (url: string): { url: string; headers: Record<string, string> } => ({
  url,
  headers: {},
});
const waitEnd = (res: FakeRes): Promise<void> =>
  new Promise((r) => (res.ended ? r() : res.on("close", () => r())));

const chat = defineSse({ events: { chat: z.object({ text: z.string() }) } });

describe("toNodeHandler", () => {
  it("streams via a ServerResponse and ends with the EOS sentinel", async () => {
    const res = new FakeRes();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    toNodeHandler(chat, async ({ emit }) => {
      await emit.chat({ text: "hi" }, { id: "7" });
    })(fakeReq("/s") as any, res as any);

    await waitEnd(res);
    expect(res.statusCode).toBe(200);
    expect(res.headers["content-type"]).toContain("text/event-stream");
    const frames = parseSseText(res.text());
    expect(frames.map((f) => f.event)).toEqual(["chat", "ts-sse-eos"]);
    expect(frames[0]).toMatchObject({ event: "chat", data: '{"text":"hi"}', id: "7" });
  });

  it("400s on an invalid query without opening the stream", async () => {
    const q = defineSse({
      query: z.object({ token: z.string() }),
      events: { chat: z.object({ text: z.string() }) },
    });
    const res = new FakeRes();
    toNodeHandler(q, async () => {})(fakeReq("/s") as any, res as any);
    await waitEnd(res);
    expect(res.statusCode).toBe(400);
    expect(res.text()).toContain("invalid query");
  });
});
