import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineSse } from "../src/core/contract.js";
import { parseSseStream } from "../src/core/parse.js";
import { sseResponse } from "../src/server/response.js";
import type { RawFrame } from "../src/core/wire.js";

/** Drain a Response's SSE body into decoded frames (comments/heartbeats are skipped). */
const collect = async (res: Response): Promise<RawFrame[]> => {
  const frames: RawFrame[] = [];
  for await (const f of parseSseStream(res.body!)) frames.push(f);
  return frames;
};
/** The raw bytes as text (keeps comments — needed to see heartbeats). */
const rawText = async (res: Response): Promise<string> =>
  new TextDecoder().decode(new Uint8Array(await res.arrayBuffer()));

const chat = defineSse({
  events: { chat: z.object({ text: z.string() }), presence: z.object({ online: z.number() }) },
});
const req = (url = "https://x.test/s"): Request => new Request(url);

describe("sseResponse — lifecycle", () => {
  it("streams events then the EOS sentinel on normal return", async () => {
    const res = sseResponse(chat, req(), async ({ emit }) => {
      await emit.presence({ online: 2 });
      await emit.chat({ text: "hi" }, { id: "1" });
    });
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    const frames = await collect(res);
    expect(frames.map((f) => f.event)).toEqual(["presence", "chat", "ts-sse-eos"]);
    expect(frames[1]).toMatchObject({ event: "chat", data: '{"text":"hi"}', id: "1" });
  });

  it("closes WITHOUT the sentinel when the handler throws (→ client reconnects)", async () => {
    const res = sseResponse(chat, req(), async ({ emit }) => {
      await emit.chat({ text: "hi" });
      throw new Error("boom");
    });
    const frames = await collect(res);
    expect(frames.map((f) => f.event)).toEqual(["chat"]); // no ts-sse-eos
  });

  it("emit.close() sends an early EOS and ends the stream", async () => {
    const res = sseResponse(chat, req(), async ({ emit }) => {
      await emit.presence({ online: 1 });
      await emit.close();
      await emit.chat({ text: "never" }).catch(() => {}); // after close: rejects
    });
    const frames = await collect(res);
    expect(frames.map((f) => f.event)).toEqual(["presence", "ts-sse-eos"]);
  });
});

describe("sseResponse — the decoder invariant on the wire", () => {
  it("serializes the schema INPUT, not the transformed output", async () => {
    const c = defineSse({ events: { n: z.string().transform((s) => s.length) } });
    const res = sseResponse(c, req(), async ({ emit }) => {
      await emit.n("hello");
    });
    const frames = await collect(res);
    expect(frames[0]?.data).toBe('"hello"'); // INPUT on the wire, not 5
  });

  it("a bad emit (input fails validation) crashes the stream with no EOS", async () => {
    const res = sseResponse(chat, req(), async ({ emit }) => {
      // @ts-expect-error wrong shape on purpose
      await emit.chat({ text: 123 });
    });
    const frames = await collect(res);
    expect(frames).toEqual([]); // threw before any write; no EOS
  });
});

describe("sseResponse — query", () => {
  const q = defineSse({
    query: z.object({ since: z.coerce.number() }),
    events: { chat: z.object({ text: z.string() }) },
  });

  it("validates + coerces the query string into ctx.query", async () => {
    const res = sseResponse(q, req("https://x.test/s?since=100"), async ({ query, emit }) => {
      await emit.chat({ text: String(query.since) });
    });
    const frames = await collect(res);
    expect(frames[0]?.data).toBe('{"text":"100"}'); // coerced to the number 100
  });

  it("returns 400 (before streaming) on an invalid query", async () => {
    const res = sseResponse(q, req("https://x.test/s"), async () => {}); // `since` missing
    expect(res.status).toBe(400);
    expect(res.headers.get("content-type")).toContain("application/json");
  });
});

describe("sseResponse — heartbeat", () => {
  it("writes a :keep-alive comment after an idle gap", async () => {
    const res = sseResponse(chat, req(), async ({ emit, init }) => {
      init({ heartbeat: 10 });
      await emit.presence({ online: 1 });
      await new Promise((r) => setTimeout(r, 40)); // idle → beats fire
    });
    const text = await rawText(res);
    expect(text).toContain(": keep-alive");
  });
});
