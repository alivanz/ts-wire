import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { createServer, type Server } from "node:http";
import { EventSource } from "eventsource"; // real network client — Node has no global EventSource
import { z } from "zod";
import { defineSse } from "../../src/sse/contract.js";
import { toNodeHandler } from "../../src/sse/node/index.js";
import { initClient } from "../../src/sse/client/index.js";
import type { EventSourceCtor } from "../../src/sse/client/types.js";

// One shared contract, imported by both ends.
const chat = defineSse({
  query: z.object({ room: z.string() }),
  events: {
    ping: z.object({ t: z.number() }),
    msg: z.object({ id: z.string(), text: z.string() }),
  },
});

let server: Server;
let base: string;

beforeAll(async () => {
  const handler = toNodeHandler(chat, async ({ query, emit }) => {
    await emit.ping({ t: 1 });
    await emit.msg({ id: "1", text: `hello ${query.room}` }, { id: "1" });
    await emit.msg({ id: "2", text: "bye" }, { id: "2" });
    emit.close(); // EOS sentinel → the client iterator completes, no reconnect
  });
  server = createServer((req, res) => handler(req, res));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  base = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}/`;
});

afterAll(() => new Promise<void>((resolve) => server.close(() => resolve())));

describe("SSE end-to-end (real http server + real EventSource)", () => {
  it("streams typed, decoded events and completes on EOS", async () => {
    const client = initClient(chat, {
      url: base,
      EventSource: EventSource as unknown as EventSourceCtor,
    });
    const sub = client.subscribe({ query: { room: "42" } });

    const events: Array<{ event: string; data: unknown; id?: string }> = [];
    for await (const ev of sub) {
      events.push({ event: ev.event, data: ev.data, id: ev.id });
    }

    expect(events.map((e) => e.event)).toEqual(["ping", "msg", "msg"]);
    expect(events[0]?.data).toEqual({ t: 1 }); // decoded, typed
    expect(events[1]?.data).toEqual({ id: "1", text: "hello 42" }); // query flowed server-side
    expect(events[1]?.id).toBe("1"); // Last-Event-ID surfaced
    expect(events[2]?.data).toEqual({ id: "2", text: "bye" });
  });

  it("rejects an invalid query with a 400 before streaming", async () => {
    const res = await fetch(base); // no `room` query → invalid
    expect(res.status).toBe(400);
    await res.body?.cancel();
  });
});
