import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { createServer, type Server } from "node:http";
import { EventSource } from "eventsource";
import { z } from "zod";
import { defineSse } from "../../src/sse/contract.js";
import { toNodeHandler } from "../../src/sse/node/index.js";
import { initClient } from "../../src/sse/client/index.js";
import type { EventSourceCtor } from "../../src/sse/client/types.js";

const counter = defineSse({ events: { n: z.object({ i: z.number() }) } });

let server: Server;
let base: string;
let connects = 0;
let resumedFrom: string | undefined = "UNSET";

beforeAll(async () => {
  const handler = toNodeHandler(counter, async ({ lastEventId, emit, init }) => {
    connects += 1;
    init({ retry: 100 }); // reconnect fast (real EventSource honors `retry:`)

    if (lastEventId === undefined) {
      // First connection: emit 1, 2 (with ids), then DROP without EOS.
      // A thrown handler closes the stream with no sentinel → the browser reconnects.
      await emit.n({ i: 1 }, { id: "1" });
      await emit.n({ i: 2 }, { id: "2" });
      throw new Error("simulated mid-stream drop");
    }

    // Reconnect: the real EventSource re-sent `Last-Event-ID: 2`. Resume from there.
    resumedFrom = lastEventId;
    const from = Number(lastEventId);
    await emit.n({ i: from + 1 }, { id: String(from + 1) }); // i:3, id:3
    emit.close(); // terminal EOS → the client iterator completes
  });
  server = createServer((req, res) => handler(req, res));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  base = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}/`;
});

afterAll(() => new Promise<void>((resolve) => server.close(() => resolve())));

describe("SSE resume end-to-end (real drop → reconnect with Last-Event-ID)", () => {
  it("resumes across a reconnect and delivers the full sequence", async () => {
    const client = initClient(counter, {
      url: base,
      EventSource: EventSource as unknown as EventSourceCtor,
    });
    const got: number[] = [];
    for await (const ev of client.subscribe()) {
      got.push(ev.data.i); // iterator keeps yielding across the retriable drop
    }

    expect(got).toEqual([1, 2, 3]); // no gap, no dupe across the reconnect
    expect(resumedFrom).toBe("2"); // server saw the browser's Last-Event-ID
    expect(connects).toBe(2); // exactly one reconnect
  });
});
