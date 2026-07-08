import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { WebSocketServer, type WebSocket as WsWebSocket } from "ws";
import { z } from "zod";
import { defineWs } from "../../src/ws/contract.js";
import type { ServerOutput } from "../../src/ws/contract.js";
import { wsSocket } from "../../src/ws/node/index.js";
import { connectWs } from "../../src/ws/client/index.js";

const chat = defineWs({
  client: z.discriminatedUnion("type", [z.object({ type: z.literal("say"), text: z.string() })]),
  server: z.discriminatedUnion("type", [
    z.object({ type: z.literal("welcome"), online: z.number() }),
    z.object({ type: z.literal("echo"), text: z.string() }),
  ]),
});

let wss: WebSocketServer;
let url: string;

beforeAll(async () => {
  wss = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  wss.on("connection", (ws: WsWebSocket) => {
    const sock = wsSocket(chat, ws); // ts-wire/ws/node — the codec bound to this socket
    void sock.send({ type: "welcome", online: 1 });
    ws.on("message", (raw) => {
      const msg = sock.decode(raw); // typed client union, validated
      if (msg.ok && msg.value.type === "say") {
        void sock.send({ type: "echo", text: msg.value.text.toUpperCase() });
      }
    });
  });
  await new Promise<void>((resolve) => wss.on("listening", () => resolve()));
  const addr = wss.address();
  url = `ws://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`;
});

afterAll(() => new Promise<void>((resolve) => wss.close(() => resolve())));

describe("WebSocket end-to-end (real ws server + real global WebSocket)", () => {
  it("does a typed bidirectional round-trip", async () => {
    const client = connectWs(chat, { url }); // uses Node 22's global WebSocket
    client.onOpen(() => {
      void client.send({ type: "say", text: "hi" }); // client → server
    });

    const received: ServerOutput<typeof chat>[] = [];
    for await (const msg of client) {
      received.push(msg); // server → client, decoded + typed
      if (msg.type === "echo") break;
    }
    client.close();

    expect(received.find((m) => m.type === "welcome")).toMatchObject({ online: 1 });
    expect(received.find((m) => m.type === "echo")).toMatchObject({ text: "HI" });
  });
});
