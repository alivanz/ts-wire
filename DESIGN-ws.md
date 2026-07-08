# ts-wire / ws — Design

Contract-first **WebSocket** for TypeScript. The bidirectional sibling of the SSE
package, under the same `ts-wire` umbrella.

Package: `ts-wire/ws` · Validator: any [Standard Schema v1] · Status: design.

> **Project reframe.** `ts-sse` is being renamed to **`ts-wire`**: one shared `core`
> (Standard Schema infer, per-message validation, the decoder invariant) with two
> *independent* transports on top — `sse` (one-way, done) and `ws` (bidirectional, this
> doc). They share primitives, **not** abstractions: `defineSse` and `defineWs` are
> separate builders with different shapes. (The npm rename is also forced — `ts-sse` is
> taken.)

---

## 0. Scope — raw capability, not a framework

`ts-wire/ws` is the **typed contract layer over a WebSocket**, nothing more:

- **JSON text frames only.** No binary (ArrayBuffer/Blob) for now.
- **No rooms, no pub/sub, no cross-instance fan-out, no presence store.** Bring your own
  WS server (Cloudflare DO, `ws`, Bun, Deno) and your own broadcast. We type the wire.
- **No req/reply/RPC in core.** Core is fire-and-forget typed messages, both directions.
- Thin like `ts-rest`: a contract is a plain value; server and client derive from it.

## 1. The mapping — one schema per direction

A WebSocket frame has **no native "event name"** (unlike SSE's `event:` field), so ts-wire
does not impose an events map. Each direction is a **single Standard Schema**: `client`
(client→server) and `server` (server→client). One message = one value validated by that
schema. Want several message types? That is your `z.discriminatedUnion` / `z.union` — not a
framework concern:

```ts
import { defineWs } from "ts-wire/ws";
import { z } from "zod";

export const chat = defineWs({
  client: z.discriminatedUnion("type", [           // client → server
    z.object({ type: z.literal("send"),   text: z.string() }),
    z.object({ type: z.literal("typing"), on: z.boolean() }),
  ]),
  server: z.discriminatedUnion("type", [           // server → client
    z.object({ type: z.literal("message"),  id: z.string(), text: z.string() }),
    z.object({ type: z.literal("presence"), online: z.number() }),
  ]),
});
// a single message type is just a bare schema:  client: z.object({ text: z.string() })
```

Both directions optional (a server-only WS behaves like SSE-over-WS). The discriminant, if
any, lives **inside your schema** — ts-wire never adds one. Mental model: **SSE is the
degenerate WS with only a `server` schema.**

## 2. Wire = your JSON, and the (symmetric) decoder invariant

Because there is no event name to carry, there is **no envelope** — a frame is literally
`JSON.stringify(message)`. Debuggable, `console.log`-able, no wrapper:

    {"type":"send","text":"hi"}          // or just {"text":"hi"} for a bare schema

The [decoder invariant] from SSE applies in **both** directions, to the single per-side
schema:

```
sender:   validate INPUT (fail-fast) → JSON.stringify the INPUT → send
receiver: JSON.parse → validate INPUT→OUTPUT (same schema) → hand OUTPUT to the caller
```

`send` takes the schema INPUT; the receiver gets the OUTPUT. A transforming schema
round-trips because the wire carries the input, never the transformed output. Every incoming
frame is re-validated against its side's schema — the moat, symmetric across both directions.

## 3. Server — a typed socket, no handler

WebSocket *server* models differ too much to hide behind one generic handler, and forcing
one actively breaks some of them. The clearest case: a **Cloudflare Durable Object** on the
hibernation API has no long-lived handler closure at all — the runtime calls
`webSocketMessage(ws, msg)` / `webSocketClose(...)` as **methods** on the DO, and the object
may hibernate between messages, so per-connection state lives in the instance/storage, not a
closure. Node `ws` (EventEmitter), Bun, and Deno each differ too.

So there is **no `serveWs(contract, socket => …)`**, and no single generic wrapper either —
the platforms differ in the very things a wrapper touches: the incoming message type and the
`send` semantics. Each ships its own `wsSocket` under a **per-platform subpath**, over one
shared string-level codec:

```
ts-wire/ws/cf     wsSocket(contract, ws)   // CF Durable Object: decode(string | ArrayBuffer)
ts-wire/ws/node   wsSocket(contract, ws)   // node `ws`: decode(RawData/Buffer), send() awaits flush
// bun / deno later — each a thin adapter over the same encode/decode(string) core
```

`wsSocket(contract, ws)` returns the same tiny surface everywhere — `send(msg)` and
`decode(raw)` — but each package normalizes its platform's raw message + send. It only wraps
what adds value (`send` = validate + encode, `decode` = parse + validate) and is
**stateless**, so you create it on demand (even inside a hibernating DO — `ws` fresh each
call, no closure). Closing stays native: `ws.close(code?, reason?)`.

### Cloudflare Durable Object (hibernation)

```ts
import { wsSocket } from "ts-wire/ws/cf";     // CF: decode handles string | ArrayBuffer
import { chat } from "./contract";

export class ChatRoom {
  constructor(private ctx: DurableObjectState) {}

  async fetch(_req: Request): Promise<Response> {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);                       // hibernation API
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  webSocketMessage(ws: WebSocket, raw: string | ArrayBuffer) {
    const sock = wsSocket(chat, ws);                         // typed socket for this connection
    const msg = sock.decode(raw);                            // typed client union, validated
    if (!msg.ok) return ws.close(1007, "invalid message");   // native close — nothing to wrap
    switch (msg.value.type) {
      case "send":
        for (const peer of this.ctx.getWebSockets())
          wsSocket(chat, peer).send({ type: "message", id: id(), text: msg.value.text });
        break;
      case "typing": /* … */ break;
    }
  }

  webSocketClose(ws: WebSocket, code: number, reason: string) { /* cleanup */ }
}
```

### Node `ws`

```ts
import { wsSocket } from "ts-wire/ws/node";    // node: decode handles Buffer/RawData

wss.on("connection", (ws) => {
  const sock = wsSocket(chat, ws);
  await sock.send({ type: "presence", online: count() });  // send() awaits flush (backpressure)
  ws.on("message", (raw) => {
    const msg = sock.decode(raw);              // raw is RawData — the node wrapper handles it
    if (msg.ok && msg.value.type === "send") sock.send({ type: "message", /* … */ });
  });
  ws.on("close", (code, reason) => cleanup());
});
```

That's the whole server surface — a per-platform `wsSocket(contract, ws)` (`ts-wire/ws/cf`,
`ts-wire/ws/node`, …) giving typed `send` / `decode` over one shared string codec, with
per-message validation and the decoder invariant. Sockets, broadcast, fan-out, hibernation,
and backpressure stay native to your platform — ts-wire never owns the connection.

## 4. Client API

The client, by contrast, *is* uniform — every browser/runtime `WebSocket` looks the same —
so it gets a real owner. The big difference from SSE: the client can **send**.

```ts
import { connectWs } from "ts-wire/ws/client";
import { chat } from "./contract"; // type-only import is enough

const ws = connectWs(chat, { url: "wss://api.example.com/chat" });

ws.send({ type: "send", text: "hi" });      // client → server (validated INPUT, serialized)

ws.onMessage((msg) => {                      // server → client (decoded OUTPUT union)
  switch (msg.type) {
    case "message":  render(msg);           break;
    case "presence": setOnline(msg.online); break;
  }
});
for await (const msg of ws) { /* same server union */ }

ws.onOpen(() => {}); ws.onClose((code, reason) => {}); ws.onError((e) => {});
ws.close();
```

Uses the browser `WebSocket` (injectable for tests/SSR). `WebSocket` does not auto-reconnect
(unlike EventSource), so the client owns reconnection — see §7.

## 5. Backpressure

On the **client**, `connectWs` uses the browser `WebSocket`'s `bufferedAmount`: `ws.send(...)`
returns a Promise that resolves once the frame is accepted, and waits when `bufferedAmount`
exceeds a high-water mark, so `await ws.send(...)` in a hot loop is real backpressure
(polling — `WebSocket` has no drain event). On the **server**, backpressure is your
platform's concern (`ws.bufferedAmount` on Node `ws`, the DO/runtime's own semantics):
`wsSocket(...).send()` delegates to the native `ws.send()`, so flow control stays yours.

## 6. Lifecycle & validation

- **Close is native.** WebSocket has real close frames with a **code + reason** (1000
  normal, 1001 going away, 1006 abnormal, 1007 invalid data, …). No EOS sentinel hack.
- **Client** (`connectWs`) exposes two structurally-distinct channels: `onMessage` /
  async-iterator → the decoded, validated `server` message; `onValidationError(err)` → a
  frame that failed its schema or wasn't valid JSON (default **skip**, never closes). Plus
  `onOpen` / `onClose(code, reason)` / `onError`.
- **Server** lifecycle is platform-native (`webSocketClose` on a DO, `ws.on("close")` on
  Node). A bad incoming frame surfaces as `sock.decode(...).ok === false` — you choose to
  ignore it or `ws.close(1007)` (native).

## 7. Reconnection (client-owned)

`WebSocket` never reconnects itself. `connectWs(chat, { url, reconnect })` re-opens with a
capped backoff. Open fork: **outgoing messages sent while disconnected** — queue-and-flush
on reopen, or drop. There is no `Last-Event-ID` equivalent, so *resume* is app-defined
(replay via an app message if you want it).

## 8. Honesty / limits (the SSE rule, applied to WS)

- **Browser `WebSocket` can't set request headers** — same limitation as EventSource. So
  the contract has **no `headers`**. Auth via subprotocol token, cookie, or query param.
- **JSON text only** — no binary frames (for now).
- **No app-level ping/pong from the browser** — the browser answers pings automatically but
  can't *send* protocol pings from JS. Server-side liveness ping is platform-native (Node
  `ws.ping()`, DO alarms), not something ts-wire owns. Apps wanting client-initiated
  heartbeats add an app message.
- **No reserved names.** There's no event-name namespace and no envelope — a frame is your
  message, validated by your schema.

## 9. Open questions (forks to pin before/while building)

| Question | Leaning |
|---|---|
| Outgoing while disconnected (client) | **queue + flush on reopen** vs drop |
| Client backpressure | `bufferedAmount` await vs fire-and-forget → **await, with a HWM knob** |
| req/reply (RPC) layer | out of core; optional opt-in add-on later |
| Server-side liveness ping | platform-native (DO alarms / `ws.ping()`) — not in ts-wire |
| Server-only / client-only contracts | **allowed** (both schemas optional) |
| Broadcast encode-once (large fan-out) | add a standalone `encode(contract, msg)` if needed |

## 10. Prior art & the gap

`zod-sockets` (Zod + Socket.IO, niche, framework-coupled) · tRPC subscriptions (whole
stack, router-first, coupled) · Socket.IO (loosely/partially typed, its own protocol) ·
`ts-rest` (no WS). **The gap:** a decoupled, shareable, per-message-validated **bidirectional
contract** you can hand across a boundary with a `type`-only import — the same moat as the
SSE package, extended to two directions.

## 11. Reuse from `core`

Shared with `sse`: Standard Schema `InferIn`/`InferOut`, the `wireType<T>()` marker,
`validateSync`, and the decoder invariant. **Not** shared: SSE's `events`-map →
discriminated-union derivation and reserved-name guard (WS uses one schema per direction, so
multiplicity is the caller's `z.union`), and the server-owns-the-socket model — WS servers
are platform-native, so ts-wire ships a shared string-level `encode`/`decode` codec plus a
thin per-platform `wsSocket` (`ts-wire/ws/cf`, `ts-wire/ws/node`, …), not a handler. New WS code is the two-schema contract, the bare-JSON codec, and the
client (`connectWs`: browser `WebSocket`, `bufferedAmount` backpressure, client-owned
reconnection).

[Standard Schema v1]: https://standardschema.dev
[decoder invariant]: ./DESIGN.md#41-the-decoder-invariant--what-goes-on-the-wire
