# ts-sse — Design

Contract-first Server-Sent Events for TypeScript. **ts-rest, but the response is a
validated, resumable stream of named events.**

Package: `@alivan/ts-sse` · Validator: any [Standard Schema v1] · Status: core in progress.

---

## 1. Why contract-first

`ts-rest` shines because the **contract is a plain value, not a running server**. That
one fact buys:

- **One source of truth, zero codegen.** Types *and* runtime validation derive from the
  same schema object. No build step, no generated SDK to drift.
- **Breaking changes become compile errors across a runtime boundary** where the two
  sides share no runtime code. The client imports the contract `type`-only.
- **True 3-way decoupling.** Contract → server (`initServer` fills handlers) → client
  (`initClient` consumes). Stronger than tRPC, where the router *is* the contract and
  server + client weld through an `AppRouter` type.
- **Discriminated unions force exhaustive handling** — which maps *perfectly* onto SSE.

## 2. The mapping

A REST route maps `status code → response schema`. **An SSE route maps
`event name → data schema`** — a long-lived GET (or POST-via-fetch) that emits a union
of named events over time.

```
responses: { 200: Post, 404: Err }        // ts-rest
events:    { message: Msg, ping: Ping }    // ts-sse   ← the whole idea
```

From that one `events` map both public surfaces fall out mechanically:

- **Server:** `emit.message(data)` / `emit.ping(data)` — method names *are* the event
  keys, argument types *are* the schemas.
- **Client:** `.on('message', data => …)` **and** an async-iterable discriminated union
  `{ event, data, id, lastEventId }`.

## 3. API surface

### Contract

```ts
import { initContract } from "@alivan/ts-sse/core";
import { z } from "zod"; // any Standard Schema v1 validator

const c = initContract();

export const chat = c.router({
  roomStream: c.sse({
    method: "GET",                       // GET → EventSource works; POST → forces fetch transport
    path: "/rooms/:id/stream",
    events: {                            // THE catalog = the discriminated union
      chat:     z.object({ id: z.string(), text: z.string(), user: z.string() }),
      presence: z.object({ online: z.number() }),
      appError: z.object({ code: z.string(), message: z.string() }), // NOT `error` (reserved)
    },
    resumable: true,                     // id required on emits; lastEventId threaded to handler
    retry: 3000,
    heartbeat: 15000,
  }),
});
```

### Server (planned `@alivan/ts-sse/server`)

```ts
const s = initServer();

export const chatRouter = s.router(chat, {
  roomStream: async ({ params, query, lastEventId, emit, signal }) => {
    emit.presence({ online: roomSize(params.id) });
    for await (const m of roomStream(params.id, { since: lastEventId, signal })) {
      await emit.chat(m, { id: m.id }); // wrong shape = compile error + runtime re-validated
    }                                    // await = natural backpressure
    emit.close();                        // writes `event: ts-sse-eos`, then ends the stream
  },
});

// framework-agnostic: one Response with a text/event-stream ReadableStream
export const handler = (req: Request) =>
  toFetchResponse({ request: req, contract: chat, router: chatRouter });
```

### Client (planned `@alivan/ts-sse/client`)

```ts
import { chat } from "./contract"; // type-only import is enough

const client = initClient(chat, { baseUrl: "https://api.example.com", transport: "auto" });
const sub = client.roomStream.subscribe({ params: { id: "42" } });

sub.on("chat", (data, meta) => console.log(data.text, meta.lastEventId)); // typed
sub.onConnectionError((err) => {});                                        // reconnect driver

for await (const ev of sub) {
  switch (ev.event) {                    // discriminated union of the whole catalog
    case "chat":     ev.data.text;   break;
    case "presence": ev.data.online; break;
  }
}
```

## 4. The four hard forks (resolved, `tsc`-verified)

### 4.1 The decoder invariant — what goes on the wire

**An event schema is a DECODER: `Input = wire shape`, `Output = consumed value`.**

```
emit.price("5")           // server passes InferIn (pre-transform)
  → validate() fail-fast pre-check
  → serialize the INPUT JSON to the wire      ← NOT the validated output
  → client JSON.parse → validate() input→output (schema's natural direction)
  → .on('price', n => …)  // client gets InferOut
```

Serializing the *output* is a latent bug: with any transform (`z.coerce.number()`,
`z.string().transform(...)`), the client can no longer re-decode the frame with the same
schema — it either rejects healthy frames or blind-casts and corrupts non-JSON outputs
(`Date`, `Map`) at runtime with no type error. **Invisible in the no-transform case, so
it needs a regression test with a genuinely non-round-tripping transform**
(`string → s.length`). Constraint: an event schema's INPUT must be JSON-serializable.

### 4.2 Reserved event names (the EventSource collision)

Native `EventSource` dispatches its synthetic `error`/`open` events and every
`event: message` frame to the same listeners as data. So a contract may not name an event
`error` / `open` / `message`, nor `comment` / `retry` / `close` (they collide with the
flat emit-control surface), nor use the `ts-sse-*` prefix (internal control frames). This
is enforced at **compile time** via `CheckEvents<E>`: a reserved key resolves to an error
string instead of a schema, failing right at the `c.sse` call site. Rename → `appError`,
`roomClosed`, etc. Because names can never collide, EventSource stays name-safe on every
contract, so the transport selector needs no name check.

### 4.3 Resume — string ids, decode-only cursor

The SSE `id:` field is inherently a wire **string** the client echoes verbatim, so
`emit` id stays `string`; `resumable: true` only makes it **required** (type-gated). A
`resumeSchema` is **decode-only** (`string → Cursor`); the typed cursor surfaces solely
as server `ctx.lastEventId`. No paired encoder is needed. A bad inbound `Last-Event-ID`
hits a route-level `onResumeError` boundary before the handler → never silently read as a
valid-but-wrong DB offset. Wire rules: reject `id` containing `U+0000`; id persists across
events until changed.

### 4.4 Backpressure + error channels

- **Backpressure:** `FrameSink.write()` resolves on **flush, not enqueue** — that single
  contract *is* the backpressure story (`await emit.x()` just works; async-generator
  handlers get it free via pull). fetch sink = pull-driven ready-gate, `HWM=1`. All frames
  (real + heartbeat + EOS) route through **one** `CoordinatedWriter` chain → whole,
  ordered. Heartbeats are idle-gated `:comment` lines (no `id:`, swallowed by
  EventSource), suppressed while a real write is parked.
- **Three structurally disjoint client channels:**
  - `.on()` / async-iterator → data (`InferOut` union)
  - `.onConnectionError(SseConnectionError)` → the **only** reconnect driver. Retriable →
    reconnect + iterator keeps yielding; fatal → iterator throws + `state='closed'`.
  - `.onValidationError()` → data-plane, skip-and-continue, **never** reconnects.
- **Terminal sentinel:** `emit.close()` writes a reserved `event: ts-sse-eos` frame *then*
  closes. Bare stream-end (no sentinel) = retriable drop → reconnect. Miss this and you
  get an infinite reconnect loop against a server that thinks it finished.
- **fetch open-time classification:** `204`/`4xx`(≠429) → fatal; `429`/`5xx`/network →
  retriable (honor `Retry-After`/`retry:`); `2xx` non-`text/event-stream` → fatal.
  EventSource can't read status → opaque, always retriable until it gives up.

## 5. Locked decisions

The `events` map is the single source of truth · `emit` = mapped type over `InferIn`,
client union = mapped-then-indexed over `InferOut` keyed on the literal `event` ·
`c.type<T>()` = conformant `StandardSchemaV1<T,T>` with identity validate · reserved-name
guard is global · wire carries INPUT JSON · string ids, decode-only resume · flush-promise
backpressure · three disjoint error channels · EOS sentinel · `const D` literal capture ·
Own-wins `commonEvents` merge · type-only helpers derive every surface from
`typeof contract`.

## 6. Open questions (defaults chosen, revisit before 1.0)

| Question | Default |
|---|---|
| Typed emit ids `{id: 42}` vs string `{id: '42'}` | **string**; codec is an opt-in escape hatch |
| Non-JSON payloads (Date/bigint/Map as input) | **docs-only** "input must be wire-shaped" |
| Server: bad inbound `Last-Event-ID` → | **full-replay** (vs reject 4xx) |
| `transport:'auto'` for *resumable* routes | **stay on EventSource** when possible |
| Delivery guarantee past a bad frame | **at-most-once** both transports |
| Defaults | heartbeat **15s**, fetch sink **HWM=1** (configurable) |

## 7. Prior art & the gap

`tRPC v11` (SSE subscriptions, but router-first/coupled) · `@effect/rpc` (separation, but
whole Effect runtime) · `NestJS @Sse()` (untyped `.data`, platform-locked) · Hono
`streamSSE` / `better-sse` (untyped plumbing — good adapter targets) ·
`@microsoft/fetch-event-source` (best client reader, untyped). **The moat: a decoupled
shareable contract + per-event client-side validation, which nothing above has.**

## 8. Roadmap

1. **`core`** ✅ — Standard Schema plumbing, `c.sse` + reserved-name guard, wire
   serializer (decoder invariant), streaming parser. *(57 core tests)*
2. **`client`** ✅ — `initClient`, transport `auto` (EventSource + fetch-stream), `.on`
   + async iterator over the decoded union, 3 error channels, resume, reconnect.
   *(37 client tests)*
3. **`server`** *(next)* — `initServer`, typed `emit.<name>` (resumable-gated opts),
   `FrameSink` (flush-promise backpressure), `CoordinatedWriter` + heartbeat,
   `toFetchResponse`. This closes the loop for a real end-to-end demo.
4. **`react`**, Node server adapter, `commonEvents` type-merge, OpenAPI-ish event
   catalog docs.

[Standard Schema v1]: https://standardschema.dev
