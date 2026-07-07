# ts-sse — Design

Contract-first Server-Sent Events for TypeScript. **ts-rest, but the response is a
validated, resumable stream of named events.**

Package: `@alivan/ts-sse` · Validator: any [Standard Schema v1] · Status: core + client shipped, server next.

**Scope: native `EventSource` only.** The contract models *only* what the browser's
`EventSource` can actually do — a one-way GET stream, cookie auth, browser-owned reconnect
with automatic `Last-Event-ID` resume. No `method`, no request headers, no request body: if
`EventSource` can't do it, the contract doesn't offer it.

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
`event name → data schema`** — one long-lived **GET** whose response is a union of named
events streamed over time. The stream is **one-way** (server → client): the browser opens
it, the server pushes, and nothing flows back on that connection (see §3.1). **One contract =
one endpoint** — no router, no `path` in the contract; the URL is supplied at connect time.

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
import { defineSse } from "@alivan/ts-sse/core";
import { z } from "zod"; // any Standard Schema v1 validator

// one contract = one SSE endpoint — no `path`, no router
export const roomStream = defineSse({
  query: z.object({ since: z.coerce.number().optional() }), // optional; typed + validated on subscribe
  events: {                              // THE catalog = the discriminated union
    chat:     z.object({ id: z.string(), text: z.string(), user: z.string() }),
    presence: z.object({ online: z.number() }),
    appError: z.object({ code: z.string(), message: z.string() }), // NOT `error` (reserved)
  },
});
// no path/config here — the URL is given at connect time; retry/resume/heartbeat are RUNTIME.
```

### Server (planned `@alivan/ts-sse/server`)

```ts
// one contract → one handler. Mount the returned handler at whatever path your framework uses.
export const handler = toFetchResponse(
  roomStream,
  async ({ query, lastEventId, emit, signal }) => {
    emit.retry(3000);                     // runtime: write a `retry:` line now
    emit.presence({ online: roomSize() });
    // lastEventId is ALWAYS provided (browser's Last-Event-ID header); decode it yourself
    for await (const m of stream({ since: query.since ?? lastEventId, signal })) {
      await emit.chat(m, { id: m.id });   // attach `id` to make THIS event resumable (id optional)
    }                                     // await = natural backpressure
    emit.close();                         // writes `event: ts-sse-eos`, then ends the stream
  },
  { heartbeat: 15000 },                   // heartbeat is a RUNTIME adapter option
);
```

### Client (planned `@alivan/ts-sse/client`)

```ts
import { roomStream } from "./contract"; // type-only import is enough

// the full endpoint URL is given here — the contract has no path
const room = initClient(roomStream, { url: "https://api.example.com/rooms/42/stream" });
const sub = room.subscribe({ query: { since: 100 } }); // query typed from schema

sub.on("chat", (data, meta) => console.log(data.text, meta.lastEventId)); // typed
sub.onConnectionError((err) => {});                                        // connection trouble

for await (const ev of sub) {
  switch (ev.event) {                    // discriminated union of the whole catalog
    case "chat":     ev.data.text;   break;
    case "presence": ev.data.online; break;
  }
}
```

### 3.1 Why EventSource-only

SSE is one-way: the browser opens a stream, the server pushes, and nothing flows back on that
connection. ts-sse targets the **native browser `EventSource`** and nothing else — no
`fetch`+`ReadableStream` fallback. That keeps the contract honest: it can only express what
`EventSource` can actually do.

What native `EventSource` gives you — and what it can't:

| Capability | native `EventSource` |
|---|---|
| Method | **GET only** — there is no method option |
| Custom request headers (`Authorization: Bearer …`) | **impossible** |
| Request body | **impossible** |
| Cookie / same-origin auth | yes — `new EventSource(url, { withCredentials })` |
| Resume via `Last-Event-ID` | **automatic** — the browser re-sends it on reconnect |
| Seed the id on the *first* connect | **no** |
| Reconnect + backoff | **automatic**, using the server's `retry:` hint |

**Design consequence — the contract offers none of the "impossible" rows.** No `method`, no
`headers`, no `body`, no `resumeFrom` seed. Auth is cookies only; reconnection and resume are
the browser's job (the server just reads the inbound `Last-Event-ID`).

The trade is deliberate: you give up `Bearer` auth and request bodies (so no POST-prompt LLM
streams), and you get a dramatically smaller surface — one transport, no open-time HTTP status
classification, no reconnect policy to own. A project that later needs headers or POST reaches
for a *separate* opt-in fetch transport; it is not something the core contract pretends to
support.

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
string instead of a schema, failing right at the `defineSse` call site. Rename → `appError`,
`roomClosed`, etc. Because names can never collide, every event stays unambiguously
dispatchable on `EventSource`.

### 4.3 Resume — runtime, not a mode

The SSE `id:` field is a wire **string** the browser echoes verbatim. There is **no
`resumable` flag**: `ctx.lastEventId: string | undefined` is *always* provided (from the
browser's `Last-Event-ID` header), and a handler makes an event resumable simply by
attaching `emit.x(data, { id })` (id is always optional). The handler decodes `lastEventId`
however it likes — it is an opaque string the server itself minted. Wire rules: reject `id`
containing `U+0000`; id persists across events until changed.

### 4.4 Backpressure + error channels

- **Backpressure:** `FrameSink.write()` resolves on **flush, not enqueue** — that single
  contract *is* the backpressure story (`await emit.x()` just works; async-generator
  handlers get it free via pull). The server's ReadableStream sink is a pull-driven
  ready-gate (`HWM=1`). All frames
  (real + heartbeat + EOS) route through **one** `CoordinatedWriter` chain → whole,
  ordered. Heartbeats are idle-gated `:comment` lines (no `id:`, swallowed by
  EventSource), suppressed while a real write is parked.
- **Three structurally disjoint client channels:**
  - `.on()` / async-iterator → data (`InferOut` union)
  - `.onConnectionError(SseConnectionError)` → surfaces connection trouble. The **browser**
    owns reconnection; while it retries, the iterator keeps yielding. Fatal → iterator
    throws + `state='closed'`.
  - `.onValidationError()` → data-plane, skip-and-continue, **never** reconnects.
- **Terminal sentinel:** `emit.close()` writes a reserved `event: ts-sse-eos` frame *then*
  closes. Bare stream-end (no sentinel) = the browser auto-reconnects. Miss this and you get
  an infinite reconnect loop against a server that thinks it finished.
- **EventSource error semantics:** the native `error` event is **opaque** — no status code.
  The library classifies by `readyState`: `CONNECTING` (0) = the browser is auto-retrying →
  retriable; `CLOSED` (2) = the browser gave up → fatal. That is the whole model — no
  open-time HTTP classification, because the client never reads the response itself.

## 5. Locked decisions

The `events` map is the single source of truth · `emit` = mapped type over `InferIn`,
client union = mapped-then-indexed over `InferOut` keyed on the literal `event` ·
`sseType<T>()` = conformant `StandardSchemaV1<T,T>` with identity validate · reserved-name
guard is global · wire carries INPUT JSON · string ids, decode-only resume · flush-promise
backpressure · three disjoint error channels · EOS sentinel · `const D` literal capture ·
type-only helpers derive every surface from
`typeof contract` · **EventSource-only**: the contract has no `method`/`headers`/`body` —
cookie auth, browser-owned reconnect + automatic `Last-Event-ID` resume · **one contract =
one SSE** (no `path`, no router); shared `defineSse(...)` = optional `query` schema + `events`, with
**no route config** — retry (`emit.retry`), resume
(`emit.x(data,{id})` + `ctx.lastEventId`) and heartbeat (adapter option) are all **runtime**.

## 6. Open questions (defaults chosen, revisit before 1.0)

| Question | Default |
|---|---|
| Non-JSON payloads (Date/bigint/Map as input) | **docs-only** "input must be wire-shaped" |
| Delivery guarantee past a bad frame | **at-most-once** (EventSource advances the id before our validation runs) |
| Defaults | heartbeat **15s**, server sink **HWM=1** (configurable) |

## 7. Prior art & the gap

`tRPC v11` (SSE subscriptions, but router-first/coupled) · `@effect/rpc` (separation, but
whole Effect runtime) · `NestJS @Sse()` (untyped `.data`, platform-locked) · Hono
`streamSSE` / `better-sse` (untyped plumbing — good server adapter targets) ·
`@microsoft/fetch-event-source` (the fetch-based escape hatch for headers/POST — deliberately
left *out* of core to stay EventSource-simple). **The moat: a decoupled shareable contract +
per-event client-side validation, which nothing above has.**

## 8. Roadmap

1. **`core`** ✅ — Standard Schema plumbing, `defineSse` + reserved-name guard, wire
   serializer (decoder invariant), streaming parser. *(57 core tests)*
2. **`client`** ✅ — `initClient(contract, { url })` over native **`EventSource` only**
   (GET, cookie auth, browser-owned reconnect + automatic `Last-Event-ID` resume), typed
   `query` on `subscribe`, `.on` + async iterator over the decoded union, 3 error channels.
3. **`server`** *(next)* — `initServer`, typed `emit.<name>` (resumable-gated opts),
   `FrameSink` (flush-promise backpressure), `CoordinatedWriter` + heartbeat,
   `toFetchResponse`. This closes the loop for a real end-to-end demo.
4. **`react`**, Node server adapter, OpenAPI-ish event catalog docs.

[Standard Schema v1]: https://standardschema.dev
