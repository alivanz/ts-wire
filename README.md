# ts-wire

Contract-first **realtime** for TypeScript — ts-rest, but for streaming. One shared
`core` (Standard Schema validation + the decoder invariant) with independent transports:

- **SSE** — one-way, native `EventSource`.
- **WebSocket** — bidirectional.

A contract is a plain value both ends derive from — no codegen, per-event runtime
validation, `type`-only client import. See `DESIGN.md` (SSE) and `DESIGN-ws.md` (WS).

## Install

```bash
npm i @alivanz/ts-wire
```

## Packages (subpath exports)

| Import | What |
| --- | --- |
| `@alivanz/ts-wire/core` | Standard Schema primitives (`InferIn`/`InferOut`, `validateSync`) |
| `@alivanz/ts-wire/sse` | `defineSse`, `sseType` — the SSE contract |
| `@alivanz/ts-wire/sse/client` | `initClient` — the EventSource client |
| `@alivanz/ts-wire/sse/fetch` | `sseResponse` — a `Response` for any fetch runtime (Hono/Next/Bun/Deno/CF Workers) |
| `@alivanz/ts-wire/sse/node` | `toNodeHandler` — `(req, res)` for Node/Express/Fastify |
| `@alivanz/ts-wire/ws` | `defineWs` — the bidirectional WS contract |
| `@alivanz/ts-wire/ws/client` | `connectWs` — the browser-`WebSocket` client |
| `@alivanz/ts-wire/ws/cf` | `wsSocket` — Cloudflare Durable Objects |
| `@alivanz/ts-wire/ws/node` | `wsSocket` — node `ws` |

## Requirements

- Node >= 22
- pnpm

## Scripts

| Script            | Does                              |
| ----------------- | --------------------------------- |
| `pnpm build`             | Compile to `dist/`                                   |
| `pnpm typecheck`         | Type-check without emitting                          |
| `pnpm test`              | Unit tests (hermetic, with fakes)                    |
| `pnpm test:integration`  | Real loopback tests — actual http/ws server ↔ client |
| `pnpm clean`             | Remove `dist/`                                       |

## License

MIT
