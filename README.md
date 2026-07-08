# ts-wire

Contract-first **realtime** for TypeScript — ts-rest, but for streaming. One shared
`core` (Standard Schema validation + the decoder invariant) with independent transports:

- **`ts-wire/sse`** — one-way, native `EventSource`. *Shipped.*
- **`ts-wire/ws`** — bidirectional WebSocket. *Design (`DESIGN-ws.md`), not yet built.*

A contract is a plain value both ends derive from — no codegen, per-event runtime
validation, `type`-only client import. See `DESIGN.md` (SSE) and `DESIGN-ws.md` (WS).

## Packages (subpath exports)

| Import | What |
| --- | --- |
| `ts-wire/core` | Standard Schema primitives (`InferIn`/`InferOut`, `validateSync`) |
| `ts-wire/sse` | `defineSse`, `sseType` — the SSE contract |
| `ts-wire/sse/client` | `initClient` — the EventSource client |
| `ts-wire/sse/fetch` | `sseResponse` — a `Response` for any fetch runtime (Hono/Next/Bun/Deno/CF Workers) |
| `ts-wire/sse/node` | `toNodeHandler` — `(req, res)` for Node/Express/Fastify |

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
