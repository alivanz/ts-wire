/**
 * ts-wire/ws — the WebSocket contract. Bidirectional, so a contract is two schemas:
 * `client` (client→server) and `server` (server→client). Each is a SINGLE Standard
 * Schema — a WS frame has no native event name, so multiplicity is the caller's
 * `z.discriminatedUnion`, not a framework concern. Both directions are optional (a
 * server-only contract behaves like SSE-over-WS).
 */
import type { InferIn, InferOut, StandardSchemaV1, TypeMarker } from "../core/schema.js";
import { typeMarker } from "../core/schema.js";

export interface WsDef {
  /** client → server message schema */
  client?: StandardSchemaV1;
  /** server → client message schema */
  server?: StandardSchemaV1;
}

/** Define a WebSocket contract. `const D` preserves the literal schemas for inference. */
export function defineWs<const D extends WsDef>(def: D): D {
  return def;
}

/** Compile-time typing without runtime validation (the no-runtime escape hatch). */
export function wsType<T>(): TypeMarker<T> {
  return typeMarker<T>();
}

// ── Message-type projections ─────────────────────────────────────────────────
// Send takes the schema's INPUT (decoder invariant); receive gets the OUTPUT.
// A missing direction projects to `never`, so e.g. a server-only contract makes
// `client.send` uncallable at the type level.

/** What the client SENDS (client-schema INPUT). */
export type ClientInput<D extends WsDef> = D extends { client: infer C extends StandardSchemaV1 }
  ? InferIn<C>
  : never;
/** What the server RECEIVES (client-schema OUTPUT). */
export type ClientOutput<D extends WsDef> = D extends { client: infer C extends StandardSchemaV1 }
  ? InferOut<C>
  : never;
/** What the server SENDS (server-schema INPUT). */
export type ServerInput<D extends WsDef> = D extends { server: infer S extends StandardSchemaV1 }
  ? InferIn<S>
  : never;
/** What the client RECEIVES (server-schema OUTPUT). */
export type ServerOutput<D extends WsDef> = D extends { server: infer S extends StandardSchemaV1 }
  ? InferOut<S>
  : never;
