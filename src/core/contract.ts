/**
 * The contract layer: `defineSse(...)` declares ONE SSE endpoint whose `events` map
 * is the single source of truth. One contract = one stream — there is no `path`, no
 * `method`, and no router. The URL is supplied at connect time (client) / mount time
 * (server); native EventSource is always a GET.
 */
import type { EventsMap, StandardSchemaV1, TypeMarker } from "./schema.js";
import { typeMarker } from "./schema.js";

export { TS_SSE_EOS } from "./wire.js";

/**
 * A single SSE endpoint definition. `events` is the emittable-event catalog (the
 * discriminated union); `query` optionally validates + types the query string.
 */
export interface SseDef<E extends EventsMap = EventsMap> {
  /** Optional query-string schema — validated + typed on the client's `subscribe`. */
  query?: StandardSchemaV1;
  /** The event catalog `{ eventName -> schema }` — the discriminated union. */
  events: E;
}

// ── Reserved event names ────────────────────────────────────────────────────
// `error`/`open`/`message` collide with EventSource's native dispatch; the control
// names collide with the flat `emit.comment/retry/close` surface. Forbidding them
// keeps every event unambiguously dispatchable on EventSource.
export type ReservedTransportName = "error" | "open" | "message";
export type ReservedControlName = "comment" | "retry" | "close";
export type ReservedEventName = ReservedTransportName | ReservedControlName;

type ReservedMsg<K extends string> =
  `ts-sse: event name '${K}' is reserved (collides with a transport or emit-control channel). Rename it, e.g. 'app${Capitalize<K>}'.`;
type ReservedPrefixMsg =
  `ts-sse: 'ts-sse-*' event names are reserved for internal control frames.`;

/**
 * Maps a legal event key to its schema, but a RESERVED key to an error *string*.
 * Since a string is not assignable to `StandardSchemaV1`, using a reserved name fails
 * to typecheck right at the `defineSse({ events: { ... } })` call site.
 */
export type CheckEvents<E extends EventsMap> = {
  [K in keyof E]: K extends `ts-sse-${string}`
    ? ReservedPrefixMsg
    : K extends ReservedEventName
      ? ReservedMsg<K & string>
      : E[K];
};

/**
 * Define an SSE contract. `const D` preserves the literal `events` map so the server
 * emitter and client union both derive from the exact event names.
 */
export function defineSse<const D extends SseDef>(
  def: D & { events: CheckEvents<D["events"]> },
): D {
  return def;
}

/** Compile-time typing without runtime validation. See {@link TypeMarker}. */
export function sseType<T>(): TypeMarker<T> {
  return typeMarker<T>();
}
