/**
 * The contract layer: `c.sse(...)` defines a route whose `events` map is the
 * single source of truth. Mirrors ts-rest's `c.router` / `AppRoute`, but a route's
 * payload is a long-lived stream of NAMED events instead of one status-coded body.
 */
import type { EventsMap, Prettify, StandardSchemaV1, TypeMarker } from "./schema.js";
import { typeMarker } from "./schema.js";

export { TS_SSE_EOS } from "./wire.js";

export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

/**
 * An SSE route definition. `events` is the emittable-event catalog; the SSE-specific
 * policy fields (`resumable`, `retry`, `heartbeat`) drive transport behaviour.
 * `pathParams`/`query`/`headers`/`body` schemas are orthogonal and land later.
 */
export interface SseDef<E extends EventsMap = EventsMap> {
  method: HttpMethod;
  path: string;
  /** The event catalog `{ eventName -> schema }` — the discriminated union. */
  events: E;
  /** When true, tracked emits must carry an `id` and `lastEventId` is threaded to the handler. */
  resumable?: boolean;
  /** Optional decoder for the inbound `Last-Event-ID` (string -> typed cursor, server-side). */
  resumeSchema?: StandardSchemaV1;
  /** Default reconnection hint (ms), emitted once at open. */
  retry?: number;
  /** Heartbeat comment interval (ms). */
  heartbeat?: number;
  summary?: string;
}

// ── Reserved event names ────────────────────────────────────────────────────
// `error`/`open`/`message` collide with EventSource's native dispatch; the control
// names collide with the flat `emit.comment/retry/close` surface. Forbidding them
// here (globally, not transport-conditionally) keeps every contract EventSource-safe.
export type ReservedTransportName = "error" | "open" | "message";
export type ReservedControlName = "comment" | "retry" | "close";
export type ReservedEventName = ReservedTransportName | ReservedControlName;

type ReservedMsg<K extends string> =
  `ts-sse: event name '${K}' is reserved (collides with a transport or emit-control channel). Rename it, e.g. 'app${Capitalize<K>}'.`;
type ReservedPrefixMsg =
  `ts-sse: 'ts-sse-*' event names are reserved for internal control frames.`;

/**
 * Maps a legal event key to its schema, but a RESERVED key to an error *string*.
 * Since a string is not assignable to `StandardSchemaV1`, using a reserved name
 * fails to typecheck right at the `c.sse({ events: { ... } })` call site.
 */
export type CheckEvents<E extends EventsMap> = {
  [K in keyof E]: K extends `ts-sse-${string}`
    ? ReservedPrefixMsg
    : K extends ReservedEventName
      ? ReservedMsg<K & string>
      : E[K];
};

/**
 * Define an SSE route. `const D` preserves the literal `events` map, method and
 * path — feeding both the union derivation (server/client) and `transport:'auto'`.
 */
export function sse<const D extends SseDef>(def: D & { events: CheckEvents<D["events"]> }): D {
  return def;
}

/** Compile-time typing without runtime validation. See {@link TypeMarker}. */
export function type<T>(): TypeMarker<T> {
  return typeMarker<T>();
}

/** Router-level options. `commonEvents` are merged into every route (Own wins). */
export interface RouterOptions<Common extends EventsMap = Record<string, never>> {
  pathPrefix?: string;
  commonEvents?: Common;
}

/**
 * Group routes into a contract. For now this is a structural pass-through that
 * preserves literal types; the type-level `commonEvents` merge into each route's
 * catalog lands with the server/client packages.
 */
export function router<const T extends Record<string, SseDef>>(
  routes: T,
  _options?: RouterOptions,
): T {
  return routes;
}

/** Merge a router's common events into a route catalog; the route (Own) wins on conflict. */
export type MergeEvents<Common extends EventsMap, Own extends EventsMap> = Prettify<
  Omit<Common, keyof Own> & Own
>;

/** The contract builder, mirroring ts-rest's `initContract()`. */
export function initContract() {
  return { sse, type, router } as const;
}

/** Convenience singleton so `import { c }` works without calling `initContract()`. */
export const c = initContract();
