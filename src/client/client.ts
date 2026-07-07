/**
 * `initClient` — the transport-agnostic ORCHESTRATOR.
 *
 * It turns a contract (a `Record<string, SseDef>`) into a `{ route.subscribe() }`
 * client. A `subscribe()` call:
 *
 *   1. resolves the request (URL + params + query, awaited headers, JSON body),
 *   2. picks a {@link Transport} via {@link selectTransport} (DESIGN §4),
 *   3. wires the four {@link TransportHandlers} callbacks to the three disjoint
 *      client channels (DESIGN §4.4): data (`.on` + async-iterator),
 *      `onConnectionError` (the ONLY reconnect driver), and `onValidationError`.
 *
 * This file is deliberately decoupled from the concrete transports: the real
 * `fetch-transport.ts` / `eventsource-transport.ts` factories are injected through
 * the `deps` seam (see {@link ClientDeps}). That keeps `client.ts` unit-testable
 * with a fake transport and free of any static import of the network layer.
 *
 * The decoder invariant (DESIGN §4.1) lives in {@link decodeFrame}: the wire carries
 * a schema's INPUT JSON, so the client re-runs the schema INPUT→OUTPUT and hands the
 * consumer the OUTPUT value. That is why a genuinely non-round-tripping transform
 * (`z.string().transform(s => s.length)`) decodes `"hello"` on the wire to `5`.
 */
import type { EventsMap } from "../core/schema.js";
import { validateSync } from "../core/schema.js";
import type { HttpMethod, SseDef } from "../core/contract.js";
import type { RawFrame } from "../core/wire.js";
import { TS_SSE_EOS } from "../core/wire.js";
import type { SseConnectionError, SseValidationError } from "./errors.js";
import type {
  ClientEvent,
  EventMeta,
  HeaderValue,
  InitClientOptions,
  ReconnectPolicy,
  SseClient,
  SseClientState,
  SseSubscription,
  SubscribeArgs,
  Transport,
  TransportConfig,
  TransportFactory,
  TransportHandlers,
  Unsubscribe,
} from "./types.js";

// ── Injection seam ────────────────────────────────────────────────────────────

/**
 * The real transports, injected by the barrel (`src/client/index.ts`) so this file
 * never statically imports the network layer. Tests pass a fake pair here.
 *
 * When it is absent, `subscribe()` throws a clear "transports not wired" error: the
 * integrator is expected to always provide it, and doing so synchronously surfaces a
 * misconfiguration immediately instead of as a stalled subscription.
 */
export interface ClientDeps {
  transports?: {
    eventsource: TransportFactory;
    fetch: TransportFactory;
  };
}

// ── Reconnect defaults ─────────────────────────────────────────────────────────

/**
 * The reconnect policy the client resolves when the caller supplies only a partial
 * one (or none). The transport owns the retry loop; `initClient` just fills the gaps.
 * `backoffMs` honours the server's `retry:`/`Retry-After` hint, else a capped
 * exponential (1s, 2s, 4s … ≤ 30s).
 */
const DEFAULT_RECONNECT: ReconnectPolicy = {
  retries: Infinity,
  backoffMs: (attempt, serverRetryMs) =>
    serverRetryMs ?? Math.min(30_000, 1_000 * 2 ** Math.max(0, attempt - 1)),
};

function resolveReconnect(
  reconnect: InitClientOptions["reconnect"],
): ReconnectPolicy | false {
  if (reconnect === false) return false;
  if (reconnect === undefined) return DEFAULT_RECONNECT;
  return {
    retries: reconnect.retries ?? DEFAULT_RECONNECT.retries,
    backoffMs: reconnect.backoffMs ?? DEFAULT_RECONNECT.backoffMs,
  };
}

// ── Transport selection (DESIGN §4) ────────────────────────────────────────────

/**
 * Decide which transport a route+args should use under `options.transport`.
 *
 * `mode = options.transport ?? "auto"`. An explicit `"eventsource"`/`"fetch"` wins.
 * `"auto"` only reaches for native EventSource when it can actually serve the
 * request: a header-less, body-less, non-resuming GET, and an `EventSource` impl was
 * provided. Anything EventSource cannot express (a body, custom headers it cannot
 * set, a `Last-Event-ID` seed, a non-GET method) falls back to the fetch transport.
 */
export function selectTransport(
  method: HttpMethod,
  args: SubscribeArgs | undefined,
  options: InitClientOptions,
  hasResolvedCustomHeaders: boolean,
): "eventsource" | "fetch" {
  const mode = options.transport ?? "auto";
  if (mode === "eventsource" || mode === "fetch") return mode;

  const canUseEventSource =
    method === "GET" &&
    args?.body === undefined &&
    args?.resumeFrom === undefined &&
    !hasResolvedCustomHeaders &&
    options.EventSource !== undefined;

  return canUseEventSource ? "eventsource" : "fetch";
}

// ── initClient ──────────────────────────────────────────────────────────────

export function initClient<C extends Record<string, SseDef>>(
  contract: C,
  options: InitClientOptions,
  deps?: ClientDeps,
): SseClient<C> {
  const transports = deps?.transports;

  // One `{ subscribe }` entry per contract route. The value types are erased to the
  // structural `EventsMap` here and re-attached by the final `as SseClient<C>` cast —
  // the route-literal generics flow through `SseClient<C>` unchanged.
  const client: Record<
    string,
    { subscribe(args?: SubscribeArgs): SseSubscription<EventsMap> }
  > = {};

  for (const routeKey of Object.keys(contract)) {
    const route = contract[routeKey] as SseDef;
    client[routeKey] = {
      subscribe: (args?: SubscribeArgs) =>
        createSubscription(route, options, transports, args),
    };
  }

  return client as SseClient<C>;
}

// ── One live subscription ─────────────────────────────────────────────────────

function createSubscription(
  route: SseDef,
  options: InitClientOptions,
  transports: ClientDeps["transports"],
  args: SubscribeArgs | undefined,
): SseSubscription<EventsMap> {
  // `subscribe()` returns synchronously, but building a transport is inescapably
  // async (a HeaderValue may be a `() => Promise<string>`). So this is a programmer
  // error we can and should report right away, not a runtime stream failure.
  if (!transports) {
    throw new Error(
      "ts-sse: transports not wired — call initClient(contract, options, { transports }). " +
        "The @alivan/ts-sse/client barrel supplies the real fetch/eventsource factories.",
    );
  }

  let state: SseClientState = "connecting";

  // ── Listener registries ──────────────────────────────────────────────────
  // `.on` handlers are keyed by event name; the four lifecycle channels are flat sets.
  const eventHandlers = new Map<string, Set<(data: unknown, meta: EventMeta) => void>>();
  const openListeners = new Set<() => void>();
  const closeListeners = new Set<() => void>();
  const connErrorListeners = new Set<(err: SseConnectionError) => void>();
  const validationListeners = new Set<(err: SseValidationError) => void>();

  function register<T>(set: Set<T>, cb: T): Unsubscribe {
    set.add(cb);
    return () => {
      set.delete(cb);
    };
  }

  // ── Async push/pull buffer ─────────────────────────────────────────────────
  // A value pushed before a pull is buffered in `queue`; a pull before a push parks
  // in the single `pending` slot (for-await consumes serially, so one slot suffices).
  const queue: ClientEvent<EventsMap>[] = [];
  let pending:
    | {
        resolve: (result: IteratorResult<ClientEvent<EventsMap>>) => void;
        reject: (reason: unknown) => void;
      }
    | undefined;
  let ended = false; // graceful done (onClose / EOS / close())
  let failed = false; // fatal — `failure` is the reject reason
  let failure: unknown;

  /** Deliver a decoded event to a waiting pull, else buffer it. No-op once terminal. */
  function push(event: ClientEvent<EventsMap>): void {
    if (ended || failed) return;
    if (pending) {
      const { resolve } = pending;
      pending = undefined;
      resolve({ value: event, done: false });
    } else {
      queue.push(event);
    }
  }

  /** Graceful end: drain what is buffered, then complete the iterator as done. */
  function finish(): void {
    if (ended || failed) return;
    ended = true;
    if (pending) {
      const { resolve } = pending;
      pending = undefined;
      resolve({ value: undefined, done: true });
    }
  }

  /** Fatal end: reject a waiting pull now, and any later one, with `err`. */
  function fail(err: unknown): void {
    if (ended || failed) return;
    failed = true;
    failure = err;
    if (pending) {
      const { reject } = pending;
      pending = undefined;
      reject(err);
    }
  }

  function next(): Promise<IteratorResult<ClientEvent<EventsMap>>> {
    // Buffered values win, so a graceful end still yields everything already queued.
    const buffered = queue.shift();
    if (buffered !== undefined) return Promise.resolve({ value: buffered, done: false });
    if (failed) return Promise.reject(failure);
    if (ended) return Promise.resolve({ value: undefined, done: true });
    return new Promise((resolve, reject) => {
      pending = { resolve, reject };
    });
  }

  // ── Validation-error plumbing ───────────────────────────────────────────────

  const makeIssues = (message: string): SseValidationError["issues"] => [{ message }];

  /** Always surface to `onValidationError` listeners; never yields, never reconnects. */
  function emitValidationError(err: SseValidationError): void {
    for (const cb of validationListeners) cb(err);
  }

  /** A schema/JSON decode failure on a KNOWN event — governed by `onValidationError`. */
  function reportValidation(err: SseValidationError): void {
    const mode = options.onValidationError ?? "skip";
    if (mode === "throw") fail(err); // the async-iterator rejects with the error
    else if (mode === "emit") emitValidationError(err);
    // "skip" → silently drop the frame.
  }

  // ── Frame decoding (the decoder invariant) ─────────────────────────────────

  function decodeFrame(raw: RawFrame): void {
    // The reserved terminal sentinel is a clean end, not a data event. (Transports
    // normally translate it to onClose; guard here too so it can never masquerade as
    // an unknown event.)
    if (raw.event === TS_SSE_EOS) {
      handleClose();
      return;
    }

    const schema = route.events[raw.event];
    if (schema === undefined) {
      // Unknown event: skip-with-onValidationError (DESIGN — no schema to run).
      emitValidationError({
        event: raw.event,
        issues: makeIssues(`ts-sse: no schema for event '${raw.event}'`),
        raw: raw.data,
        lastEventId: raw.id,
      });
      return;
    }

    // Wire → JSON (the schema's INPUT value).
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw.data);
    } catch (err) {
      reportValidation({
        event: raw.event,
        issues: makeIssues(err instanceof Error ? err.message : "invalid JSON payload"),
        raw: raw.data,
        lastEventId: raw.id,
      });
      return;
    }

    // Re-decode INPUT→OUTPUT unless validation was explicitly disabled.
    let data: unknown;
    if (options.validateEvents === false) {
      data = parsed;
    } else {
      const result = validateSync(schema, parsed);
      if (!result.ok) {
        reportValidation({
          event: raw.event,
          issues: result.issues,
          raw: raw.data,
          lastEventId: raw.id,
        });
        return;
      }
      data = result.value;
    }

    const meta: EventMeta = { id: raw.id, lastEventId: raw.id, retry: raw.retry };
    const event = {
      event: raw.event,
      data,
      ...meta,
    } as ClientEvent<EventsMap>;

    // (a) typed `.on(name)` handlers, then (b) the async-iterator queue.
    const handlers = eventHandlers.get(raw.event);
    if (handlers) for (const cb of handlers) cb(data, meta);
    push(event);
  }

  // ── Terminal helpers ────────────────────────────────────────────────────────

  /** Server-driven clean close: fire onClose listeners once, complete the iterator. */
  function handleClose(): void {
    if (state === "closed") return; // idempotent; also guards against a post-fatal close
    state = "closed";
    for (const cb of closeListeners) cb();
    finish();
  }

  let transport: Transport | undefined;
  let userClosed = false;

  /** Caller-driven permanent stop: cancel reconnection and complete the iterator. */
  function close(): void {
    if (userClosed) return;
    userClosed = true;
    state = "closed";
    finish();
    transport?.close(); // may be undefined if we close before the async start; idempotent
  }

  // ── Transport handlers (the three disjoint channels) ────────────────────────

  const handlers: TransportHandlers = {
    onOpen() {
      state = "open";
      for (const cb of openListeners) cb();
    },
    onFrame(frame) {
      decodeFrame(frame);
    },
    onError(err) {
      // The ONLY reconnect-driving channel.
      for (const cb of connErrorListeners) cb(err);
      if (err.retriable) {
        // Transport is retrying; the iterator keeps yielding (stays pending).
        state = "reconnecting";
      } else {
        // Fatal: the transport has already stopped. Reject the iterator.
        state = "closed";
        fail(err);
      }
    },
    onClose() {
      handleClose();
    },
  };

  // ── Async request build + transport start (deferred, non-blocking) ──────────
  void (async () => {
    try {
      const { config, hasCustomHeaders } = await buildTransportConfig(route, options, args);
      if (userClosed) return; // closed before we could start
      const kind = selectTransport(route.method, args, options, hasCustomHeaders);
      transport = transports[kind](config, handlers);
      transport.start();
    } catch (err) {
      // A missing path param or a rejected header function is fatal for this stream.
      if (!userClosed) fail(err);
    }
  })();

  // ── Public subscription surface ─────────────────────────────────────────────

  const subscription: SseSubscription<EventsMap> = {
    on(name, cb) {
      const key = name as string;
      let set = eventHandlers.get(key);
      if (!set) {
        set = new Set();
        eventHandlers.set(key, set);
      }
      const handler = cb as (data: unknown, meta: EventMeta) => void;
      set.add(handler);
      return () => {
        set.delete(handler);
      };
    },
    onOpen: (cb) => register(openListeners, cb),
    onClose: (cb) => register(closeListeners, cb),
    onConnectionError: (cb) => register(connErrorListeners, cb),
    onValidationError: (cb) => register(validationListeners, cb),
    get state() {
      return state;
    },
    close,
    [Symbol.asyncIterator]() {
      return {
        next,
        // `break`/`return` out of a for-await tears the subscription down.
        return: async () => {
          close();
          return { value: undefined, done: true };
        },
      };
    },
  };

  return subscription;
}

// ── Request building (URL + headers + body) ────────────────────────────────────

/**
 * Resolve everything a transport needs. Async because a {@link HeaderValue} may be a
 * `() => Promise<string>`. `hasCustomHeaders` = the caller supplied at least one
 * header (base or per-subscribe) — native EventSource cannot set those, so it feeds
 * {@link selectTransport}.
 */
async function buildTransportConfig(
  route: SseDef,
  options: InitClientOptions,
  args: SubscribeArgs | undefined,
): Promise<{ config: TransportConfig; hasCustomHeaders: boolean }> {
  // baseHeaders first, per-subscribe headers override.
  const headerSources: Record<string, HeaderValue> = {
    ...options.baseHeaders,
    ...args?.headers,
  };
  const hasCustomHeaders = Object.keys(headerSources).length > 0;

  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(headerSources)) {
    headers[name] = typeof value === "function" ? await value() : value;
  }

  // JSON body forces a Content-Type (unless the caller set one already).
  let body: string | undefined;
  if (args?.body !== undefined) {
    body = JSON.stringify(args.body);
    if (!hasHeader(headers, "content-type")) headers["Content-Type"] = "application/json";
  }

  const config: TransportConfig = {
    url: buildUrl(route.path, options.baseUrl, args),
    method: route.method,
    headers,
    body,
    eventNames: Object.keys(route.events),
    resumeFrom: args?.resumeFrom,
    withCredentials: options.withCredentials,
    reconnect: resolveReconnect(options.reconnect),
    fetchImpl: options.fetch ?? fetch,
    EventSourceImpl: options.EventSource,
    signal: args?.signal,
  };

  return { config, hasCustomHeaders };
}

/** `baseUrl` + path with `:param` substituted (encoded) + a query string. */
function buildUrl(path: string, baseUrl: string, args: SubscribeArgs | undefined): string {
  const filledPath = path.replace(/:([A-Za-z0-9_]+)/g, (_match, name: string) => {
    const value = args?.params?.[name];
    if (value === undefined) {
      throw new Error(`ts-sse: missing path parameter ':${name}' for '${path}'`);
    }
    return encodeURIComponent(String(value));
  });

  const search = new URLSearchParams();
  if (args?.query) {
    for (const [key, value] of Object.entries(args.query)) {
      if (value === undefined) continue; // skip undefined
      search.append(key, String(value)); // stringify numbers/booleans
    }
  }
  const qs = search.toString();
  return qs ? `${baseUrl}${filledPath}?${qs}` : `${baseUrl}${filledPath}`;
}

/** Case-insensitive header presence check. */
function hasHeader(headers: Record<string, string>, name: string): boolean {
  const target = name.toLowerCase();
  return Object.keys(headers).some((key) => key.toLowerCase() === target);
}
