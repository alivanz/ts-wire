/**
 * `initClient` — the transport-agnostic ORCHESTRATOR for ONE SSE endpoint.
 *
 * A contract is a single {@link SseDef} (`{ query?, events }`), so `initClient`
 * returns a single {@link SseEndpoint} with one `.subscribe()`. There is no path,
 * no method, no router: the full endpoint URL lives in `options.url`, and the
 * optional `query` schema types + validates the query string on `subscribe`.
 *
 * A `subscribe(args?)` call:
 *
 *   1. builds the final URL SYNCHRONOUSLY (base `options.url` + validated query),
 *   2. starts the injected {@link Transport} (native EventSource only),
 *   3. wires the four {@link TransportHandlers} callbacks to the three disjoint
 *      client channels: data (`.on` + async-iterator), `onConnectionError` (the
 *      reconnect signal), and `onValidationError` (decode failures).
 *
 * This file never statically imports the network layer — the real EventSource
 * transport factory is injected through the `deps` seam (see {@link ClientDeps}),
 * which keeps `client.ts` unit-testable with a fake transport.
 *
 * The decoder invariant lives in {@link createSubscription}'s `decodeFrame`: the
 * wire carries a schema's INPUT JSON, so the client re-runs the schema INPUT→OUTPUT
 * and hands the consumer the OUTPUT value. That is why a genuinely non-round-tripping
 * transform (`z.string().transform(s => s.length)`) decodes `"hello"` to `5`.
 */
import type { EventsMap } from "../../core/schema.js";
import { validateSync } from "../../core/schema.js";
import type { SseDef } from "../contract.js";
import type { RawFrame } from "../wire.js";
import { TS_SSE_EOS } from "../wire.js";
import type { SseConnectionError, SseValidationError } from "./errors.js";
import type {
  AnySubscribeArgs,
  ClientEvent,
  EventMeta,
  InitClientOptions,
  SseClientState,
  SseEndpoint,
  SseSubscription,
  Transport,
  TransportConfig,
  TransportFactory,
  TransportHandlers,
  Unsubscribe,
} from "./types.js";

// ── Injection seam ────────────────────────────────────────────────────────────

/**
 * The real EventSource transport, injected by the barrel (`src/sse/client/index.ts`)
 * so this file never statically imports the network layer. Tests pass a fake here.
 *
 * When it is absent, `subscribe()` throws a clear "transport not wired" error: the
 * integrator is expected to always provide it, and surfacing that synchronously
 * flags a misconfiguration immediately instead of as a stalled subscription.
 */
export interface ClientDeps {
  transport?: TransportFactory;
}

// ── initClient ────────────────────────────────────────────────────────────────

export function initClient<D extends SseDef>(
  contract: D,
  options: InitClientOptions,
  deps?: ClientDeps,
): SseEndpoint<D> {
  // The endpoint's public `subscribe` is generic over `D` (typed query + event
  // union); the internal `createSubscription` works on the structural `EventsMap`
  // and loose runtime args. The single cast re-attaches the contract's literal
  // types at the boundary.
  const subscribe = (args?: AnySubscribeArgs): SseSubscription<EventsMap> =>
    createSubscription(contract, options, deps?.transport, args);

  return { subscribe } as unknown as SseEndpoint<D>;
}

// ── One live subscription ─────────────────────────────────────────────────────

function createSubscription(
  contract: SseDef,
  options: InitClientOptions,
  transport: TransportFactory | undefined,
  args: AnySubscribeArgs | undefined,
): SseSubscription<EventsMap> {
  // Build the URL first: an invalid query is a programmer error, reported eagerly
  // (a throw from `subscribe`) rather than as a runtime stream failure.
  const url = buildUrl(contract, options, args);

  // `subscribe()` is now fully synchronous, so a missing transport is likewise a
  // programmer error we can report right away.
  if (!transport) {
    throw new Error(
      "ts-sse: transport not wired — call initClient(contract, options, { transport }). " +
        "The ts-wire/sse/client barrel supplies it.",
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
    // "skip" (default) → silently drop the frame.
  }

  // ── Terminal helpers ────────────────────────────────────────────────────────

  /** Server-driven clean close: fire onClose listeners once, complete the iterator. */
  function handleClose(): void {
    if (state === "closed") return; // idempotent; also guards against a post-fatal close
    state = "closed";
    for (const cb of closeListeners) cb();
    finish();
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

    const schema = contract.events[raw.event];
    if (schema === undefined) {
      // Unknown event: skip-with-onValidationError (no schema to run).
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

  // ── Transport handlers (the three disjoint channels) ────────────────────────

  const transportHandlers: TransportHandlers = {
    onOpen() {
      state = "open";
      for (const cb of openListeners) cb();
    },
    onFrame(frame) {
      decodeFrame(frame);
    },
    onError(err) {
      // The reconnect-driving channel.
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

  // ── Transport start (synchronous — no async header resolution) ──────────────

  const config: TransportConfig = {
    url,
    eventNames: Object.keys(contract.events),
    withCredentials: options.withCredentials,
    EventSourceImpl: options.EventSource,
    signal: args?.signal,
  };

  const t: Transport = transport(config, transportHandlers);
  t.start();

  let userClosed = false;

  /** Caller-driven permanent stop: close the transport and complete the iterator. */
  function close(): void {
    if (userClosed) return;
    userClosed = true;
    state = "closed";
    finish();
    t.close(); // idempotent
  }

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

// ── URL building (base URL + validated query string) ────────────────────────────

/**
 * Build the final endpoint URL. The contract has no path, so we start from
 * `options.url`. When the contract declares a `query` schema we validate the
 * caller's `args.query` (defaulting to `{}`) against it — on failure this THROWS,
 * because an invalid query is a programmer error — then append the validated OUTPUT
 * as a query string (skipping null/undefined, `String()`-ing the rest). With no
 * query schema, `options.url` is used as-is and any `args.query` is ignored.
 */
function buildUrl(
  contract: SseDef,
  options: InitClientOptions,
  args: AnySubscribeArgs | undefined,
): string {
  if (contract.query === undefined) return options.url;

  const res = validateSync(contract.query, args?.query ?? {});
  if (!res.ok) {
    throw new Error(
      `ts-sse: invalid query: ${res.issues.map((i) => i.message).join("; ")}`,
    );
  }

  const validated = res.value as Record<string, unknown>;
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(validated)) {
    if (value === undefined || value === null) continue; // skip null/undefined
    search.append(key, String(value)); // stringify numbers/booleans/etc.
  }

  const qs = search.toString();
  if (!qs) return options.url;
  const sep = options.url.includes("?") ? "&" : "?";
  return `${options.url}${sep}${qs}`;
}
