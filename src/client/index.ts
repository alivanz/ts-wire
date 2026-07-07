/**
 * Public entry for `@alivan/ts-sse/client`.
 *
 * `client.ts` is deliberately transport-agnostic (testable with fakes). This barrel
 * is the one place that wires the real fetch + EventSource transports, so end users
 * just call `initClient(contract, options)` with nothing to inject.
 */
import { initClient as initClientCore } from "./client.js";
import { fetchTransport } from "./fetch-transport.js";
import { eventSourceTransport } from "./eventsource-transport.js";
import type { SseDef } from "../core/contract.js";
import type { InitClientOptions, SseClient } from "./types.js";

/** Create a fully-typed SSE client from a contract, with the real transports wired in. */
export function initClient<C extends Record<string, SseDef>>(
  contract: C,
  options: InitClientOptions,
): SseClient<C> {
  return initClientCore(contract, options, {
    transports: { fetch: fetchTransport, eventsource: eventSourceTransport },
  });
}

export { selectTransport } from "./client.js";
export { fetchTransport } from "./fetch-transport.js";
export { eventSourceTransport } from "./eventsource-transport.js";
export * from "./errors.js";
export * from "./types.js";
