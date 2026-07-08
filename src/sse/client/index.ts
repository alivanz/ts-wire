/**
 * Public entry for `ts-wire/sse/client`.
 *
 * `client.ts` is transport-agnostic (testable with a fake). This barrel is the one
 * place that wires the real native-EventSource transport, so end users just call
 * `initClient(contract, options)`.
 */
import { initClient as initClientCore } from "./client.js";
import { eventSourceTransport } from "./eventsource-transport.js";
import type { SseDef } from "../contract.js";
import type { InitClientOptions, SseEndpoint } from "./types.js";

/** Create a fully-typed SSE client from a contract, with the EventSource transport wired in. */
export function initClient<D extends SseDef>(
  contract: D,
  options: InitClientOptions,
): SseEndpoint<D> {
  return initClientCore(contract, options, { transport: eventSourceTransport });
}

export { eventSourceTransport } from "./eventsource-transport.js";
export * from "./errors.js";
export * from "./types.js";
