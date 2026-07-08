/**
 * ts-wire/ws/cf — the Cloudflare-flavored WebSocket server socket.
 *
 * Cloudflare Durable Objects hand you the standard WHATWG {@link WebSocket}; incoming
 * frames arrive as `string | ArrayBuffer`. {@link wsSocket} is a STATELESS typed facade
 * over that socket — create one on demand (even inside a hibernating DO), use it, drop
 * it. It wraps only what adds value:
 *
 *   - {@link WsCfSocket.send}   — validate the server-schema INPUT, then `ws.send(JSON)`.
 *   - {@link WsCfSocket.decode} — normalize the raw frame to text, then re-validate the
 *                                 client-schema INPUT→OUTPUT ([decoder invariant]).
 *
 * Closing stays native (`ws.close(...)`) — there is nothing to add, so it is not wrapped.
 */
import { decodeMessage, encodeMessage } from "../codec.js";
import type { DecodeResult } from "../codec.js";
import type { ClientOutput, ServerInput, WsDef } from "../contract.js";

/** Minimal slice of the CF/standard WebSocket we send on. */
export interface CfSocket {
  send(data: string): void;
}

export interface WsCfSocket<D extends WsDef> {
  /** Send a server→client message (validated server-schema INPUT, then ws.send(JSON)). */
  send(msg: ServerInput<D>): Promise<void>;
  /** Decode an incoming client→server frame (client-schema OUTPUT, re-validated). */
  decode(raw: string | ArrayBuffer): DecodeResult<ClientOutput<D>>;
}

/** The message-type-erased shape; the per-contract projections are restored by the cast. */
interface ErasedCfSocket {
  send(msg: unknown): Promise<void>;
  decode(raw: string | ArrayBuffer): DecodeResult<unknown>;
}

/**
 * Build a stateless typed facade over a Cloudflare/standard WebSocket. `contract.server`
 * types & validates outgoing frames; `contract.client` types & validates incoming ones.
 */
export function wsSocket<D extends WsDef>(contract: D, ws: CfSocket): WsCfSocket<D> {
  const socket: ErasedCfSocket = {
    // `async` so an invalid message (encodeMessage throws) rejects the returned Promise
    // instead of throwing synchronously — the type is uniformly Promise<void>.
    async send(msg: unknown): Promise<void> {
      ws.send(encodeMessage(contract.server, msg));
    },
    decode(raw: string | ArrayBuffer): DecodeResult<unknown> {
      const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
      return decodeMessage(contract.client, text);
    },
  };
  return socket as WsCfSocket<D>;
}
