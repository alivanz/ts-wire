/**
 * ts-wire/ws/node — the server-side socket for the node `ws` library.
 *
 * The node `ws` server hands you a socket whose `send(data, cb)` invokes `cb` once the
 * frame has been flushed to the OS (real backpressure), and delivers incoming messages
 * as `RawData` (`string | ArrayBuffer | Buffer | Buffer[]`). This module wraps such a
 * socket with the shared string codec: it SENDS `server`-schema INPUT and DECODES
 * `client`-schema OUTPUT (the [decoder invariant] — the wire always carries INPUT).
 *
 * We deliberately depend on neither `ws` nor `@types/ws`: a minimal structural
 * {@link NodeSocket} interface is all we touch (mirroring `EventSourceLike` in the SSE
 * client), so the package stays dependency-free and trivially fakeable in tests.
 */
import type { ClientOutput, ServerInput, WsDef } from "../contract.js";
import type { DecodeResult } from "../codec.js";
import { decodeMessage, encodeMessage } from "../codec.js";

/** Minimal slice of a node `ws` WebSocket we use (no `ws` dependency). */
export interface NodeSocket {
  send(data: string, cb?: (err?: Error) => void): void;
}

/**
 * What node `ws` delivers as an incoming message. `RawData` is
 * `Buffer | ArrayBuffer | Buffer[]`; `Buffer`/`Uint8Array` are `ArrayBufferView`, and a
 * message split across frames arrives as an array of views.
 */
export type NodeRawData = string | ArrayBuffer | ArrayBufferView | ReadonlyArray<ArrayBufferView>;

export interface WsNodeSocket<D extends WsDef> {
  /** Send a server→client message; resolves when the socket has flushed it (backpressure). */
  send(msg: ServerInput<D>): Promise<void>;
  decode(raw: NodeRawData): DecodeResult<ClientOutput<D>>;
}

/**
 * Normalize a node `ws` `RawData` value to the JSON text the codec expects.
 *
 * - `string` — passthrough (text frames).
 * - `ArrayBuffer` / `ArrayBufferView` (`Buffer`, `Uint8Array`) — UTF-8 decode.
 * - `ArrayBufferView[]` — a single message delivered as fragments. This is rare for JSON
 *   text (`ws` coalesces text frames), so we decode each fragment and join naively; the
 *   concatenation of the UTF-8 pieces is the intended payload.
 */
function toText(raw: NodeRawData): string {
  if (typeof raw === "string") return raw;
  const decode = (view: ArrayBuffer | ArrayBufferView): string => new TextDecoder().decode(view);
  if (Array.isArray(raw)) return raw.map((part) => decode(part as ArrayBufferView)).join("");
  return decode(raw as ArrayBuffer | ArrayBufferView);
}

export function wsSocket<D extends WsDef>(contract: D, ws: NodeSocket): WsNodeSocket<D> {
  // `async` so a synchronous `encodeMessage` throw (invalid outgoing message) surfaces as
  // a rejected Promise rather than a synchronous exception.
  async function send(msg: ServerInput<D>): Promise<void> {
    const str = encodeMessage(contract.server, msg);
    return new Promise<void>((resolve, reject) => {
      ws.send(str, (err) => (err ? reject(err) : resolve()));
    });
  }

  function decode(raw: NodeRawData): DecodeResult<ClientOutput<D>> {
    return decodeMessage<ClientOutput<D>>(contract.client, toText(raw));
  }

  return { send, decode } as WsNodeSocket<D>;
}
