/**
 * Shared wire-format types + constants for the SSE codec.
 *
 * `serialize.ts` turns an {@link OutgoingFrame} into bytes; `parse.ts` turns bytes
 * back into a {@link RawFrame}. They MUST agree on these shapes, so they live here.
 *
 * The load-bearing invariant (see DESIGN.md "the decoder invariant"):
 * `OutgoingFrame.data` is the JSON of the schema's INPUT value — never the parsed
 * output — so the client can re-decode it with the same schema.
 */

/** The reserved terminal event. `emit.close()` writes this, then closes the stream. */
export const TS_SSE_EOS = "ts-sse-eos" as const;

/** One second after which native EventSource retries by default; exported for reference. */
export const DEFAULT_HEARTBEAT_MS = 15_000;

/**
 * A single event ready to be written to the wire.
 * `data` is already JSON-stringified (the schema INPUT). `event` omitted => the
 * frame carries no `event:` line and dispatches as the default "message" event.
 */
export interface OutgoingFrame {
  event?: string;
  /** JSON string of the schema INPUT value. May contain newlines (multi-line data). */
  data: string;
  /** SSE `id:` — surfaces as `lastEventId` on the client and drives resume. */
  id?: string;
  /** SSE `retry:` — reconnection time hint in ms. */
  retry?: number;
}

/**
 * A frame decoded off the wire, pre-schema. `data` is the raw string (data lines
 * joined by "\n"); `event` defaults to "message" when the stream sent no `event:`.
 */
export interface RawFrame {
  event: string;
  data: string;
  /** The most recent `id:` seen on the stream (per spec, id persists across events). */
  id?: string;
  retry?: number;
}
