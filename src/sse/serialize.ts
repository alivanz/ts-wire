/**
 * The SSE wire SERIALIZER: {@link OutgoingFrame} -> UTF-8 bytes.
 *
 * This is the encoding half of the codec; `parse.ts` is the decoding half. The two
 * MUST agree byte-for-byte, so the exact line grammar produced here is the contract:
 *
 *   `${field}: ${value}\n`         one field line — colon, ONE space, value, LF
 *   event? -> id? -> retry? -> data+ -> ""   line order within a frame
 *   ...\n\n                        a frame always ends with a blank line (LF LF)
 *
 * The load-bearing rule lives in {@link toOutgoingFrame}: what goes on the wire is
 * the schema's INPUT (the JSON of the value handed to `emit`), NEVER the validated
 * output — see DESIGN.md "the decoder invariant".
 */
import type { OutgoingFrame } from "./wire.js";
import type { InferIn, StandardSchemaV1 } from "../core/schema.js";
import { validateSync } from "../core/schema.js";

/** Single UTF-8 encoder, reused across all frames (encoders are stateless + cheap to share). */
const encoder = new TextEncoder();

/**
 * Serialize an {@link OutgoingFrame} to its wire string (no bytes). Exposed alongside
 * {@link serializeFrame} so tests can assert the exact grammar without decoding.
 *
 * Field order is fixed — `event`, `id`, `retry`, then the `data` line(s) — and the
 * frame is closed with a trailing blank line, so the whole string ends in `\n\n`.
 */
export function serializeFrameToString(frame: OutgoingFrame): string {
  let out = "";

  // `event:` — emitted only when explicitly set. `undefined` => default "message".
  if (frame.event !== undefined) {
    out += `event: ${frame.event}\n`;
  }

  // `id:` — must survive a round-trip, so reject the bytes that would break framing:
  // NUL is disallowed by the spec, and CR/LF would forge extra lines.
  if (frame.id !== undefined) {
    if (/[\0\n\r]/.test(frame.id)) {
      throw new RangeError("ts-sse: SSE id must not contain NUL, CR, or LF");
    }
    out += `id: ${frame.id}\n`;
  }

  // `retry:` — a reconnection hint in ms; only a non-negative integer is meaningful.
  if (frame.retry !== undefined) {
    assertRetry(frame.retry);
    out += `retry: ${frame.retry}\n`;
  }

  // `data:` — one line per `\n`-delimited segment, so multi-line payloads round-trip.
  // Splitting always yields >= 1 segment, so an empty string still emits `data: `.
  for (const segment of frame.data.split("\n")) {
    out += `data: ${segment}\n`;
  }

  // The blank line that terminates the frame.
  out += "\n";
  return out;
}

/** Serialize an {@link OutgoingFrame} to UTF-8 bytes for writing to the response stream. */
export function serializeFrame(frame: OutgoingFrame): Uint8Array {
  return encoder.encode(serializeFrameToString(frame));
}

/**
 * Serialize an SSE comment (a line beginning with `:`), used for heartbeats/keep-alives.
 * Produces `: <text>\n\n`; multi-line text emits one `: <segment>` line per segment.
 */
export function serializeComment(text: string): Uint8Array {
  let out = "";
  for (const segment of text.split("\n")) {
    out += `: ${segment}\n`;
  }
  out += "\n";
  return encoder.encode(out);
}

/** Serialize a standalone `retry:` directive: `retry: <ms>\n\n`. */
export function serializeRetry(ms: number): Uint8Array {
  assertRetry(ms);
  return encoder.encode(`retry: ${ms}\n\n`);
}

/** Guard shared by `retry` on a frame and {@link serializeRetry}: ms must be a whole, non-negative count. */
function assertRetry(ms: number): void {
  if (!Number.isInteger(ms) || ms < 0) {
    throw new RangeError(`ts-sse: retry must be a non-negative integer, got ${ms}`);
  }
}

/**
 * Build an {@link OutgoingFrame} for `event`, validating `input` first.
 *
 * THE DECODER INVARIANT: we run the schema purely as a fail-fast pre-check, then put
 * the *input* JSON on the wire and DISCARD `result.value`. The client re-decodes that
 * input with the same schema, so any coercion/transform happens client-side — exactly
 * once, in the schema's natural direction. Serializing `result.value` here would ship
 * the already-transformed output and corrupt that round-trip.
 */
export function toOutgoingFrame<S extends StandardSchemaV1>(
  schema: S,
  event: string,
  input: InferIn<S>,
  opts?: { id?: string; retry?: number },
): OutgoingFrame {
  const result = validateSync(schema, input);
  if (!result.ok) {
    throw new Error(`ts-sse: invalid data for event "${event}": ${JSON.stringify(result.issues)}`);
  }

  // Deliberately ignore `result.value` — the INPUT is what travels, not the output.
  return {
    event,
    data: JSON.stringify(input),
    ...(opts?.id !== undefined && { id: opts.id }),
    ...(opts?.retry !== undefined && { retry: opts.retry }),
  };
}
