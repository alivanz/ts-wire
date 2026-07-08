/**
 * The SSE wire PARSER: UTF-8 bytes / text -> {@link RawFrame}s.
 *
 * This is the decoding half of the codec; `serialize.ts` is the encoding half. It
 * implements the WHATWG HTML "event stream" parsing model EXACTLY, so anything the
 * serializer emits round-trips, and so a real server's raw stream also parses:
 *
 *   - lines split on `\r\n`, `\r`, or `\n`;
 *   - a field line is `field:value` (first `:` only, one leading value space stripped);
 *   - `:`-prefixed lines are comments; a bare line is `field` with an empty value;
 *   - `data` accumulates across lines, `id` PERSISTS across frames, `retry` is per-block;
 *   - a BLANK line dispatches the block — but only when a `data` field appeared.
 *
 * Three entry points share one core: {@link parseSseText} (whole blob),
 * {@link createSseParser} (incremental, byte-chunk safe), and {@link parseSseStream}
 * (an async generator for the client transport).
 */
import type { RawFrame } from "./wire.js";

/**
 * An incremental SSE parser. It owns the cross-chunk state the spec requires —
 * a partial-line buffer, the current block, and the persistent `lastEventId` — so
 * callers can `feed()` arbitrary byte/string splits and receive frames as they complete.
 */
export function createSseParser(): {
  feed(chunk: string | Uint8Array): RawFrame[];
  end(): RawFrame[];
} {
  // Streaming UTF-8 decoder: `{ stream: true }` holds back any multi-byte sequence
  // split across a chunk boundary. `ignoreBOM: true` passes a leading BOM through so
  // we strip it in ONE place (below), uniformly for both the byte and string paths.
  const decoder = new TextDecoder("utf-8", { ignoreBOM: true });

  // Undispatched tail: everything after the last line terminator we have committed to.
  // A lone trailing `\r` lives here too — it might be the CR of a `\r\n` split across chunks.
  let buffer = "";
  // Whether we are still at the very start of the stream (for the one-time BOM strip).
  let atStreamStart = true;

  // ---- the current block being assembled (reset on every dispatch) ----
  let eventType = "";
  let dataBuffer = "";
  let retry: number | undefined;
  // ---- persists ACROSS blocks until an `id:` line changes it ----
  let lastEventId: string | undefined;

  /** Drop a single leading BOM (U+FEFF) the first time the stream produces any character. */
  function stripBomOnce(): void {
    if (atStreamStart && buffer.length > 0) {
      if (buffer.charCodeAt(0) === 0xfeff) buffer = buffer.slice(1);
      atStreamStart = false;
    }
  }

  /** Apply one field to the current block, per the spec's field-name switch. */
  function processField(field: string, value: string): void {
    switch (field) {
      case "event":
        // The block's event type; empty/absent means the default "message" at dispatch.
        eventType = value;
        break;
      case "data":
        // Each data line contributes its value plus a trailing LF (removed at dispatch).
        dataBuffer += value + "\n";
        break;
      case "id":
        // Persists across frames. A NUL in the value is invalid framing => ignore it.
        if (!value.includes("\0")) lastEventId = value;
        break;
      case "retry":
        // Only an all-ASCII-digits value is a valid reconnection hint; else ignore.
        if (/^[0-9]+$/.test(value)) retry = parseInt(value, 10);
        break;
      default:
        // Unknown field — the spec says ignore.
        break;
    }
  }

  /** Emit the current block as a frame if it carried data, then reset for the next block. */
  function dispatch(out: RawFrame[]): void {
    // The spec's "no event when the data buffer is empty" rule: a block with no `data`
    // field (a lone comment, `event:`, or `id:`) produces nothing — just reset.
    if (dataBuffer === "") {
      eventType = "";
      retry = undefined;
      return;
    }

    // A `data` field always appended a trailing "\n"; strip exactly that one.
    const data = dataBuffer.endsWith("\n") ? dataBuffer.slice(0, -1) : dataBuffer;

    const frame: RawFrame = { event: eventType || "message", data };
    if (lastEventId !== undefined) frame.id = lastEventId;
    if (retry !== undefined) frame.retry = retry;
    out.push(frame);

    // Reset the block. `lastEventId` deliberately survives.
    eventType = "";
    dataBuffer = "";
    retry = undefined;
  }

  /** Route one complete line: blank => dispatch, `:`-prefixed => comment, else a field. */
  function processLine(line: string, out: RawFrame[]): void {
    if (line === "") {
      dispatch(out);
      return;
    }
    if (line.charCodeAt(0) === 0x3a /* ":" */) {
      // Comment line — ignored entirely (heartbeats/keep-alives arrive this way).
      return;
    }

    const colon = line.indexOf(":");
    let field: string;
    let value: string;
    if (colon === -1) {
      // No colon: the whole line is the field name, value is empty.
      field = line;
      value = "";
    } else {
      field = line.slice(0, colon);
      value = line.slice(colon + 1);
      // Strip a SINGLE U+0020 immediately after the colon; keep any further spaces.
      if (value.charCodeAt(0) === 0x20) value = value.slice(1);
    }
    processField(field, value);
  }

  /**
   * Pull every complete line out of `buffer` into `out`, leaving the unterminated tail.
   * With `final`, a trailing `\r` is a real terminator (no more bytes are coming);
   * otherwise it is held back in case the next chunk begins with `\n`.
   */
  function extractLines(out: RawFrame[], final: boolean): void {
    let i = 0;
    let lineStart = 0;
    while (i < buffer.length) {
      const code = buffer.charCodeAt(i);
      if (code === 0x0a /* \n */) {
        processLine(buffer.slice(lineStart, i), out);
        i += 1;
        lineStart = i;
      } else if (code === 0x0d /* \r */) {
        if (i === buffer.length - 1 && !final) {
          // Ambiguous lone trailing CR — wait for the next chunk to disambiguate CRLF.
          break;
        }
        processLine(buffer.slice(lineStart, i), out);
        // Consume the CR, plus a following LF so `\r\n` counts as one terminator.
        i += buffer.charCodeAt(i + 1) === 0x0a ? 2 : 1;
        lineStart = i;
      } else {
        i += 1;
      }
    }
    buffer = buffer.slice(lineStart);
  }

  function feed(chunk: string | Uint8Array): RawFrame[] {
    const out: RawFrame[] = [];
    // Strings pass through untouched; bytes go through the streaming UTF-8 decoder.
    buffer += typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true });
    stripBomOnce();
    extractLines(out, false);
    return out;
  }

  function end(): RawFrame[] {
    const out: RawFrame[] = [];
    // Flush any bytes the decoder was holding (an incomplete sequence => replacement char).
    buffer += decoder.decode();
    stripBomOnce();
    // No more input: a held trailing CR is now a genuine terminator.
    extractLines(out, true);
    // Whatever remains is the final, unterminated line. Parse it as if a newline
    // followed — that terminates the LINE, not the block, so it can never dispatch
    // (an event at EOF without its closing blank line is intentionally discarded).
    if (buffer.length > 0) {
      processLine(buffer, out);
      buffer = "";
    }
    return out;
  }

  return { feed, end };
}

/** Parse a complete SSE blob in one shot: feed it all, then flush the trailing line. */
export function parseSseText(text: string): RawFrame[] {
  const parser = createSseParser();
  return parser.feed(text).concat(parser.end());
}

/**
 * Parse a live byte stream, yielding {@link RawFrame}s as blocks complete. Accepts both
 * an async-iterable of chunks and a WHATWG {@link ReadableStream} (falling back to a
 * reader when `Symbol.asyncIterator` is absent, e.g. on browser streams).
 */
export async function* parseSseStream(
  source: AsyncIterable<Uint8Array> | ReadableStream<Uint8Array>,
): AsyncGenerator<RawFrame> {
  const parser = createSseParser();

  if (Symbol.asyncIterator in source) {
    for await (const chunk of source as AsyncIterable<Uint8Array>) {
      for (const frame of parser.feed(chunk)) yield frame;
    }
  } else {
    const reader = (source as ReadableStream<Uint8Array>).getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value !== undefined) {
          for (const frame of parser.feed(value)) yield frame;
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  // Flush the final (unterminated) line — matches `parseSseText`'s feed-then-end shape.
  for (const frame of parser.end()) yield frame;
}
