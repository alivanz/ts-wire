/**
 * Fetch-runtime output: `sseResponse` returns a WHATWG `Response` you hand back from
 * your route (Hono / Next App Router / Bun / Deno / Workers). `sseStream` exposes the
 * raw stream + headers for callers that build their own `Response`.
 */
import type { SseDef } from "../contract.js";
import { validateSync } from "../../core/schema.js";
import type { StandardSchemaV1 } from "../../core/schema.js";
import { runStream } from "../server/run.js";
import { ResponseSink } from "../server/sink.js";
import type { SseHandler } from "../server/types.js";

// Public handler-facing types (also re-exported from ts-wire/sse/node).
export type {
  Emit,
  EmitControls,
  EmitOpts,
  FrameSink,
  InitOptions,
  QueryOutput,
  SseContext,
  SseHandler,
} from "../server/types.js";
export { SinkAbortedError } from "../server/sink.js";

/** Headers every SSE response needs — long-lived, unbuffered, uncompressed. */
export const SSE_HEADERS: Record<string, string> = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  "x-accel-buffering": "no",
};

type QueryResult =
  | { ok: true; value: Record<string, unknown> }
  | { ok: false; issues: ReadonlyArray<StandardSchemaV1.Issue> };

/** Validate/coerce the query string against the contract's optional `query` schema. */
function parseQuery(contract: SseDef, params: URLSearchParams): QueryResult {
  if (!contract.query) return { ok: true, value: {} };
  const raw = Object.fromEntries(params);
  const res = validateSync(contract.query, raw);
  if (!res.ok) return { ok: false, issues: res.issues };
  return { ok: true, value: res.value as Record<string, unknown> };
}

function badQuery(issues: ReadonlyArray<StandardSchemaV1.Issue>): Response {
  return new Response(JSON.stringify({ error: "invalid query", issues }), {
    status: 400,
    headers: { "content-type": "application/json" },
  });
}

/**
 * Run an SSE handler and return a `Response` whose body streams `text/event-stream`.
 * An invalid query is a `400` *before* the stream opens.
 */
export function sseResponse<D extends SseDef>(
  contract: D,
  req: Request,
  fn: SseHandler<D>,
): Response {
  const parsed = parseQuery(contract, new URL(req.url).searchParams);
  if (!parsed.ok) return badQuery(parsed.issues);

  const sink = new ResponseSink(req.signal);
  const lastEventId = req.headers.get("last-event-id") ?? undefined;
  // Fire the handler; it feeds `sink.stream` in the background.
  void runStream(contract, { query: parsed.value, lastEventId }, fn, sink);

  return new Response(sink.stream, { headers: SSE_HEADERS });
}

/**
 * Like {@link sseResponse} but returns the raw `{ stream, headers }` for callers that
 * assemble their own `Response`. Throws on an invalid query (no `Response` to 400 with).
 */
export function sseStream<D extends SseDef>(
  contract: D,
  req: Request,
  fn: SseHandler<D>,
): { stream: ReadableStream<Uint8Array>; headers: Record<string, string> } {
  const parsed = parseQuery(contract, new URL(req.url).searchParams);
  if (!parsed.ok) {
    throw new Error(
      `ts-sse: invalid query: ${parsed.issues.map((i) => i.message).join("; ")}`,
    );
  }
  const sink = new ResponseSink(req.signal);
  const lastEventId = req.headers.get("last-event-id") ?? undefined;
  void runStream(contract, { query: parsed.value, lastEventId }, fn, sink);
  return { stream: sink.stream, headers: SSE_HEADERS };
}
