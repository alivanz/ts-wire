/**
 * Node output: `toNodeHandler` adapts a handler to the classic `(req, res)` shape used
 * by `http.createServer` / Express / Fastify. The `node:http` types are import-type
 * only (erased at runtime), so this module has no runtime Node dependency and is safe
 * to include in the barrel even for edge builds.
 */
import type { IncomingMessage, ServerResponse } from "node:http";
import type { SseDef } from "../contract.js";
import { validateSync } from "../../core/schema.js";
import type { StandardSchemaV1 } from "../../core/schema.js";
import { runStream } from "../server/run.js";
import { NodeSink } from "../server/sink.js";
import { SSE_HEADERS } from "../fetch/index.js";
import type { SseHandler } from "../server/types.js";

// Public handler-facing types (also re-exported from ts-wire/sse/fetch).
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

type QueryResult =
  | { ok: true; value: Record<string, unknown> }
  | { ok: false; issues: ReadonlyArray<StandardSchemaV1.Issue> };

function parseQuery(contract: SseDef, params: URLSearchParams): QueryResult {
  if (!contract.query) return { ok: true, value: {} };
  const res = validateSync(contract.query, Object.fromEntries(params));
  if (!res.ok) return { ok: false, issues: res.issues };
  return { ok: true, value: res.value as Record<string, unknown> };
}

/**
 * Adapt an SSE handler to a Node `(req, res)` request listener. An invalid query is a
 * `400` before the stream opens; otherwise the SSE headers are written and the handler
 * streams to `res`.
 */
export function toNodeHandler<D extends SseDef>(
  contract: D,
  fn: SseHandler<D>,
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const parsed = parseQuery(contract, url.searchParams);
    if (!parsed.ok) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "invalid query", issues: parsed.issues }));
      return;
    }

    res.writeHead(200, SSE_HEADERS);
    const sink = new NodeSink(res);
    const header = req.headers["last-event-id"];
    const lastEventId = Array.isArray(header) ? header[0] : (header ?? undefined);
    void runStream(contract, { query: parsed.value, lastEventId }, fn, sink);
  };
}
