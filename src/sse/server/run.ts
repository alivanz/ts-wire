/**
 * The runtime core: build the handler ctx (typed `emit` + runtime `init`), run the
 * handler against a {@link FrameSink}, and enforce the lifecycle —
 *
 *   - normal return  → send the `ts-sse-eos` sentinel, close (terminal; browser stops)
 *   - uncaught throw  → close WITHOUT the sentinel (browser reconnects)
 *   - `emit.close()`  → explicit early EOS
 *
 * Output adapters (`sseResponse`, `toNodeHandler`, ...) all funnel through here; only
 * the sink differs.
 */
import type { EventsMap } from "../../core/schema.js";
import type { SseDef } from "../contract.js";
import { TS_SSE_EOS } from "../wire.js";
import { serializeComment, serializeFrame, serializeRetry, toOutgoingFrame } from "../serialize.js";
import type { Emit, EmitOpts, FrameSink, InitOptions, RunBase, SseContext } from "./types.js";
import { CoordinatedWriter, startHeartbeat, type HeartbeatController } from "./sink.js";

/** The serialized EOS sentinel frame (a real `event: ts-sse-eos`, not a comment). */
const EOS_FRAME = serializeFrame({ event: TS_SSE_EOS, data: "{}" });

/** Build the typed `emit` object for a contract over a coordinated writer. */
function createEmit(
  events: EventsMap,
  writer: CoordinatedWriter,
  heartbeat: { current?: HeartbeatController },
  close: () => Promise<void>,
): Emit<EventsMap> {
  const emit: Record<string, unknown> = {};
  for (const name of Object.keys(events)) {
    const schema = events[name]!;
    emit[name] = (data: unknown, opts?: EmitOpts): Promise<void> => {
      // Decoder invariant: validate the INPUT (fail-fast) and serialize the INPUT JSON.
      const frame = toOutgoingFrame(
        schema,
        name,
        data,
        opts?.id !== undefined ? { id: opts.id } : undefined,
      );
      heartbeat.current?.reset();
      return writer.write(serializeFrame(frame));
    };
  }
  emit.retry = (ms: number): Promise<void> => {
    heartbeat.current?.reset();
    return writer.write(serializeRetry(ms));
  };
  emit.comment = (text: string): Promise<void> => {
    heartbeat.current?.reset();
    return writer.write(serializeComment(text));
  };
  emit.close = close;
  return emit as Emit<EventsMap>;
}

/**
 * Run a handler against a sink. `base` carries the already-validated query + the
 * inbound lastEventId. Never rejects — lifecycle failures close the stream.
 */
export async function runStream<D extends SseDef>(
  contract: D,
  base: RunBase,
  fn: (ctx: SseContext<D>) => void | Promise<void>,
  sink: FrameSink,
): Promise<void> {
  const writer = new CoordinatedWriter(sink);
  const heartbeat: { current?: HeartbeatController } = {};
  let terminated = false;

  const sendEos = async (): Promise<void> => {
    if (terminated || sink.aborted) return;
    terminated = true;
    heartbeat.current?.stop();
    try {
      await writer.write(EOS_FRAME);
      await writer.close();
    } catch {
      /* client vanished mid-close */
    }
  };

  const emit = createEmit(contract.events, writer, heartbeat, sendEos);

  const init = (options: InitOptions): void => {
    if (options.retry !== undefined) void emit.retry(options.retry);
    if (options.heartbeat !== undefined && !heartbeat.current) {
      heartbeat.current = startHeartbeat(
        writer,
        options.heartbeat,
        () => !sink.aborted && !terminated,
      );
    }
  };

  const ctx = {
    query: base.query,
    lastEventId: base.lastEventId,
    signal: sink.signal,
    emit,
    init,
  } as unknown as SseContext<D>;

  try {
    await fn(ctx);
    await sendEos(); // normal completion → terminal EOS (no-op if handler closed early)
  } catch {
    // Uncaught throw → close WITHOUT the sentinel so the browser reconnects.
    heartbeat.current?.stop();
    if (!sink.aborted && !terminated) {
      try {
        await writer.close();
      } catch {
        /* noop */
      }
    }
  } finally {
    heartbeat.current?.stop();
  }
}
