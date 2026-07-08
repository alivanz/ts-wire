/**
 * The frame-transport layer: two {@link FrameSink} implementations (WHATWG
 * ReadableStream + Node ServerResponse) over one backpressure contract, a
 * {@link CoordinatedWriter} that serializes all writes into whole, ordered frames,
 * and an idle-gated heartbeat timer.
 *
 * Backpressure invariant: `write()` resolves on FLUSH — when the consumer can accept
 * more — not on enqueue. `await emit.x(...)` in a loop is therefore natural
 * backpressure, and the async chain never outruns a slow client.
 */
import type { ServerResponse } from "node:http";
import type { FrameSink } from "./types.js";
import { serializeComment } from "../serialize.js";

/** Thrown from `write()`/`close()` once the client has disconnected. */
export class SinkAbortedError extends Error {
  constructor() {
    super("ts-sse: sink aborted (client disconnected)");
    this.name = "SinkAbortedError";
  }
}

const KEEPALIVE = serializeComment("keep-alive");

/**
 * A FrameSink backed by a WHATWG `ReadableStream<Uint8Array>` (for `Response`).
 * Uses `highWaterMark: 1`: a write parks after enqueue until the consumer's next
 * `pull`, so a slow reader backpressures the producer.
 */
export class ResponseSink implements FrameSink {
  readonly stream: ReadableStream<Uint8Array>;
  private controller!: ReadableStreamDefaultController<Uint8Array>;
  private drainWaiters: Array<() => void> = [];
  private ac = new AbortController();
  private closed = false;

  constructor(external?: AbortSignal) {
    this.stream = new ReadableStream<Uint8Array>(
      {
        start: (c) => {
          this.controller = c;
        },
        pull: () => {
          this.drainWaiters.shift()?.();
        },
        cancel: () => {
          this.doAbort();
        },
      },
      new CountQueuingStrategy({ highWaterMark: 1 }),
    );
    if (external) {
      if (external.aborted) this.doAbort();
      else external.addEventListener("abort", () => this.doAbort(), { once: true });
    }
  }

  get aborted(): boolean {
    return this.ac.signal.aborted;
  }
  get signal(): AbortSignal {
    return this.ac.signal;
  }

  async write(frame: Uint8Array): Promise<void> {
    if (this.aborted) throw new SinkAbortedError();
    if (this.closed) throw new Error("ts-sse: write after close");
    this.controller.enqueue(frame);
    // Consumer still has room → resolve now; else wait for the next pull.
    if ((this.controller.desiredSize ?? 1) > 0) return;
    await new Promise<void>((resolve) => this.drainWaiters.push(resolve));
    if (this.aborted) throw new SinkAbortedError();
  }

  async close(): Promise<void> {
    if (this.closed || this.aborted) return;
    this.closed = true;
    try {
      this.controller.close();
    } catch {
      /* already closed by the runtime */
    }
  }

  private doAbort(): void {
    if (this.ac.signal.aborted) return;
    this.ac.abort();
    for (const w of this.drainWaiters.splice(0)) w();
  }
}

/** A FrameSink backed by a Node `ServerResponse` (for Express / Fastify / raw http). */
export class NodeSink implements FrameSink {
  private ac = new AbortController();
  private closed = false;
  private drainWaiters: Array<() => void> = [];

  constructor(
    private res: ServerResponse,
    external?: AbortSignal,
  ) {
    res.on("close", () => this.doAbort());
    res.on("drain", () => {
      for (const w of this.drainWaiters.splice(0)) w();
    });
    if (external) {
      if (external.aborted) this.doAbort();
      else external.addEventListener("abort", () => this.doAbort(), { once: true });
    }
  }

  get aborted(): boolean {
    return this.ac.signal.aborted;
  }
  get signal(): AbortSignal {
    return this.ac.signal;
  }

  async write(frame: Uint8Array): Promise<void> {
    if (this.aborted) throw new SinkAbortedError();
    if (this.closed) throw new Error("ts-sse: write after close");
    // res.write returns false when the internal buffer is full → wait for 'drain'.
    if (this.res.write(frame)) return;
    await new Promise<void>((resolve) => this.drainWaiters.push(resolve));
    if (this.aborted) throw new SinkAbortedError();
  }

  async close(): Promise<void> {
    if (this.closed || this.aborted) return;
    this.closed = true;
    this.res.end();
  }

  private doAbort(): void {
    if (this.ac.signal.aborted) return;
    this.ac.abort();
    for (const w of this.drainWaiters.splice(0)) w();
  }
}

/**
 * Serializes every write through one tail-promise chain so frames are whole and
 * ordered — real events, heartbeats, `retry:` and the EOS sentinel never interleave.
 */
export class CoordinatedWriter {
  private tail: Promise<unknown> = Promise.resolve();
  constructor(private sink: FrameSink) {}

  write(frame: Uint8Array): Promise<void> {
    const run = this.tail.then(() => this.sink.write(frame));
    this.tail = run.catch(() => {}); // keep the chain alive past a rejected write
    return run;
  }

  close(): Promise<void> {
    const run = this.tail.then(() => this.sink.close());
    this.tail = run.catch(() => {});
    return run;
  }
}

export interface HeartbeatController {
  /** Push the next beat out by the full interval (called after every real write). */
  reset(): void;
  stop(): void;
}

/**
 * An idle-gated heartbeat: writes a `:keep-alive` comment every `ms` of inactivity.
 * Every real write calls `reset()`, so a beat only fires after a genuine idle gap —
 * it never piles onto a busy stream. Comments carry no `id:`, so resume is unaffected.
 */
export function startHeartbeat(
  writer: CoordinatedWriter,
  ms: number,
  isAlive: () => boolean,
): HeartbeatController {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const schedule = (): void => {
    timer = setTimeout(() => {
      if (!isAlive()) return;
      void writer.write(KEEPALIVE).then(
        () => {
          if (isAlive()) schedule();
        },
        () => {},
      );
    }, ms);
    // Don't keep the process alive just for heartbeats (Node).
    (timer as { unref?: () => void }).unref?.();
  };
  schedule();
  return {
    reset(): void {
      if (timer) clearTimeout(timer);
      schedule();
    },
    stop(): void {
      if (timer) clearTimeout(timer);
      timer = undefined;
    },
  };
}
