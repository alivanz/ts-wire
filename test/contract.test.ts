import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineSse, sseType } from "../src/sse/contract.js";
import type { InferIn, InferOut } from "../src/core/schema.js";

// ── tiny type-level assertion kit ────────────────────────────────────────────
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

describe("defineSse — one contract = one SSE endpoint", () => {
  it("accepts legal event names + an optional query schema, returns def unchanged", () => {
    const def = defineSse({
      query: z.object({ id: z.string() }),
      events: { chat: z.object({ text: z.string() }), presence: z.object({ online: z.number() }) },
    });
    // structural pass-through: the def is returned unchanged
    expect(def.events.chat).toBeDefined();
    expect(def.query).toBeDefined();
  });

  // Each of these MUST fail to typecheck: a reserved key maps to an error *string*
  // in CheckEvents, so passing a schema there is not assignable. Keep each call on
  // ONE physical line so @ts-expect-error covers the reported error.
  it("rejects transport-reserved and control-reserved names", () => {
    // @ts-expect-error 'error' collides with EventSource's native error event
    defineSse({ events: { error: z.object({ code: z.string() }) } });
    // @ts-expect-error 'message' collides with EventSource's default event
    defineSse({ events: { message: z.object({ t: z.string() }) } });
    // @ts-expect-error 'open' collides with EventSource's open event
    defineSse({ events: { open: z.object({}) } });
    // @ts-expect-error 'comment' collides with the emit-control channel
    defineSse({ events: { comment: z.object({}) } });
    // @ts-expect-error 'retry' collides with the emit-control channel
    defineSse({ events: { retry: z.object({}) } });
    // @ts-expect-error 'close' collides with the emit-control channel
    defineSse({ events: { close: z.object({}) } });
    // @ts-expect-error 'ts-sse-*' prefix is reserved for internal control frames
    defineSse({ events: { "ts-sse-eos": z.object({}) } });
    expect(true).toBe(true); // runtime no-op; the assertions above are type-level
  });
});

describe("sseType — no-runtime phantom marker", () => {
  it("validates as identity and infers input === output === T", () => {
    const m = sseType<{ id: number }>();
    const result = m["~standard"].validate("anything");
    expect(result).toEqual({ value: "anything" }); // identity, no checking

    type _In = Expect<Equal<InferIn<typeof m>, { id: number }>>;
    type _Out = Expect<Equal<InferOut<typeof m>, { id: number }>>;
  });
});
