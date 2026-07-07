import { describe, it, expect } from "vitest";
import { z } from "zod";
import { c, initContract } from "../src/core/contract.js";
import type { MergeEvents } from "../src/core/contract.js";
import type { InferIn, InferOut } from "../src/core/schema.js";

// ── tiny type-level assertion kit ────────────────────────────────────────────
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

describe("c.sse — reserved event names are rejected at compile time", () => {
  it("accepts legal event names", () => {
    const def = c.sse({
      method: "GET",
      path: "/rooms/:id/stream",
      events: { chat: z.object({ text: z.string() }), presence: z.object({ online: z.number() }) },
    });
    // returns the def unchanged (structural pass-through)
    expect(def.path).toBe("/rooms/:id/stream");
    expect(def.events.chat).toBeDefined();
  });

  // Each of these MUST fail to typecheck: a reserved key maps to an error *string*
  // in CheckEvents, so passing a schema there is not assignable. Keep each call on
  // ONE physical line so @ts-expect-error covers the reported error.
  it("rejects transport-reserved and control-reserved names", () => {
    // @ts-expect-error 'error' collides with EventSource's native error event
    c.sse({ method: "GET", path: "/x", events: { error: z.object({ code: z.string() }) } });
    // @ts-expect-error 'message' collides with EventSource's default event
    c.sse({ method: "GET", path: "/x", events: { message: z.object({ t: z.string() }) } });
    // @ts-expect-error 'open' collides with EventSource's open event
    c.sse({ method: "GET", path: "/x", events: { open: z.object({}) } });
    // @ts-expect-error 'comment' collides with the emit-control channel
    c.sse({ method: "GET", path: "/x", events: { comment: z.object({}) } });
    // @ts-expect-error 'retry' collides with the emit-control channel
    c.sse({ method: "GET", path: "/x", events: { retry: z.object({}) } });
    // @ts-expect-error 'close' collides with the emit-control channel
    c.sse({ method: "GET", path: "/x", events: { close: z.object({}) } });
    // @ts-expect-error 'ts-sse-*' prefix is reserved for internal control frames
    c.sse({ method: "GET", path: "/x", events: { "ts-sse-eos": z.object({}) } });
    expect(true).toBe(true); // runtime no-op; the assertions above are type-level
  });
});

describe("c.type — no-runtime phantom marker", () => {
  it("validates as identity and infers input === output === T", () => {
    const marker = c.type<{ id: number }>();
    const result = marker["~standard"].validate("anything");
    expect(result).toEqual({ value: "anything" }); // identity, no checking

    type _In = Expect<Equal<InferIn<typeof marker>, { id: number }>>;
    type _Out = Expect<Equal<InferOut<typeof marker>, { id: number }>>;
  });
});

describe("initContract", () => {
  it("exposes sse/type/router", () => {
    const k = initContract();
    expect(typeof k.sse).toBe("function");
    expect(typeof k.type).toBe("function");
    expect(typeof k.router).toBe("function");
  });
});

describe("MergeEvents — commonEvents merge is Own-wins", () => {
  it("route events override common events of the same name", () => {
    type Common = { fatal: z.ZodObject<{ m: z.ZodString }>; chat: z.ZodString };
    type Own = { chat: z.ZodNumber };
    type Merged = MergeEvents<Common, Own>;
    // `chat` resolves to Own's schema; `fatal` is preserved from Common
    type _Chat = Expect<Equal<Merged["chat"], z.ZodNumber>>;
    type _Keys = Expect<Equal<keyof Merged, "fatal" | "chat">>;
    expect(true).toBe(true);
  });
});
