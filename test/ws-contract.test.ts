import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineWs, wsType } from "../src/ws/contract.js";
import type { ClientInput, ClientOutput, ServerOutput } from "../src/ws/contract.js";
import { encodeMessage, decodeMessage } from "../src/ws/codec.js";
import type { InferIn, InferOut } from "../src/core/schema.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

describe("defineWs — two schemas, both optional", () => {
  it("returns the def unchanged; a server-only contract is legal", () => {
    const c = defineWs({ client: z.object({ a: z.string() }), server: z.object({ b: z.number() }) });
    expect(c.client).toBeDefined();
    expect(c.server).toBeDefined();
    const serverOnly = defineWs({ server: z.object({ b: z.number() }) });
    expect(serverOnly.server).toBeDefined(); // `.client` isn't even on the narrowed type — good
  });
});

describe("message-type projections", () => {
  it("send=INPUT / receive=OUTPUT; a missing direction projects to never", () => {
    const c = defineWs({
      client: z.object({ n: z.coerce.number() }), // OUTPUT: { n: number }
      server: z.object({ s: z.string().transform((x) => x.length) }), // OUTPUT: { s: number }
    });
    type C = typeof c;
    type _ClientOut = Expect<Equal<ClientOutput<C>, { n: number }>>;
    type _ServerOut = Expect<Equal<ServerOutput<C>, { s: number }>>;

    // server-only contract → the client cannot send (ClientInput = never)
    const serverOnly = defineWs({ server: z.object({ b: z.number() }) });
    type _NoSend = Expect<Equal<ClientInput<typeof serverOnly>, never>>;
    expect(true).toBe(true);
  });
});

describe("wsType — no-runtime marker", () => {
  it("validates as identity; input === output === T", () => {
    const m = wsType<{ id: string }>();
    expect(m["~standard"].validate("x")).toEqual({ value: "x" });
    type _In = Expect<Equal<InferIn<typeof m>, { id: string }>>;
    type _Out = Expect<Equal<InferOut<typeof m>, { id: string }>>;
  });
});

describe("codec — a direction with no schema", () => {
  it("just JSON round-trips (no validation)", () => {
    expect(encodeMessage(undefined, { a: 1 })).toBe('{"a":1}');
    expect(decodeMessage(undefined, '{"a":1}')).toEqual({ ok: true, value: { a: 1 } });
    expect(decodeMessage(undefined, "nope").ok).toBe(false); // still guards invalid JSON
  });
});
