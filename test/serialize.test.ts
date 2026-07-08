import { describe, it, expect } from "vitest";
import { z } from "zod";
import {
  serializeFrame,
  serializeFrameToString,
  serializeComment,
  serializeRetry,
  toOutgoingFrame,
} from "../src/sse/serialize.js";

const decode = (bytes: Uint8Array): string => new TextDecoder().decode(bytes);

describe("toOutgoingFrame — the decoder invariant", () => {
  it("puts the schema INPUT on the wire, never the transformed OUTPUT", () => {
    // InferIn = string, InferOut = number. If the serializer ever shipped
    // `result.value` (the OUTPUT), `.data` would be "5" (the length). It must
    // instead carry the JSON of the INPUT so the client can re-decode it.
    const schema = z.string().transform((s) => s.length);

    const frame = toOutgoingFrame(schema, "e", "hello");

    expect(frame.data).toBe(JSON.stringify("hello")); // => "\"hello\""
    expect(frame.data).not.toBe("5"); // the transformed output would be wrong
    expect(frame.event).toBe("e");
  });

  it("keeps the raw input JSON for a coercing schema (z.coerce.number)", () => {
    // OUTPUT would be the number 5; INPUT is the string "5". We must keep "5".
    const coerce = z.coerce.number();

    // Zod types a coerce schema's InferIn as `number`, but at runtime (and per the
    // invariant we're testing) it accepts the string "5" and coerces it — so the
    // wire must carry the raw "5", not the coerced output.
    // @ts-expect-error — deliberately exercising the coerce input at runtime.
    const frame = toOutgoingFrame(coerce, "n", "5");

    expect(frame.data).toBe(JSON.stringify("5")); // => "\"5\"", not "5"
  });

  it("throws when the input fails validation, naming the event", () => {
    const schema = z.string();

    // @ts-expect-error — deliberately passing the wrong input type at runtime.
    expect(() => toOutgoingFrame(schema, "bad", 123)).toThrow(/bad/);
  });

  it("threads id and retry through opts", () => {
    const frame = toOutgoingFrame(z.string(), "e", "hi", { id: "42", retry: 1000 });
    expect(frame).toEqual({ event: "e", data: '"hi"', id: "42", retry: 1000 });
  });
});

describe("serializeFrameToString — wire grammar", () => {
  it("orders lines event -> id -> data and closes with a blank line", () => {
    const wire = serializeFrameToString({ event: "chat", data: '{"a":1}', id: "7" });
    expect(wire).toBe('event: chat\nid: 7\ndata: {"a":1}\n\n');
  });

  it("omits the event line when event is undefined", () => {
    const wire = serializeFrameToString({ data: "x" });
    expect(wire).toBe("data: x\n\n");
    expect(wire).not.toContain("event:");
  });

  it("emits one data line per newline-delimited segment", () => {
    expect(serializeFrameToString({ data: "a\nb" })).toBe("data: a\ndata: b\n\n");
  });

  it("still emits a single empty data line for an empty string", () => {
    expect(serializeFrameToString({ data: "" })).toBe("data: \n\n");
  });

  it("emits the retry line in order after id", () => {
    const wire = serializeFrameToString({ event: "e", id: "1", retry: 3000, data: "d" });
    expect(wire).toBe("event: e\nid: 1\nretry: 3000\ndata: d\n\n");
  });
});

describe("serializeFrameToString — validation guards", () => {
  it("throws RangeError when id contains a NUL byte", () => {
    expect(() => serializeFrameToString({ data: "x", id: "a\0b" })).toThrow(RangeError);
  });

  it("throws RangeError when id contains a newline", () => {
    expect(() => serializeFrameToString({ data: "x", id: "a\nb" })).toThrow(RangeError);
  });

  it("throws RangeError when id contains a carriage return", () => {
    expect(() => serializeFrameToString({ data: "x", id: "a\rb" })).toThrow(RangeError);
  });

  it("throws RangeError when retry is not a non-negative integer", () => {
    expect(() => serializeFrameToString({ data: "x", retry: 1.5 })).toThrow(RangeError);
    expect(() => serializeFrameToString({ data: "x", retry: -1 })).toThrow(RangeError);
  });
});

describe("serializeRetry", () => {
  it("encodes a retry directive terminated by a blank line", () => {
    expect(decode(serializeRetry(3000))).toBe("retry: 3000\n\n");
  });

  it("throws RangeError on a non-integer", () => {
    expect(() => serializeRetry(3.14)).toThrow(RangeError);
    expect(() => serializeRetry(-5)).toThrow(RangeError);
  });
});

describe("serializeComment", () => {
  it("encodes a comment as a colon-prefixed line plus a blank line", () => {
    expect(decode(serializeComment("keep-alive"))).toBe(": keep-alive\n\n");
  });

  it("emits one comment line per newline-delimited segment", () => {
    expect(decode(serializeComment("a\nb"))).toBe(": a\n: b\n\n");
  });
});

describe("serializeFrame — byte encoding", () => {
  it("round-trips: decoded bytes equal the string form", () => {
    const frame = { event: "greet", id: "9", retry: 500, data: "héllo\nwörld" };
    expect(decode(serializeFrame(frame))).toBe(serializeFrameToString(frame));
  });

  it("produces valid UTF-8 for multi-byte characters", () => {
    // "🚀" is 4 UTF-8 bytes; a naive char-count encoder would corrupt it.
    expect(decode(serializeFrame({ data: "🚀" }))).toBe("data: 🚀\n\n");
  });
});
