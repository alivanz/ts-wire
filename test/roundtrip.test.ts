import { describe, it, expect } from "vitest";
import { z } from "zod";
import { serializeFrame, toOutgoingFrame } from "../src/core/serialize.js";
import { parseSseText } from "../src/core/parse.js";
import { validateSync } from "../src/core/schema.js";
import { TS_SSE_EOS } from "../src/core/wire.js";
import type { OutgoingFrame, RawFrame } from "../src/core/wire.js";

/** serialize a frame to bytes, then parse the bytes back to frames. */
const roundtrip = (f: OutgoingFrame): RawFrame[] =>
  parseSseText(new TextDecoder().decode(serializeFrame(f)));

/** The RawFrame we expect a single OutgoingFrame to decode back into. */
const expected = (f: OutgoingFrame): RawFrame => ({
  event: f.event ?? "message",
  data: f.data,
  ...(f.id !== undefined ? { id: f.id } : {}),
  ...(f.retry !== undefined ? { retry: f.retry } : {}),
});

describe("wire round-trip: serialize -> parse", () => {
  const cases: OutgoingFrame[] = [
    { event: "chat", data: '{"a":1}', id: "7" },
    { data: "hi" }, // no event -> "message"
    { data: "a\nb" }, // multi-line data
    { event: "presence", data: "{}", retry: 3000 },
    { event: TS_SSE_EOS, data: "{}" }, // reserved terminal event is a normal wire event
    { data: "" }, // empty-string data still dispatches (a data: line was present)
    { data: '"café ☕ 🚀"' }, // UTF-8 multi-byte
    { event: "chat", data: '{"a":1}', id: "42", retry: 1000 }, // all fields
  ];

  for (const f of cases) {
    it(`round-trips ${JSON.stringify(f)}`, () => {
      const frames = roundtrip(f);
      expect(frames).toHaveLength(1);
      expect(frames[0]).toEqual(expected(f));
    });
  }

  it("parses several frames from one blob", () => {
    const a = serializeFrame({ event: "chat", data: '"one"', id: "1" });
    const b = serializeFrame({ event: "chat", data: '"two"', id: "2" });
    const bytes = new Uint8Array([...a, ...b]);
    const frames = parseSseText(new TextDecoder().decode(bytes));
    expect(frames.map((x) => x.data)).toEqual(['"one"', '"two"']);
    expect(frames.map((x) => x.id)).toEqual(["1", "2"]);
  });

  it("persists id across a frame that omits it", () => {
    const a = serializeFrame({ event: "chat", data: '"one"', id: "5" });
    const b = serializeFrame({ event: "chat", data: '"two"' }); // no id
    const frames = parseSseText(new TextDecoder().decode(new Uint8Array([...a, ...b])));
    expect(frames[1]?.id).toBe("5"); // lastEventId persists per the SSE spec
  });
});

describe("the decoder invariant, end-to-end", () => {
  it("carries the schema INPUT on the wire, and the client decodes to OUTPUT", () => {
    // A genuinely non-round-tripping transform: InferIn = string, InferOut = number.
    // If the serializer put the OUTPUT (5) on the wire, the client could never
    // re-decode "5" back through z.string() -> this is the whole point.
    const schema = z.string().transform((s) => s.length);

    const frame = toOutgoingFrame(schema, "measure", "hello");
    const [decoded] = roundtrip(frame);

    // 1. the wire carries the INPUT ("hello"), not the output (5)
    expect(decoded?.data).toBe(JSON.stringify("hello"));
    expect(decoded?.data).not.toBe("5");

    // 2. the client re-runs the SAME schema in its natural direction -> OUTPUT
    const client = validateSync(schema, JSON.parse(decoded!.data));
    expect(client).toEqual({ ok: true, value: 5 });
  });
});
