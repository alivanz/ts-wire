import { describe, it, expect } from "vitest";
import { parseSseText, createSseParser, parseSseStream } from "../src/sse/parse.js";

const encode = (text: string): Uint8Array => new TextEncoder().encode(text);

describe("parseSseText — blocks & fields", () => {
  it("parses a full block with event, id, and data", () => {
    const frames = parseSseText('event: chat\nid: 7\ndata: {"a":1}\n\n');
    expect(frames).toEqual([{ event: "chat", data: '{"a":1}', id: "7" }]);
  });

  it("defaults the event type to \"message\" when no event: line is present", () => {
    expect(parseSseText("data: hi\n\n")).toEqual([{ event: "message", data: "hi" }]);
  });

  it("joins multiple data lines with a newline", () => {
    expect(parseSseText("data: a\ndata: b\n\n")).toEqual([{ event: "message", data: "a\nb" }]);
  });
});

describe("parseSseText — comments & empty blocks", () => {
  it("ignores a comment line and produces no frame", () => {
    expect(parseSseText(": keep-alive\n\n")).toEqual([]);
  });

  it("does not let a comment interleaved with data corrupt the frame", () => {
    const frames = parseSseText("data: a\n: a comment\ndata: b\n\n");
    expect(frames).toEqual([{ event: "message", data: "a\nb" }]);
  });

  it("does not dispatch a block that carried no data field", () => {
    expect(parseSseText("event: ping\n\n")).toEqual([]);
  });
});

describe("parseSseText — id persistence", () => {
  it("carries the last id forward to a later frame that omits its own", () => {
    const frames = parseSseText("id: 5\ndata: a\n\ndata: b\n\n");
    expect(frames).toEqual([
      { event: "message", data: "a", id: "5" },
      { event: "message", data: "b", id: "5" },
    ]);
  });

  it("ignores an id containing a NUL, leaving the persisted id unchanged", () => {
    const frames = parseSseText("id: 9\ndata: a\n\nid: ba\x00d\ndata: b\n\n");
    expect(frames).toEqual([
      { event: "message", data: "a", id: "9" },
      { event: "message", data: "b", id: "9" }, // NUL id was ignored, "9" persists
    ]);
  });

  it("has no id at all until an id: line appears", () => {
    expect(parseSseText("data: a\n\n")).toEqual([{ event: "message", data: "a" }]);
  });
});

describe("parseSseText — retry", () => {
  it("parses an all-digit retry into a number on that block's frame", () => {
    expect(parseSseText("retry: 3000\ndata: x\n\n")).toEqual([
      { event: "message", data: "x", retry: 3000 },
    ]);
  });

  it("ignores a non-digit retry value", () => {
    expect(parseSseText("retry: 12ab\ndata: x\n\n")).toEqual([{ event: "message", data: "x" }]);
  });

  it("does not carry a retry to the next block (retry is per-block)", () => {
    const frames = parseSseText("retry: 500\ndata: a\n\ndata: b\n\n");
    expect(frames).toEqual([
      { event: "message", data: "a", retry: 500 },
      { event: "message", data: "b" },
    ]);
  });
});

describe("parseSseText — line endings", () => {
  const base = "event: chat\ndata: x\ndata: y\n\n";
  const expected = [{ event: "chat", data: "x\ny" }];

  it("parses \\n, \\r\\n, and \\r line separators identically", () => {
    expect(parseSseText(base)).toEqual(expected);
    expect(parseSseText(base.replaceAll("\n", "\r\n"))).toEqual(expected);
    expect(parseSseText(base.replaceAll("\n", "\r"))).toEqual(expected);
  });
});

describe("parseSseText — the one-space rule", () => {
  it("strips exactly one leading space after the colon", () => {
    expect(parseSseText("data:  x\n\n")).toEqual([{ event: "message", data: " x" }]);
  });

  it("keeps the value verbatim when there is no space after the colon", () => {
    expect(parseSseText("data:x\n\n")).toEqual([{ event: "message", data: "x" }]);
  });
});

describe("parseSseText — BOM", () => {
  it("strips a single leading BOM before the first field", () => {
    expect(parseSseText("﻿data: hi\n\n")).toEqual([{ event: "message", data: "hi" }]);
  });
});

describe("createSseParser — chunk boundaries", () => {
  it("reassembles frames split at arbitrary character boundaries", () => {
    const parser = createSseParser();
    const frames = [
      ...parser.feed("data: hel"),
      ...parser.feed("lo\n\ndata: wo"),
      ...parser.feed("rld\n\n"),
      ...parser.end(),
    ];
    expect(frames.map((f) => f.data)).toEqual(["hello", "world"]);
  });

  it("holds a lone trailing CR until the next chunk resolves the CRLF", () => {
    const parser = createSseParser();
    expect(parser.feed("data: hello\r")).toEqual([]); // CR held, could become CRLF
    const frames = [...parser.feed("\n\n"), ...parser.end()];
    expect(frames).toEqual([{ event: "message", data: "hello" }]);
  });

  it("decodes UTF-8 bytes split mid multi-byte character", () => {
    const parser = createSseParser();
    const bytes = encode("data: 🚀\n\n"); // rocket is 4 UTF-8 bytes
    // Split partway through the emoji's byte sequence.
    const cut = "data: ".length + 2;
    const frames = [
      ...parser.feed(bytes.slice(0, cut)),
      ...parser.feed(bytes.slice(cut)),
      ...parser.end(),
    ];
    expect(frames).toEqual([{ event: "message", data: "🚀" }]);
  });

  it("does not dispatch an event that lacks its closing blank line at end()", () => {
    const parser = createSseParser();
    expect(parser.feed("data: x")).toEqual([]);
    expect(parser.end()).toEqual([]); // incomplete event is discarded, per spec
  });
});

describe("parseSseStream", () => {
  it("yields frames from an async iterable of arbitrarily split byte chunks", async () => {
    async function* source(): AsyncGenerator<Uint8Array> {
      yield encode("data: on");
      yield encode("e\n\ndata: two");
      yield encode("\n\n");
    }
    const frames = [];
    for await (const frame of parseSseStream(source())) frames.push(frame);
    expect(frames).toEqual([
      { event: "message", data: "one" },
      { event: "message", data: "two" },
    ]);
  });

  it("yields frames from a WHATWG ReadableStream", async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encode("event: a\ndata: 1\n\n"));
        controller.enqueue(encode("event: b\ndata: 2\n\n"));
        controller.close();
      },
    });
    const frames = [];
    for await (const frame of parseSseStream(stream)) frames.push(frame);
    expect(frames).toEqual([
      { event: "a", data: "1" },
      { event: "b", data: "2" },
    ]);
  });
});
