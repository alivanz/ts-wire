/**
 * The shared string-level codec every WS platform builds on. A frame is literally
 * `JSON.stringify(message)` — no envelope — and the [decoder invariant] holds in both
 * directions: the wire carries the schema's INPUT; the receiver re-validates
 * INPUT→OUTPUT with the same schema.
 *
 * Platform packages (ws/cf, ws/node) normalize their raw message to a string and then
 * call {@link decodeMessage}; they call {@link encodeMessage} to send.
 */
import { validateSync } from "../core/schema.js";
import type { StandardSchemaV1 } from "../core/schema.js";

export type DecodeResult<T> =
  | { ok: true; value: T }
  | { ok: false; issues: ReadonlyArray<StandardSchemaV1.Issue> };

/**
 * Validate an outgoing message (fail-fast) and serialize its INPUT. Throws on invalid
 * input — that is a programmer error (the value is typed). A missing schema (a contract
 * with no schema for this direction) skips validation.
 */
export function encodeMessage(schema: StandardSchemaV1 | undefined, msg: unknown): string {
  if (schema) {
    const res = validateSync(schema, msg);
    if (!res.ok) {
      throw new Error(
        `ts-wire/ws: invalid outgoing message — ${res.issues.map((i) => i.message).join("; ")}`,
      );
    }
  }
  return JSON.stringify(msg); // the wire carries the INPUT, never the transformed output
}

/**
 * Parse + validate an incoming message against its schema (INPUT→OUTPUT). Returns a
 * result rather than throwing — a bad frame is a data-plane condition, not fatal.
 */
export function decodeMessage<T>(
  schema: StandardSchemaV1 | undefined,
  text: string,
): DecodeResult<T> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    return { ok: false, issues: [{ message: e instanceof Error ? e.message : "invalid JSON" }] };
  }
  if (!schema) return { ok: true, value: parsed as T };
  const res = validateSync(schema, parsed);
  return res.ok ? { ok: true, value: res.value as T } : { ok: false, issues: res.issues };
}
