/**
 * Standard Schema v1 plumbing + the `c.type<T>()` escape hatch.
 *
 * The whole library is validator-agnostic: any Standard Schema v1 implementation
 * (Zod 4, Valibot, ArkType, ...) plugs into an `events` map. We only ever touch
 * the two directions the spec exposes:
 *
 *   - INPUT  (pre-transform)  — what the SERVER passes to `emit`, and what goes
 *                               on the wire (the "schema is a DECODER" invariant).
 *   - OUTPUT (post-parse)     — what the CLIENT receives after re-validating a
 *                               frame in the schema's natural direction.
 *
 * These differ the moment a schema coerces/transforms (`z.coerce.number()`,
 * `z.string().transform(...)`), which is exactly why the wire must carry INPUT.
 */
import type { StandardSchemaV1 } from "@standard-schema/spec";

export type { StandardSchemaV1 };

/** The schema's INPUT type — what `emit.<name>(data)` accepts and what is serialized. */
export type InferIn<S extends StandardSchemaV1> = StandardSchemaV1.InferInput<S>;

/** The schema's OUTPUT type — what the client sees after decoding a frame. */
export type InferOut<S extends StandardSchemaV1> = StandardSchemaV1.InferOutput<S>;

/** The event catalog: `{ eventName -> schema }`. This IS the discriminated union. */
export type EventsMap = Record<string, StandardSchemaV1>;

/** Flatten intersections so editor hovers show resolved members, not `InferOut<...> & ...`. */
export type Prettify<T> = { [K in keyof T]: T[K] } & {};

/**
 * A `c.type<T>()` marker: a fully-conformant `StandardSchemaV1<T, T>` whose
 * `validate` is the identity function (NO runtime checking — the escape hatch).
 * Because input === output === T, every downstream mapped type treats it exactly
 * like a real schema with zero special-casing.
 */
export interface TypeMarker<T> extends StandardSchemaV1<T, T> {
  readonly "~standard": StandardSchemaV1.Props<T, T> & { readonly vendor: "ts-sse.type" };
}

/** Build a `TypeMarker<T>` — a phantom schema that validates nothing at runtime. */
export function typeMarker<T>(): TypeMarker<T> {
  return {
    "~standard": {
      version: 1,
      vendor: "ts-sse.type",
      // Identity: the escape hatch performs no validation.
      validate: (value): StandardSchemaV1.Result<T> => ({ value: value as T }),
      // `types` is a compile-time phantom only — intentionally absent at runtime.
    },
  };
}

/**
 * Run a Standard Schema synchronously, returning the parsed OUTPUT or throwing on
 * issues. Used by the serializer as a fail-fast pre-check and by the parser as the
 * client-side decode step. Async validators are rejected (SSE frames are hot-path).
 */
export function validateSync<S extends StandardSchemaV1>(
  schema: S,
  value: unknown,
): { ok: true; value: InferOut<S> } | { ok: false; issues: ReadonlyArray<StandardSchemaV1.Issue> } {
  const result = schema["~standard"].validate(value);
  if (result instanceof Promise) {
    throw new TypeError("ts-sse: async Standard Schema validators are not supported for SSE events");
  }
  if (result.issues) return { ok: false, issues: result.issues };
  return { ok: true, value: result.value as InferOut<S> };
}
