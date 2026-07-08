import type { StandardSchemaV1 } from "../core/schema.js";

/** A data-plane failure: an incoming frame failed its schema, or wasn't valid JSON. */
export interface WsValidationError {
  readonly issues: ReadonlyArray<StandardSchemaV1.Issue>;
  /** The raw text that failed to decode. */
  readonly raw: string;
}
