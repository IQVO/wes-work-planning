import { POOL_MODES } from "./types";
import type { CPTBucket, PoolMode } from "./types";

/**
 * Pure client-side validation for the planner write screens, kept free of any
 * React/DOM import so it runs in vitest's node environment (same convention as
 * config.test.ts). Each validator mirrors the OpenAPI constraints exactly --
 * the server re-validates and answers problem+json; this only stops the
 * obviously-invalid request before it leaves the browser.
 */

/** PathId pattern from apis/openapi.yaml's components.parameters.PathId: the
 *  deployment's declared process-path families. Write endpoints reject ids
 *  outside them with 400 unknown-path-id. */
const PATH_ID_PATTERN = /^(pick|pack|rebin|slam)(-.*)?$/;

/** int64 on the wire, but JSON numbers beyond 2^53-1 lose integer precision in
 *  JavaScript -- refuse rather than silently corrupt a quantity/limit. */
const MAX_SAFE = Number.MAX_SAFE_INTEGER;

export type Validated<T> = { ok: true; value: T } | { ok: false; errors: string[] };

export function validatePathId(pathId: string): Validated<string> {
  const trimmed = pathId.trim();
  if (!trimmed) return { ok: false, errors: ["Path ID is required."] };
  if (!PATH_ID_PATTERN.test(trimmed)) {
    return {
      ok: false,
      errors: [
        'Path ID must start with pick, pack, rebin or slam (e.g. "pick-a").',
      ],
    };
  }
  return { ok: true, value: trimmed };
}

/** An integer >= 1 (OpenAPI: format int64, minimum 1 -- "a positive
 *  integer", the invalid-wip-limit problem's own words). */
export function parseWipLimit(raw: string): Validated<number> {
  const trimmed = raw.trim();
  if (!trimmed) return { ok: false, errors: ["WIP limit is required."] };
  const value = Number(trimmed);
  // Number() already rejects "12.5" (not an integer) and "abc" (NaN); it also
  // accepts a number-input's scientific notation ("2e2" -> 200), which IS the
  // integer the user meant.
  if (!Number.isInteger(value) || value < 1) {
    return { ok: false, errors: ["WIP limit must be a positive integer (minimum 1)."] };
  }
  if (value > MAX_SAFE) {
    return { ok: false, errors: ["WIP limit is too large."] };
  }
  return { ok: true, value };
}

export function isPoolMode(value: string): value is PoolMode {
  return (POOL_MODES as readonly string[]).includes(value);
}

/** The pool configuration form as a whole (mode + WIP limit). */
export function validatePoolConfig(
  mode: string,
  wipLimitRaw: string,
): Validated<{ mode: PoolMode; wipLimit: number }> {
  const errors: string[] = [];
  if (!isPoolMode(mode)) {
    errors.push('Feed mode must be "ReleaseFed" or "FlowFed".');
  }
  const wipLimit = parseWipLimit(wipLimitRaw);
  if (!wipLimit.ok) errors.push(...wipLimit.errors);
  if (!isPoolMode(mode) || !wipLimit.ok) return { ok: false, errors };
  return { ok: true, value: { mode, wipLimit: wipLimit.value } };
}

/** An integer >= 0 (OpenAPI CPTBucket.quantity: int64, minimum 0; negative is
 *  the invalid-quantity problem). */
export function parseQuantity(raw: string): Validated<number> {
  const trimmed = raw.trim();
  if (!trimmed) return { ok: false, errors: ["Quantity is required."] };
  const value = Number(trimmed);
  if (!Number.isInteger(value) || value < 0) {
    return { ok: false, errors: ["Quantity must be a non-negative integer."] };
  }
  if (value > MAX_SAFE) {
    return { ok: false, errors: ["Quantity is too large."] };
  }
  return { ok: true, value };
}

/**
 * Normalizes a <input type="datetime-local"> value ("2026-08-21T12:00" or
 * "2026-08-21T12:00:00" -- wall time, no zone) into the exact RFC 3339 UTC
 * timestamp the CPT field demands. CPTs are warehouse-clock (UTC) wall times,
 * and the form labels the input as UTC, so the wall time IS the UTC time.
 * Returns null for anything unparseable or impossible (e.g. February 30th:
 * Date rolls it over to March, so the round-trip check fails).
 */
export function toRfc3339Cpt(raw: string): string | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(raw.trim());
  if (!m) return null;
  const [, y, mo, d, h, mi, s] = m;
  const candidate = `${y}-${mo}-${d}T${h}:${mi}:${s ?? "00"}Z`;
  const parsed = new Date(candidate);
  if (Number.isNaN(parsed.getTime())) return null;
  // Reject rollovers: "2026-02-30T12:00:00Z" parses as March 2nd, so the UTC
  // calendar components no longer match what was typed. (toISOString() cannot
  // be compared directly -- it always appends milliseconds.)
  if (
    parsed.getUTCFullYear() !== Number(y) ||
    parsed.getUTCMonth() !== Number(mo) - 1 ||
    parsed.getUTCDate() !== Number(d) ||
    parsed.getUTCHours() !== Number(h) ||
    parsed.getUTCMinutes() !== Number(mi) ||
    parsed.getUTCSeconds() !== Number(s ?? "00")
  ) {
    return null;
  }
  return candidate;
}

/** A charge-forecast form's bucket rows as a whole: at least one bucket
 *  (OpenAPI minItems 1), each with a valid CPT and quantity. */
export function validateChargeBuckets(
  rows: { cpt: string; quantity: string }[],
): Validated<CPTBucket[]> {
  const errors: string[] = [];
  if (rows.length === 0) {
    return { ok: false, errors: ["At least one CPT bucket is required."] };
  }
  const buckets: CPTBucket[] = [];
  rows.forEach((row, i) => {
    const cpt = toRfc3339Cpt(row.cpt);
    if (cpt === null) {
      errors.push(`Bucket ${i + 1}: CPT must be a valid date and time (UTC).`);
    }
    const quantity = parseQuantity(row.quantity);
    if (!quantity.ok) {
      errors.push(`Bucket ${i + 1}: ${quantity.errors[0]}`);
    }
    if (cpt !== null && quantity.ok) {
      buckets.push({ cpt, quantity: quantity.value });
    }
  });
  if (errors.length > 0) return { ok: false, errors };
  return { ok: true, value: buckets };
}
