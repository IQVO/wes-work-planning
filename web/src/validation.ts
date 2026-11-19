import { POOL_MODES } from "./types";
import type { PoolMode } from "./types";

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
