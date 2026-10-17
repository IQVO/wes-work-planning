import { describe, expect, it } from "vitest";
import {
  isPoolMode,
  parseQuantity,
  parseWipLimit,
  toRfc3339Cpt,
  validateChargeBuckets,
  validatePathId,
  validatePoolConfig,
} from "./validation";

describe("validatePathId (spec pattern ^(pick|pack|rebin|slam)(-.*)?$)", () => {
  it.each(["pick", "pick-a", "pack-station-1", "rebin-2", "slam"])("accepts %s", (pathId) => {
    const result = validatePathId(pathId);
    expect(result.ok).toBe(true);
  });

  it.each(["", "   ", "warehouse", "Pick-a", "pick?", "pick a"])("rejects %s", (pathId) => {
    const result = validatePathId(pathId);
    expect(result.ok).toBe(false);
  });

  it("trims before validating", () => {
    const result = validatePathId("  pick-a  ");
    expect(result).toEqual({ ok: true, value: "pick-a" });
  });
});

describe("parseWipLimit (int64, minimum 1)", () => {
  it("accepts a positive integer", () => {
    expect(parseWipLimit("200")).toEqual({ ok: true, value: 200 });
    expect(parseWipLimit(" 1 ")).toEqual({ ok: true, value: 1 });
  });

  it.each(["", "0", "-5", "12.5", "abc", `${Number.MAX_SAFE_INTEGER + 1}`])(
    "rejects %s",
    (raw) => {
      expect(parseWipLimit(raw).ok).toBe(false);
    },
  );

  it("accepts a number-input's scientific notation for the integer it denotes", () => {
    expect(parseWipLimit("2e2")).toEqual({ ok: true, value: 200 });
  });
});

describe("validatePoolConfig", () => {
  it("passes a legal mode + limit through", () => {
    expect(validatePoolConfig("FlowFed", "40")).toEqual({
      ok: true,
      value: { mode: "FlowFed", wipLimit: 40 },
    });
  });

  it("collects both problems at once", () => {
    const result = validatePoolConfig("WaveFed", "0");
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errors).toHaveLength(2);
      expect(result.errors[0]).toContain("ReleaseFed");
      expect(result.errors[1]).toContain("positive integer");
    }
  });

  it("isPoolMode admits exactly the two spec values", () => {
    expect(isPoolMode("ReleaseFed")).toBe(true);
    expect(isPoolMode("FlowFed")).toBe(true);
    expect(isPoolMode("releasefed")).toBe(false);
    expect(isPoolMode("")).toBe(false);
  });
});

describe("parseQuantity (int64, minimum 0)", () => {
  it("accepts zero and positive integers", () => {
    expect(parseQuantity("0")).toEqual({ ok: true, value: 0 });
    expect(parseQuantity("4200")).toEqual({ ok: true, value: 4200 });
  });

  it.each(["", "-1", "1.5", "abc", `${Number.MAX_SAFE_INTEGER + 1}`])(
    "rejects %s",
    (raw) => {
      expect(parseQuantity(raw).ok).toBe(false);
    },
  );
});

describe("toRfc3339Cpt (datetime-local -> RFC 3339 UTC)", () => {
  it("normalizes minute precision to a Z-suffixed UTC timestamp", () => {
    expect(toRfc3339Cpt("2026-08-21T12:00")).toBe("2026-08-21T12:00:00Z");
  });

  it("keeps second precision", () => {
    expect(toRfc3339Cpt("2026-08-21T12:00:30")).toBe("2026-08-21T12:00:30Z");
  });

  it.each(["", "not-a-date", "2026-08-21", "12:00", "2026-02-30T12:00"])(
    "rejects %s",
    (raw) => {
      expect(toRfc3339Cpt(raw)).toBeNull();
    },
  );

  it("rejects February 30th (Date rollover) rather than silently shifting the CPT", () => {
    expect(toRfc3339Cpt("2026-02-30T12:00")).toBeNull();
  });
});

describe("validateChargeBuckets (minItems 1, valid CPT + quantity per bucket)", () => {
  it("normalizes valid rows into wire buckets", () => {
    const result = validateChargeBuckets([
      { cpt: "2026-08-21T12:00", quantity: "4200" },
      { cpt: "2026-08-21T18:00:00", quantity: "0" },
    ]);
    expect(result).toEqual({
      ok: true,
      value: [
        { cpt: "2026-08-21T12:00:00Z", quantity: 4200 },
        { cpt: "2026-08-21T18:00:00Z", quantity: 0 },
      ],
    });
  });

  it("rejects an empty bucket set (OpenAPI minItems: 1)", () => {
    const result = validateChargeBuckets([]);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.errors[0]).toContain("At least one CPT bucket");
  });

  it("reports every offending bucket by its 1-based position", () => {
    const result = validateChargeBuckets([
      { cpt: "", quantity: "10" },
      { cpt: "2026-08-21T12:00", quantity: "-1" },
    ]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errors).toHaveLength(2);
      expect(result.errors[0]).toContain("Bucket 1");
      expect(result.errors[1]).toContain("Bucket 2");
    }
  });
});
