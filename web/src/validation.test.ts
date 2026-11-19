import { describe, expect, it } from "vitest";
import {
  isPoolMode,
  parseWipLimit,
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
