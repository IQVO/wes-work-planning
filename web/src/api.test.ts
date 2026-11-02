import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  configurePool,
  fetchWorkUnitsByReference,
  newIdempotencyKey,
  receiveChargeForecast,
  toApiError,
} from "./api";
import { WES_API_BASE } from "./config";

// In vitest's node environment config.ts resolves the dev base
// (window is undefined, import.meta.env.PROD is false), so URL assertions are
// deterministic.
type Call = { url: string; init: RequestInit };

function lastCall(): Call {
  const calls = (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mock.calls;
  const [url, init] = calls[calls.length - 1];
  return { url: url as string, init: init as RequestInit };
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": body && typeof body === "object" && "type" in body ? "application/problem+json" : "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("configurePool (PUT /paths/{pathId}/pool)", () => {
  it("PUTs the snapshot body and sends NO Idempotency-Key", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, { pathId: "pick-a", mode: "FlowFed", wipLimit: 40, wip: 0, backlogDepth: 0 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const pool = await configurePool("pick-a", { mode: "FlowFed", wipLimit: 40 });

    expect(pool).toEqual({ pathId: "pick-a", mode: "FlowFed", wipLimit: 40, wip: 0, backlogDepth: 0 });
    const call = lastCall();
    expect(call.url).toBe(`${WES_API_BASE}/paths/pick-a/pool`);
    expect(call.init.method).toBe("PUT");
    expect(call.init.headers).toEqual({ "Content-Type": "application/json" });
    expect(JSON.parse(call.init.body as string)).toEqual({ mode: "FlowFed", wipLimit: 40 });
  });

  it("encodes the path id", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, { pathId: "pick-a", mode: "ReleaseFed", wipLimit: 200, wip: 4, backlogDepth: 12 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await configurePool("pick a/b", { mode: "ReleaseFed", wipLimit: 200 });
    expect(lastCall().url).toBe(`${WES_API_BASE}/paths/pick%20a%2Fb/pool`);
  });

  it("surfaces a 400 problem+json body verbatim as an ApiError", async () => {
    const problem = {
      type: "https://errors.wes-work-planning.warehouse-systems.dev/invalid-wip-limit",
      title: "Invalid WIP limit",
      status: 400,
      detail: "work pool WIP limit must be a positive integer",
      instance: "/paths/pick-a/pool",
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(400, problem)));

    const err = await configurePool("pick-a", { mode: "ReleaseFed", wipLimit: 0 }).catch((e) => e);

    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(400);
    expect(err.title).toBe("Invalid WIP limit");
    expect(err.detail).toBe("work pool WIP limit must be a positive integer");
    expect(err.slug).toBe("invalid-wip-limit");
    expect(err.problem?.instance).toBe("/paths/pick-a/pool");
  });
});

describe("receiveChargeForecast (POST /paths/{pathId}/charge)", () => {
  it("POSTs the buckets with the caller's Idempotency-Key", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(201, {
        pathId: "pick-a",
        totalQuantity: 6000,
        buckets: [
          { cpt: "2026-08-21T12:00:00Z", quantity: 4200 },
          { cpt: "2026-08-21T18:00:00Z", quantity: 1800 },
        ],
        receivedAt: "2026-08-21T08:00:00Z",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const forecast = await receiveChargeForecast(
      "pick-a",
      { buckets: [{ cpt: "2026-08-21T12:00:00Z", quantity: 4200 }, { cpt: "2026-08-21T18:00:00Z", quantity: 1800 }] },
      "6f1c9a52-8f6e-4a0b-9d44-2c1b0f7a3e55",
    );

    expect(forecast.totalQuantity).toBe(6000);
    const call = lastCall();
    expect(call.url).toBe(`${WES_API_BASE}/paths/pick-a/charge`);
    expect(call.init.method).toBe("POST");
    expect(call.init.headers).toEqual({
      "Content-Type": "application/json",
      "Idempotency-Key": "6f1c9a52-8f6e-4a0b-9d44-2c1b0f7a3e55",
    });
    expect(JSON.parse(call.init.body as string)).toEqual({
      buckets: [
        { cpt: "2026-08-21T12:00:00Z", quantity: 4200 },
        { cpt: "2026-08-21T18:00:00Z", quantity: 1800 },
      ],
    });
  });

  it("surfaces a 400 invalid-quantity problem verbatim", async () => {
    const problem = {
      type: "https://errors.wes-work-planning.warehouse-systems.dev/invalid-quantity",
      title: "Invalid quantity",
      status: 400,
      detail: "quantity must be non-negative",
      instance: "/paths/pick-a/charge",
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(400, problem)));

    const err = await receiveChargeForecast(
      "pick-a",
      { buckets: [{ cpt: "2026-08-21T12:00:00Z", quantity: -1 }] },
      "key",
    ).catch((e) => e);

    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(400);
    expect(err.message).toBe("quantity must be non-negative");
  });

  it("falls back to a plain ApiError when the error body is not JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("gateway exploded", { status: 502, statusText: "Bad Gateway" }),
      ),
    );

    const err = await receiveChargeForecast("pick-a", { buckets: [] }, "key").catch((e) => e);

    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(502);
    expect(err.problem).toBeNull();
    expect(err.title).toBe("502 Bad Gateway");
  });
});

describe("network failures and key minting", () => {
  it("normalizes a thrown fetch into a status-0 ApiError (outcome unknown)", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("fetch failed")));

    const err = toApiError(await receiveChargeForecast("pick-a", { buckets: [] }, "key").catch((e) => e));

    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(0);
    expect(err.title).toBe("Network error");
    expect(toApiError(err)).toBe(err); // already an ApiError passes through
  });

  it("mints distinct RFC 4122 v4 UUIDs per call", () => {
    const a = newIdempotencyKey();
    const b = newIdempotencyKey();
    expect(a).not.toBe(b);
    for (const key of [a, b]) {
      expect(key).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    }
  });

  it("falls back to getRandomValues when randomUUID is unavailable", () => {
    const realCrypto = globalThis.crypto;
    vi.stubGlobal("crypto", { getRandomValues: realCrypto.getRandomValues.bind(realCrypto) });
    const key = newIdempotencyKey();
    expect(key).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});

describe("fetchWorkUnitsByReference", () => {
  it("follows nextCursor until absent and returns every item", async () => {
    const wu = (id: string) => ({ id });
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(200, { items: [wu("a"), wu("b")], nextCursor: "c1" }))
      .mockResolvedValueOnce(jsonResponse(200, { items: [wu("c")] }));
    vi.stubGlobal("fetch", fetchMock);

    const rows = await fetchWorkUnitsByReference("order-1");

    expect(rows.map((r) => r.id)).toEqual(["a", "b", "c"]);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toBe(`${WES_API_BASE}/work-units?reference=order-1&limit=500`);
    expect(fetchMock.mock.calls[1][0]).toContain("cursor=c1");
  });
});
