import { WES_API_BASE } from "./config";
import type {
  ChargeForecastResponse,
  ConfigurePoolRequest,
  PoolConfigResponse,
  WorkUnit,
  ReceiveChargeForecastRequest,
} from "./types";

/** RFC 7807 problem+json body every error response from wes-work-planning
 *  returns (see Problem in apis/openapi.yaml: `type` =
 *  https://errors.wes-work-planning.warehouse-systems.dev/<slug>). Mirrors
 *  order-management / inbound-receiving's own ApiError shape. */
export interface ProblemDetails {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
}

/** Every failed call -- an HTTP error with a problem+json body, an HTTP error
 *  without one, or a network failure -- surfaces as one ApiError, so a screen
 *  can render the problem's own words verbatim (`title`, `detail`) plus
 *  `status`/`slug` to branch on a specific problem. */
export class ApiError extends Error {
  readonly status: number;
  readonly problem: ProblemDetails | null;
  readonly title: string;
  readonly detail: string;
  /** Last path segment of problem.type (e.g. "invalid-wip-limit"); "" when unknown. */
  readonly slug: string;

  constructor(status: number, problem: ProblemDetails | null, fallbackTitle: string) {
    const title = problem?.title || fallbackTitle;
    const detail = problem?.detail ?? "";
    super(detail || title);
    this.name = "ApiError";
    this.status = status;
    this.problem = problem;
    this.title = title;
    this.detail = detail;
    this.slug = problem?.type ? (problem.type.split("/").pop() ?? "") : "";
  }
}

/** Normalizes anything thrown by a request into an ApiError. A raw network
 *  failure carries status 0: no HTTP answer arrived, so the request's outcome
 *  is UNKNOWN -- callers keep their Idempotency-Key in that case (see
 *  ChargeForecastScreen). */
export function toApiError(err: unknown): ApiError {
  if (err instanceof ApiError) return err;
  const message = err instanceof Error ? err.message : String(err);
  return new ApiError(0, { detail: message }, "Network error");
}

/** A fresh RFC 4122 v4 UUID for an Idempotency-Key header: minted once per
 *  logical submission (the order-management convention, ADR-0022 client side).
 *  `crypto.randomUUID` only exists in secure contexts, and the console can be
 *  opened over plain http on a non-localhost host, so fall back to
 *  getRandomValues. */
export function newIdempotencyKey(): string {
  if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

async function parseProblemOrThrow(res: Response): Promise<never> {
  let problem: ProblemDetails | null = null;
  try {
    problem = (await res.json()) as ProblemDetails;
  } catch {
    // non-JSON error body -- fall through with problem = null
  }
  throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`.trim());
}

/**
 * PUT for configuring a path's work pool (PUT /paths/{pathId}/pool). Sends NO
 * Idempotency-Key on purpose: the fleet's idempotency middleware only guards
 * resource-creating POSTs, and a PUT is a snapshot replace -- the body fully
 * describes the desired configuration, so an identical repeat writes nothing
 * (see the endpoint's OpenAPI description).
 */
export async function apiPut<TResponse>(path: string, body: unknown): Promise<TResponse> {
  let res: Response;
  try {
    res = await fetch(`${WES_API_BASE}${path}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    throw toApiError(err);
  }
  if (!res.ok) await parseProblemOrThrow(res);
  return (await res.json()) as TResponse;
}

/** Per-request options for apiPost. */
interface PostOptions {
  /** Sent as the `Idempotency-Key` header. wes-work-planning's ADR-0022
   *  client rule: every resource-creating POST carries a caller-supplied key,
   *  minted per logical submission and reused only to retry that same
   *  request with the same body. */
  idempotencyKey?: string;
}

/**
 * POST for the planner's manual charge-forecast intake (POST
 * /paths/{pathId}/charge). Parses an RFC 7807 problem+json body on failure so
 * the form can surface the exact domain-error detail instead of a generic
 * "request failed".
 */
export async function apiPost<TResponse>(
  path: string,
  body: unknown,
  options: PostOptions = {},
): Promise<TResponse> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (options.idempotencyKey) headers["Idempotency-Key"] = options.idempotencyKey;
  let res: Response;
  try {
    res = await fetch(`${WES_API_BASE}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
  } catch (err) {
    throw toApiError(err);
  }
  if (!res.ok) await parseProblemOrThrow(res);
  return (await res.json()) as TResponse;
}

const seg = encodeURIComponent;

/** PUT /paths/{pathId}/pool -> 200 (400 invalid-wip-limit / unknown-feed-mode /
 *  unknown-path-id, 409 concurrent-modification). */
export function configurePool(
  pathId: string,
  input: ConfigurePoolRequest,
): Promise<PoolConfigResponse> {
  return apiPut<PoolConfigResponse>(`/paths/${seg(pathId)}/pool`, input);
}

/** POST /paths/{pathId}/charge -> 201 (400 invalid-quantity & malformed body,
 *  409 idempotency-key-reused when the key was used for a different request). The caller owns the key's lifecycle: same key retries the same
 *  submission, a fresh key is a genuinely new forecast. */
export function receiveChargeForecast(
  pathId: string,
  input: ReceiveChargeForecastRequest,
  idempotencyKey: string,
): Promise<ChargeForecastResponse> {
  return apiPost<ChargeForecastResponse>(`/paths/${seg(pathId)}/charge`, input, {
    idempotencyKey,
  });
}

/** GET /work-units?reference= follows `nextCursor` until it is absent and
 *  returns every work unit (REST guideline 5.2/5.3: the list is `items` of a
 *  {items,nextCursor} page, never a bare array; one page is not the whole
 *  history). */
export async function fetchWorkUnitsByReference(reference: string): Promise<WorkUnit[]> {
  const all: WorkUnit[] = [];
  let cursor: string | undefined;
  do {
    const qs = new URLSearchParams({ reference, limit: "500" });
    if (cursor) qs.set("cursor", cursor);
    let res: Response;
    try {
      res = await fetch(`${WES_API_BASE}/work-units?${qs.toString()}`);
    } catch (err) {
      throw toApiError(err);
    }
    if (!res.ok) await parseProblemOrThrow(res);
    const page = (await res.json()) as { items: WorkUnit[]; nextCursor?: string };
    all.push(...page.items);
    cursor = page.nextCursor;
  } while (cursor);
  return all;
}
