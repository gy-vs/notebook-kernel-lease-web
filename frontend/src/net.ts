/** REST 调用。所有变更请求都带上 fencing token（client_id + epoch）。 */

export interface LockToken {
  client_id: string;
  epoch: number;
}

export interface ApiErrorDetail {
  code: string;
  lock?: unknown;
}

export class ApiError extends Error {
  status: number;
  detail: ApiErrorDetail | null;
  constructor(status: number, detail: ApiErrorDetail | null, message: string) {
    super(message);
    this.status = status;
    this.detail = detail;
  }
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let detail: ApiErrorDetail | null = null;
    try {
      detail = (await res.json()).detail ?? null;
    } catch {
      /* ignore */
    }
    throw new ApiError(res.status, detail, `HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

export const api = {
  state: () => request<any>("GET", "/api/state"),
  queryRequest: (requestId: string) =>
    request<{ execution: any }>("GET", `/api/requests/${requestId}`),
  createCell: (token: LockToken, source = "", afterId: string | null = null) =>
    request<{ cell: any }>("POST", "/api/cells", {
      token, source, after_id: afterId,
    }),
  updateCell: (token: LockToken, cellId: string, source: string) =>
    request("PATCH", `/api/cells/${cellId}`, { token, source }),
  deleteCell: (token: LockToken, cellId: string) =>
    request("POST", `/api/cells/${cellId}/delete`, { token, source: "" }),
  execute: (token: LockToken, cellId: string, requestId: string) =>
    request<{ execution: any; deduped: boolean }>("POST", "/api/execute", {
      token, cell_id: cellId, request_id: requestId,
    }),
  interrupt: (token: LockToken) =>
    request("POST", "/api/interrupt", { token }),
  restart: (token: LockToken) =>
    request<{ gen_id: string }>("POST", "/api/restart", { token }),
};

export function newRequestId(): string {
  const c = globalThis.crypto as Crypto | undefined;
  if (c && typeof c.randomUUID === "function") return c.randomUUID();
  return "req-" + Math.random().toString(36).slice(2) + Date.now().toString(36);
}

export function getClientId(): string {
  // sessionStorage：每个标签页独立、刷新后保留、标签页关闭即清除。
  // 不能用 localStorage —— 同一浏览器的两个标签页会共享同一个身份，
  // 那样控制权的“单写者”语义在浏览器侧就被破坏了。
  const key = "local-lab-client-id";
  let id = sessionStorage.getItem(key);
  if (!id) {
    id = ("c-" + Math.random().toString(36).slice(2) + Date.now().toString(36));
    sessionStorage.setItem(key, id);
  }
  return id;
}
