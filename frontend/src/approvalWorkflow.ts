export type ApprovalStatus =
  | "pending"
  | "approved"
  | "executing"
  | "uncertain"
  | "completed"
  | "expired"
  | "unknown";

export interface ApprovalRecord {
  approval_id: string;
  shopping_session_id: string;
  state: ApprovalStatus;
  tool: string;
  arguments: unknown;
  expires_at?: number | string;
  result?: unknown;
}

export interface ApprovalResolution {
  record: ApprovalRecord | null;
  completed: boolean;
  error?: string;
}

const SERVER_STATES = new Set<ApprovalStatus>([
  "pending", "approved", "executing", "uncertain", "completed", "expired",
]);

function apiUrl(apiBase: string, path: string): string {
  return `${apiBase.replace(/\/$/, "")}${path}`;
}

function headers(accessToken: string): HeadersInit {
  return { Authorization: `Bearer ${accessToken}` };
}

async function responseBody(response: Response): Promise<unknown> {
  return response.json().catch(() => null) as Promise<unknown>;
}

function detailFrom(body: unknown, status: number): string {
  if (body && typeof body === "object" && "detail" in body) {
    return String((body as { detail: unknown }).detail);
  }
  return `HTTP ${status}`;
}

function normalizeRecord(value: unknown, fallbackSessionId = ""): ApprovalRecord | null {
  if (!value || typeof value !== "object") return null;
  const raw = value as Record<string, unknown>;
  if (typeof raw.approval_id !== "string" || !raw.approval_id) return null;
  const sessionId = typeof raw.shopping_session_id === "string"
    ? raw.shopping_session_id
    : fallbackSessionId;
  if (!sessionId) return null;

  const state = typeof raw.state === "string" && SERVER_STATES.has(raw.state as ApprovalStatus)
    ? raw.state as ApprovalStatus
    : "unknown";
  const record: ApprovalRecord = {
    approval_id: raw.approval_id,
    shopping_session_id: sessionId,
    state,
    tool: typeof raw.tool === "string" ? raw.tool : "未知操作",
    arguments: raw.arguments ?? {},
  };
  if (typeof raw.expires_at === "number" || typeof raw.expires_at === "string") {
    record.expires_at = raw.expires_at;
  }
  if (Object.prototype.hasOwnProperty.call(raw, "result")) record.result = raw.result;
  return record;
}

export function hasReceipt(value: unknown): boolean {
  if (typeof value === "string") return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  return value !== null && typeof value === "object" && Object.keys(value).length > 0;
}

function resolutionFromRecord(record: ApprovalRecord, error?: string): ApprovalResolution {
  if (record.state === "completed" && hasReceipt(record.result)) {
    return { record, completed: true };
  }
  // A state without a durable receipt is not sufficient to tell the buyer that
  // the write completed. Keep it visible as unresolved until a later status read.
  const unresolvedRecord = record.state === "completed" ? { ...record, state: "unknown" as const } : record;
  return {
    record: unresolvedRecord,
    completed: false,
    error: error ?? `审批当前状态为 ${record.state}，尚未取得已保存的执行回执。`,
  };
}

async function readApproval(
  apiBase: string,
  accessToken: string,
  approvalId: string,
  fetcher: typeof fetch,
  fallbackSessionId: string,
): Promise<ApprovalRecord> {
  const response = await fetcher(
    apiUrl(apiBase, `/commerce/approvals/${encodeURIComponent(approvalId)}`),
    { headers: headers(accessToken) },
  );
  const body = await responseBody(response);
  if (!response.ok) throw new Error(detailFrom(body, response.status));
  const record = normalizeRecord(body, fallbackSessionId);
  if (!record) throw new Error("审批状态响应格式无效");
  return record;
}

async function latestResolution(
  apiBase: string,
  accessToken: string,
  approvalId: string,
  fetcher: typeof fetch,
  fallbackSessionId: string,
  cause: string,
): Promise<ApprovalResolution> {
  try {
    return resolutionFromRecord(await readApproval(
      apiBase, accessToken, approvalId, fetcher, fallbackSessionId,
    ), cause);
  } catch (error) {
    const statusError = error instanceof Error ? error.message : String(error);
    return { record: null, completed: false, error: `${cause}；状态查询失败：${statusError}` };
  }
}

export async function fetchApprovalStatus(
  apiBase: string,
  accessToken: string,
  approvalId: string,
  fallbackSessionId = "",
  fetcher: typeof fetch = fetch,
): Promise<ApprovalResolution> {
  try {
    return resolutionFromRecord(await readApproval(
      apiBase, accessToken, approvalId, fetcher, fallbackSessionId,
    ));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { record: null, completed: false, error: message };
  }
}

export async function fetchSessionApprovals(
  apiBase: string,
  accessToken: string,
  sessionId: string,
  fetcher: typeof fetch = fetch,
): Promise<ApprovalRecord[]> {
  const query = new URLSearchParams({ shopping_session_id: sessionId });
  const response = await fetcher(
    apiUrl(apiBase, `/commerce/approvals?${query.toString()}`),
    { headers: headers(accessToken) },
  );
  const body = await responseBody(response);
  if (!response.ok) throw new Error(detailFrom(body, response.status));
  if (!body || typeof body !== "object" || !Array.isArray((body as { approvals?: unknown }).approvals)) {
    throw new Error("审批列表响应格式无效");
  }
  return ((body as { approvals: unknown[] }).approvals)
    .map((value) => normalizeRecord(value))
    .filter((record): record is ApprovalRecord => record !== null);
}

export async function approveAndExecuteApproval(
  apiBase: string,
  accessToken: string,
  approvalId: string,
  fetcher: typeof fetch = fetch,
  fallbackSessionId = "",
): Promise<ApprovalResolution> {
  const approveUrl = apiUrl(apiBase, `/commerce/approvals/${encodeURIComponent(approvalId)}/approve`);
  let approvalResponse: Response;
  try {
    approvalResponse = await fetcher(approveUrl, {
      method: "POST",
      headers: headers(accessToken),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return latestResolution(apiBase, accessToken, approvalId, fetcher, fallbackSessionId, message);
  }

  const approvalBody = await responseBody(approvalResponse);
  if (!approvalResponse.ok) {
    return latestResolution(
      apiBase,
      accessToken,
      approvalId,
      fetcher,
      fallbackSessionId,
      detailFrom(approvalBody, approvalResponse.status),
    );
  }
  const approved = normalizeRecord(approvalBody, fallbackSessionId);
  if (!approved || approved.state !== "approved") {
    return latestResolution(
      apiBase,
      accessToken,
      approvalId,
      fetcher,
      fallbackSessionId,
      "审批接口未确认 approved 状态",
    );
  }

  return executeAndConfirm(apiBase, accessToken, approvalId, fetcher, approved.shopping_session_id);
}

export async function executeApprovedApproval(
  apiBase: string,
  accessToken: string,
  approvalId: string,
  fetcher: typeof fetch = fetch,
  fallbackSessionId = "",
): Promise<ApprovalResolution> {
  try {
    const record = await readApproval(apiBase, accessToken, approvalId, fetcher, fallbackSessionId);
    if (record.state !== "approved") return resolutionFromRecord(record);
    return executeAndConfirm(apiBase, accessToken, approvalId, fetcher, record.shopping_session_id);
  } catch (error) {
    return { record: null, completed: false, error: error instanceof Error ? error.message : String(error) };
  }
}

async function executeAndConfirm(
  apiBase: string,
  accessToken: string,
  approvalId: string,
  fetcher: typeof fetch,
  sessionId: string,
): Promise<ApprovalResolution> {

  const executeUrl = apiUrl(apiBase, `/commerce/approvals/${encodeURIComponent(approvalId)}/execute`);
  let executeResponse: Response;
  try {
    executeResponse = await fetcher(executeUrl, {
      method: "POST",
      headers: headers(accessToken),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return latestResolution(apiBase, accessToken, approvalId, fetcher, sessionId, message);
  }

  const executeBody = await responseBody(executeResponse);
  if (!executeResponse.ok) {
    return latestResolution(
      apiBase,
      accessToken,
      approvalId,
      fetcher,
      sessionId,
      detailFrom(executeBody, executeResponse.status),
    );
  }

  const execution = executeBody && typeof executeBody === "object"
    ? executeBody as Record<string, unknown>
    : {};
  if (execution.state !== "completed" || !hasReceipt(execution.result)) {
    return latestResolution(
      apiBase,
      accessToken,
      approvalId,
      fetcher,
      sessionId,
      `执行接口返回状态 ${String(execution.state ?? "unknown")}，正在核对持久状态`,
    );
  }

  // The execute response is useful only when the durable record also confirms
  // completion and carries a receipt. This closes the gap after lost responses.
  return latestResolution(
    apiBase,
    accessToken,
    approvalId,
    fetcher,
    sessionId,
    "执行回执已收到，但尚未从审批记录确认完成",
  );
}
