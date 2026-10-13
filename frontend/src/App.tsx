import { useEffect, useRef, useState } from "react";
import EventTimeline from "./components/EventTimeline";
import ProductCards from "./components/ProductCards";
import type { TradeEvent } from "./types";
import {
  approveAndExecuteApproval,
  executeApprovedApproval,
  fetchApprovalStatus,
  fetchSessionApprovals,
  hasReceipt,
  type ApprovalRecord,
  type ApprovalStatus,
} from "./approvalWorkflow";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:8000";
const WS_BASE = API_BASE.replace(/^http/, "ws");
const ACCESS_TOKEN_KEY = "globex.access_token";

function loadOrCreate(key: string, prefix: string): string {
  const existing = localStorage.getItem(key);
  if (existing) return existing;
  const created = `${prefix}-${Math.random().toString(36).slice(2, 14)}`;
  localStorage.setItem(key, created);
  return created;
}

interface Identity {
  tenant_id: string;
  subject: string;
  permissions: string[];
}

interface Turn {
  role: "buyer" | "agent";
  text: string;
  failed?: boolean;
}

type ApprovalState = ApprovalStatus | "approving" | "checking";

const APPROVAL_STATE_LABELS: Record<ApprovalState, string> = {
  pending: "等待确认",
  approving: "正在确认并执行…",
  approved: "已批准，尚未执行",
  executing: "正在执行",
  uncertain: "执行结果待核实",
  completed: "已完成",
  expired: "已过期",
  unknown: "状态未知",
  checking: "正在查询状态…",
};

interface ApprovalRequest {
  approvalId: string;
  tool: string;
  arguments: unknown;
  expiresAt?: number | string;
  sessionId: string;
  state: ApprovalState;
  result?: unknown;
  error?: string;
}

interface ActiveIntent {
  sessionId: string;
}

function errorMessage(value: unknown): string {
  if (value instanceof Error) return value.message;
  return String(value);
}

function responseDetail(body: unknown, status: number): string {
  if (body && typeof body === "object" && "detail" in body) {
    return String((body as { detail: unknown }).detail);
  }
  return `HTTP ${status}`;
}

function validFinalText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0 && !value.trimStart().startsWith("[error]");
}

function approvalFromRecord(record: ApprovalRecord): ApprovalRequest {
  return {
    approvalId: record.approval_id,
    tool: record.tool,
    arguments: record.arguments,
    expiresAt: record.expires_at,
    sessionId: record.shopping_session_id,
    state: record.state,
    result: record.result,
  };
}

function approvalFromEvent(payload: Record<string, any>, active: ActiveIntent | null): ApprovalRequest | null {
  const record = payload.approval ?? payload.proposal ?? payload;
  const approvalId = record.approval_id ?? record.id;
  if (typeof approvalId !== "string" || !approvalId) return null;
  const sessionId = record.shopping_session_id ?? payload.shopping_session_id ?? active?.sessionId;
  if (typeof sessionId !== "string" || !sessionId) return null;
  const state = ["pending", "approved", "executing", "uncertain", "completed", "expired", "unknown"]
    .includes(record.state) ? record.state as ApprovalStatus : "pending";
  const verifiedState = state === "completed" && !hasReceipt(record.result) ? "unknown" : state;

  return {
    approvalId,
    tool: String(record.tool ?? record.tool_name ?? "未知操作"),
    arguments: record.arguments ?? record.args ?? {},
    expiresAt: record.expires_at,
    sessionId,
    state: verifiedState,
    result: record.result,
  };
}

function mergeApprovals(previous: ApprovalRequest[], incoming: ApprovalRequest[]): ApprovalRequest[] {
  const merged = new Map(previous.map((approval) => [approval.approvalId, approval]));
  for (const approval of incoming) {
    const existing = merged.get(approval.approvalId);
    if (existing?.state === "completed" && approval.state !== "completed") continue;
    merged.set(approval.approvalId, { ...existing, ...approval, error: existing?.error });
  }
  return [...merged.values()];
}

function formatExpiry(value: number | string | undefined): string {
  if (value === undefined || value === "") return "未提供";
  const date = typeof value === "number"
    ? new Date(value < 100_000_000_000 ? value * 1000 : value)
    : new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

export default function App() {
  const [rawSessionId] = useState(() => loadOrCreate("globex.session", "web"));
  const [sessionId, setSessionId] = useState(rawSessionId);
  const [accessToken, setAccessToken] = useState(() => sessionStorage.getItem(ACCESS_TOKEN_KEY) ?? "");
  const [tokenInput, setTokenInput] = useState(() => sessionStorage.getItem(ACCESS_TOKEN_KEY) ?? "");
  const [identity, setIdentity] = useState<Identity | null>(null);
  const [verifiedToken, setVerifiedToken] = useState("");
  const [authAttempt, setAuthAttempt] = useState(0);
  const [authBusy, setAuthBusy] = useState(false);
  const [authError, setAuthError] = useState("");
  const [events, setEvents] = useState<TradeEvent[]>([]);
  const [turns, setTurns] = useState<Turn[]>([]);
  const [approvals, setApprovals] = useState<ApprovalRequest[]>([]);
  const [approvalLoadError, setApprovalLoadError] = useState("");
  const [streaming, setStreaming] = useState("");
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const activeIntentRef = useRef<ActiveIntent | null>(null);

  useEffect(() => {
    let cancelled = false;
    setIdentity(null);
    setVerifiedToken("");
    setAuthError("");
    if (!accessToken) {
      setAuthBusy(false);
      return () => {
        cancelled = true;
      };
    }

    setAuthBusy(true);
    fetch(`${API_BASE}/commerce/me`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    })
      .then(async (response) => {
        const body: unknown = await response.json().catch(() => null);
        if (!response.ok) throw new Error(responseDetail(body, response.status));
        if (!body || typeof body !== "object" || !("subject" in body) || !("tenant_id" in body)) {
          throw new Error("身份响应格式无效");
        }
        if (!cancelled) {
          setIdentity(body as Identity);
          setVerifiedToken(accessToken);
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) setAuthError(`身份验证失败：${errorMessage(error)}`);
      })
      .finally(() => {
        if (!cancelled) setAuthBusy(false);
      });

    return () => {
      cancelled = true;
    };
  }, [accessToken, authAttempt]);

  // A session scope belongs to one authenticated principal. Switching tokens starts
  // from the raw client session again; the server returns its scoped form on intent.
  useEffect(() => {
    setSessionId(rawSessionId);
    setEvents([]);
    setTurns([]);
    setApprovals([]);
    setApprovalLoadError("");
    setStreaming("");
    setInput("");
    activeIntentRef.current = null;
  }, [accessToken, rawSessionId]);

  const authenticated = Boolean(accessToken && identity && verifiedToken === accessToken);
  const canSubmit = authenticated && identity?.permissions.includes("intent:submit") === true;
  const canApprove = authenticated && identity?.permissions.includes("commerce:write") === true;

  useEffect(() => {
    let cancelled = false;
    if (!authenticated || !accessToken) {
      setApprovalLoadError("");
      return () => {
        cancelled = true;
      };
    }

    fetchSessionApprovals(API_BASE, accessToken, rawSessionId)
      .then((records) => {
        if (cancelled) return;
        setApprovals((previous) => mergeApprovals(previous, records.map(approvalFromRecord)));
        setApprovalLoadError("");
      })
      .catch((error: unknown) => {
        if (!cancelled) setApprovalLoadError(`审批列表暂不可用：${errorMessage(error)}`);
      });

    return () => {
      cancelled = true;
    };
  }, [accessToken, authenticated, rawSessionId, verifiedToken]);

  useEffect(() => {
    let closed = false;
    let retryTimer: number | undefined;
    const ready = accessToken && identity && verifiedToken === accessToken;

    if (!ready) {
      setConnected(false);
      wsRef.current?.close();
      wsRef.current = null;
      return () => {
        closed = true;
      };
    }

    const connect = () => {
      if (closed) return;
      const ws = new WebSocket(`${WS_BASE}/commerce/events`);
      wsRef.current = ws;
      ws.onopen = () => {
        if (closed) {
          ws.close();
          return;
        }
        // The access token is sent only as the first WebSocket data frame, never in the URL.
        ws.send(JSON.stringify({ access_token: accessToken, shopping_session_id: sessionId }));
        setConnected(true);
      };
      ws.onclose = (event) => {
        setConnected(false);
        if (!closed && event.code !== 4401 && event.code !== 4403) {
          retryTimer = window.setTimeout(connect, 1500);
        }
      };
      ws.onmessage = (message) => {
        let event: TradeEvent;
        try {
          event = JSON.parse(message.data) as TradeEvent;
        } catch {
          return;
        }

        if (event.type === "token.delta") {
          setStreaming((previous) => previous + (event.payload?.token ?? ""));
          return;
        }
        if (event.type === "final.result") {
          const text = event.payload?.text;
          setStreaming("");
          if (!validFinalText(text)) {
            event = { ...event, type: "error", payload: { message: "Agent 返回失败结果" } };
          } else {
            // The REST response is the completion authority. Add a successful final event
            // only after that response has also passed validation.
            return;
          }
        }
        if (event.type === "error") setStreaming("");

        setEvents((previous) => [...previous, event]);
        if (event.type === "approval.required") {
          const approval = approvalFromEvent(event.payload ?? {}, activeIntentRef.current);
          if (!approval) {
            setTurns((previous) => [...previous, { role: "agent", text: "[error] 审批事件缺少有效的 approval_id。", failed: true }]);
            return;
          }
          setApprovals((previous) => previous.some((item) => item.approvalId === approval.approvalId)
            ? previous
            : [...previous, approval]);
        }
      };
    };

    connect();
    return () => {
      closed = true;
      if (retryTimer) window.clearTimeout(retryTimer);
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [accessToken, identity, sessionId, verifiedToken]);

  const useToken = () => {
    const nextToken = tokenInput.trim();
    if (!nextToken) {
      setAuthError("请填写访问令牌。");
      return;
    }
    sessionStorage.setItem(ACCESS_TOKEN_KEY, nextToken);
    setAuthError("");
    setAccessToken(nextToken);
    setAuthAttempt((previous) => previous + 1);
  };

  const clearToken = () => {
    sessionStorage.removeItem(ACCESS_TOKEN_KEY);
    setTokenInput("");
    setAccessToken("");
    setIdentity(null);
    setVerifiedToken("");
  };

  const requestIntent = async (query: string, intentSessionId: string): Promise<boolean> => {
    if (!canSubmit || !accessToken) {
      setTurns((previous) => [...previous, { role: "agent", text: "[error] 当前身份没有提交购物意图的权限。", failed: true }]);
      return false;
    }
    activeIntentRef.current = { sessionId: intentSessionId };
    setStreaming("");
    setTurns((previous) => [...previous, { role: "buyer", text: query }]);
    try {
      const response = await fetch(`${API_BASE}/commerce/intents`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify({
          shopping_session_id: intentSessionId,
          locale: "zh-CN",
          currency: "CNY",
          raw_query: query,
        }),
      });
      const body: any = await response.json().catch(() => null);
      if (!response.ok) throw new Error(responseDetail(body, response.status));
      if (!body || typeof body !== "object" || typeof body.shopping_session_id !== "string" || !body.shopping_session_id) {
        throw new Error("意图响应缺少有效的 shopping_session_id");
      }
      if (!validFinalText(body.final_text)) throw new Error("Agent 未返回有效的最终结果");

      // The server owns tenant scoping. Reuse its value for future requests and WS subscriptions.
      setSessionId(body.shopping_session_id);
      // Recover durable proposals when a WS event was missed during reconnect.
      try {
        const records = await fetchSessionApprovals(API_BASE, accessToken, body.shopping_session_id);
        setApprovals((previous) => mergeApprovals(previous, records.map(approvalFromRecord)));
      } catch {
        // The completed intent remains valid; proposals can be fetched again on the next turn.
      }
      setTurns((previous) => [...previous, { role: "agent", text: body.final_text }]);
      setEvents((previous) => [...previous, {
        type: "final.result",
        payload: { text: body.final_text },
        occurred_at: new Date().toISOString(),
      }]);
      return true;
    } catch (error) {
      setStreaming("");
      setTurns((previous) => [...previous, {
        role: "agent",
        text: `[error] 请求失败：${errorMessage(error)}`,
        failed: true,
      }]);
      return false;
    }
  };

  const submit = async () => {
    const query = input.trim();
    if (!query || busy || !canSubmit) return;
    setInput("");
    setBusy(true);
    try {
      await requestIntent(query, sessionId);
    } finally {
      setBusy(false);
    }
  };

  const applyApprovalResolution = (approvalId: string, resolution: {
    record: ApprovalRecord | null;
    completed: boolean;
    error?: string;
  }) => {
    setApprovals((previous) => previous.map((item) => {
      if (item.approvalId !== approvalId) return item;
      if (!resolution.record) {
        return { ...item, state: "unknown", error: resolution.error ?? "审批状态尚未确认。" };
      }
      const record = approvalFromRecord(resolution.record);
      return {
        ...item,
        ...record,
        state: resolution.completed ? "completed" : record.state,
        error: resolution.completed ? undefined : resolution.error,
      };
    }));
  };

  const approve = async (approval: ApprovalRequest) => {
    if (busy || approval.state !== "pending" || !accessToken || !canApprove) return;
    setBusy(true);
    setApprovals((previous) => previous.map((item) => item.approvalId === approval.approvalId
      ? { ...item, state: "approving", error: undefined }
      : item));
    try {
      const resolution = await approveAndExecuteApproval(
        API_BASE,
        accessToken,
        approval.approvalId,
        fetch,
        approval.sessionId,
      );
      applyApprovalResolution(approval.approvalId, resolution);
    } catch (error) {
      applyApprovalResolution(approval.approvalId, {
        record: null,
        completed: false,
        error: errorMessage(error),
      });
    } finally {
      setBusy(false);
    }
  };

  const refreshApproval = async (approval: ApprovalRequest) => {
    if (busy || !accessToken) return;
    setBusy(true);
    setApprovals((previous) => previous.map((item) => item.approvalId === approval.approvalId
      ? { ...item, state: "checking", error: undefined }
      : item));
    try {
      const resolution = await fetchApprovalStatus(
        API_BASE,
        accessToken,
        approval.approvalId,
        approval.sessionId,
      );
      applyApprovalResolution(approval.approvalId, resolution);
    } catch (error) {
      applyApprovalResolution(approval.approvalId, {
        record: null,
        completed: false,
        error: errorMessage(error),
      });
    } finally {
      setBusy(false);
    }
  };

  const continueApproved = async (approval: ApprovalRequest) => {
    if (busy || approval.state !== "approved" || !accessToken || !canApprove) return;
    setBusy(true);
    try {
      const resolution = await executeApprovedApproval(API_BASE, accessToken, approval.approvalId,
        fetch, approval.sessionId);
      applyApprovalResolution(approval.approvalId, resolution);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="layout">
      <header>
        <h1>Globex 跨境购物助手</h1>
        <div className="meta">
          <span>会话 {sessionId}</span>
          {identity && <span>{identity.tenant_id} / {identity.subject}</span>}
          <span className={connected ? "dot on" : "dot off"}>{connected ? "事件流已连接" : "事件流断开"}</span>
        </div>
      </header>

      <section className="auth-panel" aria-label="身份验证">
        <label htmlFor="access-token">访问令牌</label>
        <input
          id="access-token"
          type="password"
          autoComplete="off"
          value={tokenInput}
          placeholder="显式填写访问令牌"
          onChange={(event) => setTokenInput(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              useToken();
            }
          }}
          disabled={busy}
        />
        <button type="button" onClick={useToken} disabled={busy || authBusy || !tokenInput.trim()}>
          {authBusy ? "验证中…" : "使用令牌"}
        </button>
        {accessToken && <button type="button" className="secondary" onClick={clearToken} disabled={busy}>清除令牌</button>}
        {identity && <span className="identity-status">已验证：{identity.subject}（{identity.tenant_id}）</span>}
        {authError && <span className="auth-error" role="alert">{authError}</span>}
      </section>

      <main>
        <section className="chat">
          <div className="turns">
            {turns.map((turn, index) => (
              <div key={index} className={`turn ${turn.role}${turn.failed ? " failed" : ""}`}>
                <div className="who">{turn.role === "buyer" ? "我" : "Globex"}</div>
                <div className="text">{turn.text}</div>
              </div>
            ))}
            {streaming && (
              <div className="turn agent streaming">
                <div className="who">Globex</div>
                <div className="text">{streaming}</div>
              </div>
            )}
            {busy && !streaming && <div className="hint">Agent 正在处理……</div>}
            {!authenticated && <div className="hint">填写访问令牌并完成身份验证后即可开始。</div>}
            {authenticated && !canSubmit && <div className="auth-error">当前身份没有 intent:submit 权限。</div>}
            {approvalLoadError && <div className="auth-error" role="alert">{approvalLoadError}</div>}
          </div>

          {approvals.length > 0 && (
            <section className="approvals" aria-label="待处理审批">
              <h2>操作审批记录</h2>
              {approvals.map((approval) => (
                <article className={`approval-card approval-${approval.state}`} key={approval.approvalId}>
                  <div className="approval-heading">
                    <strong>{approval.tool}</strong>
                    <span>{APPROVAL_STATE_LABELS[approval.state]}</span>
                  </div>
                  <div className="approval-id">审批编号：{approval.approvalId}</div>
                  <div className="approval-expiry">有效期至：{formatExpiry(approval.expiresAt)}</div>
                  <div className="approval-label">将执行的准确参数</div>
                  <pre>{JSON.stringify(approval.arguments, null, 2)}</pre>
                  {approval.state === "completed" && hasReceipt(approval.result) && (
                    <>
                      <div className="approval-label">后端保存的执行回执</div>
                      <pre>{JSON.stringify(approval.result, null, 2)}</pre>
                    </>
                  )}
                  {approval.error && <p className="auth-error" role="alert">{approval.error}</p>}
                  {approval.state === "pending" && (
                    <button type="button" onClick={() => void approve(approval)} disabled={busy || !canApprove}>
                      批准并执行准确操作
                    </button>
                  )}
                  {approval.state === "approved" && (
                    <button type="button" onClick={() => void continueApproved(approval)} disabled={busy || !canApprove}>
                      继续执行已批准操作
                    </button>
                  )}
                  {["approved", "executing", "uncertain", "unknown"].includes(approval.state) && (
                    <button type="button" className="secondary" onClick={() => void refreshApproval(approval)} disabled={busy}>
                      重新检查状态
                    </button>
                  )}
                </article>
              ))}
            </section>
          )}

          <ProductCards events={events} />

          <div className="composer">
            <textarea
              value={input}
              placeholder="例如：我人在美国，250 美元预算买个降噪耳机寄美国，到手价多少？"
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  void submit();
                }
              }}
              disabled={!canSubmit || busy}
            />
            <button onClick={() => void submit()} disabled={!canSubmit || busy || !input.trim()}>
              {busy ? "处理中" : "发送"}
            </button>
          </div>
        </section>

        <EventTimeline events={events} />
      </main>
    </div>
  );
}
