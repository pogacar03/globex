# -*- coding: utf-8 -*-
"""FastAPI 服务入口

路由：
    POST /commerce/intents                 提交买家意图（同步返回最终回复；启用队列时内部入队后等结果）
    POST /commerce/intents/async           提交买家意图（立即返回 task_id，结果走 WS 或轮询）
    GET  /commerce/tasks/{task_id}         查任务状态（queued / running / done / failed）
    WS   /commerce/events                  订阅会话事件流
    GET  /commerce/orders/{order_id}       查询订单（直连 UseCase，不过 Agent）
    POST /commerce/orders/{order_id}/cancel  取消订单（直连 UseCase）
    GET  /health                           健康检查（含依赖连通性与队列深度）

启动：
    uv run uvicorn app.presentation.server:app --port 8000
    uv run python -m app.worker          # 启用队列时另起消费进程

同步接口为什么保留：13 case 评测脚本与前端都依赖它直接返回 final_text，
改成纯异步会一次性搞挂回归与前端。削峰由 worker 并发度保证，与接口形态无关。
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.application.agents.orchestrator import SubmitIntentInput
from app.composition import Container, build_container
from app.domain.queue.ports.task_queue import IntentTask, TaskStatus
from app.presentation.connection import ConnectionManager
from app.presentation.dto import (
    CancelOrderRequest,
    SubmitIntentRequest,
    SubmitIntentResponse,
)
from app.infrastructure.security.auth_identity import AuthDirectory, AuthError, Principal
from app.infrastructure.security.approval_store import ApprovalError
from app.infrastructure.security.approved_actions import ApprovedActionError
from app.infrastructure.runtime_checkpoint import CheckpointConflictError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

logger = logging.getLogger(__name__)

# 轮数计数器存活时长：比幂等窗口长得多，让一整段会话都能被正确分类
_TURN_COUNTER_TTL_SECONDS = 86400


def build_app() -> FastAPI:
    state: dict = {}

    async def _forward_remote_events(c: Container) -> None:
        """把其他进程（worker）广播的事件转发给本进程的 WS 订阅者。"""
        if c.backplane is None:
            return
        try:
            async for event in c.backplane.listen():
                c.bus.deliver_local(event)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logger.warning("事件背板监听中断：%s", err)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        c = await build_container()
        state["c"] = c
        state["auth"] = AuthDirectory.from_json(getattr(c.settings, "auth_tokens_json", "{}"))
        state["connections"] = ConnectionManager(c.bus, state["auth"])
        await c.startup()
        if c.backplane is not None:
            # 跨进程事件转发：不开这个任务，worker 产生的流式事件到不了前端
            state["forwarder"] = asyncio.create_task(_forward_remote_events(c))
        try:
            yield
        finally:
            forwarder = state.pop("forwarder", None)
            if forwarder is not None:
                forwarder.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await forwarder
            state.pop("c", None)
            await c.shutdown()

    api = FastAPI(title="Globex 跨境电商 Agent", version="0.4.0", lifespan=lifespan)

    @api.middleware("http")
    async def authenticate_request(request: Request, call_next):
        if request.url.path.startswith("/commerce/"):
            scheme, _, token = request.headers.get("authorization", "").partition(" ")
            try:
                if scheme.lower() != "bearer" or "auth" not in state:
                    raise AuthError("authentication required")
                request.state.principal = state["auth"].authenticate(token)
            except AuthError:
                return JSONResponse({"detail": "AUTHENTICATION_REQUIRED"}, status_code=401,
                    headers={"WWW-Authenticate": "Bearer"})
        return await call_next(request)

    def authority(request: Request, permission: str) -> Principal:
        principal = request.state.principal
        if permission not in principal.permissions:
            raise HTTPException(status_code=403, detail="PERMISSION_DENIED")
        return principal

    def scoped_session(principal: Principal, raw: str | None) -> str:
        try:
            return principal.scope_session(raw)
        except AuthError as err:
            raise HTTPException(status_code=403, detail="SESSION_ACCESS_DENIED") from err

    def intent_input(request: Request, body: SubmitIntentRequest) -> SubmitIntentInput:
        principal = authority(request, "intent:submit")
        if body.buyer_id is not None and body.buyer_id != principal.subject:
            raise HTTPException(status_code=403, detail="IDENTITY_OVERRIDE_DENIED")
        key = request.headers.get("idempotency-key")
        if key is not None and (not key or len(key) > 128):
            raise HTTPException(status_code=400, detail="INVALID_IDEMPOTENCY_KEY")
        raw_session = body.shopping_session_id
        if raw_session is None and key is not None:
            raw_session = "idem-" + hashlib.sha256(key.encode()).hexdigest()[:32]
        session_id = scoped_session(principal, raw_session)
        task_id = "turn-" + (hashlib.sha256(f"{principal.buyer_key}\n{key}".encode()).hexdigest()
            if key is not None else uuid.uuid4().hex)
        return SubmitIntentInput(shopping_session_id=session_id, buyer_id=principal.buyer_key,
            locale=body.locale, currency=body.currency, raw_query=body.raw_query,
            task_id=task_id, approval_id=body.approval_id, permissions=tuple(sorted(principal.permissions)))

    def successful_reply(text: str) -> str:
        if not text.strip() or text.lstrip().startswith("[error]"):
            raise HTTPException(status_code=502, detail="AGENT_EXECUTION_FAILED")
        return text

    def container() -> Container:
        if "c" not in state:
            raise HTTPException(status_code=503, detail="服务尚未就绪")
        return state["c"]

    settings_origins = build_container_origins()
    api.add_middleware(
        CORSMiddleware,
        allow_origins=settings_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @api.get("/health")
    async def health() -> dict:
        """依赖连通性一并报出，避免"进程活着但存储已挂"被当成健康。"""
        c = container()
        database = "disabled"
        if c.db_engine is not None:
            try:
                async with c.db_engine.connect() as conn:
                    await conn.execute(text("select 1"))
                database = c.db_engine.url.get_backend_name()
            except Exception:  # noqa: BLE001
                database = "error"
        redis_state = "disabled"
        if c.cache.enabled:
            redis_state = "ok" if await c.cache.ping() else "error"
        unavailable = database == "error" or (c.task_queue is not None and redis_state == "error")
        result = {
            "status": "degraded" if unavailable else "ok",
            "model": "synthetic-catalog-v1" if getattr(c.settings, "llm_mode", "live") == "mock" else c.settings.llm_model,
            "model_mode": getattr(c.settings, "llm_mode", "live"),
            "database": database,
            "redis": redis_state,
            "semantic_cache": c.semantic_cache.enabled,
            "queue": "enabled" if c.task_queue is not None else "disabled",
            "queue_depth": await c.task_queue.depth() if c.task_queue is not None else 0,
        }
        return JSONResponse(result, status_code=503) if unavailable else result

    @api.post("/commerce/intents", response_model=SubmitIntentResponse)
    async def submit_intent(body: SubmitIntentRequest, request: Request) -> SubmitIntentResponse:
        c = container()
        intent = intent_input(request, body)
        session_id = intent.shopping_session_id
        if c.task_queue is None:
            try:
                result = await c.orchestrator.handle_intent(intent)
            except CheckpointConflictError as error:
                raise HTTPException(status_code=409, detail="IDEMPOTENCY_INPUT_CONFLICT") from error
            except TimeoutError as error:
                raise HTTPException(status_code=504, detail="TASK_DEADLINE_EXCEEDED") from error
            return SubmitIntentResponse(
                shopping_session_id=result.shopping_session_id, final_text=successful_reply(result.final_text),
            )

        task_id = await _enqueue(c, intent)
        final_text = await _await_result(c, task_id, session_id)
        return SubmitIntentResponse(shopping_session_id=session_id, final_text=successful_reply(final_text))

    @api.post("/commerce/intents/async")
    async def submit_intent_async(body: SubmitIntentRequest, request: Request) -> dict:
        c = container()
        intent = intent_input(request, body)
        session_id = intent.shopping_session_id
        if c.task_queue is None:
            raise HTTPException(status_code=503, detail="队列未启用，请使用 /commerce/intents")
        task_id = await _enqueue(c, intent)
        return {"shopping_session_id": session_id, "task_id": task_id, "state": "queued"}

    @api.get("/commerce/tasks/{task_id}")
    async def get_task(task_id: str, request: Request) -> dict:
        c = container()
        principal = authority(request, "commerce:read")
        if c.task_queue is None:
            raise HTTPException(status_code=503, detail="队列未启用")
        status = await c.task_queue.get_status(task_id)
        if status is None or status.buyer_id != principal.buyer_key:
            raise HTTPException(status_code=404, detail="TASK_NOT_FOUND")
        return {
            "task_id": status.task_id,
            "state": status.state,
            "final_text": status.final_text,
            "error": status.error,
            "queue_position": status.queue_position,
        }

    @api.websocket("/commerce/events")
    async def commerce_events(websocket: WebSocket) -> None:
        await state["connections"].serve(websocket)

    @api.get("/commerce/orders/{order_id}")
    async def get_order(order_id: str, request: Request) -> dict:
        principal = authority(request, "commerce:read")
        try:
            return await container().query_order.execute_owned(order_id, principal.buyer_key)
        except ValueError as err:
            raise HTTPException(status_code=404, detail="ORDER_NOT_FOUND") from err

    @api.post("/commerce/orders/{order_id}/cancel")
    async def cancel_order_endpoint(order_id: str, body: CancelOrderRequest, request: Request):
        principal = authority(request, "commerce:write")
        c = container()
        session_id = scoped_session(principal, body.shopping_session_id)
        try:
            await c.query_order.execute_owned(order_id, principal.buyer_key)
        except ValueError as err:
            raise HTTPException(status_code=404, detail="ORDER_NOT_FOUND") from err
        arguments = {"order_id": order_id, "reason": body.reason}
        permit = await c.approvals.begin(principal.buyer_key, session_id, "cancel_order_tool", arguments, body.approval_id)
        if permit.decision == "REQUIRE_APPROVAL":
            proposal = await c.approvals.get(principal.buyer_key, permit.approval_id)
            return JSONResponse({"decision": permit.decision, **proposal}, status_code=202)
        if permit.decision == "REPLAY":
            return permit.result
        if permit.decision != "ALLOW":
            raise HTTPException(status_code=409 if permit.decision == "RECONCILIATION_REQUIRED" else 403,
                detail=permit.decision)
        try:
            result = await c.cancel_order.execute_owned(order_id, body.reason, principal.buyer_key)
            await c.approvals.complete(permit.approval_id, result)
            return result
        except BaseException:
            await c.approvals.mark_uncertain(permit.approval_id)
            raise

    @api.get("/commerce/me")
    async def me(request: Request):
        principal = request.state.principal
        return {"tenant_id": principal.tenant_id, "subject": principal.subject,
            "permissions": sorted(principal.permissions)}

    @api.get("/commerce/approvals")
    async def pending_approvals(shopping_session_id: str, request: Request):
        principal = authority(request, "commerce:read")
        session_id = scoped_session(principal, shopping_session_id)
        return {"approvals": await container().approvals.pending(principal.buyer_key, session_id)}

    @api.get("/commerce/approvals/{approval_id}")
    async def get_approval(approval_id: str, request: Request):
        principal = authority(request, "commerce:read")
        try:
            return await container().approvals.get(principal.buyer_key, approval_id)
        except ApprovalError as err:
            raise HTTPException(status_code=404, detail="APPROVAL_NOT_FOUND") from err

    @api.post("/commerce/approvals/{approval_id}/approve")
    async def approve(approval_id: str, request: Request):
        principal = authority(request, "commerce:write")
        try:
            return await container().approvals.approve(principal.buyer_key, approval_id)
        except ApprovalError as err:
            raise HTTPException(status_code=409, detail="APPROVAL_UNAVAILABLE") from err

    @api.post("/commerce/approvals/{approval_id}/execute")
    async def execute_approval(approval_id: str, request: Request):
        principal = authority(request, "commerce:write")
        try:
            return await container().approved_actions.execute(principal.buyer_key, approval_id,
                tuple(sorted(principal.permissions)))
        except ApprovedActionError as error:
            raise HTTPException(status_code=error.status_code, detail=error.reason) from error

    return api


async def _queue_priority(c: Container, session_id: str) -> int:
    """按对话轮数定队列优先级（0 = 正常，1 = 大请求）。

    长会话上下文大、单次耗时长，分到低优先流，避免堵住新会话。
    轮数计数存 Redis（计数不原子，但优先级本身是启发式，差一两次无影响）；
    Redis 不可用或开关关闭时一律返回 0，退化为单队列。
    """
    if not c.settings.queue_priority_enabled or not c.cache.enabled:
        return 0
    key = f"globex:turns:{session_id}"
    try:
        current = int(await c.cache.get_raw(key) or 0) + 1
        await c.cache.set_json(key, current, _TURN_COUNTER_TTL_SECONDS)
    except Exception:  # noqa: BLE001 —— 计数失败不能影响入队
        return 0
    return 1 if current >= c.settings.queue_large_request_turns else 0


async def _enqueue(c: Container, intent: SubmitIntentInput) -> str:
    """Stable task identity; Redis atomically binds input, queue entry and status."""
    fingerprint = intent.fingerprint()
    task_id = intent.task_id or f"turn-{uuid.uuid4().hex}"
    runtime_checkpoints = getattr(c, "runtime_checkpoints", None)
    if runtime_checkpoints is not None:
        try:
            await runtime_checkpoints.validate_binding(task_id, intent.buyer_id,
                intent.shopping_session_id, fingerprint)
        except CheckpointConflictError as error:
            raise HTTPException(status_code=409, detail="IDEMPOTENCY_INPUT_CONFLICT") from error
    task = IntentTask(
            task_id=task_id,
            shopping_session_id=intent.shopping_session_id,
            buyer_id=intent.buyer_id,
            locale=intent.locale,
            currency=intent.currency,
            raw_query=intent.raw_query,
            approval_id=intent.approval_id,
            permissions=intent.permissions,
            priority=await _queue_priority(c, intent.shopping_session_id),
        )
    try:
        created = await c.task_queue.enqueue_once(task, fingerprint)  # type: ignore[union-attr]
    except ValueError as error:
        raise HTTPException(status_code=409, detail="IDEMPOTENCY_INPUT_CONFLICT") from error
    if created:
        c.bus.publish(intent.shopping_session_id, "task.queued", {"task_id": task_id})
    return task_id


async def _await_result(c: Container, task_id: str, session_id: str) -> str:
    """等 worker 跑完。

    优先等 final.result 事件（实时）；同时定期查任务状态兜底——
    worker 崩溃或任务进死信时事件永远不会来，只靠等事件会把请求挂死。
    """
    queue = c.bus.subscribe(session_id)
    deadline = time.monotonic() + c.settings.queue_wait_seconds
    try:
        while time.monotonic() < deadline:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                status = await c.task_queue.get_status(task_id)  # type: ignore[union-attr]
                if status is not None and status.state == "done":
                    return status.final_text
                if status is not None and status.state == "failed":
                    return f"[error] {status.error}"
                continue
            status = await c.task_queue.get_status(task_id)
            if status is not None and status.state == "done":
                return status.final_text
            if status is not None and status.state == "failed":
                return "[error] AGENT_EXECUTION_FAILED"
        return "[error] 处理超时，请稍后重试或改用异步接口查询任务状态"
    finally:
        c.bus.unsubscribe(session_id, queue)


def build_container_origins() -> list[str]:
    """CORS 需要在 app 构造期就确定，此处单独读一次配置。"""
    from app.infrastructure.settings import load_settings

    return load_settings().cors_origins


app = build_app()


if __name__ == "__main__":
    import uvicorn

    from app.infrastructure.settings import load_settings

    uvicorn.run(app, host="0.0.0.0", port=load_settings().port)
