# -*- coding: utf-8 -*-
"""ConnectionManager

WebSocket 连接管理：客户端连上 /commerce/events 后发送
{"shopping_session_id": "..."} 完成订阅，服务端把 TradeEventBus
中对应会话的事件推送给该连接。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict

from fastapi import WebSocket, WebSocketDisconnect

from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.security.auth_identity import AuthDirectory, AuthError

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, bus: TradeEventBus, auth: AuthDirectory | None = None) -> None:
        self._bus = bus
        self._auth = auth

    async def serve(self, websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            subscribe_payload = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        except (WebSocketDisconnect, asyncio.TimeoutError, ValueError):
            await websocket.close(code=4401)
            return
        try:
            if self._auth is None or not isinstance(subscribe_payload, dict):
                raise AuthError("authentication required")
            principal = self._auth.authenticate(subscribe_payload.get("access_token"))
            if "commerce:read" not in principal.permissions:
                raise AuthError("permission denied")
            raw = subscribe_payload.get("shopping_session_id")
            if not raw:
                raise AuthError("session required")
            session_id = principal.scope_session(raw)
        except AuthError:
            await websocket.close(code=4403, reason="SESSION_ACCESS_DENIED")
            return

        queue = self._bus.subscribe(session_id)
        logger.info("WebSocket 已订阅会话：%s", session_id)
        async def send_events():
            while True:
                event = await queue.get()
                payload = asdict(event)
                payload.pop("shopping_session_id", None)
                await websocket.send_json(payload)

        async def watch_disconnect():
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return

        sender = asyncio.create_task(send_events())
        receiver = asyncio.create_task(watch_disconnect())
        try:
            done, _ = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            sender.cancel()
            receiver.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)
            self._bus.unsubscribe(session_id, queue)
            logger.info("WebSocket 已退订会话：%s", session_id)
