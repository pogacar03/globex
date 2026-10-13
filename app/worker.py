# -*- coding: utf-8 -*-
"""worker：意图任务消费进程

用法：
    uv run python -m app.worker

与 API 进程共用同一个装配容器（app/composition.py），差别只在于：
API 进程负责收请求、入队、等结果；worker 进程负责把队列里的意图真正跑完。

削峰的实质是「worker 并发度」限制了同时在跑的 Agent 轮次，
与接口是不是同步无关。

优雅退出：收到 SIGTERM/SIGINT 后停止领新消息，等在途任务跑完再退；
未 ack 的消息由 Redis Stream 的 pending 机制重投，不会丢。
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
from typing import Any

from app.application.agents.orchestrator import SubmitIntentInput
from app.composition import build_container
from app.domain.queue.ports.task_queue import IntentTask, TaskStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("app.worker")


def _build_handler(container: Any, in_flight: dict[str, int]):
    async def handle(task: IntentTask) -> None:
        in_flight["count"] += 1
        try:
            await container.task_queue.set_status(
                TaskStatus(task_id=task.task_id, state="running", buyer_id=task.buyer_id),
            )
            container.bus.publish(task.shopping_session_id, "task.started", {"task_id": task.task_id})
            result = await container.orchestrator.handle_intent(
                SubmitIntentInput(
                    shopping_session_id=task.shopping_session_id,
                    buyer_id=task.buyer_id,
                    locale=task.locale,
                    currency=task.currency,
                    raw_query=task.raw_query,
                    task_id=task.task_id,
                    approval_id=task.approval_id,
                    permissions=task.permissions,
                ),
            )
            final_text = result.final_text or ""
            if not final_text.strip() or final_text.strip().startswith("[error]"):
                raise RuntimeError(final_text or "Agent returned no final text")
            await container.task_queue.set_status(
                TaskStatus(
                    task_id=task.task_id,
                    state="done",
                    final_text=final_text,
                    buyer_id=task.buyer_id,
                ),
            )
        except Exception as err:  # noqa: BLE001 —— 标记失败后抛出，交给队列决定重投
            await container.task_queue.set_status(
                TaskStatus(
                    task_id=task.task_id,
                    state="retrying",
                    error=str(err),
                    buyer_id=task.buyer_id,
                ),
            )
            raise
        finally:
            in_flight["count"] -= 1

    return handle


async def main() -> None:
    container = await build_container()
    if container.task_queue is None:
        raise RuntimeError("未启用队列（需配置 REDIS_URL 且 QUEUE_ENABLED 不为 0），worker 无事可做")

    consumer_name = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
    stopping = asyncio.Event()
    in_flight = {"count": 0}

    def request_stop(*_args) -> None:
        if not stopping.is_set():
            logger.info("收到退出信号，停止领取新任务（在途 %d 个跑完后退出）", in_flight["count"])
            stopping.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, request_stop)

    handle = _build_handler(container, in_flight)

    logger.info(
        "worker 启动：%s（并发度 %d）", consumer_name, container.settings.worker_concurrency,
    )
    await container.startup()
    try:
        await container.task_queue.consume(
            consumer_name=consumer_name,
            handler=handle,
            should_stop=stopping.is_set,
            concurrency=container.settings.worker_concurrency,
            claim_idle_ms=getattr(container.settings, "queue_claim_idle_ms", 60000),
        )
    finally:
        logger.info("worker 退出：%s", consumer_name)
        await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
