# -*- coding: utf-8 -*-
"""RedisStreamTaskQueue + RedisEventBackplane

削峰用 Redis Stream（消费者组 + ack + pending 重投 + 死信），
跨进程事件广播用 Redis Pub/Sub。两者共用一个 Redis 连接。

为什么用 Stream 而不是 List：Stream 有消费者组与未确认（pending）列表，
worker 崩溃后未 ack 的消息能被重新领取，List 做不到这点。
"""
from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import os
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Optional

from app.domain.queue.ports.task_queue import IntentTask, TaskQueue, TaskStatus
from app.infrastructure.eventbus import TradeEvent

logger = logging.getLogger(__name__)

_STREAM = "globex:intents"
# 大请求（长会话）单独一条流：与正常流同一消费者组名，
# 但 xreadgroup 里正常流排在前面——Redis 按传入顺序返回，天然形成优先级。
_LARGE_STREAM = "globex:intents:large"
_DEAD_STREAM = "globex:intents:dead"
_GROUP = "globex-workers"
_STATUS_PREFIX = "globex:task:"
_STATUS_TTL = 3600
_EVENT_CHANNEL_PREFIX = "globex:events:"

_ACK_IF_OWNED_LUA = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then
    return 0
end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
"""

_SET_STATUS_IF_OWNED_LUA = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then
    return 0
end
redis.call('SET', KEYS[2], ARGV[4], 'EX', tonumber(ARGV[5]))
return 1
"""

_HEARTBEAT_IF_OWNED_LUA = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then
    return 0
end
local claimed = redis.call(
    'XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'IDLE', 0, 'JUSTID'
)
if #claimed == 0 then
    return 0
end
return 1
"""

_ENQUEUE_ONCE_LUA = """
local existing = redis.call('GET', KEYS[1])
if existing then
    if existing == ARGV[1] then
        return 0
    end
    return -1
end
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
redis.call('SET', KEYS[3], ARGV[3], 'EX', tonumber(ARGV[5]))
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[4]))
return 1
"""


@dataclass(frozen=True)
class _PendingMessage:
    stream: str
    message_id: str
    fields: dict


_owner_context: contextvars.ContextVar[Optional[tuple[int, str, str, str]]] = (
    contextvars.ContextVar("globex_queue_pending_owner", default=None)
)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class RedisStreamTaskQueue(TaskQueue):
    def __init__(self, client: Any) -> None:
        self._client = client
        self._claim_cursors = {_STREAM: "0-0", _LARGE_STREAM: "0-0"}

    async def ensure_group(self) -> None:
        """幂等创建消费者组（两条流各一个）。组已存在时 Redis 抛 BUSYGROUP，属正常情况。"""
        for stream in (_STREAM, _LARGE_STREAM):
            try:
                await self._client.xgroup_create(stream, _GROUP, id="0", mkstream=True)
            except Exception as err:  # noqa: BLE001
                if "BUSYGROUP" not in str(err):
                    raise

    async def enqueue(self, task: IntentTask) -> None:
        stream = _LARGE_STREAM if task.priority > 0 else _STREAM
        await self._client.xadd(stream, {"payload": json.dumps(task.to_dict(), ensure_ascii=False)})

    async def enqueue_once(self, task: IntentTask, input_fingerprint: str) -> bool:
        stream = _LARGE_STREAM if task.priority > 0 else _STREAM
        queued_status = json.dumps(
            {
                "task_id": task.task_id,
                "state": "queued",
                "buyer_id": task.buyer_id,
                "final_text": "",
                "error": "",
            },
            ensure_ascii=False,
        )
        result = await self._client.eval(
            _ENQUEUE_ONCE_LUA,
            3,
            f"globex:enqueue:{task.task_id}",
            stream,
            f"{_STATUS_PREFIX}{task.task_id}",
            input_fingerprint,
            json.dumps(task.to_dict(), ensure_ascii=False),
            queued_status,
            600,
            _STATUS_TTL,
        )
        if result == 1:
            return True
        if result == 0:
            return False
        if result == -1:
            raise ValueError(
                f"task_id {task.task_id!r} was already enqueued with a different input fingerprint"
            )
        raise RuntimeError(f"unexpected enqueue_once Redis script result: {result!r}")

    async def set_status(self, status: TaskStatus) -> None:
        owner = _owner_context.get()
        value = json.dumps(
            {
                "task_id": status.task_id,
                "state": status.state,
                "buyer_id": status.buyer_id,
                "final_text": status.final_text,
                "error": status.error,
            },
            ensure_ascii=False,
        )
        if owner is not None and owner[0] == id(self):
            updated = await self._client.eval(
                _SET_STATUS_IF_OWNED_LUA,
                2,
                owner[1],
                f"{_STATUS_PREFIX}{status.task_id}",
                _GROUP,
                owner[2],
                owner[3],
                value,
                _STATUS_TTL,
            )
            if not updated:
                logger.info("任务消息所有权已转移，跳过状态更新：%s", status.task_id)
                return
            return
        await self._client.set(
            f"{_STATUS_PREFIX}{status.task_id}",
            value,
            ex=_STATUS_TTL,
        )

    async def get_status(self, task_id: str) -> Optional[TaskStatus]:
        raw = await self._client.get(f"{_STATUS_PREFIX}{task_id}")
        if raw is None:
            return None
        data = json.loads(raw)
        position = await self.depth() if data.get("state") == "queued" else 0
        return TaskStatus(
            task_id=data["task_id"],
            state=data["state"],
            buyer_id=data.get("buyer_id", ""),
            final_text=data.get("final_text", ""),
            error=data.get("error", ""),
            queue_position=position,
        )

    async def depth(self) -> int:
        """两条流未被消费者组读取的消息数之和（lag）。取不到时退回 0，不让观测拖垮主链路。"""
        total = 0
        for stream in (_STREAM, _LARGE_STREAM):
            total += await self._stream_depth(stream)
        return total

    async def _stream_depth(self, stream: str) -> int:
        try:
            groups = await self._client.xinfo_groups(stream)
        except Exception:  # noqa: BLE001 —— stream 还没创建
            return 0
        for group in groups:
            if group.get("name") == _GROUP:
                lag = group.get("lag")
                if lag is not None:
                    return int(lag)
                return int(group.get("pending", 0))
        return 0

    async def consume(
        self,
        consumer_name: str,
        handler: Callable[[IntentTask], Any],
        should_stop: Callable[[], bool],
        block_ms: int = 2000,
        max_deliveries: int = 3,
        concurrency: int = 1,
        claim_idle_ms: int = 60000,
        heartbeat_interval_ms: Optional[int] = None,
    ) -> None:
        """消费循环。

        - 定期回收两条流中的 stale pending；处理成功才 ack
        - 投递次数超过 max_deliveries 进死信流，避免坏消息无限重放阻塞队列
        - 所有 stream 共用 concurrency 个槽，长任务通过 XCLAIM 续租
        - 退出前等在途任务跑完；未 ack 的由 pending 机制重投，不会丢
        """
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        await self.ensure_group()
        in_flight: set[asyncio.Task] = set()
        deferred: deque[_PendingMessage] = deque()
        active_messages: set[tuple[str, str]] = set()
        if heartbeat_interval_ms is None:
            heartbeat_interval_ms = max(1, claim_idle_ms // 3)

        def schedule(message: _PendingMessage) -> bool:
            message_key = (message.stream, message.message_id)
            if message_key in active_messages:
                return False
            active_messages.add(message_key)
            task = asyncio.create_task(
                self._handle_one(
                    message.stream,
                    message.message_id,
                    message.fields,
                    handler,
                    max_deliveries,
                    consumer_name=consumer_name,
                    heartbeat_interval_ms=heartbeat_interval_ms,
                ),
            )
            in_flight.add(task)
            task.add_done_callback(
                lambda _task, key=message_key: active_messages.discard(key),
            )
            return True

        while not should_stop():
            in_flight = {task for task in in_flight if not task.done()}
            free_slots = max(0, concurrency - len(in_flight))
            if free_slots == 0:
                await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                continue

            while deferred and free_slots:
                if schedule(deferred.popleft()):
                    free_slots -= 1
            if free_slots == 0:
                continue

            # Claim at most the number of currently free slots across both streams.
            # Keep each original stream and message ID for status, retry and ack.
            claimed: list[_PendingMessage] = []
            claim_error = False
            for stream in (_STREAM, _LARGE_STREAM):
                if len(claimed) >= free_slots:
                    break
                entries, failed = await self.claim_stale(
                    consumer_name,
                    idle_ms=claim_idle_ms,
                    stream=stream,
                    count=free_slots - len(claimed),
                )
                claimed.extend(entries)
                claim_error = claim_error or failed
            scheduled_claims = sum(schedule(message) for message in claimed)
            free_slots -= scheduled_claims
            if claim_error:
                # Avoid a hot loop after Redis errors, including tests or workers
                # configured with block_ms=0.
                await asyncio.sleep(0.2)
            if free_slots == 0:
                # Claim adapters can return immediately when there are no
                # messages; yield so newly scheduled handlers get their slot.
                await asyncio.sleep(0)
                continue

            try:
                # 正常流排前：Redis 按传入顺序返回，短会话不会被长会话堵在后面
                batches = await self._client.xreadgroup(
                    _GROUP,
                    consumer_name,
                    {_STREAM: ">", _LARGE_STREAM: ">"},
                    count=free_slots,
                    block=block_ms,
                )
            except Exception as err:  # noqa: BLE001
                logger.warning("队列读取失败，稍后重试：%s", err)
                await asyncio.sleep(1)
                continue
            if not batches:
                if block_ms <= 0:
                    await asyncio.sleep(0.01)
                continue
            read_messages: list[_PendingMessage] = []
            for stream, entries in batches:
                stream_name = _text(stream)
                for message_id, fields in entries:
                    read_messages.append(_PendingMessage(stream_name, _text(message_id), fields))
            for message in read_messages[:free_slots]:
                schedule(message)
            # Redis COUNT is a global bound across streams. Retain any over-read
            # entries locally in case a client implementation returns more.
            deferred.extend(read_messages[free_slots:])

        if in_flight:
            logger.info("等待 %d 个在途任务完成后退出", len(in_flight))
            await asyncio.gather(*in_flight, return_exceptions=True)

    async def claim_stale(
        self,
        consumer_name: str,
        idle_ms: int = 60000,
        stream: Optional[str] = None,
        count: int = 10,
    ) -> tuple[list[_PendingMessage], bool]:
        """Claim stale pending records with the stream and Redis message ID intact."""
        if count <= 0:
            return [], False
        streams = (stream,) if stream is not None else (_STREAM, _LARGE_STREAM)
        claimed: list[_PendingMessage] = []
        failed = False
        for stream_name in streams:
            items, stream_failed = await self._claim_stale_stream(
                stream_name, consumer_name, idle_ms, count - len(claimed),
            )
            claimed.extend(items)
            failed = failed or stream_failed
            if len(claimed) >= count:
                break
        return claimed, failed

    async def _claim_stale_stream(
        self, stream: str, consumer_name: str, idle_ms: int, count: int,
    ) -> tuple[list[_PendingMessage], bool]:
        if count <= 0:
            return [], False
        try:
            result = await self._client.xautoclaim(
                stream,
                _GROUP,
                consumer_name,
                min_idle_time=idle_ms,
                start_id=self._claim_cursors[stream],
                count=count,
            )
            if result:
                self._claim_cursors[stream] = _text(result[0])
            entries = result[1] if len(result) > 1 else []
            return [
                _PendingMessage(stream, _text(message_id), fields)
                for message_id, fields in entries
            ], False
        except Exception as err:  # noqa: BLE001
            logger.warning("回收 pending 消息失败（%s）：%s", stream, err)
            return [], True

    async def _handle_one(
        self,
        stream: str,
        message_id: str,
        fields: dict,
        handler: Callable[[IntentTask], Any],
        max_deliveries: int,
        consumer_name: Optional[str] = None,
        heartbeat_interval_ms: int = 20000,
    ) -> None:
        owner_token = None
        if consumer_name is not None:
            owner_token = _owner_context.set((id(self), stream, message_id, consumer_name))
        raw = fields.get("payload")
        if not raw:
            await self._ack_if_owned(stream, message_id, consumer_name)
            if owner_token is not None:
                _owner_context.reset(owner_token)
            return
        try:
            task = IntentTask.from_dict(json.loads(raw))
        except Exception as err:  # noqa: BLE001 —— 解不开的消息直接进死信，不能卡住队列
            logger.warning("任务解析失败，进死信：%s（%s）", message_id, err)
            if not await self._owns_if_consumer(stream, message_id, consumer_name):
                logger.info("任务解析期间消息所有权已转移，跳过死信：%s", message_id)
                if owner_token is not None:
                    _owner_context.reset(owner_token)
                return
            await self._client.xadd(_DEAD_STREAM, {"payload": raw, "reason": str(err)})
            await self._ack_if_owned(stream, message_id, consumer_name)
            if owner_token is not None:
                _owner_context.reset(owner_token)
            return

        heartbeat_task: Optional[asyncio.Task] = None
        try:
            if consumer_name is not None:
                heartbeat_task = asyncio.create_task(
                    self._heartbeat(
                        stream,
                        message_id,
                        consumer_name,
                        max(1, heartbeat_interval_ms) / 1000,
                    ),
                )
            await handler(task)
            await self._ack_if_owned(stream, message_id, consumer_name)
        except Exception as err:  # noqa: BLE001
            if not await self._owns_if_consumer(stream, message_id, consumer_name):
                logger.info("任务处理期间消息所有权已转移，跳过死信或重试状态：%s", task.task_id)
                return
            deliveries = await self._delivery_count(stream, message_id)
            if deliveries >= max_deliveries:
                logger.error("任务重试超限进死信：%s（%s）", task.task_id, err)
                await self.set_status(
                    TaskStatus(
                        task_id=task.task_id,
                        state="failed",
                        error=str(err),
                        buyer_id=task.buyer_id,
                    ),
                )
                await self._client.xadd(
                    _DEAD_STREAM, {"payload": raw, "reason": str(err)},
                )
                await self._ack_if_owned(stream, message_id, consumer_name)
            else:
                # 不 ack：留在 pending 里等重投
                logger.warning("任务处理失败（第 %d 次投递）：%s（%s）", deliveries, task.task_id, err)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if owner_token is not None:
                _owner_context.reset(owner_token)

    async def _heartbeat(
        self, stream: str, message_id: str, consumer_name: str, interval_seconds: float,
    ) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                renewed = await self._client.eval(
                    _HEARTBEAT_IF_OWNED_LUA,
                    1,
                    stream,
                    _GROUP,
                    message_id,
                    consumer_name,
                )
            except Exception as err:  # noqa: BLE001
                logger.warning("任务 pending 续租失败（%s）：%s", message_id, err)
                return
            if not renewed:
                logger.debug("pending 消息续租未确认所有权：%s", message_id)
                return

    async def _owns_if_consumer(
        self, stream: str, message_id: str, consumer_name: Optional[str],
    ) -> bool:
        if consumer_name is None:
            return True
        return await self._owns_message(stream, message_id, consumer_name)

    async def _owns_message(self, stream: str, message_id: str, consumer_name: str) -> bool:
        try:
            pending = await self._client.xpending_range(
                stream, _GROUP, message_id, message_id, 1,
            )
            if not pending:
                return False
            owner = pending[0].get("consumer")
            return owner is not None and _text(owner) == consumer_name
        except Exception as err:  # noqa: BLE001
            logger.warning("检查任务消息所有权失败（%s）：%s", message_id, err)
            return False

    async def _ack_if_owned(
        self, stream: str, message_id: str, consumer_name: Optional[str],
    ) -> bool:
        if consumer_name is None:
            # Compatibility for direct internal callers; consume() always supplies
            # a consumer so production acknowledgments use the fenced Lua path.
            await self._client.xack(stream, _GROUP, message_id)
            return True
        acknowledged = await self._client.eval(
            _ACK_IF_OWNED_LUA,
            1,
            stream,
            _GROUP,
            message_id,
            consumer_name,
        )
        if not acknowledged:
            logger.info("任务消息所有权已转移，跳过 ack：%s", message_id)
            return False
        return True

    async def _delivery_count(self, stream: str, message_id: str) -> int:
        try:
            pending = await self._client.xpending_range(stream, _GROUP, message_id, message_id, 1)
            return int(pending[0]["times_delivered"]) if pending else 1
        except Exception:  # noqa: BLE001
            return 1

class RedisEventBackplane:
    """跨进程事件广播：worker 发布 → API 进程订阅 → 转发给本地 WS。

    必须带发送方标识并跳过自己发的消息：Pub/Sub 不会排除发布者，
    API 进程既发布又订阅同一频道，不过滤就会把自己的事件再投递一次
    （实测现象：前端收到两条 task.queued）。
    """

    def __init__(self, client: Any, origin: Optional[str] = None) -> None:
        self._client = client
        self._origin = origin or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"

    @property
    def origin(self) -> str:
        return self._origin

    async def publish(self, event: TradeEvent) -> None:
        channel = f"{_EVENT_CHANNEL_PREFIX}{event.shopping_session_id}"
        envelope = {"origin": self._origin, "event": event.to_dict()}
        try:
            await self._client.publish(channel, json.dumps(envelope, ensure_ascii=False))
        except Exception as err:  # noqa: BLE001 —— 广播失败不影响本进程投递
            logger.warning("事件广播失败：%s（%s）", channel, err)

    async def listen(self) -> AsyncIterator[TradeEvent]:
        """订阅所有会话频道，逐条产出**其他进程**的事件。"""
        pubsub = self._client.pubsub()
        await pubsub.psubscribe(f"{_EVENT_CHANNEL_PREFIX}*")
        try:
            async for message in pubsub.listen():
                if message.get("type") != "pmessage":
                    continue
                try:
                    envelope = json.loads(message["data"])
                    if envelope.get("origin") == self._origin:
                        continue  # 自己发的，本地已投递过
                    yield TradeEvent.from_dict(envelope["event"])
                except Exception as err:  # noqa: BLE001
                    logger.warning("远端事件解析失败，跳过：%s", err)
        finally:
            await pubsub.punsubscribe()
            await pubsub.aclose()
