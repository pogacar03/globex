from __future__ import annotations

import json
import os
import uuid

import pytest
from redis.asyncio import Redis

from app.domain.queue.ports.task_queue import IntentTask, TaskStatus
from app.infrastructure.queue.redis_stream_queue import (
    _GROUP,
    _LARGE_STREAM,
    _STATUS_PREFIX,
    _STREAM,
    RedisStreamTaskQueue,
    _owner_context,
)

pytestmark = pytest.mark.asyncio


class AtomicEnqueueRedis:
    """Model the result of one atomic enqueue script invocation."""

    def __init__(self) -> None:
        self.entries: dict[str, list[dict]] = {}
        self.values: dict[str, tuple[str, int]] = {}
        self.eval_calls: list[tuple[str, int, tuple]] = []
        self.failure: Exception | None = None

    async def eval(self, script: str, numkeys: int, *values):
        self.eval_calls.append((script, numkeys, values))
        if self.failure is not None:
            raise self.failure

        marker_key, stream, status_key = values[:numkeys]
        fingerprint, payload, status, marker_ttl, status_ttl = values[numkeys:]
        existing = self.values.get(marker_key)
        if existing is not None:
            if existing[0] == fingerprint:
                return 0
            return -1

        self.entries.setdefault(stream, []).append({"payload": payload})
        self.values[status_key] = (status, status_ttl)
        self.values[marker_key] = (fingerprint, marker_ttl)
        return 1

    async def get(self, key: str):
        value = self.values.get(key)
        return value[0] if value is not None else None

    async def xinfo_groups(self, _stream: str):
        return [{"name": _GROUP, "lag": 1}]

    async def xlen(self, stream: str):
        return len(self.entries.get(stream, []))


def _enqueue_task(task_id: str, *, priority: int = 0) -> IntentTask:
    return IntentTask(
        task_id=task_id,
        shopping_session_id="session-1",
        buyer_id="buyer-1",
        locale="zh-CN",
        currency="CNY",
        raw_query="露营灯",
        priority=priority,
    )


async def test_real_redis_enqueue_once_replays_and_rejects_fingerprint_conflict():
    redis_url = os.environ.get("GLOBEX_TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("set GLOBEX_TEST_REDIS_URL to run the real Redis enqueue check")

    client = Redis.from_url(redis_url, decode_responses=True)
    task = _enqueue_task(f"enqueue-once-{uuid.uuid4().hex}", priority=1)
    marker_key = f"globex:enqueue:{task.task_id}"
    status_key = f"{_STATUS_PREFIX}{task.task_id}"
    queue = RedisStreamTaskQueue(client)
    message_ids: list[str] = []
    try:
        assert await queue.enqueue_once(task, "fingerprint-owner-a") is True
        assert await queue.enqueue_once(task, "fingerprint-owner-a") is False
        with pytest.raises(ValueError, match="fingerprint"):
            await queue.enqueue_once(task, "fingerprint-owner-b")

        entries = await client.xrange(_LARGE_STREAM, min="-", max="+")
        matching = [
            (message_id, fields)
            for message_id, fields in entries
            if json.loads(fields["payload"]).get("task_id") == task.task_id
        ]
        assert len(matching) == 1
        message_ids = [message_id for message_id, _fields in matching]
        assert json.loads(matching[0][1]["payload"])["task_id"] == task.task_id
        assert await client.get(marker_key) == "fingerprint-owner-a"
        marker_ttl = await client.ttl(marker_key)
        assert 0 < marker_ttl <= 600

        status = await queue.get_status(task.task_id)
        assert status is not None
        assert status.state == "queued"
        assert status.buyer_id == task.buyer_id
    finally:
        if message_ids:
            await client.xdel(_LARGE_STREAM, *message_ids)
        await client.delete(marker_key, status_key)
        await client.aclose()


async def test_enqueue_once_atomically_writes_priority_entry_status_and_marker():
    client = AtomicEnqueueRedis()
    queue = RedisStreamTaskQueue(client)
    task = _enqueue_task("idempotent-new", priority=1)

    assert await queue.enqueue_once(task, "fingerprint-a") is True

    assert await client.xlen(_LARGE_STREAM) == 1
    assert await client.xlen(_STREAM) == 0
    payload = json.loads(client.entries[_LARGE_STREAM][0]["payload"])
    assert payload["task_id"] == task.task_id
    status = json.loads(client.values[f"{_STATUS_PREFIX}{task.task_id}"][0])
    assert status == {
        "task_id": task.task_id,
        "state": "queued",
        "buyer_id": "buyer-1",
        "final_text": "",
        "error": "",
    }
    assert client.values[f"globex:enqueue:{task.task_id}"] == ("fingerprint-a", 600)
    assert len(client.eval_calls) == 1


async def test_enqueue_once_replay_returns_false_without_adding_another_entry():
    client = AtomicEnqueueRedis()
    queue = RedisStreamTaskQueue(client)
    task = _enqueue_task("idempotent-replay")

    assert await queue.enqueue_once(task, "same-input") is True
    assert await queue.enqueue_once(task, "same-input") is False

    assert await client.xlen(_STREAM) == 1
    assert len(client.eval_calls) == 2


async def test_enqueue_once_rejects_same_task_id_with_different_fingerprint():
    client = AtomicEnqueueRedis()
    queue = RedisStreamTaskQueue(client)
    task = _enqueue_task("idempotent-conflict")

    assert await queue.enqueue_once(task, "first-input") is True
    with pytest.raises(ValueError, match="fingerprint"):
        await queue.enqueue_once(task, "different-input")

    assert await client.xlen(_STREAM) == 1
    assert client.values[f"globex:enqueue:{task.task_id}"][0] == "first-input"


async def test_enqueue_once_redis_error_propagates_without_fallback_enqueue():
    client = AtomicEnqueueRedis()
    client.failure = RuntimeError("Redis EVAL unavailable")
    queue = RedisStreamTaskQueue(client)

    with pytest.raises(RuntimeError, match="EVAL unavailable"):
        await queue.enqueue_once(_enqueue_task("idempotent-error"), "fingerprint")

    assert client.entries == {}
    assert client.values == {}
    assert len(client.eval_calls) == 1


async def test_real_redis_lua_operations_reject_replaced_owner():
    redis_url = os.environ.get("GLOBEX_TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("set GLOBEX_TEST_REDIS_URL to run the real Redis ownership check")

    client = Redis.from_url(redis_url, decode_responses=True)
    suffix = uuid.uuid4().hex
    stream = f"{_STREAM}:atomic-check:{suffix}"
    task_id = f"atomic-check-{suffix}"
    status_key = f"{_STATUS_PREFIX}{task_id}"
    queue = RedisStreamTaskQueue(client)
    try:
        await client.xgroup_create(stream, _GROUP, id="0", mkstream=True)
        await client.xadd(stream, {"payload": "{}"})
        batch = await client.xreadgroup(_GROUP, "old-worker", {stream: ">"}, count=1)
        message_id = batch[0][1][0][0]

        transferred = await client.xclaim(
            stream,
            _GROUP,
            "replacement-worker",
            min_idle_time=0,
            message_ids=[message_id],
            idle=0,
        )
        assert transferred

        token = _owner_context.set((id(queue), stream, message_id, "old-worker"))
        try:
            await queue.set_status(
                TaskStatus(task_id=task_id, state="done", buyer_id="buyer-1"),
            )
        finally:
            _owner_context.reset(token)

        assert await queue.get_status(task_id) is None
        assert not await queue._ack_if_owned(stream, message_id, "old-worker")
        await queue._heartbeat(stream, message_id, "old-worker", interval_seconds=0.001)

        pending = await client.xpending_range(stream, _GROUP, message_id, message_id, 1)
        assert pending[0]["consumer"] == "replacement-worker"

        token = _owner_context.set((id(queue), stream, message_id, "replacement-worker"))
        try:
            await queue.set_status(
                TaskStatus(task_id=task_id, state="done", buyer_id="buyer-1"),
            )
        finally:
            _owner_context.reset(token)

        assert await queue.get_status(task_id) is not None
        assert await queue._ack_if_owned(stream, message_id, "replacement-worker")
        assert await client.xpending_range(stream, _GROUP, message_id, message_id, 1) == []
    finally:
        await client.delete(stream, status_key)
        await client.aclose()
