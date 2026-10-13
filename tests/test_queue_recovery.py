from __future__ import annotations

import asyncio
import json

import pytest

from app.domain.queue.ports.task_queue import IntentTask, TaskStatus
from app.infrastructure.queue.redis_stream_queue import (
    _DEAD_STREAM,
    _LARGE_STREAM,
    _STREAM,
    RedisStreamTaskQueue,
)

pytestmark = pytest.mark.asyncio


class RecoveryRedis:
    """Small Redis Streams fake with consumer ownership and stale claims."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, str, dict]] = []
        self.pending: dict[str, dict] = {}
        self.acked: list[tuple[str, str]] = []
        self.dead: list[dict] = []
        self.kv: dict[str, str] = {}
        self.set_calls: list[str] = []
        self.eval_calls: list[str] = []
        self.claim_calls: list[str] = []
        self.claim_start_ids: list[tuple[str, str]] = []
        self.heartbeat_calls: list[tuple[str, str]] = []
        self.transfer_owner_after_check: str | None = None
        self._seq = 0

    async def xgroup_create(self, *_args, **_kwargs):
        return True

    async def xadd(self, stream: str, fields: dict):
        self._seq += 1
        message_id = f"{self._seq}-0"
        if stream == _DEAD_STREAM:
            self.dead.append(fields)
        else:
            self.entries.append((stream, message_id, fields))
        return message_id

    async def xreadgroup(self, _group, consumer, streams, count=1, block=0):
        result: list[tuple[str, list[tuple[str, dict]]]] = []
        selected = 0
        for stream in streams:
            available = [
                (message_id, fields)
                for entry_stream, message_id, fields in self.entries
                if entry_stream == stream and message_id not in self.pending
            ]
            batch = available[: max(0, count - selected)]
            for message_id, _fields in batch:
                self.pending[message_id] = {
                    "consumer": consumer,
                    "times_delivered": 1,
                    "idle_ms": 0,
                }
            if batch:
                result.append((stream, batch))
                selected += len(batch)
            if selected >= count:
                break
        if not result and block:
            await asyncio.sleep(block / 1000)
        return result

    async def xautoclaim(self, stream, _group, consumer, min_idle_time, start_id="0-0", count=10):
        self.claim_calls.append(stream)
        self.claim_start_ids.append((stream, start_id))
        claimed = []
        matching = [
            entry for entry in self.entries
            if entry[0] == stream and entry[1] > start_id
        ]
        scanned = matching[:count]
        for entry_stream, message_id, fields in scanned:
            pending = self.pending.get(message_id)
            if (
                entry_stream == stream
                and pending is not None
                and pending["idle_ms"] >= min_idle_time
                and len(claimed) < count
            ):
                pending["consumer"] = consumer
                pending["times_delivered"] += 1
                pending["idle_ms"] = 0
                claimed.append((message_id, fields))
        next_id = scanned[-1][1] if len(matching) > len(scanned) and scanned else "0-0"
        return next_id, claimed, []

    async def xclaim(
        self, stream, _group, consumer, min_idle_time, message_ids, idle=0, **_kwargs,
    ):
        claimed = []
        for message_id in message_ids:
            pending = self.pending.get(message_id)
            if pending is None or pending["idle_ms"] < min_idle_time:
                continue
            pending["consumer"] = consumer
            pending["idle_ms"] = idle
            self.heartbeat_calls.append((stream, message_id))
            entry = next(
                ((entry_id, fields) for entry_stream, entry_id, fields in self.entries
                 if entry_stream == stream and entry_id == message_id),
                None,
            )
            if entry:
                claimed.append(entry)
        return claimed

    async def xpending_range(self, _stream, _group, start, _end, _count):
        pending = self.pending.get(start)
        if pending is None:
            return []
        result = [{
            "consumer": pending["consumer"],
            "times_delivered": pending["times_delivered"],
        }]
        if self.transfer_owner_after_check == start:
            pending["consumer"] = "replacement-worker"
            self.transfer_owner_after_check = None
        return result

    async def xack(self, stream, _group, message_id):
        self.acked.append((stream, message_id))
        self.pending.pop(message_id, None)

    async def xinfo_groups(self, _stream):
        return [{"name": "globex-workers", "lag": 0}]

    async def eval(self, script, numkeys, *values):
        self.eval_calls.append(script)
        keys = values[:numkeys]
        args = values[numkeys:]
        stream = keys[0]
        message_id = args[1]
        consumer = args[2]
        pending = self.pending.get(message_id)
        if self.transfer_owner_after_check == message_id and pending is not None:
            pending["consumer"] = "replacement-worker"
            self.transfer_owner_after_check = None
        if pending is None or pending["consumer"] != consumer:
            return 0
        if "XACK" in script:
            self.acked.append((stream, message_id))
            self.pending.pop(message_id, None)
            return 1
        if "XCLAIM" in script:
            pending["idle_ms"] = 0
            self.heartbeat_calls.append((stream, message_id))
            return 1
        if "SET" in script:
            self.kv[keys[1]] = args[3]
            return 1
        raise AssertionError(f"unknown Redis Lua script: {script}")

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.kv:
            return False
        self.set_calls.append(key)
        self.kv[key] = value
        return True

    async def get(self, key):
        return self.kv.get(key)

    def add_pending(self, stream: str, task: IntentTask, *, consumer="old", idle_ms=120000):
        message_id = f"{len(self.entries) + 1}-0"
        fields = {"payload": json.dumps(task.to_dict())}
        self.entries.append((stream, message_id, fields))
        self.pending[message_id] = {
            "consumer": consumer,
            "times_delivered": 1,
            "idle_ms": idle_ms,
        }
        return message_id


def task(task_id: str, priority: int = 0) -> IntentTask:
    return IntentTask(
        task_id=task_id,
        shopping_session_id="session-1",
        buyer_id="buyer-1",
        locale="zh-CN",
        currency="CNY",
        raw_query="露营灯",
        priority=priority,
        approval_id="approval-1",
    )


async def test_task_and_status_owner_fields_roundtrip():
    restored = IntentTask.from_dict(json.loads(json.dumps(task("t1").to_dict())))
    assert restored.approval_id == "approval-1"

    client = RecoveryRedis()
    queue = RedisStreamTaskQueue(client)
    await queue.set_status(TaskStatus(task_id="t1", state="queued", buyer_id="buyer-1"))
    status = await queue.get_status("t1")
    assert status is not None and status.buyer_id == "buyer-1"


async def test_consume_recovers_stale_messages_from_both_streams_with_original_ids():
    client = RecoveryRedis()
    normal_id = client.add_pending(_STREAM, task("normal"))
    large_id = client.add_pending(_LARGE_STREAM, task("large", priority=1))
    queue = RedisStreamTaskQueue(client)
    started = asyncio.Event()
    release = asyncio.Event()
    handled: list[str] = []
    stop = asyncio.Event()

    async def handler(item: IntentTask) -> None:
        handled.append(item.task_id)
        if len(handled) == 2:
            started.set()
        await release.wait()

    consume_task = asyncio.create_task(
        queue.consume("new-worker", handler, stop.is_set, block_ms=0, concurrency=2),
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    stop.set()
    release.set()
    await asyncio.wait_for(consume_task, timeout=1)

    assert set(handled) == {"normal", "large"}
    assert set(client.claim_calls) == {_STREAM, _LARGE_STREAM}
    assert set(client.acked) == {(_STREAM, normal_id), (_LARGE_STREAM, large_id)}


async def test_recovery_cursor_reaches_stale_pending_after_active_prefix():
    client = RecoveryRedis()
    for idx in range(4):
        client.add_pending(_STREAM, task(f"active-{idx}"), idle_ms=0)
    stale_id = client.add_pending(_STREAM, task("stale"), idle_ms=120000)
    queue = RedisStreamTaskQueue(client)
    handled: list[str] = []

    async def handler(item: IntentTask) -> None:
        handled.append(item.task_id)

    await asyncio.wait_for(
        queue.consume("new-worker", handler, lambda: bool(handled), block_ms=0, concurrency=1),
        timeout=2,
    )

    assert handled == ["stale"]
    assert (_STREAM, stale_id) in client.acked
    assert any(stream == _STREAM and start_id != "0-0" for stream, start_id in client.claim_start_ids)


async def test_reading_two_streams_never_exceeds_total_concurrency():
    client = RecoveryRedis()
    queue = RedisStreamTaskQueue(client)
    for idx in range(2):
        await queue.enqueue(task(f"normal-{idx}"))
        await queue.enqueue(task(f"large-{idx}", priority=1))

    active = 0
    maximum = 0
    completed = 0

    async def handler(_item: IntentTask) -> None:
        nonlocal active, maximum, completed
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        completed += 1

    def should_stop() -> bool:
        return completed == 4

    await asyncio.wait_for(
        queue.consume("worker", handler, should_stop, block_ms=1, concurrency=2),
        timeout=2,
    )
    assert completed == 4
    assert maximum <= 2


async def test_same_worker_does_not_reschedule_its_active_pending_message():
    client = RecoveryRedis()
    queue = RedisStreamTaskQueue(client)
    await queue.enqueue(task("one-active-copy"))
    entered = asyncio.Event()
    release = asyncio.Event()
    stop = asyncio.Event()
    calls = 0

    async def handler(_item: IntentTask) -> None:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()

    consume_task = asyncio.create_task(
        queue.consume(
            "worker", handler, stop.is_set, block_ms=0, concurrency=2,
            claim_idle_ms=0, heartbeat_interval_ms=1000,
        ),
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.sleep(0.05)
    stop.set()
    release.set()
    await asyncio.wait_for(consume_task, timeout=1)

    assert calls == 1


async def test_long_handler_renews_pending_idle_time():
    client = RecoveryRedis()
    queue = RedisStreamTaskQueue(client)
    await queue.enqueue(task("long"))
    completed = False

    async def handler(_item: IntentTask) -> None:
        nonlocal completed
        await asyncio.sleep(0.08)
        completed = True

    await asyncio.wait_for(
        queue.consume(
            "worker", handler, lambda: completed, block_ms=1, concurrency=1,
            claim_idle_ms=30, heartbeat_interval_ms=5,
        ),
        timeout=2,
    )
    assert client.heartbeat_calls


async def test_lost_pending_owner_cannot_ack_or_overwrite_status():
    client = RecoveryRedis()
    queue = RedisStreamTaskQueue(client)
    await queue.enqueue(task("owned"))
    completed = False

    async def handler(item: IntentTask) -> None:
        nonlocal completed
        message_id = client.entries[0][1]
        client.pending[message_id]["consumer"] = "replacement-worker"
        await queue.set_status(
            TaskStatus(task_id=item.task_id, state="done", buyer_id=item.buyer_id),
        )
        completed = True

    await asyncio.wait_for(
        queue.consume("worker", handler, lambda: completed, block_ms=0, concurrency=1),
        timeout=2,
    )
    assert client.acked == []
    assert await queue.get_status("owned") is None


async def test_owner_transition_between_read_and_ack_cannot_ack_stale_message():
    client = RecoveryRedis()
    message_id = client.add_pending(_STREAM, task("ack-race"), consumer="worker")
    client.transfer_owner_after_check = message_id
    queue = RedisStreamTaskQueue(client)

    acknowledged = await queue._ack_if_owned(_STREAM, message_id, "worker")

    assert not acknowledged
    assert client.acked == []
    assert client.pending[message_id]["consumer"] == "replacement-worker"


async def test_owner_transition_between_read_and_status_write_cannot_write_stale_status():
    client = RecoveryRedis()
    message_id = client.add_pending(_STREAM, task("status-race"), consumer="worker")
    client.transfer_owner_after_check = message_id
    queue = RedisStreamTaskQueue(client)

    async def handler(item: IntentTask) -> None:
        await queue.set_status(
            TaskStatus(task_id=item.task_id, state="done", buyer_id=item.buyer_id),
        )

    fields = {"payload": json.dumps(task("status-race").to_dict())}
    await queue._handle_one(
        _STREAM, message_id, fields, handler, max_deliveries=3,
        consumer_name="worker",
    )

    assert await queue.get_status("status-race") is None
    assert client.pending[message_id]["consumer"] == "replacement-worker"


async def test_owner_checked_status_write_is_single_atomic_action():
    client = RecoveryRedis()
    message_id = client.add_pending(_STREAM, task("atomic-status"), consumer="worker")
    queue = RedisStreamTaskQueue(client)

    async def handler(item: IntentTask) -> None:
        await queue.set_status(
            TaskStatus(task_id=item.task_id, state="done", buyer_id=item.buyer_id),
        )

    await queue._handle_one(
        _STREAM,
        message_id,
        {"payload": json.dumps(task("atomic-status").to_dict())},
        handler,
        max_deliveries=3,
        consumer_name="worker",
    )

    status = await queue.get_status("atomic-status")
    assert status is not None and status.state == "done"
    assert client.set_calls == [], "owned status writes must not fall through to a separate SET"
    assert len(client.eval_calls) == 2


async def test_heartbeat_cannot_reclaim_pending_message_after_owner_changes():
    client = RecoveryRedis()
    message_id = client.add_pending(_STREAM, task("heartbeat-race"), consumer="replacement-worker")
    queue = RedisStreamTaskQueue(client)

    await asyncio.wait_for(
        queue._heartbeat(_STREAM, message_id, "old-worker", interval_seconds=0.001),
        timeout=0.1,
    )

    assert client.pending[message_id]["consumer"] == "replacement-worker"
    assert client.heartbeat_calls == []


async def test_owner_checked_ack_fails_closed_without_redis_eval_adapter():
    client = RecoveryRedis()
    message_id = client.add_pending(_STREAM, task("no-eval"), consumer="worker")
    queue = RedisStreamTaskQueue(client)
    client.eval = None

    with pytest.raises(TypeError):
        await queue._ack_if_owned(_STREAM, message_id, "worker")

    assert client.acked == []


async def test_dead_letter_also_sets_terminal_failed_status():
    client = RecoveryRedis()
    queue = RedisStreamTaskQueue(client)

    async def fail(_item: IntentTask) -> None:
        raise RuntimeError("provider unavailable")

    stream = _LARGE_STREAM
    message_id = client.add_pending(stream, task("dead", priority=1))
    await queue._handle_one(
        stream, message_id, {"payload": json.dumps(task("dead", priority=1).to_dict())},
        fail, max_deliveries=1,
    )
    status = await queue.get_status("dead")
    assert status is not None
    assert status.state == "failed"
    assert status.buyer_id == "buyer-1"
    assert "provider unavailable" in status.error
    assert client.acked == [(stream, message_id)]


async def test_worker_passes_recovery_context_and_marks_error_results_failed():
    from types import SimpleNamespace

    from app import worker

    class Queue:
        def __init__(self):
            self.statuses = []

        async def set_status(self, status):
            self.statuses.append(status)

    class Orchestrator:
        def __init__(self, result):
            self.result = result
            self.input = None

        async def handle_intent(self, submit_input):
            self.input = submit_input
            return self.result

    queue = Queue()
    orchestrator = Orchestrator(SimpleNamespace(final_text="已完成"))
    container = SimpleNamespace(task_queue=queue, bus=SimpleNamespace(publish=lambda *_: None),
                                orchestrator=orchestrator)
    handler = worker._build_handler(container, {"count": 0})
    await handler(task("worker-task"))

    assert orchestrator.input.task_id == "worker-task"
    assert orchestrator.input.approval_id == "approval-1"
    assert queue.statuses[-1].state == "done"
    assert queue.statuses[-1].buyer_id == "buyer-1"

    for final_text in ("[error] model failed", ""):
        queue = Queue()
        container = SimpleNamespace(
            task_queue=queue,
            bus=SimpleNamespace(publish=lambda *_: None),
            orchestrator=Orchestrator(SimpleNamespace(final_text=final_text)),
        )
        handler = worker._build_handler(container, {"count": 0})
        with pytest.raises(RuntimeError):
            await handler(task(f"failed-{len(final_text)}"))
        assert queue.statuses[-1].state == "retrying"
        assert queue.statuses[-1].buyer_id == "buyer-1"
