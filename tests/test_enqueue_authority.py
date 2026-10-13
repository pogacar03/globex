import importlib
from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from app.application.agents.orchestrator import SubmitIntentInput
from app.infrastructure.eventbus import TradeEventBus


async def test_api_enqueues_stable_task_identity_once_without_cache_window(monkeypatch):
    from app.infrastructure import settings
    monkeypatch.setattr(settings, "load_settings", lambda: SimpleNamespace(cors_origins=[]))
    server = importlib.import_module("app.presentation.server")
    calls = []
    class Queue:
        async def enqueue_once(self, task, fingerprint):
            calls.append((task, fingerprint))
            return len(calls) == 1
    c = SimpleNamespace(task_queue=Queue(), settings=SimpleNamespace(queue_priority_enabled=False),
        bus=TradeEventBus())
    intent = SubmitIntentInput("fixture-session", "buyer-fixture", "zh-CN", "CNY", "露营灯",
        task_id="turn-fixture", permissions=("commerce:read",))
    assert await server._enqueue(c, intent) == "turn-fixture"
    assert await server._enqueue(c, intent) == "turn-fixture"
    assert calls[0][0].permissions == ("commerce:read",)
    assert calls[0][1] == calls[1][1]


async def test_idempotency_parameter_conflict_is_http_409(monkeypatch):
    from app.infrastructure import settings
    monkeypatch.setattr(settings, "load_settings", lambda: SimpleNamespace(cors_origins=[]))
    server = importlib.import_module("app.presentation.server")
    class Queue:
        async def enqueue_once(self, task, fingerprint):
            raise ValueError("input fingerprint conflict")
    c = SimpleNamespace(task_queue=Queue(), settings=SimpleNamespace(queue_priority_enabled=False), bus=TradeEventBus())
    with pytest.raises(HTTPException) as rejected:
        await server._enqueue(c, SubmitIntentInput("fixture", "buyer", "zh-CN", "CNY", "露营灯", task_id="turn-fixture"))
    assert rejected.value.status_code == 409


async def test_persistent_binding_rejects_conflict_before_redis_can_enqueue(monkeypatch, tmp_path):
    from app.infrastructure import settings
    from app.infrastructure.runtime_checkpoint import RuntimeCheckpointStore
    monkeypatch.setattr(settings, "load_settings", lambda: SimpleNamespace(cors_origins=[]))
    server = importlib.import_module("app.presentation.server")
    store = RuntimeCheckpointStore(tmp_path / "runtime.sqlite3")
    initial = SubmitIntentInput("fixture", "buyer", "zh-CN", "CNY", "露营灯", task_id="turn-fixture")
    lease = await store.acquire_lease("buyer", "fixture", "fixture-worker")
    await store.begin_turn(initial.task_id, initial.buyer_id, initial.shopping_session_id,
        initial.fingerprint(), lease.holder_id, lease.fencing_token)
    class Queue:
        async def enqueue_once(self, task, fingerprint):
            raise AssertionError("Persistent input conflict must not touch Redis")
    c = SimpleNamespace(task_queue=Queue(), runtime_checkpoints=store,
        settings=SimpleNamespace(queue_priority_enabled=False), bus=TradeEventBus())
    changed = SubmitIntentInput("fixture", "buyer", "zh-CN", "CNY", "不同的参数", task_id="turn-fixture")
    with pytest.raises(HTTPException) as rejected:
        await server._enqueue(c, changed)
    assert rejected.value.status_code == 409
