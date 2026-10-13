"""HTTP authority boundaries; all dependencies are local and no LLM is contacted."""
import importlib
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.infrastructure.eventbus import TradeEventBus

ALICE_TOKEN = "a" * 40
BOB_TOKEN = "b" * 40
TOKENS = json.dumps({
    ALICE_TOKEN: {"tenant_id": "tenant-a", "subject": "alice", "permissions": ["intent:submit", "commerce:read", "commerce:write"]},
    BOB_TOKEN: {"tenant_id": "tenant-b", "subject": "bob", "permissions": ["intent:submit", "commerce:read", "commerce:write"]},
})


@pytest.fixture
def client(monkeypatch, tmp_path):
    from app.infrastructure import settings
    configuration = SimpleNamespace(cors_origins=[], auth_tokens_json=TOKENS, data_dir=tmp_path, llm_model="fixture")
    monkeypatch.setattr(settings, "load_settings", lambda: configuration)
    server = importlib.import_module("app.presentation.server")

    async def noop():
        return None

    async def intent(value):
        return SimpleNamespace(shopping_session_id=value.shopping_session_id, final_text="SYNTHETIC fixture result")

    async def query(order_id):
        return {"order_id": order_id}

    container = SimpleNamespace(settings=configuration, bus=TradeEventBus(), backplane=None,
        task_queue=None, orchestrator=SimpleNamespace(handle_intent=intent),
        query_order=SimpleNamespace(execute=query), startup=noop, shutdown=noop, db_engine=None,
        cache=SimpleNamespace(enabled=False), semantic_cache=SimpleNamespace(enabled=False))

    async def build():
        return container
    monkeypatch.setattr(server, "build_container", build)
    with TestClient(server.build_app()) as session:
        yield session


def test_unauthenticated_intent_is_rejected(client):
    response = client.post("/commerce/intents", json={"buyer_id": "alice", "raw_query": "露营灯"})
    assert response.status_code == 401


def test_buyer_override_does_not_change_authority(client):
    response = client.post("/commerce/intents", headers={"Authorization": f"Bearer {ALICE_TOKEN}"},
        json={"buyer_id": "bob", "raw_query": "露营灯"})
    assert response.status_code == 403


def test_foreign_scoped_session_cannot_be_reused(client):
    first = client.post("/commerce/intents", headers={"Authorization": f"Bearer {ALICE_TOKEN}"},
        json={"buyer_id": "alice", "raw_query": "露营灯"})
    assert first.status_code == 200
    session_id = first.json()["shopping_session_id"]
    second = client.post("/commerce/intents", headers={"Authorization": f"Bearer {BOB_TOKEN}"},
        json={"buyer_id": "bob", "shopping_session_id": session_id, "raw_query": "继续"})
    assert second.status_code == 403


def test_order_lookup_requires_authentication(client):
    assert client.get("/commerce/orders/GBX-000001").status_code == 401


def test_idempotency_key_also_stabilizes_an_omitted_session(client):
    headers = {"Authorization": f"Bearer {ALICE_TOKEN}", "Idempotency-Key": "fixture-retry"}
    responses = [client.post("/commerce/intents", headers=headers, json={"raw_query": "露营灯"}) for _ in range(2)]
    assert all(response.status_code == 200 for response in responses)
    assert responses[0].json()["shopping_session_id"] == responses[1].json()["shopping_session_id"]


def test_health_reports_required_database_failure_without_exposing_error(client):
    import asyncio
    from app.presentation import server
    container = asyncio.run(server.build_container())
    class BrokenEngine:
        def connect(self):
            raise RuntimeError("fixture sensitive connection details")
    container.db_engine = BrokenEngine()
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["status"] == "degraded"
    assert response.json()["database"] == "error"
    assert "sensitive" not in response.text
