"""WebSocket authentication and tenant-scoped session subscription tests."""
from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.security.auth_identity import AuthDirectory

ALICE_TOKEN = "a" * 40
BOB_TOKEN = "b" * 40
TOKENS = json.dumps({
    ALICE_TOKEN: {
        "tenant_id": "tenant-a",
        "subject": "alice",
        "permissions": ["intent:submit", "commerce:read", "commerce:write"],
    },
    BOB_TOKEN: {
        "tenant_id": "tenant-b",
        "subject": "bob",
        "permissions": ["intent:submit", "commerce:read", "commerce:write"],
    },
})


@pytest.fixture
def client(monkeypatch, tmp_path):
    from app.infrastructure import settings

    configuration = SimpleNamespace(cors_origins=[], auth_tokens_json=TOKENS, data_dir=tmp_path)
    monkeypatch.setattr(settings, "load_settings", lambda: configuration)
    server = importlib.import_module("app.presentation.server")

    async def noop():
        return None

    async def intent(value):
        return SimpleNamespace(shopping_session_id=value.shopping_session_id, final_text="fixture result")

    container = SimpleNamespace(
        settings=configuration,
        bus=TradeEventBus(),
        backplane=None,
        task_queue=None,
        orchestrator=SimpleNamespace(handle_intent=intent),
        startup=noop,
        shutdown=noop,
    )

    async def build():
        return container

    monkeypatch.setattr(server, "build_container", build)
    with TestClient(server.build_app()) as session:
        yield session


def test_websocket_rejects_missing_access_token(client):
    with client.websocket_connect("/commerce/events") as websocket:
        websocket.send_json({"shopping_session_id": "session-a"})
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_json()

    assert closed.value.code == 4403


def test_websocket_rejects_foreign_tenant_scoped_session(client):
    alice = AuthDirectory.from_json(TOKENS).authenticate(ALICE_TOKEN)
    alice_session = alice.scope_session("shared-session")

    with client.websocket_connect("/commerce/events") as websocket:
        websocket.send_json({
            "access_token": BOB_TOKEN,
            "shopping_session_id": alice_session,
        })
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_json()

    assert closed.value.code == 4403
