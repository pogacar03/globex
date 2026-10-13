import asyncio
import json
from app.infrastructure.security.auth_identity import AuthDirectory
from app.infrastructure.eventbus import TradeEventBus
from app.presentation.connection import ConnectionManager


async def test_disconnected_idle_socket_releases_subscription_without_new_events():
    token = "x" * 40
    auth = AuthDirectory.from_json(json.dumps({token: {"tenant_id": "fixture", "subject": "alice", "permissions": ["commerce:read"]}}))
    bus = TradeEventBus()
    class Socket:
        async def accept(self): pass
        async def receive_json(self):
            return {"access_token": token, "shopping_session_id": "fixture"}
        async def receive(self):
            return {"type": "websocket.disconnect"}
        async def send_json(self, value): pass
        async def close(self, **kwargs): pass
    serving = asyncio.create_task(ConnectionManager(bus, auth).serve(Socket()))
    try:
        completed, _ = await asyncio.wait({serving}, timeout=.3)
        assert serving in completed, "Disconnect must finish serving without cancellation or another event"
        await serving
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
    assert bus._subscribers == {}
