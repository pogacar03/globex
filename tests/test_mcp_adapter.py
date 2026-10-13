import json
import httpx
import pytest


async def test_mcp_uses_workspace_credential_not_model_supplied_identity():
    from app.presentation.mcp_server import CommerceMCPAdapter
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"task_id": "fixture", "state": "done"})
    async with httpx.AsyncClient(base_url="http://api.test", transport=httpx.MockTransport(respond)) as client:
        adapter = CommerceMCPAdapter(client, "fixture-secret")
        await adapter.task_status("fixture")
    assert requests[0].headers["Authorization"] == "Bearer fixture-secret"
    assert str(requests[0].url) == "http://api.test/commerce/tasks/fixture"


async def test_mcp_rejects_path_injection_and_surfaces_failed_api():
    from app.presentation.mcp_server import CommerceMCPAdapter
    async with httpx.AsyncClient(base_url="http://api.test", transport=httpx.MockTransport(
        lambda request: httpx.Response(403, json={"detail": "PERMISSION_DENIED"}))) as client:
        adapter = CommerceMCPAdapter(client, "fixture-secret")
        with pytest.raises(ValueError):
            await adapter.task_status("../../orders")
        with pytest.raises(httpx.HTTPStatusError):
            await adapter.task_status("fixture")


async def test_mcp_research_uses_async_endpoint_and_returns_task_handle():
    from app.presentation.mcp_server import CommerceMCPAdapter
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={
            "task_id": "task-123", "state": "queued", "shopping_session_id": "session-1",
        })

    async with httpx.AsyncClient(base_url="http://api.test", transport=httpx.MockTransport(respond)) as client:
        adapter = CommerceMCPAdapter(client, "fixture-secret")
        result = await adapter.research("Find a quiet headset", session_id="session-1")

    assert result["task_id"] == "task-123"
    assert result["state"] == "queued"
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/commerce/intents/async"
    assert requests[0].headers["Authorization"] == "Bearer fixture-secret"
    assert json.loads(requests[0].content) == {
        "raw_query": "Find a quiet headset", "shopping_session_id": "session-1",
    }


async def test_mcp_research_surfaces_queue_disabled_instead_of_falling_back_to_sync():
    from app.presentation.mcp_server import CommerceMCPAdapter
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(503, json={"detail": "queue disabled; use synchronous API"})

    async with httpx.AsyncClient(base_url="http://api.test", transport=httpx.MockTransport(respond)) as client:
        adapter = CommerceMCPAdapter(client, "fixture-secret")
        with pytest.raises(httpx.HTTPStatusError) as raised:
            await adapter.research("Find a quiet headset")

    assert raised.value.response.status_code == 503
    assert len(requests) == 1
    assert requests[0].url.path == "/commerce/intents/async"


def test_mcp_client_timeout_is_bounded_for_async_task_submission(monkeypatch):
    import app.presentation.mcp_server as mcp_server

    observed = {}

    class FakeClient:
        def __init__(self, **kwargs):
            observed.update(kwargs)

    class FakeServer:
        def run(self, transport):
            observed["transport"] = transport

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(mcp_server, "build_mcp_server", lambda _adapter: FakeServer())
    monkeypatch.setenv("GLOBEX_API_URL", "http://api.test")
    monkeypatch.setenv("GLOBEX_ACCESS_TOKEN", "fixture-secret")

    mcp_server.main()

    assert observed["base_url"] == "http://api.test"
    assert observed["timeout"] == 30.0
    assert observed["transport"] == "stdio"


def test_mcp_does_not_expose_human_approval_as_an_agent_tool():
    from app.presentation.mcp_server import build_mcp_server, CommerceMCPAdapter
    client = httpx.AsyncClient(base_url="http://api.test")
    server = build_mcp_server(CommerceMCPAdapter(client, "fixture-secret"))
    assert "approve_action" not in server._tool_manager._tools
    assert set(server._tool_manager._tools) == {"research", "task_status", "order_status"}
