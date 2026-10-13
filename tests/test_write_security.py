from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest
from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import ToolChunk

from app.application.tools.order_tools import (
    build_cancel_order_tool,
    build_create_order_tool,
    build_query_order_tool,
)
from app.infrastructure.context import ShoppingContext, ShoppingContextSnapshot
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.security.approval_store import ApprovalStore


def _middleware_type():
    try:
        module = importlib.import_module("app.infrastructure.security.write_governance")
    except ModuleNotFoundError:
        pytest.fail("WriteApprovalMiddleware is not implemented")
    return module.WriteApprovalMiddleware


def _context(
    *,
    owner: str = "owner-a",
    session: str = "session-a",
    approval_id: str | None = None,
    permissions: tuple[str, ...] = ("commerce:write",),
) -> ShoppingContextSnapshot:
    return ShoppingContextSnapshot(
        session,
        owner,
        "zh-CN",
        "CNY",
        approval_id=approval_id,
        permissions=permissions,
    )


async def _yield_chunk(**_arguments):
    yield ToolChunk(
        content=[TextBlock(type="text", text='{"order_id":"GBX-000001"}')],
        state=ToolResultState.SUCCESS,
    )


async def _invoke(middleware, tool_name, arguments, handler=_yield_chunk):
    return [
        chunk
        async for chunk in middleware.on_tool_call(
            SimpleNamespace(name=tool_name), arguments, handler,
        )
    ]


@pytest.mark.asyncio
async def test_write_proposal_publishes_flat_approval_event(tmp_path):
    WriteApprovalMiddleware = _middleware_type()
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    bus = TradeEventBus()
    events = bus.subscribe("session-a")
    middleware = WriteApprovalMiddleware(store, bus)
    arguments = {"order_id": "GBX-000001", "reason": "buyer request"}
    token = ShoppingContext.set(_context())
    try:
        chunks = await _invoke(middleware, "cancel_order_tool", arguments)
    finally:
        ShoppingContext.reset(token)

    result = json.loads(chunks[-1].content[0].text)
    event = events.get_nowait()
    assert result["decision"] == "REQUIRE_APPROVAL"
    assert event.type == "approval.required"
    assert event.payload["decision"] == "REQUIRE_APPROVAL"
    assert event.payload["tool"] == "cancel_order_tool"
    assert event.payload["arguments"] == arguments
    assert event.payload["approval_id"] == result["approval_id"]
    assert event.payload["expires_at"] > 0


@pytest.mark.asyncio
async def test_write_without_trusted_permission_is_denied_before_handler(tmp_path):
    WriteApprovalMiddleware = _middleware_type()
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    middleware = WriteApprovalMiddleware(store, TradeEventBus())
    called = False

    async def handler(**_arguments):
        nonlocal called
        called = True
        async for chunk in _yield_chunk():
            yield chunk

    token = ShoppingContext.set(_context(permissions=("commerce:read",)))
    try:
        chunks = await _invoke(
            middleware,
            "create_order_tool",
            {"items": [], "shipping_address": {}},
            handler,
        )
    finally:
        ShoppingContext.reset(token)

    assert chunks[-1].state == ToolResultState.ERROR
    assert called is False


@pytest.mark.asyncio
async def test_write_without_context_is_denied_before_handler(tmp_path):
    WriteApprovalMiddleware = _middleware_type()
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    middleware = WriteApprovalMiddleware(store, TradeEventBus())
    called = False

    async def handler(**_arguments):
        nonlocal called
        called = True
        async for chunk in _yield_chunk():
            yield chunk

    chunks = await _invoke(middleware, "cancel_order_tool", {"order_id": "x", "reason": "r"}, handler)

    assert chunks[-1].state == ToolResultState.ERROR
    assert called is False


@pytest.mark.asyncio
async def test_approved_arguments_and_owner_are_bound_to_tool_call(tmp_path):
    WriteApprovalMiddleware = _middleware_type()
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    bus = TradeEventBus()
    middleware = WriteApprovalMiddleware(store, bus)
    arguments = {"order_id": "GBX-000001", "reason": "buyer request"}
    proposal = await store.propose("owner-a", "session-a", "cancel_order_tool", arguments)
    await store.approve("owner-a", proposal["approval_id"])
    called = False

    async def handler(**_arguments):
        nonlocal called
        called = True
        async for chunk in _yield_chunk():
            yield chunk

    token = ShoppingContext.set(_context(owner="owner-b", approval_id=proposal["approval_id"]))
    try:
        foreign = await _invoke(middleware, "cancel_order_tool", arguments, handler)
    finally:
        ShoppingContext.reset(token)
    token = ShoppingContext.set(_context(approval_id=proposal["approval_id"]))
    try:
        changed = await _invoke(
            middleware,
            "cancel_order_tool",
            {**arguments, "reason": "changed by model"},
            handler,
        )
    finally:
        ShoppingContext.reset(token)

    assert json.loads(foreign[-1].content[0].text)["decision"] == "DENY"
    assert json.loads(changed[-1].content[0].text)["decision"] == "DENY"
    assert called is False


@pytest.mark.asyncio
async def test_uncertain_write_is_marked_and_cannot_be_replayed(tmp_path):
    WriteApprovalMiddleware = _middleware_type()
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    bus = TradeEventBus()
    middleware = WriteApprovalMiddleware(store, bus)
    arguments = {"items": [], "shipping_address": {}}
    proposal = await store.propose("owner-a", "session-a", "create_order_tool", arguments)
    await store.approve("owner-a", proposal["approval_id"])

    async def fail_after_claim(**_arguments):
        raise RuntimeError("simulated uncertain side effect")
        yield  # pragma: no cover

    token = ShoppingContext.set(_context(approval_id=proposal["approval_id"]))
    try:
        first = await _invoke(middleware, "create_order_tool", arguments, fail_after_claim)
        retry = await _invoke(middleware, "create_order_tool", arguments)
    finally:
        ShoppingContext.reset(token)

    assert first[-1].state == ToolResultState.ERROR
    assert json.loads(retry[-1].content[0].text)["decision"] == "RECONCILIATION_REQUIRED"


class _OwnedQuery:
    def __init__(self):
        self.calls = []

    async def execute_owned(self, order_id, buyer_id):
        self.calls.append((order_id, buyer_id))
        return {"order_id": order_id, "buyer_id": buyer_id}

    async def execute(self, _order_id):
        raise AssertionError("unowned query must not be used")


class _OwnedCancel:
    def __init__(self):
        self.calls = []

    async def execute_owned(self, order_id, reason, buyer_id):
        self.calls.append((order_id, reason, buyer_id))
        return {"order_id": order_id, "buyer_id": buyer_id, "status": "CANCELLED"}

    async def execute(self, _order_id, _reason):
        raise AssertionError("unowned cancellation must not be used")


@pytest.mark.asyncio
async def test_order_tools_use_owned_query_and_cancel_with_context_buyer():
    bus = TradeEventBus()
    query_usecase, cancel_usecase = _OwnedQuery(), _OwnedCancel()
    query = build_query_order_tool(query_usecase, bus)
    cancel = build_cancel_order_tool(cancel_usecase, bus)
    token = ShoppingContext.set(_context(owner="buyer-scoped"))
    try:
        query_result = await query(order_id="GBX-000001")
        cancel_result = await cancel(order_id="GBX-000001", reason="buyer request")
    finally:
        ShoppingContext.reset(token)

    assert query_usecase.calls == [("GBX-000001", "buyer-scoped")]
    assert cancel_usecase.calls == [("GBX-000001", "buyer request", "buyer-scoped")]
    assert json.loads(query_result.content[0].text)["buyer_id"] == "buyer-scoped"
    assert json.loads(cancel_result.content[0].text)["buyer_id"] == "buyer-scoped"


@pytest.mark.asyncio
async def test_order_tools_reject_missing_context_without_unowned_fallback():
    bus = TradeEventBus()
    query_usecase, cancel_usecase = _OwnedQuery(), _OwnedCancel()
    query = build_query_order_tool(query_usecase, bus)
    cancel = build_cancel_order_tool(cancel_usecase, bus)

    query_result = await query(order_id="GBX-000001")
    cancel_result = await cancel(order_id="GBX-000001", reason="buyer request")

    assert query_result.content[0].text.startswith("[error]")
    assert cancel_result.content[0].text.startswith("[error]")
    assert query_usecase.calls == []
    assert cancel_usecase.calls == []


@pytest.mark.asyncio
async def test_create_order_tool_rejects_missing_context_instead_of_using_anonymous():
    class PlaceOrder:
        def __init__(self):
            self.called = False

        async def execute(self, **_arguments):
            self.called = True
            return {"order_id": "GBX-000001"}

    usecase = PlaceOrder()
    tool = build_create_order_tool(usecase, TradeEventBus())

    result = await tool(
        items=[{"product_id": "P1001", "sku_id": "P1001-S1", "quantity": 1}],
        shipping_address={
            "recipient_name": "Fixture",
            "country": "CN",
            "city": "HZ",
            "state": "ZJ",
            "address_line": "SYNTHETIC address",
            "postal_code": "00000",
            "phone": "00000",
        },
    )

    assert result.content[0].text.startswith("[error]")
    assert usecase.called is False


@pytest.mark.asyncio
async def test_trade_factory_attaches_fail_closed_write_middleware_when_store_is_absent():
    from app.application.agents.trade_agent import TradeAgentFactory
    from app.infrastructure.resilience import CircuitBreakerRegistry
    from app.infrastructure.throttle import GatewayThrottle

    class PlaceOrder:
        def __init__(self):
            self.called = False

        async def execute(self, **_arguments):
            self.called = True
            return {"order_id": "GBX-000001"}

    place_order = PlaceOrder()
    factory = TradeAgentFactory(
        settings=object(),
        place_order=place_order,
        query_order=_OwnedQuery(),
        cancel_order=_OwnedCancel(),
        bus=TradeEventBus(),
        circuit_registry=CircuitBreakerRegistry(),
        throttle=GatewayThrottle(1, 0),
    )
    create_tool, query_tool, cancel_tool = factory.build_tools()
    token = ShoppingContext.set(_context())
    try:
        stream = await create_tool(
            items=[{"product_id": "P1001", "sku_id": "P1001-S1", "quantity": 1}],
            shipping_address={
                "recipient_name": "Fixture",
                "country": "CN",
                "city": "HZ",
                "state": "ZJ",
                "address_line": "SYNTHETIC address",
                "postal_code": "00000",
                "phone": "00000",
            },
        )
        chunks = [chunk async for chunk in stream]
    finally:
        ShoppingContext.reset(token)

    assert chunks[-1].state == ToolResultState.ERROR
    assert json.loads(chunks[-1].content[0].text)["decision"] == "DENY"
    assert place_order.called is False
    assert type(create_tool._middlewares[0]).__name__ == "WriteApprovalMiddleware"
    assert type(cancel_tool._middlewares[0]).__name__ == "WriteApprovalMiddleware"
    assert type(query_tool._middlewares[0]).__name__ != "WriteApprovalMiddleware"
