import json
from types import SimpleNamespace
from agentscope.message import ToolResultState
from app.infrastructure.context import ShoppingContext, ShoppingContextSnapshot
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.security.approval_store import ApprovalStore
from app.infrastructure.persistence.in_memory_repositories import InMemoryOrderRepository, InMemoryProductRepository
from app.application.usecases.order_usecases import PlaceOrderUseCase
from app.application.tools.order_tools import build_create_order_tool

ARGS = {"items": [{"product_id": "P1001", "sku_id": "P1001-S1", "quantity": 1}],
    "shipping_address": {"recipient_name": "Fixture", "country": "CN", "city": "HZ",
        "state": "ZJ", "address_line": "SYNTHETIC address", "postal_code": "00000", "phone": "00000"}}


async def test_unapproved_write_has_no_effect_and_receipt_replay_does_not_duplicate(tmp_path):
    from app.infrastructure.security.write_governance import WriteApprovalMiddleware
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    bus, orders = TradeEventBus(), InMemoryOrderRepository()
    actual_tool = build_create_order_tool(PlaceOrderUseCase(InMemoryProductRepository(), orders), bus)
    middleware = WriteApprovalMiddleware(store, bus)
    async def invoke(**arguments):
        yield await actual_tool(**arguments)
    context = ShoppingContextSnapshot("session-a", "owner-a", "zh-CN", "CNY", permissions=("commerce:write",))
    token = ShoppingContext.set(context)
    try:
        chunks = [chunk async for chunk in middleware.on_tool_call(SimpleNamespace(name="create_order_tool"), ARGS, invoke)]
        proposal = json.loads(chunks[-1].content[0].text)
        assert proposal["decision"] == "REQUIRE_APPROVAL"
        assert orders._orders == {}
        await store.approve("owner-a", proposal["approval_id"])
    finally:
        ShoppingContext.reset(token)
    token = ShoppingContext.set(ShoppingContextSnapshot("session-a", "owner-a", "zh-CN", "CNY",
        approval_id=proposal["approval_id"], permissions=("commerce:write",)))
    try:
        first = [chunk async for chunk in middleware.on_tool_call(SimpleNamespace(name="create_order_tool"), ARGS, invoke)]
        repeated = [chunk async for chunk in middleware.on_tool_call(SimpleNamespace(name="create_order_tool"), ARGS, invoke)]
        assert first[-1].state == ToolResultState.SUCCESS
        assert repeated[-1].state == ToolResultState.SUCCESS
        assert len(orders._orders) == 1
        assert json.loads(first[-1].content[0].text)["order_id"] == json.loads(repeated[-1].content[0].text)["order_id"]
    finally:
        ShoppingContext.reset(token)
