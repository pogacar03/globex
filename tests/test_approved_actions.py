"""Tests for executing durable order approvals without accepting model arguments."""
from __future__ import annotations

import pytest

from app.application.tools.order_tools import (
    build_cancel_order_tool,
    build_create_order_tool,
)
from app.application.usecases.order_usecases import (
    CancelOrderUseCase,
    OrderItemInput,
    PlaceOrderUseCase,
)
from app.domain.order.address import Address
from app.infrastructure.context import ShoppingContext
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.persistence.in_memory_repositories import (
    InMemoryOrderRepository,
    InMemoryProductRepository,
)
from app.infrastructure.security.approval_store import ApprovalStore
from app.infrastructure.security.approved_actions import (
    ApprovedActionError,
    ApprovedActionExecutor,
)


CREATE_ARGUMENTS = {
    "items": [{"product_id": "P1001", "sku_id": "P1001-S1", "quantity": 1}],
    "shipping_address": {
        "recipient_name": "Fixture",
        "country": "CN",
        "state": "ZJ",
        "city": "HZ",
        "address_line": "SYNTHETIC address",
        "postal_code": "00000",
        "phone": "00000",
    },
}


async def _create_fixture(store_path):
    store = ApprovalStore(store_path)
    bus = TradeEventBus()
    products = InMemoryProductRepository()
    orders = InMemoryOrderRepository()
    place_order = build_create_order_tool(PlaceOrderUseCase(products, orders), bus)
    cancel_order = build_cancel_order_tool(CancelOrderUseCase(products, orders), bus)
    executor = ApprovedActionExecutor(store, place_order, cancel_order, bus)
    return store, products, orders, bus, executor


@pytest.mark.asyncio
async def test_pending_order_approval_has_no_business_effect(tmp_path):
    store, products, orders, _, executor = await _create_fixture(tmp_path / "approvals.sqlite")
    proposal = await store.propose("owner-a", "session-a", "create_order_tool", CREATE_ARGUMENTS)

    with pytest.raises(ApprovedActionError) as error:
        await executor.execute("owner-a", proposal["approval_id"], ("commerce:write",))

    assert error.value.reason == "approval_required"
    assert error.value.status_code == 409
    assert orders._orders == {}
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 50


@pytest.mark.asyncio
async def test_approved_order_uses_stored_arguments_and_replay_is_idempotent(tmp_path):
    store, products, orders, _, executor = await _create_fixture(tmp_path / "approvals.sqlite")
    proposal = await store.propose("owner-a", "session-a", "create_order_tool", CREATE_ARGUMENTS)
    assert proposal["shopping_session_id"] == "session-a"
    await store.approve("owner-a", proposal["approval_id"])

    first = await executor.execute("owner-a", proposal["approval_id"], ("commerce:write",))
    second = await executor.execute("owner-a", proposal["approval_id"], ("commerce:write",))

    assert first["state"] == "completed"
    assert second == first
    assert len(orders._orders) == 1
    order = first["result"]
    assert order["buyer_id"] == "owner-a"
    assert order["lines"][0]["quantity"] == 1
    assert "SYNTHETIC address" in order["shipping_address"]
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 49


@pytest.mark.asyncio
async def test_foreign_owner_cannot_execute_an_approved_order(tmp_path):
    store, products, orders, _, executor = await _create_fixture(tmp_path / "approvals.sqlite")
    proposal = await store.propose("owner-a", "session-a", "create_order_tool", CREATE_ARGUMENTS)
    await store.approve("owner-a", proposal["approval_id"])

    with pytest.raises(ApprovedActionError) as error:
        await executor.execute("owner-b", proposal["approval_id"], ("commerce:write",))

    assert error.value.status_code == 404
    assert orders._orders == {}
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 50


@pytest.mark.asyncio
async def test_missing_write_permission_cannot_claim_or_execute_approval(tmp_path):
    store, products, orders, _, executor = await _create_fixture(tmp_path / "approvals.sqlite")
    proposal = await store.propose("owner-a", "session-a", "create_order_tool", CREATE_ARGUMENTS)
    await store.approve("owner-a", proposal["approval_id"])

    with pytest.raises(ApprovedActionError) as error:
        await executor.execute("owner-a", proposal["approval_id"], ())

    assert error.value.status_code == 403
    assert (await store.get("owner-a", proposal["approval_id"]))["state"] == "approved"
    assert orders._orders == {}
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 50


@pytest.mark.asyncio
async def test_cancel_approval_uses_owned_context_and_stored_arguments(tmp_path):
    store, products, orders, _, executor = await _create_fixture(tmp_path / "approvals.sqlite")
    preexisting = await PlaceOrderUseCase(products, orders).execute(
        "owner-a",
        [OrderItemInput("P1001", "P1001-S1", 1)],
        Address("Fixture", "CN", "ZJ", "HZ", "SYNTHETIC address", "00000", "00000"),
    )
    arguments = {"order_id": preexisting["order_id"], "reason": "buyer requested cancellation"}
    proposal = await store.propose("owner-a", "session-a", "cancel_order_tool", arguments)
    await store.approve("owner-a", proposal["approval_id"])

    result = await executor.execute("owner-a", proposal["approval_id"], ("commerce:write",))
    replay = await executor.execute("owner-a", proposal["approval_id"], ("commerce:write",))

    assert result["state"] == "completed"
    assert result["result"]["status"] == "CANCELLED"
    assert replay == result
    assert len(orders._orders) == 1
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 50
    assert ShoppingContext.current() is None


@pytest.mark.asyncio
async def test_uncertain_approval_requires_reconciliation_without_retrying_write(tmp_path):
    store, products, orders, _, executor = await _create_fixture(tmp_path / "approvals.sqlite")
    proposal = await store.propose("owner-a", "session-a", "create_order_tool", CREATE_ARGUMENTS)
    await store.approve("owner-a", proposal["approval_id"])
    permit = await store.begin(
        "owner-a", "session-a", "create_order_tool", CREATE_ARGUMENTS, proposal["approval_id"],
    )
    assert permit.decision == "ALLOW"
    await store.mark_uncertain(proposal["approval_id"])

    with pytest.raises(ApprovedActionError) as error:
        await executor.execute("owner-a", proposal["approval_id"], ("commerce:write",))

    assert error.value.reason == "reconciliation_required"
    assert error.value.status_code == 409
    assert orders._orders == {}
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 50
