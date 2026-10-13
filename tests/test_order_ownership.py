import pytest
from app.application.usecases.order_usecases import PlaceOrderUseCase, QueryOrderUseCase, CancelOrderUseCase, OrderItemInput
from app.domain.order.address import Address
from app.infrastructure.persistence.in_memory_repositories import InMemoryProductRepository, InMemoryOrderRepository


async def fixture_order():
    products, orders = InMemoryProductRepository(), InMemoryOrderRepository()
    address = Address("Fixture", "CN", "ZJ", "HZ", "SYNTHETIC street", "000000", "000000")
    order = await PlaceOrderUseCase(products, orders).execute("owner-a",
        [OrderItemInput("P1001", "P1001-S1", 1)], address)
    return products, orders, order


async def test_cross_owner_query_hides_order():
    products, orders, order = await fixture_order()
    with pytest.raises(ValueError, match="订单不存在"):
        await QueryOrderUseCase(orders).execute_owned(order["order_id"], "owner-b")
    own = await QueryOrderUseCase(orders).execute_owned(order["order_id"], "owner-a")
    assert own["order_id"] == order["order_id"]


async def test_cross_owner_cancel_does_not_change_order_or_inventory():
    products, orders, order = await fixture_order()
    with pytest.raises(ValueError, match="订单不存在"):
        await CancelOrderUseCase(products, orders).execute_owned(order["order_id"], "fixture", "owner-b")
    assert (await orders.find_by_id(order["order_id"])).status.value == "CONFIRMED"
    assert (await products.find_by_id("P1001")).find_sku("P1001-S1").stock == 49
