"""Deterministic, conservative task routing policy v1."""
from __future__ import annotations

import pytest

from app.infrastructure.task_routing import (
    RoutingDecision,
    SMALL_MODEL_CONFIDENCE_THRESHOLD,
    can_use_small_model,
    current_routing_decision,
    decide_route,
    reset_routing_decision,
    set_routing_decision,
)


def test_explicit_simple_single_item_search_routes_to_small_with_explanation():
    decision = decide_route("搜索一个蓝牙耳机")

    assert decision.route == "small"
    assert decision.confidence >= SMALL_MODEL_CONFIDENCE_THRESHOLD
    assert decision.reason == "simple_single_item_search"
    assert decision.policy_version == "v1"
    assert can_use_small_model(decision)


@pytest.mark.parametrize(
    ("query", "reason"),
    [
        ("比较 iPhone 和 Pixel 哪个更适合我", "comparison_or_multi_item"),
        ("搜索耳机和音箱", "comparison_or_multi_item"),
        ("推荐耳机，预算 300 美元并寄到美国", "complex_constraints"),
        ("购买一副耳机", "transaction_intent"),
    ],
)
def test_complex_comparison_multi_item_and_transaction_requests_stay_main(query, reason):
    decision = decide_route(query)

    assert decision.route == "main"
    assert decision.reason == reason
    assert not can_use_small_model(decision)


def test_long_search_stays_on_main():
    decision = decide_route("搜索" + "耳机" * 50)

    assert decision.route == "main"
    assert decision.reason == "long_input"


def test_unknown_intent_stays_on_main_with_low_confidence():
    decision = decide_route("帮我看看这个")

    assert decision.route == "main"
    assert decision.reason == "unrecognized_intent"
    assert decision.confidence < SMALL_MODEL_CONFIDENCE_THRESHOLD
    assert not can_use_small_model(decision)


def test_low_confidence_small_decision_is_never_eligible_for_small_model():
    decision = RoutingDecision(
        route="small",
        confidence=0.5,
        reason="uncertain",
        policy_version="v1",
    )

    assert not can_use_small_model(decision)


def test_routing_context_is_optional_and_resettable():
    assert current_routing_decision() is None
    decision = decide_route("Search for a laptop")

    token = set_routing_decision(decision)
    assert current_routing_decision() == decision

    reset_routing_decision(token)
    assert current_routing_decision() is None
