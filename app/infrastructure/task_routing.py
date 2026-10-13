"""Deterministic, conservative task-routing policy v1.

This policy selects a model size only. It never grants tool or business
permissions; authorization remains the responsibility of the application.
"""
from __future__ import annotations

import re
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Literal


SMALL_MODEL_CONFIDENCE_THRESHOLD = 0.8
MAX_SMALL_INPUT_LENGTH = 96

_CHINESE_SEARCH_PREFIX = re.compile(
    r"^(?:(?:请|帮我|给我)\s*)*(?:搜索|搜|查找|查|找|检索|推荐)"
    r"(?:一下)?(?:一个|一款|一件|一台|单个|个|款)?\s*"
)
_ENGLISH_SEARCH_PREFIX = re.compile(
    r"^(?:please\s+)?(?:search(?:\s+for)?|find|look\s+up|recommend)\s+"
    r"(?:(?:a|an|one|single)\s+)?",
    re.IGNORECASE,
)
_TRANSACTION_INTENT = re.compile(
    r"下单|购买|买|付款|支付|结账|取消|退款|退货|换货|加购|加入购物车|订单|"
    r"\b(?:order|purchase|buy|checkout|cancel|refund|return|pay)\b",
    re.IGNORECASE,
)
_COMPARISON_INTENT = re.compile(
    r"比较|对比|相比|区别|差异|哪个更|哪一个|选哪个|哪个好|更好|最优|最差|"
    r"\b(?:compare|compares|compared|comparing|versus|vs\.?|which(?:\s+one)?|better|best|worst)\b",
    re.IGNORECASE,
)
_COMPLEX_CONSTRAINT = re.compile(
    r"预算|不超过|不高于|低于|以下|以上|至少|价格|价位|多少钱|便宜|最贵|最便宜|"
    r"运费|邮费|配送|运送|寄到|邮寄|发货|到手价|税费|关税|库存|现货|品牌|型号|"
    r"颜色|尺寸|重量|保修|要求|适合|续航|性能|"
    r"\b(?:budget|under|over|below|above|within|price|cost|shipping|delivery|"
    r"ship\s+to|deliver\s+to|tax|tariff|dut(?:y|ies)|stock|brand|model|color|"
    r"colour|size|weight|warranty|requirement|must)\b|\d|[$£€￥¥]",
    re.IGNORECASE,
)
_MULTIPLE_ITEMS = re.compile(
    r"和|与|及|以及|或者|还是|或|、|还有|另外|再加上|"
    r"\b(?:and|or|plus|along\s+with)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RoutingDecision:
    route: Literal["small", "main"]
    confidence: float
    reason: str
    policy_version: str = "v1"


_routing_decision: ContextVar[RoutingDecision | None] = ContextVar("task_routing_decision", default=None)


def decide_route(query: str) -> RoutingDecision:
    text = query.strip() if isinstance(query, str) else ""
    if not text:
        return RoutingDecision("main", 0.0, "empty_input")
    if len(text) > MAX_SMALL_INPUT_LENGTH:
        return RoutingDecision("main", 0.99, "long_input")
    if _TRANSACTION_INTENT.search(text):
        return RoutingDecision("main", 0.99, "transaction_intent")
    if _COMPARISON_INTENT.search(text) or _MULTIPLE_ITEMS.search(text):
        return RoutingDecision("main", 0.99, "comparison_or_multi_item")
    if _COMPLEX_CONSTRAINT.search(text):
        return RoutingDecision("main", 0.95, "complex_constraints")

    match = _CHINESE_SEARCH_PREFIX.match(text) or _ENGLISH_SEARCH_PREFIX.match(text)
    if match and text[match.end():].strip():
        return RoutingDecision("small", 0.97, "simple_single_item_search")
    return RoutingDecision("main", 0.25, "unrecognized_intent")


def can_use_small_model(decision: RoutingDecision | None) -> bool:
    return bool(
        decision is not None
        and decision.route == "small"
        and decision.confidence >= SMALL_MODEL_CONFIDENCE_THRESHOLD
        and decision.policy_version == "v1"
    )


def current_routing_decision() -> RoutingDecision | None:
    return _routing_decision.get()


def set_routing_decision(decision: RoutingDecision) -> Token[RoutingDecision | None]:
    return _routing_decision.set(decision)


def reset_routing_decision(token: Token[RoutingDecision | None]) -> None:
    _routing_decision.reset(token)
