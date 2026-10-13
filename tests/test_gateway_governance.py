"""Gateway deadlines, fallback safety, and usage accounting regressions."""
from __future__ import annotations

import asyncio
import sys
from types import ModuleType, SimpleNamespace

import pytest
from agentscope.credential import OpenAICredential
from agentscope.message import Msg, TextBlock
from agentscope.model import OpenAIChatModel

from app.infrastructure.budget import MINIMAL_MODE_HINT, init_budget
from app.infrastructure.llm import ThrottledChatModel
from app.infrastructure.llm import create_chat_model
from app.infrastructure.model_usage import ModelUsageLedger
from app.infrastructure.task_routing import decide_route, reset_routing_decision, set_routing_decision
from app.infrastructure.throttle import GatewayThrottle

pytestmark = pytest.mark.asyncio

_CREDENTIAL = OpenAICredential(api_key="test-key", base_url="http://127.0.0.1:9/v1")


class StubModel(ThrottledChatModel):
    def __init__(self, behaviors, **kwargs):
        super().__init__(
            credential=_CREDENTIAL,
            model="primary-model",
            throttle=kwargs.pop("throttle", GatewayThrottle(1, 0)),
            **kwargs,
        )
        self.behaviors = list(behaviors)
        self.calls = 0

    async def _invoke_upstream(self, messages, tools, tool_choice, **kwargs):
        self.calls += 1
        behavior = self.behaviors.pop(0)
        if isinstance(behavior, BaseException):
            raise behavior
        if callable(behavior):
            behavior = behavior()
        if asyncio.iscoroutine(behavior):
            behavior = await behavior
        return behavior


class FakeFallback:

    def __init__(self, response="fallback", model="fallback-model"):
        self.response = response
        self.model = model
        self.calls = 0
        self.messages_seen = []

    async def __call__(self, messages, tools=None, tool_choice=None, **kwargs):
        self.calls += 1
        self.messages_seen.append(messages)
        response = self.response() if callable(self.response) else self.response
        if isinstance(response, BaseException):
            raise response
        if asyncio.iscoroutine(response):
            response = await response
        return response


async def test_per_request_timeout_releases_gateway_slot():
    async def slow_response():
        await asyncio.sleep(0.1)
        return "late"

    throttle = GatewayThrottle(1, 0)
    model = StubModel(
        [slow_response, "next"],
        throttle=throttle,
        request_timeout_seconds=0.01,
        total_timeout_seconds=0.2,
        max_transient_retries=0,
    )

    with pytest.raises(TimeoutError):
        await model([])
    assert await asyncio.wait_for(model([]), timeout=0.1) == "next"


async def test_total_deadline_includes_retry_backoff():
    model = StubModel(
        [RuntimeError("429 rate limit"), "should-not-run"],
        max_transient_retries=2,
        retry_base_seconds=0.05,
        request_timeout_seconds=1,
        total_timeout_seconds=0.01,
    )

    with pytest.raises(TimeoutError):
        await model([])
    assert model.calls == 1


async def test_non_retryable_http_client_error_does_not_fallback():
    class UnauthorizedError(RuntimeError):
        status_code = 401

    fallback = FakeFallback()
    error = UnauthorizedError("invalid credentials")
    model = StubModel([error], fallback=fallback, max_transient_retries=3)

    with pytest.raises(UnauthorizedError) as raised:
        await model([])
    assert raised.value is error
    assert model.calls == 1
    assert fallback.calls == 0


async def test_retryable_http_5xx_uses_status_code_even_without_marker_text():
    class ServiceUnavailable(RuntimeError):
        status_code = 503

    model = StubModel(
        [ServiceUnavailable("upstream response"), "recovered"],
        max_transient_retries=1,
        retry_base_seconds=0,
    )

    assert await model([]) == "recovered"
    assert model.calls == 2


async def test_late_stream_error_is_not_retried_or_appended_from_fallback():
    async def partial_then_error():
        yield "first"
        raise RuntimeError("429 rate limit")

    fallback = FakeFallback("different answer")
    model = StubModel(
        [partial_then_error()],
        fallback=fallback,
        max_transient_retries=2,
        retry_base_seconds=0,
    )

    stream = await model([])
    seen = []
    with pytest.raises(RuntimeError, match="429"):
        async for item in stream:
            seen.append(item)
    assert seen == ["first"]
    assert model.calls == 1
    assert fallback.calls == 0


async def test_stream_error_before_any_output_can_retry():
    async def fail_before_output():
        raise RuntimeError("429 rate limit")
        yield "unreachable"

    model = StubModel(
        [fail_before_output(), "clean retry"],
        max_transient_retries=1,
        retry_base_seconds=0,
    )

    stream = await model([])
    assert [item async for item in stream] == ["clean retry"]
    assert model.calls == 2


async def test_request_timeout_applies_while_consuming_stream_and_releases_slot():
    async def slow_stream():
        await asyncio.sleep(0.1)
        yield "late"

    throttle = GatewayThrottle(1, 0)
    model = StubModel(
        [slow_stream(), "next"],
        throttle=throttle,
        request_timeout_seconds=0.01,
        total_timeout_seconds=0.2,
        max_transient_retries=0,
    )
    stream = await model([])
    with pytest.raises(TimeoutError):
        await anext(stream)
    assert await asyncio.wait_for(model([]), timeout=0.1) == "next"


async def test_total_timeout_applies_across_the_entire_stream():
    async def slow_stream():
        yield "first"
        await asyncio.sleep(0.05)
        yield "second"

    throttle = GatewayThrottle(1, 0)
    model = StubModel(
        [slow_stream(), "next"],
        throttle=throttle,
        request_timeout_seconds=1,
        total_timeout_seconds=0.02,
        max_transient_retries=0,
    )
    stream = await model([])
    assert await anext(stream) == "first"
    with pytest.raises(TimeoutError):
        await anext(stream)
    assert await asyncio.wait_for(model([]), timeout=0.1) == "next"


async def test_cancellation_while_consuming_stream_releases_slot():
    stream_started = asyncio.Event()

    async def stalled_stream():
        stream_started.set()
        await asyncio.sleep(5)
        yield "late"

    throttle = GatewayThrottle(1, 0)
    model = StubModel(
        [stalled_stream(), "next"],
        throttle=throttle,
        request_timeout_seconds=10,
        total_timeout_seconds=10,
        max_transient_retries=0,
    )

    async def consume():
        stream = await model([])
        async for _chunk in stream:
            pass

    task = asyncio.create_task(consume())
    await stream_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await asyncio.wait_for(model([]), timeout=0.1) == "next"


async def test_gateway_slot_is_released_during_retry_backoff():
    throttle = GatewayThrottle(1, 0)
    first_started = asyncio.Event()
    second_started = asyncio.Event()

    class BackoffModel(StubModel):
        async def _invoke_upstream(self, messages, tools, tool_choice, **kwargs):
            self.calls += 1
            if self.calls == 1:
                first_started.set()
                raise RuntimeError("429 rate limit")
            if self.calls == 2:
                second_started.set()
                return "second"
            return "retry"

    model = BackoffModel(
        [],
        throttle=throttle,
        max_transient_retries=1,
        retry_base_seconds=0.08,
        request_timeout_seconds=1,
        total_timeout_seconds=1,
    )
    first = asyncio.create_task(model([]))
    await first_started.wait()
    second = asyncio.create_task(model([]))
    assert await asyncio.wait_for(second, timeout=0.04) == "second"
    assert second_started.is_set()
    assert await first == "retry"


async def test_usage_ledger_marks_missing_provider_usage_unknown():
    ledger = ModelUsageLedger(
        pricing_version="prices-2026-10",
        rates={
            "primary-model": {
                "input_usd_per_million_tokens": 2.0,
                "output_usd_per_million_tokens": 8.0,
            },
        },
    )

    unknown = ledger.record({"usage": None}, model="primary-model")
    assert unknown.usage_status == "unknown"
    assert unknown.input_tokens is None
    assert unknown.estimated_cost_usd is None
    assert unknown.billed_cost_usd is None


async def test_usage_ledger_keeps_actual_usage_and_versioned_estimate_separate():
    ledger = ModelUsageLedger(
        pricing_version="prices-2026-10",
        rates={
            "primary-model": {
                "input_usd_per_million_tokens": 2.0,
                "output_usd_per_million_tokens": 8.0,
            },
        },
    )

    usage = ledger.record(
        {"usage": {"prompt_tokens": 1000, "completion_tokens": 500}},
        model="primary-model",
    )
    assert usage.usage_status == "actual"
    assert usage.input_tokens == 1000
    assert usage.output_tokens == 500
    assert usage.total_tokens == 1500
    assert usage.estimated_cost_usd == pytest.approx(0.006)
    assert usage.pricing_version == "prices-2026-10"
    assert usage.billed_cost_usd is None


async def test_chat_model_records_actual_provider_usage():
    ledger = ModelUsageLedger(
        pricing_version="rates-v1",
        rates={
            "primary-model": {
                "input_usd_per_million_tokens": 1.0,
                "output_usd_per_million_tokens": 1.0,
            },
        },
    )
    model = StubModel(
        [{"usage": {"input_tokens": 4, "output_tokens": 6}}],
        usage_recorder=ledger,
    )

    await model([])
    assert len(ledger.records) == 1
    assert ledger.records[0].usage_status == "actual"
    assert ledger.records[0].total_tokens == 10


async def test_chat_model_records_missing_provider_usage_as_unknown():
    model = StubModel([{"content": "response without usage"}])
    await model([])

    assert model.usage_ledger.records[0].usage_status == "unknown"
    assert model.usage_ledger.records[0].billed_cost_usd is None


async def test_factory_reads_optional_timeouts_and_versioned_pricing():
    settings = SimpleNamespace(
        llm_mode="live",
        llm_api_key="test-key",
        llm_base_url="http://127.0.0.1:9/v1",
        llm_model="primary-model",
        llm_fallback_model="",
        llm_max_concurrency=1,
        llm_min_interval_seconds=0,
        llm_max_retries=0,
        llm_request_timeout_seconds=0.4,
        llm_total_timeout_seconds=1.2,
        llm_pricing_version="factory-v1",
        llm_pricing_rates={
            "primary-model": {
                "input_usd_per_million_tokens": 1,
                "output_usd_per_million_tokens": 2,
            },
        },
        context_size=32768,
    )

    model = create_chat_model(settings, stream=False)
    assert model._request_timeout_seconds == 0.4
    assert model._total_timeout_seconds == 1.2
    assert model.usage_ledger.pricing_version == "factory-v1"


async def test_factory_dispatches_mock_mode_without_reading_live_credentials(monkeypatch):
    class FakeSynthetic:
        def __init__(self, *, context_size, delay_seconds):
            self.context_size = context_size
            self.delay_seconds = delay_seconds

    module = ModuleType("app.infrastructure.synthetic_model")
    module.SyntheticChatModel = FakeSynthetic
    monkeypatch.setitem(sys.modules, "app.infrastructure.synthetic_model", module)

    model = create_chat_model(SimpleNamespace(llm_mode="mock", context_size=32768))
    assert isinstance(model, FakeSynthetic)
    assert model.context_size == 32768
    assert model.delay_seconds == 0.0


async def test_small_route_calls_small_model_and_records_actual_model_name(monkeypatch):
    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    small = FakeFallback("small-response", model="small-model")
    ledger = ModelUsageLedger()
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        usage_recorder=ledger,
        max_transient_retries=0,
    )
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        result = await model([])
    finally:
        reset_routing_decision(token)

    assert result == "small-response"
    assert small.calls == 1
    assert main_calls == []
    assert ledger.records[0].model == "small-model"


async def test_small_route_without_configured_small_model_uses_main(monkeypatch):
    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        throttle=GatewayThrottle(1, 0),
        max_transient_retries=0,
    )
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        result = await model([])
    finally:
        reset_routing_decision(token)

    assert result == "main-response"
    assert main_calls == ["primary-model"]


async def test_complex_route_uses_main_even_when_small_is_configured(monkeypatch):
    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    small = FakeFallback("small-response", model="small-model")
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        max_transient_retries=0,
    )
    token = set_routing_decision(decide_route("比较 iPhone 和 Pixel"))
    try:
        result = await model([])
    finally:
        reset_routing_decision(token)

    assert result == "main-response"
    assert main_calls == ["primary-model"]
    assert small.calls == 0


async def test_transient_small_error_escalates_to_main_within_deadline(monkeypatch):
    class ServiceUnavailable(RuntimeError):
        status_code = 503

    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    small = FakeFallback(ServiceUnavailable("temporary"), model="small-model")
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        request_timeout_seconds=0.2,
        total_timeout_seconds=0.5,
        max_transient_retries=0,
    )
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        result = await model([])
    finally:
        reset_routing_decision(token)

    assert result == "main-response"
    assert small.calls == 1
    assert main_calls == ["primary-model"]


async def test_non_retryable_small_error_does_not_escalate(monkeypatch):
    class UnauthorizedError(RuntimeError):
        status_code = 401

    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    error = UnauthorizedError("bad small-model credentials")
    small = FakeFallback(error, model="small-model")
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        max_transient_retries=2,
    )
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        with pytest.raises(UnauthorizedError) as raised:
            await model([])
    finally:
        reset_routing_decision(token)

    assert raised.value is error
    assert small.calls == 1
    assert main_calls == []


@pytest.mark.parametrize(
    ("spent", "expected_tier", "expects_hint"),
    [(60, "lite", False), (90, "minimal", True)],
)
async def test_budget_fallback_keeps_priority_and_usage_over_task_small_route(
    monkeypatch, spent, expected_tier, expects_hint
):
    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    fallback = FakeFallback(
        {"content": "budget-response", "usage": {"input_tokens": 4, "output_tokens": 6}},
        model="budget-model",
    )
    small = FakeFallback("small-response", model="small-model")
    ledger = ModelUsageLedger()
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        fallback=fallback,
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        usage_recorder=ledger,
        max_transient_retries=0,
    )
    budget = init_budget(100)
    assert budget is not None
    budget.charge("test", spent)
    assert budget.tier == expected_tier
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        result = await model([])
    finally:
        reset_routing_decision(token)
        init_budget(0)

    assert result["content"] == "budget-response"
    assert fallback.calls == 1
    assert small.calls == 0
    assert main_calls == []
    assert ledger.records[0].model == "budget-model"
    assert ledger.records[0].usage_status == "actual"
    assert ledger.records[0].input_tokens == 4
    assert ledger.records[0].output_tokens == 6

    forwarded = fallback.messages_seen[0]
    if expects_hint:
        hint_message = forwarded[-1]
        assert isinstance(hint_message, Msg)
        assert isinstance(hint_message.content, list)
        assert len(hint_message.content) == 1
        assert isinstance(hint_message.content[0], TextBlock)
        assert hint_message.content[0].text == MINIMAL_MODE_HINT
    else:
        assert all(
            not (
                isinstance(message, Msg)
                and isinstance(message.content, list)
                and any(
                    isinstance(block, TextBlock) and block.text == MINIMAL_MODE_HINT
                    for block in message.content
                )
            )
            for message in forwarded
        )


async def test_stream_error_after_output_does_not_escalate_small_route(monkeypatch):
    class ServiceUnavailable(RuntimeError):
        status_code = 503

    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    async def partial_then_error():
        yield "partial"
        raise ServiceUnavailable("late failure")

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    small = FakeFallback(partial_then_error, model="small-model")
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        max_transient_retries=2,
    )
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        stream = await model([])
        assert await anext(stream) == "partial"
        with pytest.raises(ServiceUnavailable, match="late failure"):
            await anext(stream)
    finally:
        reset_routing_decision(token)

    assert small.calls == 1
    assert main_calls == []


async def test_small_stream_error_before_output_escalates_after_retry_budget_is_spent(monkeypatch):
    class ServiceUnavailable(RuntimeError):
        status_code = 503

    main_calls = []

    async def primary_call(self, messages, tools=None, tool_choice=None, **kwargs):
        main_calls.append(self.model)
        return "main-response"

    async def fail_before_output():
        raise ServiceUnavailable("temporary stream failure")
        yield "unreachable"

    monkeypatch.setattr(OpenAIChatModel, "__call__", primary_call)
    small = FakeFallback(fail_before_output, model="small-model")
    model = ThrottledChatModel(
        credential=_CREDENTIAL,
        model="primary-model",
        small_model=small,
        throttle=GatewayThrottle(1, 0),
        max_transient_retries=0,
    )
    token = set_routing_decision(decide_route("搜索一个耳机"))
    try:
        stream = await model([])
        result = [chunk async for chunk in stream]
    finally:
        reset_routing_decision(token)

    assert result == ["main-response"]
    assert small.calls == 1
    assert main_calls == ["primary-model"]


async def test_factory_configures_optional_small_model_and_disables_sdk_retries():
    settings = SimpleNamespace(
        llm_mode="live",
        llm_api_key="test-key",
        llm_base_url="http://127.0.0.1:9/v1",
        llm_model="primary-model",
        llm_fallback_model="",
        llm_small_model="small-model",
        llm_max_concurrency=1,
        llm_min_interval_seconds=0,
        llm_max_retries=1,
        llm_request_timeout_seconds=1,
        llm_total_timeout_seconds=2,
        context_size=32768,
    )

    model = create_chat_model(settings, stream=False)

    assert model._small_model.model == "small-model"
    assert model.max_retries == 0
