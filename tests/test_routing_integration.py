import asyncio
from types import SimpleNamespace
import pytest
from app.application.agents.orchestrator import MainAgentOrchestrator, SubmitIntentInput, SubmitIntentOutput
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.task_routing import current_routing_decision


async def test_concurrent_turns_route_independently_and_reset_after_failure():
    orchestrator = MainAgentOrchestrator(SimpleNamespace(), TradeEventBus(), SimpleNamespace())
    observed = []
    async def run(intent):
        await asyncio.sleep(0)
        observed.append((intent.raw_query, current_routing_decision().route))
        if "下单" in intent.raw_query:
            raise RuntimeError("fixture interruption")
        return SubmitIntentOutput(intent.shopping_session_id, "fixture")
    orchestrator._handle_intent_inner = run
    def intent(query):
        return SubmitIntentInput("session", "buyer", "zh-CN", "CNY", query)
    results = await asyncio.gather(orchestrator.handle_intent(intent("搜索露营灯")),
        orchestrator.handle_intent(intent("下单露营灯")), return_exceptions=True)
    assert observed == [("搜索露营灯", "small"), ("下单露营灯", "main")]
    assert isinstance(results[1], RuntimeError)
    assert current_routing_decision() is None


async def test_task_deadline_is_enforced_even_if_inner_sdk_swallows_cancellation():
    orchestrator = MainAgentOrchestrator(SimpleNamespace(), TradeEventBus(), SimpleNamespace(),
        task_timeout_seconds=.02)
    async def slow(intent):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return SubmitIntentOutput(intent.shopping_session_id, "late result")
    orchestrator._handle_intent_inner = slow
    with pytest.raises(TimeoutError):
        await orchestrator.handle_intent(SubmitIntentInput("fixture", "buyer", "zh-CN", "CNY", "搜索露营灯"))
    assert current_routing_decision() is None


async def test_checkpointed_turn_does_not_add_another_model_retry_layer(monkeypatch):
    orchestrator = MainAgentOrchestrator(SimpleNamespace(), TradeEventBus(), SimpleNamespace())
    calls = []
    async def failure(*args):
        calls.append(True)
        raise RuntimeError("429 Too Many Requests")
    async def immediate(seconds): pass
    orchestrator._consume_reply = failure
    monkeypatch.setattr("app.application.agents.orchestrator.asyncio.sleep", immediate)
    with pytest.raises(RuntimeError):
        await orchestrator._reply_with_retry("fixture", None, [], runtime_turn=object())
    assert len(calls) == 1
