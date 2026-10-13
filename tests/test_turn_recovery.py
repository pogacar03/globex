from __future__ import annotations

import asyncio

import pytest

from agentscope.agent import Agent, ReActConfig
from agentscope.event import ToolResultEndEvent
from agentscope.message import (
    AssistantMsg,
    TextBlock,
    ToolCallBlock,
    ToolResultBlock,
    ToolResultState,
)
from agentscope.model import ChatResponse
from agentscope.state import AgentState
from agentscope.tool import FunctionTool, Toolkit

from app.application.agents.main_agent import SessionRegistry
from app.application.agents.orchestrator import MainAgentOrchestrator, SubmitIntentInput
from app.infrastructure.context import ShoppingContext
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.runtime_checkpoint import (
    CheckpointConflictError,
    RuntimeCheckpointStore,
)
from app.infrastructure.synthetic_model import SyntheticChatModel

pytestmark = pytest.mark.asyncio


class MemorySessionStore:
    def __init__(self):
        self.states: dict[str, str] = {}

    async def save(self, session_id: str, state_json: str) -> None:
        self.states[session_id] = state_json

    async def load(self, session_id: str):
        return self.states.get(session_id)


class EmptyPreferences:
    async def list_by_buyer(self, _buyer_id):
        return []


class InterruptOnceModel(SyntheticChatModel):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    async def _call_api(self, model_name, messages, tools=None, tool_choice=None, **kwargs):
        self.owner.model_calls += 1
        if self.owner.model_calls == 2:
            self.owner.interrupted = True
            raise RuntimeError("injected process interruption after tool result")
        return await super()._call_api(model_name, messages, tools, tool_choice, **kwargs)


class InterruptibleAgentFactory:
    def __init__(self):
        self.model_calls = 0
        self.interrupted = False
        self.tool_calls: list[str] = []
        self.context_snapshots = []

    async def product_search_tool(self, normalized_query: str, top_k: int = 3):
        self.tool_calls.append(normalized_query)
        self.context_snapshots.append(ShoppingContext.current())
        from agentscope.message import TextBlock, ToolResultState
        from agentscope.tool import ToolChunk
        import json

        return ToolChunk(
            content=[TextBlock(text=json.dumps({
                "hits": [{"title": "SYNTHETIC recovery lamp"}],
                "evidence_status": "SYNTHETIC",
            }))],
            state=ToolResultState.SUCCESS,
        )

    def build(self, restored_state: AgentState | None = None) -> Agent:
        return Agent(
            name="fixture-agent",
            system_prompt="Use only SYNTHETIC fixtures.",
            model=InterruptOnceModel(self),
            toolkit=Toolkit(tools=[FunctionTool(self.product_search_tool, is_read_only=True)]),
            state=restored_state,
            react_config=ReActConfig(max_iters=4),
        )


async def test_native_tool_result_checkpoint_resumes_and_completed_task_replays(tmp_path):
    factory = InterruptibleAgentFactory()
    sessions = SessionRegistry(factory, MemorySessionStore())
    checkpoints = RuntimeCheckpointStore(tmp_path / "runtime.sqlite3")
    orchestrator = MainAgentOrchestrator(
        sessions=sessions,
        bus=TradeEventBus(),
        preference_store=EmptyPreferences(),
        output_guard_enabled=False,
        runtime_checkpoints=checkpoints,
    )
    intent = SubmitIntentInput(
        shopping_session_id="session-1",
        buyer_id="buyer-1",
        locale="zh-CN",
        currency="CNY",
        raw_query="露营灯",
        task_id="task-1",
        approval_id="approval-1",
        permissions=("commerce:read", "intent:submit"),
    )

    first = await orchestrator.handle_intent(intent)
    assert first.final_text.startswith("[error]")
    assert factory.interrupted
    assert len(factory.tool_calls) == 1

    resumed = await orchestrator.handle_intent(intent)
    assert "SYNTHETIC recovery lamp" in resumed.final_text
    assert len(factory.tool_calls) == 1, "native tool-result state must prevent replaying the completed tool"
    assert factory.model_calls == 3
    snapshot = factory.context_snapshots[0]
    assert snapshot.task_id == intent.task_id
    assert snapshot.approval_id == intent.approval_id
    assert snapshot.permissions == intent.permissions

    replay = await orchestrator.handle_intent(intent)
    assert replay.final_text == resumed.final_text
    assert factory.model_calls == 3, "a completed task must replay its stored result without another model call"

    changed = SubmitIntentInput(
        shopping_session_id=intent.shopping_session_id,
        buyer_id=intent.buyer_id,
        locale=intent.locale,
        currency=intent.currency,
        raw_query="different input",
        task_id=intent.task_id,
        approval_id=intent.approval_id,
        permissions=intent.permissions,
    )
    with pytest.raises(CheckpointConflictError):
        await orchestrator.handle_intent(changed)

    next_intent = SubmitIntentInput(
        shopping_session_id=intent.shopping_session_id,
        buyer_id=intent.buyer_id,
        locale=intent.locale,
        currency=intent.currency,
        raw_query="second turn",
        task_id="task-2",
        approval_id="approval-2",
        permissions=intent.permissions,
    )
    next_result = await orchestrator.handle_intent(next_intent)
    assert not next_result.final_text.startswith("[error]")
    assert len(factory.tool_calls) == 2
    latest_session_state = sessions._agents[intent.shopping_session_id].state.model_dump_json()

    old_replay = await orchestrator.handle_intent(intent)
    assert old_replay.final_text == resumed.final_text
    assert factory.model_calls == 5
    assert sessions._agents[intent.shopping_session_id].state.model_dump_json() == latest_session_state


class BlockingFinalModel(SyntheticChatModel):
    def __init__(self, factory):
        super().__init__()
        self.factory = factory

    async def _call_api(self, model_name, messages, tools=None, tool_choice=None, **kwargs):
        factory = self.factory
        factory.active_calls += 1
        factory.maximum_active_calls = max(factory.maximum_active_calls, factory.active_calls)
        factory.model_calls += 1
        try:
            if factory.model_calls == 1:
                factory.first_call_started.set()
                await factory.release_first_call.wait()
            return ChatResponse(content=[TextBlock(text="SYNTHETIC serialized response")], is_last=True)
        finally:
            factory.active_calls -= 1


class BlockingFinalAgentFactory:
    def __init__(self):
        self.first_call_started = asyncio.Event()
        self.release_first_call = asyncio.Event()
        self.active_calls = 0
        self.maximum_active_calls = 0
        self.model_calls = 0
        self.restored_states: list[str | None] = []

    def build(self, restored_state: AgentState | None = None) -> Agent:
        self.restored_states.append(
            restored_state.model_dump_json() if restored_state is not None else None,
        )
        return Agent(
            name="fixture-agent",
            system_prompt="Use only SYNTHETIC fixtures.",
            model=BlockingFinalModel(self),
            toolkit=Toolkit(tools=[]),
            state=restored_state,
            react_config=ReActConfig(max_iters=2),
        )


async def test_separate_registries_wait_for_session_lease_and_refresh_state(tmp_path):
    class ContentionObservedStore(RuntimeCheckpointStore):
        def __init__(self, db_path):
            super().__init__(db_path)
            self.contended = asyncio.Event()

        async def acquire_lease(self, owner_id, session_id, holder_id):
            lease = await super().acquire_lease(owner_id, session_id, holder_id)
            if lease is None:
                self.contended.set()
            return lease

    factory = BlockingFinalAgentFactory()
    shared_sessions = MemorySessionStore()
    checkpoint_path = tmp_path / "runtime.sqlite3"
    first_checkpoints = RuntimeCheckpointStore(checkpoint_path)
    second_checkpoints = ContentionObservedStore(checkpoint_path)

    def build_orchestrator(checkpoints):
        return MainAgentOrchestrator(
            sessions=SessionRegistry(factory, shared_sessions),
            bus=TradeEventBus(),
            preference_store=EmptyPreferences(),
            output_guard_enabled=False,
            runtime_checkpoints=checkpoints,
        )

    def make_intent(task_id: str, raw_query: str) -> SubmitIntentInput:
        return SubmitIntentInput(
            shopping_session_id="shared-session",
            buyer_id="buyer-1",
            locale="zh-CN",
            currency="CNY",
            raw_query=raw_query,
            task_id=task_id,
        )

    first_task = asyncio.create_task(
        build_orchestrator(first_checkpoints).handle_intent(make_intent("task-1", "first query")),
    )
    await asyncio.wait_for(factory.first_call_started.wait(), timeout=3)
    second_task = asyncio.create_task(
        build_orchestrator(second_checkpoints).handle_intent(make_intent("task-2", "second query")),
    )
    await asyncio.wait_for(second_checkpoints.contended.wait(), timeout=3)
    second_was_waiting = not second_task.done()
    factory.release_first_call.set()
    first_result, second_result = await asyncio.gather(
        first_task, second_task, return_exceptions=True,
    )

    assert second_was_waiting, f"second same-session turn was rejected instead of queued: {second_result!r}"
    assert not isinstance(first_result, BaseException), repr(first_result)
    assert not isinstance(second_result, BaseException), repr(second_result)
    assert factory.maximum_active_calls == 1
    assert any(
        state is not None and "first query" in state
        for state in factory.restored_states
    ), "the second registry must refresh persisted AgentScope state after acquiring the lease"


async def test_session_lease_wait_is_bounded(tmp_path):
    checkpoints = RuntimeCheckpointStore(
        tmp_path / "runtime.sqlite3",
        lease_ttl_seconds=1.0,
        lease_wait_timeout_seconds=0.03,
    )
    held = await checkpoints.acquire_lease("buyer-1", "session-1", "worker-1")
    assert held is not None

    with pytest.raises(TimeoutError, match="timed out waiting for session lease"):
        await MainAgentOrchestrator._acquire_session_lease(
            checkpoints, "buyer-1", "session-1", "worker-2",
        )


async def test_tool_result_event_is_checkpointed_only_after_native_state_contains_result():
    class EventOnlyAgent:
        def __init__(self):
            self.state = type("State", (), {})()
            self.state.context = [
                AssistantMsg(
                    name="fixture-agent",
                    content=[ToolCallBlock(id="call-1", name="fixture_tool", input="{}")],
                ),
            ]

        async def reply_stream(self, *_args, **_kwargs):
            yield ToolResultEndEvent(
                reply_id="reply-1",
                tool_call_id="call-1",
                state=ToolResultState.SUCCESS,
            )

    orchestrator = MainAgentOrchestrator(
        sessions=SessionRegistry(InterruptibleAgentFactory(), MemorySessionStore()),
        bus=TradeEventBus(),
        preference_store=EmptyPreferences(),
        output_guard_enabled=False,
    )
    saved_agents = []

    async def record_snapshot(_runtime_turn, agent):
        saved_agents.append(agent)

    orchestrator._save_runtime_snapshot = record_snapshot
    agent = EventOnlyAgent()
    await orchestrator._consume_reply("session-1", agent, [], runtime_turn=object())
    assert saved_agents == [], "an external-result event may be yielded before AgentScope updates context"

    agent.state.context[-1].content.append(
        ToolResultBlock(
            id="call-1",
            name="fixture_tool",
            output="completed",
            state=ToolResultState.SUCCESS,
        ),
    )
    await orchestrator._consume_reply("session-1", agent, [], runtime_turn=object())
    assert saved_agents == [agent], "a normal result block should make the safe checkpoint eligible"
