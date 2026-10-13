import json
import pytest
from agentscope.agent import Agent, ReActConfig
from agentscope.event import ToolResultEndEvent
from agentscope.message import Msg, UserMsg, TextBlock, ToolResultState
from agentscope.state import AgentState
from agentscope.tool import FunctionTool, Toolkit, ToolChunk


async def test_native_checkpoint_resumes_without_repeating_completed_tool():
    from app.infrastructure.synthetic_model import SyntheticChatModel
    calls = []
    async def product_search_tool(normalized_query: str, top_k: int = 3):
        """Read a SYNTHETIC catalog fixture."""
        calls.append(normalized_query)
        return ToolChunk(content=[TextBlock(text=json.dumps({"hits": [{"title": "SYNTHETIC fixture lamp"}],
            "evidence_status": "SYNTHETIC"}))], state=ToolResultState.SUCCESS)

    def build(state=None):
        return Agent(name="fixture-agent", system_prompt="Use only SYNTHETIC fixtures.",
            model=SyntheticChatModel(), toolkit=Toolkit(tools=[FunctionTool(product_search_tool, is_read_only=True)]),
            state=state, react_config=ReActConfig(max_iters=4))

    agent = build()
    stream = agent.reply_stream([UserMsg(name="buyer", content="露营灯")], yield_final_msg=True)
    checkpoint = None
    async for event in stream:
        if isinstance(event, ToolResultEndEvent):
            checkpoint = agent.state.model_dump_json()
            break
    assert checkpoint is not None
    await stream.aclose()
    restored = build(AgentState.model_validate_json(checkpoint))
    answer = ""
    async for event in restored.reply_stream(None, yield_final_msg=True):
        if isinstance(event, Msg):
            answer = event.get_text_content()
    assert len(calls) == 1, "restored AgentState must reuse the completed tool result"
    assert "SYNTHETIC fixture lamp" in answer


async def test_mock_does_not_report_success_after_tool_failure():
    from app.infrastructure.synthetic_model import SyntheticChatModel
    async def product_search_tool(normalized_query: str, top_k: int = 3):
        """Fail the SYNTHETIC fixture read."""
        return ToolChunk(content=[TextBlock(text="[error] injected fixture failure")], state=ToolResultState.ERROR)
    agent = Agent(name="fixture-agent", system_prompt="Fixtures only", model=SyntheticChatModel(),
        toolkit=Toolkit(tools=[FunctionTool(product_search_tool, is_read_only=True)]))
    answer = ""
    async for event in agent.reply_stream([UserMsg(name="buyer", content="露营灯")], yield_final_msg=True):
        if isinstance(event, Msg):
            answer = event.get_text_content()
    assert answer.startswith("[error]")
