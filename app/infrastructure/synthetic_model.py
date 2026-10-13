"""Deterministic offline AgentScope model; no credentials or network transport."""
from __future__ import annotations
import asyncio
import json
from pydantic import BaseModel
from agentscope.model import ChatModelBase, ChatResponse
from agentscope.formatter import OpenAIChatFormatter
from agentscope.message import TextBlock, ToolCallBlock, ToolResultBlock, ToolResultState


class SyntheticParameters(BaseModel):
    pass

class SyntheticChatModel(ChatModelBase):
    def __init__(self, context_size: int = 32768, delay_seconds: float = 0.0):
        super().__init__(credential=None, model="synthetic-catalog-v1", parameters=SyntheticParameters(),
            stream=False, max_retries=0, context_size=context_size)
        self.delay_seconds = delay_seconds
        self.calls = 0
        self.formatter = OpenAIChatFormatter()

    async def _call_api(self, model_name, messages, tools=None, tool_choice=None, **kwargs):
        self.calls += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        last_user = max((index for index, message in enumerate(messages) if message.role == "user"), default=-1)
        results = [block for message in messages[last_user + 1:] for block in (message.content or [])
            if isinstance(block, ToolResultBlock)]
        if results:
            result = results[-1]
            output = result.output if isinstance(result.output, str) else "\n".join(
                getattr(block, "text", "") for block in result.output)
            text = "[error] SYNTHETIC_TOOL_FAILED" if result.state != ToolResultState.SUCCESS else (
                "SYNTHETIC 离线商品研究：" + output + "。种子目录仅用于工程验收，无真实模型 Token 或购买行为。")
            return ChatResponse(content=[TextBlock(text=text)], is_last=True)
        names = {tool.get("function", tool).get("name") for tool in (tools or [])}
        if "product_search_tool" not in names:
            return ChatResponse(content=[TextBlock(text="[error] SYNTHETIC_RESEARCH_TOOL_UNAVAILABLE")], is_last=True)
        query = messages[last_user].get_text_content() if last_user >= 0 else "露营灯"
        return ChatResponse(content=[ToolCallBlock(id="synthetic-search-" + str(self.calls),
            name="product_search_tool", input=json.dumps({"normalized_query": query, "top_k": 3}))], is_last=True)
