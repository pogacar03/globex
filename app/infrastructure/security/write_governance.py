"""Approval middleware for order writes invoked by the trusted Agent runtime."""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncGenerator, Callable

from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import ToolBase, ToolChunk, ToolMiddlewareBase

from app.infrastructure.context import ShoppingContext
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.security.approval_store import ApprovalStore


_WRITE_TOOLS = frozenset({"create_order_tool", "cancel_order_tool"})


def _chunk(payload: Any, state: ToolResultState = ToolResultState.SUCCESS) -> ToolChunk:
    if isinstance(payload, str):
        text = payload
    else:
        text = json.dumps(payload, ensure_ascii=False)
    return ToolChunk(content=[TextBlock(type="text", text=text)], state=state)


def _receipt(chunks: list[ToolChunk]) -> Any:
    text = "\n".join(
        block.text
        for chunk in chunks
        for block in chunk.content
        if getattr(block, "type", None) == "text" and isinstance(block.text, str)
    )
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return text


class WriteApprovalMiddleware(ToolMiddlewareBase):
    """Require a trusted, argument-bound approval before order writes.

    A missing store disables writes. Approval identifiers are read only from
    ``ShoppingContext`` and are never taken from model-generated tool args.
    """

    def __init__(self, store: ApprovalStore | None, bus: TradeEventBus) -> None:
        self._store = store
        self._bus = bus

    async def on_tool_call(
        self,
        tool: ToolBase,
        input_kwargs: dict[str, Any],
        next_handler: Callable[..., AsyncGenerator[ToolChunk, None]],
    ) -> AsyncGenerator[ToolChunk, None]:
        tool_name = tool.name
        if tool_name not in _WRITE_TOOLS:
            async for chunk in next_handler(**input_kwargs):
                yield chunk
            return

        context = ShoppingContext.current()
        permissions = getattr(context, "permissions", ()) if context is not None else ()
        if context is None or not context.buyer_id or not context.shopping_session_id:
            yield _chunk(
                {"decision": "DENY", "reason": "authenticated buyer context required"},
                ToolResultState.ERROR,
            )
            return
        if "commerce:write" not in permissions:
            yield _chunk(
                {"decision": "DENY", "reason": "commerce:write permission required"},
                ToolResultState.ERROR,
            )
            return
        if self._store is None:
            yield _chunk(
                {"decision": "DENY", "reason": "approval service unavailable"},
                ToolResultState.ERROR,
            )
            return

        try:
            permit = await self._store.begin(
                owner=context.buyer_id,
                session=context.shopping_session_id,
                tool=tool_name,
                arguments=input_kwargs,
                approval_id=context.approval_id,
            )
        except Exception:
            yield _chunk(
                {"decision": "DENY", "reason": "approval check failed"},
                ToolResultState.ERROR,
            )
            return

        if permit.decision == "REQUIRE_APPROVAL":
            proposal = await self._store.get(context.buyer_id, permit.approval_id)
            self._bus.publish(
                context.shopping_session_id,
                "approval.required",
                {
                    "decision": "REQUIRE_APPROVAL",
                    "approval_id": proposal["approval_id"],
                    "tool": proposal["tool"],
                    "arguments": proposal["arguments"],
                    "expires_at": proposal["expires_at"],
                },
            )
            yield _chunk({"decision": "REQUIRE_APPROVAL", **proposal})
            return
        if permit.decision == "REPLAY":
            yield _chunk(permit.result)
            return
        if permit.decision == "DENY":
            yield _chunk(
                {"decision": "DENY", "reason": "approval does not authorize this write"},
                ToolResultState.ERROR,
            )
            return
        if permit.decision == "RECONCILIATION_REQUIRED":
            yield _chunk(
                {
                    "decision": "RECONCILIATION_REQUIRED",
                    "approval_id": permit.approval_id,
                    "reason": "write outcome must be reconciled before retrying",
                },
                ToolResultState.ERROR,
            )
            return
        if permit.decision != "ALLOW":
            yield _chunk(
                {"decision": "DENY", "reason": "unknown approval decision"},
                ToolResultState.ERROR,
            )
            return

        try:
            chunks = [chunk async for chunk in next_handler(**input_kwargs)]
        except asyncio.CancelledError:
            await asyncio.shield(self._mark_uncertain(permit.approval_id))
            raise
        except Exception:
            await self._mark_uncertain(permit.approval_id)
            yield _chunk(
                {
                    "decision": "RECONCILIATION_REQUIRED",
                    "approval_id": permit.approval_id,
                    "reason": "write outcome is uncertain",
                },
                ToolResultState.ERROR,
            )
            return

        if not chunks:
            await self._mark_uncertain(permit.approval_id)
            yield _chunk(
                {
                    "decision": "RECONCILIATION_REQUIRED",
                    "approval_id": permit.approval_id,
                    "reason": "write returned no receipt",
                },
                ToolResultState.ERROR,
            )
            return

        if chunks[-1].state != ToolResultState.SUCCESS:
            await self._mark_uncertain(permit.approval_id)
        else:
            try:
                await self._store.complete(permit.approval_id, _receipt(chunks))
            except Exception:
                await self._mark_uncertain(permit.approval_id)
        for chunk in chunks:
            yield chunk

    async def _mark_uncertain(self, approval_id: str) -> None:
        if self._store is None:
            return
        try:
            await self._store.mark_uncertain(approval_id)
        except Exception:
            # The initial claim still blocks a replay if persisting the marker fails.
            return
