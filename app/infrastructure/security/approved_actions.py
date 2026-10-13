"""Execute only previously approved, durably stored order actions."""
from __future__ import annotations

import inspect
import json
from types import SimpleNamespace
from typing import Any, Callable

from agentscope.message import ToolResultState
from agentscope.tool import ToolChunk

from app.infrastructure.context import ShoppingContext, ShoppingContextSnapshot
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.security.approval_store import ApprovalError, ApprovalStore
from app.infrastructure.security.write_governance import WriteApprovalMiddleware


class ApprovedActionError(ValueError):
    """A stable domain error suitable for the approvals HTTP boundary."""

    def __init__(self, reason: str, status_code: int) -> None:
        self.reason = reason
        self.status_code = status_code
        super().__init__(reason)


class ApprovedActionExecutor:
    """Run a durable order approval using only its persisted tool and arguments.

    The method deliberately accepts no tool name or argument payload. Those
    values are loaded from the owner-scoped approval record and then passed
    through the same write middleware used by agent-initiated writes.
    """

    _WRITE_TOOLS = frozenset({"create_order_tool", "cancel_order_tool"})

    def __init__(
        self,
        store: ApprovalStore,
        place_order: Callable[..., Any],
        cancel_order: Callable[..., Any],
        bus: TradeEventBus,
    ) -> None:
        self._store = store
        self._place_order = place_order
        self._cancel_order = cancel_order
        self._middleware = WriteApprovalMiddleware(store, bus)

    async def execute(
        self,
        owner: str,
        approval_id: str,
        permissions: tuple,
    ) -> dict:
        if not isinstance(permissions, tuple) or "commerce:write" not in permissions:
            raise ApprovedActionError("permission_denied", 403)

        try:
            record = await self._store.get(owner, approval_id)
        except (ApprovalError, ValueError, TypeError):
            raise ApprovedActionError("approval_unavailable", 404) from None

        tool_name = record.get("tool")
        if tool_name not in self._WRITE_TOOLS:
            raise ApprovedActionError("action_not_supported", 403)

        state = record.get("state")
        if state == "pending":
            raise ApprovedActionError("approval_required", 409)
        if state == "expired":
            raise ApprovedActionError("approval_expired", 409)
        if state in ("executing", "uncertain"):
            raise ApprovedActionError("reconciliation_required", 409)
        if state == "completed":
            if "result" not in record:
                raise ApprovedActionError("reconciliation_required", 409)
            return {"state": "completed", "result": record["result"]}
        if state != "approved":
            raise ApprovedActionError("action_denied", 403)

        session_id = record.get("shopping_session_id")
        arguments = record.get("arguments")
        if not isinstance(session_id, str) or not session_id or not isinstance(arguments, dict):
            raise ApprovedActionError("approval_unavailable", 404)

        action = self._place_order if tool_name == "create_order_tool" else self._cancel_order
        if action is None or not callable(action):
            raise ApprovedActionError("action_unavailable", 503)

        snapshot = ShoppingContextSnapshot(
            shopping_session_id=session_id,
            buyer_id=owner,
            locale="zh-CN",
            currency="CNY",
            approval_id=approval_id,
            permissions=permissions,
        )
        context_token = ShoppingContext.set(snapshot)
        try:
            async def next_handler(**stored_arguments):
                result = action(**stored_arguments)
                if inspect.isawaitable(result):
                    result = await result
                if hasattr(result, "__aiter__"):
                    async for chunk in result:
                        yield chunk
                else:
                    yield result

            chunks = [
                chunk
                async for chunk in self._middleware.on_tool_call(
                    SimpleNamespace(name=tool_name),
                    arguments,
                    next_handler,
                )
            ]
        finally:
            ShoppingContext.reset(context_token)

        if not chunks:
            raise ApprovedActionError("reconciliation_required", 409)

        result = self._receipt(chunks)
        if chunks[-1].state != ToolResultState.SUCCESS:
            decision = result.get("decision") if isinstance(result, dict) else None
            if decision == "REQUIRE_APPROVAL":
                raise ApprovedActionError("approval_required", 409)
            if decision == "DENY":
                raise ApprovedActionError("action_denied", 403)
            raise ApprovedActionError("reconciliation_required", 409)

        if isinstance(result, dict) and result.get("decision") == "REQUIRE_APPROVAL":
            raise ApprovedActionError("approval_required", 409)
        return {"state": "completed", "result": result}

    @staticmethod
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
