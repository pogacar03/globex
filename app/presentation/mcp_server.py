"""Workspace-bound MCP stdio adapter. All calls pass through the authenticated API.

One process belongs to one workspace identity. It exposes no approval mutation,
arbitrary URL, filesystem or shell tool. Human approval stays in the web UI.
"""
from __future__ import annotations
import os
import re
import httpx
from mcp.server.fastmcp import FastMCP

MCP_CLIENT_TIMEOUT_SECONDS = 30.0


class CommerceMCPAdapter:
    def __init__(self, client: httpx.AsyncClient, access_token: str):
        if not access_token:
            raise ValueError("GLOBEX_ACCESS_TOKEN is required")
        self.client = client
        self._headers = {"Authorization": "Bearer " + access_token}

    @staticmethod
    def _identifier(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
            raise ValueError("invalid identifier")
        return value

    async def _call(self, method: str, path: str, body=None) -> dict:
        response = await self.client.request(method, path, headers=self._headers, json=body)
        response.raise_for_status()
        return response.json()

    async def research(self, query: str, session_id: str | None = None) -> dict:
        """Queue research and return its task handle without waiting for completion.

        The async API requires the worker queue. HTTP 503 means queueing is
        disabled; callers should report that state instead of retrying the
        synchronous endpoint, which may wait much longer than this client.
        """
        if not query.strip() or len(query) > 20000:
            raise ValueError("query must contain 1..20000 characters")
        return await self._call("POST", "/commerce/intents/async", {
            "raw_query": query, "shopping_session_id": session_id})

    async def task_status(self, task_id: str) -> dict:
        return await self._call("GET", "/commerce/tasks/" + self._identifier(task_id))

    async def order_status(self, order_id: str) -> dict:
        return await self._call("GET", "/commerce/orders/" + self._identifier(order_id))


def build_mcp_server(adapter: CommerceMCPAdapter) -> FastMCP:
    server = FastMCP("Globex workspace commerce")
    server.tool(name="research", description=(
        "Queue catalog research and immediately return a task_id for status polling. "
        "Writes require human approval. If the API returns HTTP 503 because the queue "
        "is disabled, report that explicitly; do not retry through the synchronous endpoint."
    ))(adapter.research)
    server.tool(name="task_status", description="Read the current identity's task; foreign tasks are hidden.")(adapter.task_status)
    server.tool(name="order_status", description="Read the current identity's order; foreign orders are hidden.")(adapter.order_status)
    return server


def main():
    base_url = os.getenv("GLOBEX_API_URL", "http://127.0.0.1:8000")
    client = httpx.AsyncClient(base_url=base_url, timeout=MCP_CLIENT_TIMEOUT_SECONDS)
    build_mcp_server(CommerceMCPAdapter(client, os.getenv("GLOBEX_ACCESS_TOKEN", ""))).run(transport="stdio")


if __name__ == "__main__":
    main()
