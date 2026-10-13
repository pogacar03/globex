"""Actual offline process acceptance. No model keys or remote accounts required.

Run with --redis-url for an isolated disposable Redis database. Otherwise this
script creates and removes its own Redis container. Evidence is SYNTHETIC.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import sys
import time
import httpx
import websockets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def checkpoint(path, task_id):
    if not path.exists():
        return None
    try:
        with sqlite3.connect(path) as connection:
            row = connection.execute("SELECT snapshot_json FROM runtime_turn_checkpoints WHERE task_id=?", (task_id,)).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


async def accept(base_url, token, foreign_token, env, processes, evidence):
    headers = {"Authorization": "Bearer " + token}
    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=45) as client:
        for _ in range(600):
            if processes[0].poll() is not None:
                raise RuntimeError("API exited during startup; inspect api.log")
            try:
                health = await client.get("/health")
                if health.status_code == 200:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(.1)
        else:
            raise RuntimeError("API readiness timed out")
        assert health.json()["database"] == "sqlite"
        assert health.json()["redis"] == "ok"
        assert (await client.get("/commerce/me", headers={"Authorization": ""})).status_code == 401
        assert (await client.post("/commerce/intents", json={"buyer_id": "intruder", "raw_query": "露营灯"})).status_code == 403
        submitted = await client.post("/commerce/intents/async", json={"shopping_session_id": "restart-fixture", "raw_query": "露营灯"})
        submitted.raise_for_status()
        task = submitted.json()
        assert (await client.get("/commerce/tasks/" + task["task_id"], headers={"Authorization": "Bearer " + foreign_token})).status_code == 404
        assert (await client.post("/commerce/intents", headers={"Authorization": "Bearer " + foreign_token}, json={
            "shopping_session_id": task["shopping_session_id"], "raw_query": "继续"})).status_code == 403

        # Kill only after a native snapshot contains the completed tool result.
        database = Path(env["DATA_DIR"]) / "runtime-checkpoints.sqlite3"
        for _ in range(180):
            saved = checkpoint(database, task["task_id"])
            if saved and '"tool_result"' in saved and '"product_search_tool"' in saved:
                break
            await asyncio.sleep(.05)
        else:
            raise RuntimeError("No completed-tool checkpoint; recovery acceptance failed")
        processes[1].kill()
        processes[1].wait(timeout=5)
        evidence["killed_worker_exit_code"] = processes[1].returncode
        processes[1] = subprocess.Popen([sys.executable, "-m", "app.worker"], cwd=ROOT, env=env,
            stdout=processes[2], stderr=subprocess.STDOUT)
        for _ in range(300):
            status = await client.get("/commerce/tasks/" + task["task_id"])
            if status.json().get("state") == "done":
                break
            await asyncio.sleep(.1)
        else:
            raise RuntimeError("Task did not complete after Worker restart")
        final = status.json()["final_text"]
        assert "SYNTHETIC" in final and not final.startswith("[error]")
        # The persistent input binding must outlive the ephemeral Redis marker.
        from redis.asyncio import Redis
        redis = Redis.from_url(env["REDIS_URL"], decode_responses=True)
        try:
            repeated_headers = {"Idempotency-Key": "persistent-binding-fixture"}
            submitted_binding = await client.post("/commerce/intents/async", headers=repeated_headers,
                json={"shopping_session_id": "binding-fixture", "raw_query": "露营灯"})
            submitted_binding.raise_for_status()
            binding = submitted_binding.json()
            for _ in range(150):
                binding_status = await client.get("/commerce/tasks/" + binding["task_id"])
                if binding_status.json().get("state") == "done":
                    break
                await asyncio.sleep(.1)
            else:
                raise RuntimeError("Persistent binding fixture did not complete")
            await redis.delete("globex:enqueue:" + binding["task_id"])
            conflict = await client.post("/commerce/intents/async", headers=repeated_headers,
                json={"shopping_session_id": "binding-fixture", "raw_query": "更换参数"})
            assert conflict.status_code == 409
            intact = await client.get("/commerce/tasks/" + binding["task_id"])
            assert intact.json()["state"] == "done"
            evidence["persistent_idempotency_conflict_http"] = conflict.status_code
        finally:
            await redis.aclose()
        saved_final = checkpoint(database, task["task_id"])
        from agentscope.state import AgentState
        state = AgentState.model_validate_json(saved_final)
        results = [block for message in state.context for block in message.content
            if getattr(block, "type", "") == "tool_result" and getattr(block, "name", "") == "product_search_tool"]
        assert len(results) == 1, "Restart repeated a completed tool"
        evidence.update({"health": health.json(), "task": status.json(), "completed_tool_results": len(results),
            "api_pid": processes[0].pid, "restarted_worker_pid": processes[1].pid})

        ws_url = base_url.replace("http://", "ws://") + "/commerce/events"
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"access_token": foreign_token, "shopping_session_id": task["shopping_session_id"]}))
            try:
                await ws.recv()
                raise AssertionError("Foreign WS subscription was accepted")
            except websockets.exceptions.ConnectionClosed as error:
                assert error.code == 4403
        # Real API request, real Worker, real Redis backplane and authenticated WS.
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"access_token": token, "shopping_session_id": "ws-fixture"}))
            await asyncio.sleep(.1)
            request = asyncio.create_task(client.post("/commerce/intents", json={
                "shopping_session_id": "ws-fixture", "raw_query": "露营灯"}))
            events = []
            while not request.done():
                try:
                    events.append(json.loads(await asyncio.wait_for(ws.recv(), .5))["type"])
                except asyncio.TimeoutError:
                    pass
            response = await request
            response.raise_for_status()
            assert "SYNTHETIC" in response.json()["final_text"]
            assert "tool.result" in events
            evidence["ws_events"] = events
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        mcp_env = dict(env, GLOBEX_API_URL=base_url, GLOBEX_ACCESS_TOKEN=token)
        with (Path(env["DATA_DIR"]).parent / "mcp.log").open("w") as mcp_log:
            parameters = StdioServerParameters(command=sys.executable,
                args=["-m", "app.presentation.mcp_server"], env=mcp_env, cwd=str(ROOT))
            async with stdio_client(parameters, errlog=mcp_log) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    listing = await session.list_tools()
                    assert {tool.name for tool in listing.tools} == {"research", "task_status", "order_status"}
                    result = await session.call_tool("task_status", {"task_id": task["task_id"]})
                    assert not result.isError
                    assert '"done"' in result.content[0].text
                    research = await session.call_tool("research", {"query": "搜索露营灯", "session_id": "mcp-fixture"})
                    assert not research.isError
                    queued_research = json.loads(research.content[0].text)
                    assert queued_research["task_id"] and queued_research["state"] == "queued"
                    for _ in range(100):
                        outcome = await session.call_tool("task_status", {"task_id": queued_research["task_id"]})
                        research_status = json.loads(outcome.content[0].text)
                        if research_status["state"] == "done":
                            break
                        await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("MCP research did not complete")
                    assert "SYNTHETIC" in research_status["final_text"]
                    evidence["mcp_async_research"] = {"returned_handle": True, "final_state": research_status["state"]}
                    evidence["mcp_tools"] = [tool.name for tool in listing.tools]
        # Seed one SYNTHETIC order through the real SQL repository; the assertions
        # below exercise the HTTP approval/cancellation boundary, not an LLM purchase.
        from app.infrastructure.security.auth_identity import Principal
        from app.infrastructure.persistence.sql.repositories import create_engine, SqlOrderRepository
        from app.infrastructure.persistence.in_memory_repositories import InMemoryProductRepository
        from app.application.usecases.order_usecases import PlaceOrderUseCase, OrderItemInput
        from app.domain.order.address import Address
        principal = Principal("fixture-a", "alice", frozenset({"commerce:write", "commerce:read"}))
        engine = create_engine(env["DATABASE_URL"])
        try:
            fixture = await PlaceOrderUseCase(InMemoryProductRepository(), SqlOrderRepository(engine)).execute(
                principal.buyer_key, [OrderItemInput("P1001", "P1001-S1", 1)],
                Address(recipient_name="Fixture", country="CN", state="ZJ", city="HZ",
                    address_line="SYNTHETIC address", postal_code="00000", phone="00000"))
        finally:
            await engine.dispose()
        order_id = fixture["order_id"]
        assert (await client.get("/commerce/orders/" + order_id, headers={"Authorization": "Bearer " + foreign_token})).status_code == 404
        original_order = await client.get("/commerce/orders/" + order_id)
        original_order.raise_for_status()
        cancellation = {"reason": "SYNTHETIC cancellation", "shopping_session_id": "approval-fixture"}
        proposal = await client.post(f"/commerce/orders/{order_id}/cancel", json=cancellation)
        assert proposal.status_code == 202
        approval_id = proposal.json()["approval_id"]
        before = await client.get("/commerce/orders/" + order_id)
        assert before.json() == original_order.json(), "Unapproved cancellation changed the order"
        recovered = await client.get("/commerce/approvals", params={"shopping_session_id": "approval-fixture"})
        assert approval_id in [item["approval_id"] for item in recovered.json()["approvals"]]
        assert (await client.get("/commerce/approvals/" + approval_id, headers={"Authorization": "Bearer " + foreign_token})).status_code == 404
        approved = await client.post(f"/commerce/approvals/{approval_id}/approve")
        approved.raise_for_status()
        replaced = await client.post(f"/commerce/orders/{order_id}/cancel", json={
            **cancellation, "approval_id": approval_id, "reason": "replaced parameters"})
        assert replaced.status_code == 403
        permitted = {**cancellation, "approval_id": approval_id}
        first = await client.post(f"/commerce/approvals/{approval_id}/execute")
        replay = await client.post(f"/commerce/approvals/{approval_id}/execute")
        first.raise_for_status()
        replay.raise_for_status()
        assert first.json() == replay.json()
        assert first.json()["state"] == "completed"
        persisted = await client.get(f"/commerce/approvals/{approval_id}")
        assert persisted.json()["state"] == "completed"
        old_endpoint_replay = await client.post(f"/commerce/orders/{order_id}/cancel", json=permitted)
        old_endpoint_replay.raise_for_status()
        assert old_endpoint_replay.json() == first.json()["result"]
        evidence["approval_boundary"] = {"proposal_http": proposal.status_code,
            "parameter_substitution_http": replaced.status_code, "receipt_replay_equal": True,
            "order": first.json()["result"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url")
    parser.add_argument("--output-dir", default="artifacts/runtime-acceptance")
    parser.add_argument("--benchmark", action="store_true", help="Run three SYNTHETIC serial/bounded-concurrency repetitions")
    args = parser.parse_args()
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    data = output / ("data-" + secrets.token_hex(6))
    data.mkdir()
    container_name = None
    port = free_port()
    if args.redis_url:
        redis_url = args.redis_url
    else:
        redis_port = free_port()
        container_name = "globex-accept-" + secrets.token_hex(6)
        subprocess.run(["docker", "run", "--rm", "-d", "--name", container_name, "-p",
            f"127.0.0.1:{redis_port}:6379", "redis:7-alpine"], check=True, capture_output=True)
        redis_url = f"redis://127.0.0.1:{redis_port}/0"
    token, foreign_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    env = dict(os.environ, LLM_MODE="mock", LLM_API_KEY="", EMBEDDING_API_KEY="", TAVILY_API_KEY="",
        OTEL_EXPORTER_OTLP_ENDPOINT="", DATA_DIR=str(data), DATABASE_URL=f"sqlite+aiosqlite:///{data / 'globex.db'}",
        REDIS_URL=redis_url, QUEUE_ENABLED="1", MOCK_DELAY_SECONDS="1", QUEUE_CLAIM_IDLE_MS="1000",
        RUNTIME_LEASE_SECONDS="5", SEMANTIC_CACHE_ENABLED="0", WORKER_CONCURRENCY="2",
        GLOBEX_AUTH_TOKENS=json.dumps({token: {"tenant_id": "fixture-a", "subject": "alice",
            "permissions": ["intent:submit", "commerce:read", "commerce:write"]}, foreign_token: {
            "tenant_id": "fixture-b", "subject": "bob", "permissions": ["intent:submit", "commerce:read"]}}))
    evidence = {"evidence_status": "SYNTHETIC", "started_at": time.time(), "checks": "authentication, tenant isolation, native checkpoint, Worker kill/restart, authenticated WS"}
    api_log, worker_log = (output / "api.log").open("w"), (output / "worker.log").open("w")
    processes = []
    try:
        processes.append(subprocess.Popen([sys.executable, "-m", "uvicorn", "app.presentation.server:app", "--host", "127.0.0.1", "--port", str(port)], cwd=ROOT, env=env, stdout=api_log, stderr=subprocess.STDOUT))
        processes.append(subprocess.Popen([sys.executable, "-m", "app.worker"], cwd=ROOT, env=env, stdout=worker_log, stderr=subprocess.STDOUT))
        processes.append(worker_log)
        asyncio.run(accept(f"http://127.0.0.1:{port}", token, foreign_token, env, processes, evidence))
        if args.benchmark:
            with (output / "loadtest.log").open("w") as log:
                benchmark = subprocess.run([sys.executable, "scripts/loadtest.py", "--base-url",
                    f"http://127.0.0.1:{port}", "--stages", "1,2", "--repetitions", "3",
                    "--model-kind", "synthetic", "--output-dir", str(output / "benchmark")],
                    cwd=ROOT, env=dict(env, GLOBEX_ACCESS_TOKEN=token), stdout=log,
                    stderr=subprocess.STDOUT, timeout=150)
            evidence["benchmark_exit_code"] = benchmark.returncode
            if benchmark.returncode != 0:
                raise RuntimeError("SYNTHETIC benchmark failed; inspect loadtest.log")
        evidence["result"] = "PASS"
    except BaseException as error:
        evidence.update(result="FAIL", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        cleanup_errors = []
        for process in processes[:2]:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        cleanup_errors.append(f"Process {process.pid} did not exit after SIGKILL")
        evidence["cleanup_exit_codes"] = [process.poll() for process in processes[:2]]
        api_log.close()
        worker_log.close()
        if container_name:
            subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
        if cleanup_errors:
            evidence.update(result="FAIL", cleanup_errors=cleanup_errors)
        evidence["finished_at"] = time.time()
        (output / "result.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
        print(json.dumps({"result": evidence["result"], "evidence_status": "SYNTHETIC", "evidence": str(output / "result.json")}))
        if cleanup_errors:
            raise RuntimeError("Process cleanup failed; inspect result.json")


if __name__ == "__main__":
    main()
