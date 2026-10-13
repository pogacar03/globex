# Globex Runtime Hardening Implementation Plan

> For agentic workers: use superpowers:executing-plans for task-by-task implementation; existing user authorization supplies direct execution and up to three isolated subagents.

**Goal:** Fix demonstrated authorization, recovery and measurement gaps without replacing the existing Agent application.
**Architecture:** Trusted HTTP identity produces namespaced storage/session keys; durable approvals guard writes; Redis recovery and native snapshots preserve work; gateway and evaluation report actual failures and usage.
**Tech Stack:** Python 3.11, locked AgentScope 2.0.6, SQLite, Redis, FastAPI, React/TypeScript, Node 22.22.2.
**Spec:** ../specs/2026-10-13-runtime-hardening-design.md

**Execution record:** 本轮实现与后续审查修复已完成，最终实际命令、448项回归、12项前端契约测试、进程恢复和三次离线对照见 [验收记录](../../runtime-acceptance.md)。以下为初始计划，最终设计及未实现边界以 [运行时设计](../../runtime-hardening.md) 为准。

## Global constraints
- Preserve existing 291 tests and domain/retrieval behavior; no credentials in source or logs.
- No remote push, service publication, real purchase or model call without existing credentials.
- Mock execution is SYNTHETIC; skipped or unrun external checks are UNVERIFIED.

## Review focus
- Same subject or session ID across tenants must not share stored state.
- A confirmation cannot authorize modified arguments or another owner.
- Crash after a write but before a receipt cannot cause automatic duplicate effects.
- Slow active tasks cannot be reclaimed as abandoned work.
- HTTP 200 with an error reply cannot improve benchmark success rate.

## Task 1: Trusted identity and approval ledger
Files: create app/infrastructure/security/auth_identity.py, approval_store.py; tests/test_identity_authority.py, test_durable_approvals.py. These files belong exclusively to security implementer.
- [ ] Write and run RED tests for missing/invalid tokens, scope collisions, foreign session IDs, expired approvals, parameter substitution, concurrent claim and receipt replay.
- [ ] Implement the interfaces in the spec with SQLite atomic state changes and deterministic canonicalization.
- [ ] Run focused tests; report exact commands and results without touching shared integration files.

## Task 2: Redis/worker reliability
Files: app/infrastructure/queue/redis_stream_queue.py, app/domain/queue/ports/task_queue.py, app/worker.py, tests/test_queue_recovery.py, original queue tests when extending contract.
- [ ] RED tests for actual consume-loop reclaim, both streams, global concurrency cap, pending heartbeat, dead-letter terminal status and [error] result failure.
- [ ] Implement bounded reclaim/heartbeat and owner-bearing statuses; carry approval_id through the task envelope.
- [ ] Verify focused tests and, when available, actual Redis integration.

## Task 3: Gateway and evidence quality
Files: app/infrastructure/llm.py, app/infrastructure/budget.py, scripts/loadtest.py, tests/test_gateway_governance.py, tests/test_loadtest_correctness.py. Model implementer owns these exclusively.
- [ ] RED tests for 200 error payload miscount, cancellation/timeout, nonretryable 4xx, finite fallback, missing usage and stable route selection.
- [ ] Fix logic; emit repeated raw JSON/CSV and nonzero verdict for failed business requests.
- [ ] Verify focused tests and offline repetitions; do not call real providers.

## Task 4: Integration and UI
Root owns settings.py, composition.py, server.py, connection.py, DTO/context/orchestrator/main-agent/order-tool changes, frontend and new integration tests.
- [ ] RED HTTP tests for unauthenticated request, buyer override, foreign task/order, unapproved cancellation and rejected WS subscription.
- [ ] Bind server identities, namespaced sessions, durable approval service and task owner checks; preserve exposed business features.
- [ ] Add native safe-boundary snapshots and same-session serialization; test actual recovery behavior.
- [ ] Add frontend token identity and approval UI; typecheck/build.
- [ ] Add deterministic offline AgentScope model and actual end-to-end acceptance.

## Task 5: Verification and delivery
Root owns docs, scripts acceptance, Docker and .github/workflows.
- [ ] Run all tests and frontend build; fix integration failures, retaining original assertions.
- [ ] Run isolated Redis/process recovery and offline E2E; record commands, exit codes and artifacts.
- [ ] Run at least three benchmark repetitions; report measured results and unverified online metrics.
- [ ] Add reproducible CI and startup instructions; inspect diff for secrets, unfinished paths and regressions.
- [ ] Deliver architecture sequence diagrams, source pointers and truthful result matrix; do not claim remote CI or online provider success.
