from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from app.application.agents.orchestrator import MainAgentOrchestrator, SubmitIntentInput
from app.infrastructure.runtime_checkpoint import (
    CheckpointConflictError,
    RuntimeCheckpointStore,
)

def _intent(**changes) -> SubmitIntentInput:
    values = {
        "shopping_session_id": "session-1",
        "buyer_id": "buyer-1",
        "locale": "zh-CN",
        "currency": "CNY",
        "raw_query": "露营灯",
        "task_id": "task-1",
        "approval_id": "approval-1",
        "permissions": ("commerce:read", "intent:submit"),
    }
    values.update(changes)
    return SubmitIntentInput(**values)


def test_submit_intent_exposes_the_canonical_checkpoint_fingerprint():
    intent = _intent(permissions=("intent:submit", "commerce:read"))
    canonical = json.dumps(
        {
            "owner_id": intent.buyer_id,
            "session_id": intent.shopping_session_id,
            "locale": intent.locale,
            "currency": intent.currency,
            "raw_query": intent.raw_query,
            "approval_id": intent.approval_id,
            "permissions": sorted(intent.permissions),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    assert intent.fingerprint() == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert intent.fingerprint() == MainAgentOrchestrator._input_fingerprint(intent)
    assert intent.fingerprint() == _intent().fingerprint()
    assert intent.fingerprint() != _intent(raw_query="不同查询").fingerprint()


@pytest.mark.asyncio
async def test_binding_validation_is_read_only_for_an_unknown_task(tmp_path):
    db_path = tmp_path / "runtime.sqlite3"
    store = RuntimeCheckpointStore(db_path)

    assert await store.validate_binding("missing-task", "buyer-1", "session-1", "fingerprint") is None

    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runtime_session_leases").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM runtime_turn_checkpoints").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_task_binding_survives_store_restart_and_rejects_conflicts(tmp_path):
    db_path = tmp_path / "runtime.sqlite3"
    first_store = RuntimeCheckpointStore(db_path)
    lease = await first_store.acquire_lease("buyer-1", "session-1", "worker-1")
    turn = await first_store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1",
        lease.holder_id, lease.fencing_token,
    )
    assert await first_store.complete_turn(
        turn.task_id, turn.owner_id, turn.session_id, turn.input_fingerprint,
        lease.holder_id, lease.fencing_token, '{"native":"final"}', "finished",
    )
    await first_store.release_lease(
        lease.owner_id, lease.session_id, lease.holder_id, lease.fencing_token,
    )

    restarted_store = RuntimeCheckpointStore(db_path)
    assert await restarted_store.validate_binding(
        "task-1", "buyer-1", "session-1", "fingerprint-1",
    ) is None
    for owner_id, session_id, fingerprint in (
        ("buyer-2", "session-1", "fingerprint-1"),
        ("buyer-1", "session-2", "fingerprint-1"),
        ("buyer-1", "session-1", "fingerprint-2"),
    ):
        with pytest.raises(CheckpointConflictError):
            await restarted_store.validate_binding(
                "task-1", owner_id, session_id, fingerprint,
            )
