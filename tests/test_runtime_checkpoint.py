from __future__ import annotations

import asyncio
import math

import pytest

from app.infrastructure.runtime_checkpoint import (
    CheckpointConflictError,
    RuntimeCheckpointStore,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("lease_ttl_seconds", [math.nan, math.inf, -math.inf])
async def test_nonfinite_lease_ttl_is_rejected(tmp_path, lease_ttl_seconds):
    with pytest.raises(ValueError, match="finite and positive"):
        RuntimeCheckpointStore(
            tmp_path / "runtime.sqlite3",
            lease_ttl_seconds=lease_ttl_seconds,
        )


async def test_session_lease_is_exclusive_and_fencing_token_increases(tmp_path):
    store = RuntimeCheckpointStore(tmp_path / "runtime.sqlite3", lease_ttl_seconds=0.05)
    first = await store.acquire_lease("buyer-1", "session-1", "worker-1")
    assert first is not None
    assert await store.acquire_lease("buyer-1", "session-1", "worker-2") is None

    assert await store.renew_lease(
        first.owner_id, first.session_id, first.holder_id, first.fencing_token,
    )
    await asyncio.sleep(0.06)
    second = await store.acquire_lease("buyer-1", "session-1", "worker-2")
    assert second is not None
    assert second.fencing_token > first.fencing_token
    assert not await store.renew_lease(
        first.owner_id, first.session_id, first.holder_id, first.fencing_token,
    )


async def test_turn_binds_task_owner_session_and_fingerprint_and_replays_completion(tmp_path):
    store = RuntimeCheckpointStore(tmp_path / "runtime.sqlite3")
    first_lease = await store.acquire_lease("buyer-1", "session-1", "worker-1")
    turn = await store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1",
        first_lease.holder_id, first_lease.fencing_token,
    )
    assert turn.state == "running" and not turn.replay
    assert await store.save_snapshot(
        "task-1", "buyer-1", "session-1", "fingerprint-1", first_lease.holder_id,
        first_lease.fencing_token, '{"native":"tool-result"}',
    )
    assert await store.complete_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1", first_lease.holder_id,
        first_lease.fencing_token, '{"native":"final"}', "finished",
    )
    await store.release_lease(
        first_lease.owner_id, first_lease.session_id, first_lease.holder_id,
        first_lease.fencing_token,
    )

    second_lease = await store.acquire_lease("buyer-1", "session-1", "worker-2")
    replay = await store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1",
        second_lease.holder_id, second_lease.fencing_token,
    )
    assert replay.replay and replay.state == "completed"
    assert replay.final_text == "finished"
    assert replay.snapshot_json == '{"native":"final"}'

    with pytest.raises(CheckpointConflictError):
        await store.begin_turn(
            "task-1", "buyer-1", "session-1", "fingerprint-2",
            second_lease.holder_id, second_lease.fencing_token,
        )
    with pytest.raises(CheckpointConflictError):
        other_owner_lease = await store.acquire_lease("buyer-2", "session-1", "worker-2")
        await store.begin_turn(
            "task-1", "buyer-2", "session-1", "fingerprint-1",
            other_owner_lease.holder_id, other_owner_lease.fencing_token,
        )


async def test_expired_fence_cannot_save_or_complete_turn(tmp_path):
    store = RuntimeCheckpointStore(tmp_path / "runtime.sqlite3", lease_ttl_seconds=0.04)
    first = await store.acquire_lease("buyer-1", "session-1", "worker-1")
    await store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1", first.holder_id,
        first.fencing_token,
    )
    await asyncio.sleep(0.06)
    second = await store.acquire_lease("buyer-1", "session-1", "worker-2")
    assert second is not None
    assert second.fencing_token > first.fencing_token

    assert not await store.save_snapshot(
        "task-1", "buyer-1", "session-1", "fingerprint-1", first.holder_id,
        first.fencing_token, '{"stale":true}',
    )
    resumed = await store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1", second.holder_id,
        second.fencing_token,
    )
    assert resumed.state == "running" and resumed.snapshot_json is None


async def test_replaying_older_turn_exposes_latest_session_snapshot(tmp_path):
    store = RuntimeCheckpointStore(tmp_path / "runtime.sqlite3")
    lease = await store.acquire_lease("buyer-1", "session-1", "worker-1")

    first = await store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1", lease.holder_id,
        lease.fencing_token,
    )
    assert await store.complete_turn(
        first.task_id, first.owner_id, first.session_id, first.input_fingerprint,
        lease.holder_id, lease.fencing_token, '{"turn":1}', "first result",
    )
    second = await store.begin_turn(
        "task-2", "buyer-1", "session-1", "fingerprint-2", lease.holder_id,
        lease.fencing_token,
    )
    assert await store.complete_turn(
        second.task_id, second.owner_id, second.session_id, second.input_fingerprint,
        lease.holder_id, lease.fencing_token, '{"turn":2}', "second result",
    )

    replay = await store.begin_turn(
        "task-1", "buyer-1", "session-1", "fingerprint-1", lease.holder_id,
        lease.fencing_token,
    )
    assert replay.replay
    assert replay.snapshot_json == '{"turn":1}'
    assert replay.session_snapshot_json == '{"turn":2}'
