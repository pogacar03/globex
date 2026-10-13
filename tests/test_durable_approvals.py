from __future__ import annotations

import asyncio

import pytest

from app.infrastructure.security.approval_store import ApprovalError, ApprovalStore


def _arguments() -> dict:
    return {"order_id": "ord-1042", "reason": "buyer requested cancellation"}


@pytest.mark.asyncio
async def test_begin_without_approval_proposes_a_durable_confirmation(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite", ttl_seconds=300)

    permit = await store.begin("buyer-a", "session-a", "cancel_order", _arguments())
    proposal = await store.get("buyer-a", permit.approval_id)

    assert permit.decision == "REQUIRE_APPROVAL"
    assert proposal["state"] == "pending"
    assert proposal["tool"] == "cancel_order"
    assert proposal["arguments"] == _arguments()
    assert proposal["expires_at"] > 0


@pytest.mark.asyncio
async def test_completed_approval_replays_receipt_after_store_restart(tmp_path):
    path = tmp_path / "approvals.sqlite"
    first_store = ApprovalStore(path, ttl_seconds=300)
    proposal = await first_store.propose(
        "buyer-a", "session-a", "cancel_order", _arguments(),
    )
    await first_store.approve("buyer-a", proposal["approval_id"])
    claimed = await first_store.begin(
        "buyer-a", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
    )
    assert claimed.decision == "ALLOW"
    receipt = {"order_id": "ord-1042", "state": "cancelled"}
    await first_store.complete(proposal["approval_id"], receipt)

    restarted = ApprovalStore(path, ttl_seconds=300)
    replay = await restarted.begin(
        "buyer-a", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
    )

    assert replay.decision == "REPLAY"
    assert replay.result == receipt


@pytest.mark.asyncio
async def test_concurrent_claim_has_one_executor_and_one_reconciliation_result(tmp_path):
    path = tmp_path / "approvals.sqlite"
    first = ApprovalStore(path)
    second = ApprovalStore(path)
    proposal = await first.propose("buyer-a", "session-a", "cancel_order", _arguments())
    await first.approve("buyer-a", proposal["approval_id"])

    results = await asyncio.gather(*[
        store.begin(
            "buyer-a", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
        )
        for store in (first, second)
    ])

    assert sorted(result.decision for result in results) == ["ALLOW", "RECONCILIATION_REQUIRED"]


@pytest.mark.asyncio
async def test_parameter_substitution_cannot_use_approved_write(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    proposal = await store.propose("buyer-a", "session-a", "cancel_order", _arguments())
    await store.approve("buyer-a", proposal["approval_id"])

    changed = {**_arguments(), "order_id": "ord-9999"}
    denied = await store.begin(
        "buyer-a", "session-a", "cancel_order", changed, proposal["approval_id"],
    )

    assert denied.decision == "DENY"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner", "session", "tool"),
    [
        ("buyer-b", "session-a", "cancel_order"),
        ("buyer-a", "session-b", "cancel_order"),
        ("buyer-a", "session-a", "delete_account"),
    ],
)
async def test_owner_session_or_tool_substitution_is_denied(tmp_path, owner, session, tool):
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    proposal = await store.propose("buyer-a", "session-a", "cancel_order", _arguments())
    await store.approve("buyer-a", proposal["approval_id"])

    denied = await store.begin(owner, session, tool, _arguments(), proposal["approval_id"])

    assert denied.decision == "DENY"


@pytest.mark.asyncio
async def test_expired_approval_cannot_be_approved_or_executed(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite", ttl_seconds=0)
    proposal = await store.propose("buyer-a", "session-a", "cancel_order", _arguments())

    with pytest.raises(ApprovalError):
        await store.approve("buyer-a", proposal["approval_id"])
    denied = await store.begin(
        "buyer-a", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
    )

    assert denied.decision == "DENY"


@pytest.mark.asyncio
async def test_foreign_owner_cannot_read_or_approve_an_approval(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    proposal = await store.propose("buyer-a", "session-a", "cancel_order", _arguments())

    with pytest.raises(ApprovalError):
        await store.get("buyer-b", proposal["approval_id"])
    with pytest.raises(ApprovalError):
        await store.approve("buyer-b", proposal["approval_id"])
    denied = await store.begin(
        "buyer-b", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
    )

    assert denied.decision == "DENY"


@pytest.mark.asyncio
async def test_executing_or_uncertain_write_requires_reconciliation(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    proposal = await store.propose("buyer-a", "session-a", "cancel_order", _arguments())
    await store.approve("buyer-a", proposal["approval_id"])
    first = await store.begin(
        "buyer-a", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
    )
    await store.mark_uncertain(proposal["approval_id"])

    retry = await store.begin(
        "buyer-a", "session-a", "cancel_order", _arguments(), proposal["approval_id"],
    )

    assert first.decision == "ALLOW"
    assert retry.decision == "RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
async def test_nested_argument_key_order_is_canonicalized(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite")
    first_arguments = {"payload": {"b": [2, {"y": 4, "x": 3}], "a": 1}}
    reordered = {"payload": {"a": 1, "b": [2, {"x": 3, "y": 4}]}}
    proposal = await store.propose("buyer-a", "session-a", "write", first_arguments)
    await store.approve("buyer-a", proposal["approval_id"])

    permit = await store.begin(
        "buyer-a", "session-a", "write", reordered, proposal["approval_id"],
    )

    assert permit.decision == "ALLOW"


@pytest.mark.asyncio
async def test_non_string_argument_keys_are_rejected_to_avoid_json_key_collisions(tmp_path):
    store = ApprovalStore(tmp_path / "approvals.sqlite")

    with pytest.raises(ValueError):
        await store.propose("buyer-a", "session-a", "write", {1: "value"})
async def test_pending_approvals_can_be_recovered_after_event_disconnect(tmp_path):
    from app.infrastructure.security.approval_store import ApprovalStore
    store = ApprovalStore(tmp_path / "recovery.sqlite3")
    first = await store.propose("alice", "session-a", "create_order_tool", {"quantity": 1})
    await store.propose("bob", "session-a", "create_order_tool", {"quantity": 2})
    await store.propose("alice", "session-b", "create_order_tool", {"quantity": 3})
    assert [item["approval_id"] for item in await store.pending("alice", "session-a")] == [first["approval_id"]]


async def test_uncertain_write_remains_visible_for_reconciliation_after_ttl(tmp_path):
    import sqlite3
    from app.infrastructure.security.approval_store import ApprovalStore
    path = tmp_path / "uncertain.sqlite3"
    store = ApprovalStore(path)
    proposal = await store.propose("alice", "session-a", "create_order_tool", {"quantity": 1})
    await store.approve("alice", proposal["approval_id"])
    permit = await store.begin("alice", "session-a", "create_order_tool", {"quantity": 1}, proposal["approval_id"])
    assert permit.decision == "ALLOW"
    await store.mark_uncertain(proposal["approval_id"])
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE approvals SET expires_at=0 WHERE approval_id=?", (proposal["approval_id"],))
    recovered = await store.pending("alice", "session-a")
    assert len(recovered) == 1 and recovered[0]["state"] == "uncertain"
