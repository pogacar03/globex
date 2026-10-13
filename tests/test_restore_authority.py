from types import SimpleNamespace
import pytest
from app.application.agents.main_agent import SessionRegistry


async def test_production_restore_failure_never_starts_an_empty_session():
    async def broken_load(session):
        raise OSError("fixture storage outage")
    registry = SessionRegistry(SimpleNamespace(), SimpleNamespace(load=broken_load), strict_restore=True)
    with pytest.raises(RuntimeError, match="SESSION_RESTORE_UNAVAILABLE"):
        await registry.get_or_create("fixture")


async def test_production_corrupt_snapshot_is_explicitly_rejected():
    async def corrupt_load(session):
        return "not-json"
    registry = SessionRegistry(SimpleNamespace(), SimpleNamespace(load=corrupt_load), strict_restore=True)
    with pytest.raises(RuntimeError, match="SESSION_SNAPSHOT_INVALID"):
        await registry.get_or_create("fixture")
