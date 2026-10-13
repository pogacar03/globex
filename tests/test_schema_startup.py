import asyncio
import pytest
from sqlalchemy import text
from app.infrastructure.persistence.sql.repositories import create_engine, bootstrap_schema


async def test_parallel_api_worker_schema_bootstrap_is_serialized(tmp_path):
    from app.infrastructure.persistence.startup_lock import schema_startup_lock
    engines = [create_engine(f"sqlite+aiosqlite:///{tmp_path / 'shared.db'}") for _ in range(2)]
    async def start(engine):
        async with schema_startup_lock(tmp_path):
            await bootstrap_schema(engine)
        async with engine.connect() as connection:
            assert (await connection.execute(text("select count(*) from conversation_sessions"))).scalar() == 0
    try:
        await asyncio.gather(*(start(engine) for engine in engines))
    finally:
        await asyncio.gather(*(engine.dispose() for engine in engines))
