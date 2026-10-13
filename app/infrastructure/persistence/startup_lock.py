"""Linux local bootstrap lock: API and Worker share one SQLite directory."""
import asyncio
from contextlib import asynccontextmanager
import fcntl
from pathlib import Path


@asynccontextmanager
async def schema_startup_lock(data_dir: Path):
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / ".schema-startup.lock").open("a") as handle:
        try:
            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(.05)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
