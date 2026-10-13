"""SQLite fencing leases and native AgentScope turn checkpoints.

The store deliberately records only task identity, AgentScope's serialized native
state, and a final response. It cannot make an external tool side effect exactly-once;
approved writes still rely on the approval ledger and its replay/uncertain decisions.
"""
from __future__ import annotations

import asyncio
import math
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class CheckpointConflictError(RuntimeError):
    """A task ID was reused for another owner, session, or input fingerprint."""


class SessionLeaseLostError(RuntimeError):
    """The caller's fencing token no longer owns the session lease."""


@dataclass(frozen=True)
class SessionLease:
    owner_id: str
    session_id: str
    holder_id: str
    fencing_token: int
    expires_at: float


@dataclass(frozen=True)
class TurnCheckpoint:
    task_id: str
    owner_id: str
    session_id: str
    input_fingerprint: str
    holder_id: str
    fencing_token: int
    state: str
    snapshot_json: Optional[str] = None
    final_text: Optional[str] = None
    replay: bool = False
    session_snapshot_json: Optional[str] = None


class RuntimeCheckpointStore:
    """Small SQLite store for serialized session leases and task recovery records."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        lease_ttl_seconds: float = 30.0,
        lease_wait_timeout_seconds: float = 90.0,
    ) -> None:
        lease_ttl_seconds = float(lease_ttl_seconds)
        lease_wait_timeout_seconds = float(lease_wait_timeout_seconds)
        if not math.isfinite(lease_ttl_seconds) or lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be finite and positive")
        if not math.isfinite(lease_wait_timeout_seconds) or lease_wait_timeout_seconds <= 0:
            raise ValueError("lease_wait_timeout_seconds must be finite and positive")
        self._db_path = str(db_path)
        self.lease_ttl_seconds = lease_ttl_seconds
        self.lease_wait_timeout_seconds = lease_wait_timeout_seconds
        path = Path(self._db_path)
        if self._db_path != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_session_leases (
                    owner_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    holder_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    lease_expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (owner_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS runtime_turn_checkpoints (
                    task_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    input_fingerprint TEXT NOT NULL,
                    holder_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('running', 'completed')),
                    snapshot_json TEXT,
                    final_text TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS runtime_turn_session_idx
                    ON runtime_turn_checkpoints(owner_id, session_id, updated_at);
                """,
            )

    async def acquire_lease(
        self, owner_id: str, session_id: str, holder_id: str,
    ) -> Optional[SessionLease]:
        return await asyncio.to_thread(
            self._acquire_lease, owner_id, session_id, holder_id,
        )

    def _acquire_lease(self, owner_id: str, session_id: str, holder_id: str) -> Optional[SessionLease]:
        now = time.time()
        expires_at = now + self.lease_ttl_seconds
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT holder_id, fencing_token, lease_expires_at FROM runtime_session_leases "
                "WHERE owner_id = ? AND session_id = ?",
                (owner_id, session_id),
            ).fetchone()
            if row is not None and row["lease_expires_at"] > now and row["holder_id"] != holder_id:
                return None
            if row is not None and row["lease_expires_at"] > now:
                fencing_token = int(row["fencing_token"])
            else:
                fencing_token = (int(row["fencing_token"]) if row is not None else 0) + 1
            connection.execute(
                "INSERT INTO runtime_session_leases "
                "(owner_id, session_id, holder_id, fencing_token, lease_expires_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(owner_id, session_id) DO UPDATE SET holder_id = excluded.holder_id, "
                "fencing_token = excluded.fencing_token, lease_expires_at = excluded.lease_expires_at, "
                "updated_at = excluded.updated_at",
                (owner_id, session_id, holder_id, fencing_token, expires_at, now),
            )
        return SessionLease(owner_id, session_id, holder_id, fencing_token, expires_at)

    async def renew_lease(
        self, owner_id: str, session_id: str, holder_id: str, fencing_token: int,
    ) -> bool:
        return await asyncio.to_thread(
            self._renew_lease, owner_id, session_id, holder_id, fencing_token,
        )

    def _renew_lease(
        self, owner_id: str, session_id: str, holder_id: str, fencing_token: int,
    ) -> bool:
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE runtime_session_leases SET lease_expires_at = ?, updated_at = ? "
                "WHERE owner_id = ? AND session_id = ? AND holder_id = ? AND fencing_token = ? "
                "AND lease_expires_at > ?",
                (now + self.lease_ttl_seconds, now, owner_id, session_id, holder_id,
                 fencing_token, now),
            )
            return cursor.rowcount == 1

    async def release_lease(
        self, owner_id: str, session_id: str, holder_id: str, fencing_token: int,
    ) -> bool:
        return await asyncio.to_thread(
            self._release_lease, owner_id, session_id, holder_id, fencing_token,
        )

    def _release_lease(
        self, owner_id: str, session_id: str, holder_id: str, fencing_token: int,
    ) -> bool:
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE runtime_session_leases SET lease_expires_at = 0, updated_at = ? "
                "WHERE owner_id = ? AND session_id = ? AND holder_id = ? AND fencing_token = ?",
                (now, owner_id, session_id, holder_id, fencing_token),
            )
            return cursor.rowcount == 1

    async def validate_binding(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
    ) -> None:
        """Reject a task ID already bound to different durable input.

        This lookup is intentionally read-only: an unknown task does not acquire
        a lease or create a turn. The eventual begin_turn still performs the
        authoritative binding check under its write transaction.
        """
        await asyncio.to_thread(
            self._validate_binding, task_id, owner_id, session_id, input_fingerprint,
        )

    def _validate_binding(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
    ) -> None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT owner_id, session_id, input_fingerprint "
                "FROM runtime_turn_checkpoints WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        if row is not None and (
            row["owner_id"] != owner_id
            or row["session_id"] != session_id
            or row["input_fingerprint"] != input_fingerprint
        ):
            raise CheckpointConflictError("task_id is already bound to different input")

    async def begin_turn(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
        holder_id: str,
        fencing_token: int,
    ) -> TurnCheckpoint:
        return await asyncio.to_thread(
            self._begin_turn, task_id, owner_id, session_id, input_fingerprint,
            holder_id, fencing_token,
        )

    def _begin_turn(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
        holder_id: str,
        fencing_token: int,
    ) -> TurnCheckpoint:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if not self._has_lease(connection, owner_id, session_id, holder_id, fencing_token, now):
                raise SessionLeaseLostError("session lease is no longer current")
            row = connection.execute(
                "SELECT * FROM runtime_turn_checkpoints WHERE task_id = ?", (task_id,),
            ).fetchone()
            if row is not None:
                if (
                    row["owner_id"] != owner_id
                    or row["session_id"] != session_id
                    or row["input_fingerprint"] != input_fingerprint
                ):
                    raise CheckpointConflictError("task_id is already bound to different input")
                if row["state"] == "completed":
                    latest = connection.execute(
                        "SELECT snapshot_json FROM runtime_turn_checkpoints "
                        "WHERE owner_id = ? AND session_id = ? AND state = 'completed' "
                        "ORDER BY updated_at DESC, created_at DESC LIMIT 1",
                        (owner_id, session_id),
                    ).fetchone()
                    return self._turn_from_row(
                        row,
                        replay=True,
                        session_snapshot_json=latest["snapshot_json"] if latest is not None else None,
                    )
                connection.execute(
                    "UPDATE runtime_turn_checkpoints SET holder_id = ?, fencing_token = ?, updated_at = ? "
                    "WHERE task_id = ? AND state = 'running'",
                    (holder_id, fencing_token, now, task_id),
                )
                row = connection.execute(
                    "SELECT * FROM runtime_turn_checkpoints WHERE task_id = ?", (task_id,),
                ).fetchone()
                latest = connection.execute(
                    "SELECT snapshot_json FROM runtime_turn_checkpoints "
                    "WHERE owner_id = ? AND session_id = ? AND state = 'completed' "
                    "ORDER BY updated_at DESC, created_at DESC LIMIT 1",
                    (owner_id, session_id),
                ).fetchone()
                return self._turn_from_row(
                    row,
                    session_snapshot_json=latest["snapshot_json"] if latest is not None else None,
                )

            latest = connection.execute(
                "SELECT snapshot_json FROM runtime_turn_checkpoints "
                "WHERE owner_id = ? AND session_id = ? AND state = 'completed' "
                "ORDER BY updated_at DESC, created_at DESC LIMIT 1",
                (owner_id, session_id),
            ).fetchone()
            connection.execute(
                "INSERT INTO runtime_turn_checkpoints "
                "(task_id, owner_id, session_id, input_fingerprint, holder_id, fencing_token, "
                "state, snapshot_json, final_text, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'running', NULL, NULL, ?, ?)",
                (task_id, owner_id, session_id, input_fingerprint, holder_id, fencing_token, now, now),
            )
            row = connection.execute(
                "SELECT * FROM runtime_turn_checkpoints WHERE task_id = ?", (task_id,),
            ).fetchone()
            return self._turn_from_row(
                row,
                session_snapshot_json=latest["snapshot_json"] if latest is not None else None,
            )

    async def save_snapshot(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
        holder_id: str,
        fencing_token: int,
        snapshot_json: str,
    ) -> bool:
        return await asyncio.to_thread(
            self._save_snapshot, task_id, owner_id, session_id, input_fingerprint,
            holder_id, fencing_token, snapshot_json,
        )

    def _save_snapshot(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
        holder_id: str,
        fencing_token: int,
        snapshot_json: str,
    ) -> bool:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if not self._has_lease(connection, owner_id, session_id, holder_id, fencing_token, now):
                return False
            cursor = connection.execute(
                "UPDATE runtime_turn_checkpoints SET snapshot_json = ?, updated_at = ? "
                "WHERE task_id = ? AND owner_id = ? AND session_id = ? AND input_fingerprint = ? "
                "AND holder_id = ? AND fencing_token = ? AND state = 'running'",
                (snapshot_json, now, task_id, owner_id, session_id, input_fingerprint,
                 holder_id, fencing_token),
            )
            return cursor.rowcount == 1

    async def complete_turn(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
        holder_id: str,
        fencing_token: int,
        snapshot_json: str,
        final_text: str,
    ) -> bool:
        return await asyncio.to_thread(
            self._complete_turn, task_id, owner_id, session_id, input_fingerprint,
            holder_id, fencing_token, snapshot_json, final_text,
        )

    def _complete_turn(
        self,
        task_id: str,
        owner_id: str,
        session_id: str,
        input_fingerprint: str,
        holder_id: str,
        fencing_token: int,
        snapshot_json: str,
        final_text: str,
    ) -> bool:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if not self._has_lease(connection, owner_id, session_id, holder_id, fencing_token, now):
                return False
            cursor = connection.execute(
                "UPDATE runtime_turn_checkpoints SET state = 'completed', snapshot_json = ?, "
                "final_text = ?, updated_at = ? WHERE task_id = ? AND owner_id = ? AND session_id = ? "
                "AND input_fingerprint = ? AND holder_id = ? AND fencing_token = ? AND state = 'running'",
                (snapshot_json, final_text, now, task_id, owner_id, session_id,
                 input_fingerprint, holder_id, fencing_token),
            )
            return cursor.rowcount == 1

    @staticmethod
    def _has_lease(
        connection: sqlite3.Connection,
        owner_id: str,
        session_id: str,
        holder_id: str,
        fencing_token: int,
        now: float,
    ) -> bool:
        row = connection.execute(
            "SELECT 1 FROM runtime_session_leases WHERE owner_id = ? AND session_id = ? "
            "AND holder_id = ? AND fencing_token = ? AND lease_expires_at > ?",
            (owner_id, session_id, holder_id, fencing_token, now),
        ).fetchone()
        return row is not None

    @staticmethod
    def _turn_from_row(
        row: sqlite3.Row,
        *,
        replay: bool = False,
        session_snapshot_json: Optional[str] = None,
    ) -> TurnCheckpoint:
        return TurnCheckpoint(
            task_id=row["task_id"],
            owner_id=row["owner_id"],
            session_id=row["session_id"],
            input_fingerprint=row["input_fingerprint"],
            holder_id=row["holder_id"],
            fencing_token=int(row["fencing_token"]),
            state=row["state"],
            snapshot_json=row["snapshot_json"],
            final_text=row["final_text"],
            replay=replay,
            session_snapshot_json=session_snapshot_json,
        )
