"""Durable, single-use write approvals backed by SQLite."""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ApprovalError(ValueError):
    """Raised when an approval cannot be read, approved, or finalized."""


@dataclass(frozen=True, slots=True)
class WritePermit:
    decision: str
    approval_id: str
    result: Any = None


def _normalize_json(value: Any) -> Any:
    """Validate and normalize JSON values without implicit key coercion."""
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("non-finite numbers are not supported")
        return value
    if type(value) is list:
        return [_normalize_json(item) for item in value]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: _normalize_json(value[key]) for key in sorted(value)}
    raise ValueError("value must contain JSON-compatible data")


def _canonical_json(value: dict[str, Any]) -> str:
    if type(value) is not dict:
        raise ValueError("arguments must be a JSON object")
    normalized = _normalize_json(value)
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _require_identity(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256 or "\0" in value:
        raise ValueError(f"invalid {name}")


class ApprovalStore:
    """Persistent approval state with atomic one-time execution claims."""

    def __init__(self, db_path: Path, ttl_seconds: float = 300) -> None:
        if not math.isfinite(ttl_seconds) or ttl_seconds < 0:
            raise ValueError("ttl_seconds must be finite and non-negative")
        self._db_path = Path(db_path)
        self._ttl_seconds = float(ttl_seconds)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS approvals (
                    approval_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    session TEXT NOT NULL,
                    tool TEXT NOT NULL,
                    arguments_json TEXT NOT NULL,
                    arguments_hash TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN ('pending', 'approved', 'executing', 'completed', 'uncertain', 'expired')
                    ),
                    result_json TEXT
                )
                """,
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS approvals_owner_created "
                "ON approvals(owner, created_at DESC)",
            )
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self._db_path), timeout=30, isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    async def propose(
        self,
        owner: str,
        session: str,
        tool: str,
        arguments: dict,
    ) -> dict:
        return await asyncio.to_thread(self._propose, owner, session, tool, arguments)

    def _propose(self, owner: str, session: str, tool: str, arguments: dict) -> dict:
        _require_identity(owner, "owner")
        _require_identity(session, "session")
        _require_identity(tool, "tool")
        arguments_json = _canonical_json(arguments)
        arguments_hash = hashlib.sha256(arguments_json.encode("utf-8")).hexdigest()
        created_at = time.time()
        expires_at = created_at + self._ttl_seconds
        approval_id = uuid.uuid4().hex
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO approvals (
                    approval_id, owner, session, tool, arguments_json,
                    arguments_hash, created_at, expires_at, state, result_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL)
                """,
                (
                    approval_id, owner, session, tool, arguments_json,
                    arguments_hash, created_at, expires_at,
                ),
            )
        finally:
            connection.close()
        return {
            "approval_id": approval_id,
            "state": "pending",
            "tool": tool,
            "shopping_session_id": session,
            "arguments": json.loads(arguments_json),
            "expires_at": expires_at,
        }

    async def approve(self, owner: str, approval_id: str) -> dict:
        return await asyncio.to_thread(self._approve, owner, approval_id)

    def _approve(self, owner: str, approval_id: str) -> dict:
        _require_identity(owner, "owner")
        if not isinstance(approval_id, str) or not approval_id:
            raise ApprovalError("approval unavailable")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
            ).fetchone()
            if row is None or row["owner"] != owner:
                connection.rollback()
                raise ApprovalError("approval unavailable")
            now = time.time()
            if row["expires_at"] <= now:
                connection.execute(
                    "UPDATE approvals SET state = 'expired' "
                    "WHERE approval_id = ? AND state IN ('pending', 'approved')",
                    (approval_id,),
                )
                connection.commit()
                raise ApprovalError("approval expired")
            if row["state"] != "pending":
                connection.rollback()
                raise ApprovalError("approval cannot be approved in its current state")
            updated = connection.execute(
                "UPDATE approvals SET state = 'approved' "
                "WHERE approval_id = ? AND owner = ? AND state = 'pending' AND expires_at > ?",
                (approval_id, owner, now),
            )
            if updated.rowcount != 1:
                connection.rollback()
                raise ApprovalError("approval unavailable")
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
            ).fetchone()
            connection.commit()
            return self._public_record(row)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    async def get(self, owner: str, approval_id: str) -> dict:
        return await asyncio.to_thread(self._get, owner, approval_id)

    def _get(self, owner: str, approval_id: str) -> dict:
        _require_identity(owner, "owner")
        if not isinstance(approval_id, str) or not approval_id:
            raise ApprovalError("approval unavailable")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
            ).fetchone()
            if row is None or row["owner"] != owner:
                connection.rollback()
                raise ApprovalError("approval unavailable")
            if row["expires_at"] <= time.time() and row["state"] in ("pending", "approved"):
                connection.execute(
                    "UPDATE approvals SET state = 'expired' WHERE approval_id = ?",
                    (approval_id,),
                )
                row = connection.execute(
                    "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
                ).fetchone()
            connection.commit()
            return self._public_record(row)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    async def begin(
        self,
        owner: str,
        session: str,
        tool: str,
        arguments: dict,
        approval_id: str | None = None,
    ) -> WritePermit:
        if approval_id is None:
            proposal = await self.propose(owner, session, tool, arguments)
            return WritePermit("REQUIRE_APPROVAL", proposal["approval_id"])
        _require_identity(owner, "owner")
        _require_identity(session, "session")
        _require_identity(tool, "tool")
        arguments_json = _canonical_json(arguments)
        arguments_hash = hashlib.sha256(arguments_json.encode("utf-8")).hexdigest()
        return await asyncio.to_thread(
            self._begin,
            owner,
            session,
            tool,
            arguments_json,
            arguments_hash,
            approval_id,
        )

    def _begin(
        self,
        owner: str,
        session: str,
        tool: str,
        arguments_json: str,
        arguments_hash: str,
        approval_id: str,
    ) -> WritePermit:
        if not isinstance(approval_id, str) or not approval_id:
            return WritePermit("DENY", str(approval_id or ""))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
            ).fetchone()
            if row is None:
                connection.rollback()
                return WritePermit("DENY", approval_id)
            if (
                row["owner"] != owner
                or row["session"] != session
                or row["tool"] != tool
                or row["arguments_hash"] != arguments_hash
                or row["arguments_json"] != arguments_json
            ):
                connection.rollback()
                return WritePermit("DENY", approval_id)

            state = row["state"]
            if state == "completed":
                result = json.loads(row["result_json"])
                connection.commit()
                return WritePermit("REPLAY", approval_id, result)
            if state in ("executing", "uncertain"):
                connection.commit()
                return WritePermit("RECONCILIATION_REQUIRED", approval_id)
            now = time.time()
            if row["expires_at"] <= now:
                if state in ("pending", "approved"):
                    connection.execute(
                        "UPDATE approvals SET state = 'expired' WHERE approval_id = ?",
                        (approval_id,),
                    )
                connection.commit()
                return WritePermit("DENY", approval_id)
            if state != "approved":
                connection.commit()
                return WritePermit("DENY", approval_id)
            changed = connection.execute(
                "UPDATE approvals SET state = 'executing' "
                "WHERE approval_id = ? AND state = 'approved' AND expires_at > ?",
                (approval_id, now),
            )
            if changed.rowcount != 1:
                connection.commit()
                return WritePermit("DENY", approval_id)
            connection.commit()
            return WritePermit("ALLOW", approval_id)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    async def complete(self, approval_id: str, result: Any) -> dict:
        result_json = json.dumps(
            _normalize_json(result),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return await asyncio.to_thread(self._complete, approval_id, result_json)

    def _complete(self, approval_id: str, result_json: str) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                "UPDATE approvals SET state = 'completed', result_json = ? "
                "WHERE approval_id = ? AND state = 'executing'",
                (result_json, approval_id),
            )
            if changed.rowcount != 1:
                connection.rollback()
                raise ApprovalError("approval is not executing")
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
            ).fetchone()
            connection.commit()
            return self._public_record(row)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    async def mark_uncertain(self, approval_id: str) -> dict:
        return await asyncio.to_thread(self._mark_uncertain, approval_id)

    def _mark_uncertain(self, approval_id: str) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                "UPDATE approvals SET state = 'uncertain' "
                "WHERE approval_id = ? AND state = 'executing'",
                (approval_id,),
            )
            if changed.rowcount != 1:
                connection.rollback()
                raise ApprovalError("approval is not executing")
            row = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,),
            ).fetchone()
            connection.commit()
            return self._public_record(row)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    async def pending(self, owner: str, session: str) -> list[dict]:
        _require_identity(owner, "owner")
        _require_identity(session, "session")
        return await asyncio.to_thread(self._pending, owner, session)

    def _pending(self, owner: str, session: str) -> list[dict]:
        connection = self._connect()
        try:
            rows = connection.execute("SELECT * FROM approvals WHERE owner=? AND session=? "
                "AND ((state IN ('pending', 'approved') AND expires_at>?) "
                "OR state IN ('executing', 'uncertain')) ORDER BY created_at DESC LIMIT 100",
                (owner, session, time.time())).fetchall()
            return [self._public_record(row) for row in rows]
        finally:
            connection.close()

    @staticmethod
    def _public_record(row: sqlite3.Row) -> dict:
        result = {
            "approval_id": row["approval_id"],
            "state": row["state"],
            "tool": row["tool"],
            "shopping_session_id": row["session"],
            "arguments": json.loads(row["arguments_json"]),
            "expires_at": row["expires_at"],
        }
        if row["result_json"] is not None:
            result["result"] = json.loads(row["result_json"])
        return result
