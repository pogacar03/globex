"""Trusted identity primitives for tenant-scoped application state."""
from __future__ import annotations

import hashlib
import json
import re
import secrets
from dataclasses import dataclass
from typing import Any
import unicodedata


class AuthError(ValueError):
    """Raised when a credential or identity-scoped value is not trusted."""


def _validate_component(value: str, name: str, *, max_length: int = 256) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > max_length
        or any(unicodedata.category(character) == "Cc" for character in value)
    ):
        raise ValueError(f"invalid {name}")


def _owner_digest(tenant_id: str, subject: str) -> str:
    value = f"{tenant_id}\0{subject}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class Principal:
    tenant_id: str
    subject: str
    permissions: frozenset[str]

    def __post_init__(self) -> None:
        _validate_component(self.tenant_id, "tenant_id")
        _validate_component(self.subject, "subject")
        try:
            permissions = frozenset(self.permissions)
        except TypeError as exc:
            raise ValueError("permissions must be an iterable of names") from exc
        for permission in permissions:
            _validate_component(permission, "permission", max_length=128)
        object.__setattr__(self, "permissions", permissions)

    @property
    def buyer_key(self) -> str:
        return "buyer-" + _owner_digest(self.tenant_id, self.subject)

    def scope_session(self, raw: str | None) -> str:
        """Return a bounded session key tied to this tenant and subject.

        Already-scoped keys are idempotent for their owner and rejected for
        every other owner. Raw client identifiers use a deliberately small
        alphabet so they cannot become paths or unbounded storage keys.
        """
        owner = _owner_digest(self.tenant_id, self.subject)
        if raw is None:
            raw = secrets.token_urlsafe(18)
        if not isinstance(raw, str):
            raise AuthError("invalid session identifier")

        scoped = re.fullmatch(r"s-([0-9a-f]{32})-([A-Za-z0-9_-]{1,128})", raw)
        if scoped:
            if scoped.group(1) != owner:
                raise AuthError("session belongs to another owner")
            return raw
        if raw.startswith("s-") or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", raw):
            raise AuthError("invalid session identifier")
        return f"s-{owner}-{raw}"


class AuthDirectory:
    """In-memory lookup of token hashes to immutable principals.

    The configured bearer tokens are opaque and never retained after parsing.
    """

    def __init__(self, principals_by_token_hash: dict[str, Principal]) -> None:
        self._principals_by_token_hash = dict(principals_by_token_hash)

    @classmethod
    def from_json(cls, value: str) -> "AuthDirectory":
        try:
            records: Any = json.loads(value)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid authentication configuration") from exc
        if not isinstance(records, dict):
            raise ValueError("authentication configuration must be an object")

        principals: dict[str, Principal] = {}
        for token, record in records.items():
            if not isinstance(token, str) or len(token) < 32:
                raise ValueError("configured authentication keys must be at least 32 characters")
            if not isinstance(record, dict):
                raise ValueError("authentication records must be objects")
            tenant_id = record.get("tenant_id")
            subject = record.get("subject")
            permissions = record.get("permissions")
            _validate_component(tenant_id, "tenant_id")
            _validate_component(subject, "subject")
            if (
                not isinstance(permissions, list)
                or any(not isinstance(permission, str) or not permission for permission in permissions)
            ):
                raise ValueError("authentication record permissions must be a list of names")
            token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
            principals[token_hash] = Principal(
                tenant_id=tenant_id,
                subject=subject,
                permissions=frozenset(permissions),
            )
        return cls(principals)

    def authenticate(self, token: str | None) -> Principal:
        if not isinstance(token, str) or not token:
            raise AuthError("invalid bearer token")
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        principal = self._principals_by_token_hash.get(token_hash)
        if principal is None:
            raise AuthError("invalid bearer token")
        return principal
