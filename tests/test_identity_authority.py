from __future__ import annotations

import hashlib
import json

import pytest

from app.infrastructure.security.auth_identity import AuthDirectory, AuthError


def _config(token: str, tenant: str = "tenant-a", subject: str = "buyer-1") -> str:
    return json.dumps({
        token: {
            "tenant_id": tenant,
            "subject": subject,
            "permissions": ["orders:read", "orders:write"],
        },
    })


def test_authenticate_uses_opaque_token_and_immutable_principal():
    token = "clear-test-token-value-that-is-long-enough-0001"
    directory = AuthDirectory.from_json(_config(token))

    principal = directory.authenticate(token)

    assert principal.tenant_id == "tenant-a"
    assert principal.subject == "buyer-1"
    assert principal.permissions == frozenset({"orders:read", "orders:write"})
    with pytest.raises((AttributeError, TypeError)):
        principal.tenant_id = "tenant-b"
    with pytest.raises(AuthError):
        directory.authenticate("missing-token")


def test_auth_directory_stores_only_token_hashes():
    token = "clear-test-token-value-that-is-long-enough-0002"
    directory = AuthDirectory.from_json(_config(token))

    assert token not in repr(directory)
    stored = repr(vars(directory))
    assert token not in stored
    assert hashlib.sha256(token.encode()).hexdigest() in stored


def test_auth_directory_rejects_short_token_keys():
    with pytest.raises(ValueError):
        AuthDirectory.from_json(_config("too-short"))


@pytest.mark.parametrize(
    ("tenant", "subject"),
    [
        ("tenant-a\0alias", "buyer-1"),
        ("tenant-a", "buyer\0alias"),
        ("tenant-a\nb", "buyer-1"),
        ("tenant-a", "buyer-1\x7f"),
        ("t" * 257, "buyer-1"),
        ("tenant-a", "b" * 257),
    ],
)
def test_auth_directory_rejects_unbounded_or_controlled_identity_components(tenant, subject):
    token = "clear-test-token-value-that-is-long-enough-0009"

    with pytest.raises(ValueError):
        AuthDirectory.from_json(_config(token, tenant, subject))


def test_nul_delimiters_cannot_make_distinct_principals_share_a_buyer_key():
    token_a = "clear-test-token-value-that-is-long-enough-0010"
    token_b = "clear-test-token-value-that-is-long-enough-0011"
    config = json.dumps({
        token_a: {"tenant_id": "a\0b", "subject": "c", "permissions": []},
        token_b: {"tenant_id": "a", "subject": "b\0c", "permissions": []},
    })

    with pytest.raises(ValueError):
        AuthDirectory.from_json(config)


def test_missing_auth_configuration_does_not_create_a_default_user():
    directory = AuthDirectory.from_json("{}")

    with pytest.raises(AuthError):
        directory.authenticate(None)


def test_same_subject_in_different_tenants_has_distinct_storage_scopes():
    token_a = "clear-test-token-value-that-is-long-enough-0003"
    token_b = "clear-test-token-value-that-is-long-enough-0004"
    directory = AuthDirectory.from_json(json.dumps({
        token_a: {"tenant_id": "tenant-a", "subject": "same-user", "permissions": []},
        token_b: {"tenant_id": "tenant-b", "subject": "same-user", "permissions": []},
    }))
    tenant_a = directory.authenticate(token_a)
    tenant_b = directory.authenticate(token_b)

    assert tenant_a.buyer_key != tenant_b.buyer_key
    assert tenant_a.buyer_key == "buyer-" + hashlib.sha256(
        b"tenant-a\0same-user",
    ).hexdigest()[:32]
    assert tenant_a.scope_session("same-session") != tenant_b.scope_session("same-session")


def test_scope_session_recognizes_its_own_id_and_rejects_another_owner():
    token_a = "clear-test-token-value-that-is-long-enough-0005"
    token_b = "clear-test-token-value-that-is-long-enough-0006"
    directory = AuthDirectory.from_json(json.dumps({
        token_a: {"tenant_id": "tenant-a", "subject": "buyer-1", "permissions": []},
        token_b: {"tenant_id": "tenant-b", "subject": "buyer-1", "permissions": []},
    }))
    tenant_a = directory.authenticate(token_a)
    tenant_b = directory.authenticate(token_b)
    scoped = tenant_a.scope_session("client-session-1")

    assert tenant_a.scope_session(scoped) == scoped
    with pytest.raises(AuthError):
        tenant_b.scope_session(scoped)


@pytest.mark.parametrize("raw", ["../private", "has space", "x" * 129, "s-not-a-valid-scope"])
def test_scope_session_rejects_unsafe_or_unbounded_raw_ids(raw: str):
    token = "clear-test-token-value-that-is-long-enough-0007"
    principal = AuthDirectory.from_json(_config(token)).authenticate(token)

    with pytest.raises(AuthError):
        principal.scope_session(raw)


def test_scope_session_generates_unique_namespaced_id_when_missing():
    token = "clear-test-token-value-that-is-long-enough-0008"
    principal = AuthDirectory.from_json(_config(token)).authenticate(token)

    first = principal.scope_session(None)
    second = principal.scope_session(None)

    assert first != second
    assert first.startswith("s-")
    assert principal.scope_session(first) == first
