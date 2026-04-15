from __future__ import annotations

import pytest

from registry.domain import SandboxProfileAuthorityRecord
from registry.sandbox_profile_authority import (
    SandboxProfileAuthorityError,
    load_runtime_sandbox_profile_authority,
    sandbox_profile_execution_payload,
)


class _FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args: object):
        self.calls.append((query, args))
        return self.rows


def test_load_runtime_sandbox_profile_authority_rejects_unknown_runtime_profile(monkeypatch) -> None:
    monkeypatch.setattr("registry.sandbox_profile_authority.is_native_runtime_profile_ref", lambda _: False)
    conn = _FakeConn(rows=[])

    with pytest.raises(SandboxProfileAuthorityError, match="missing sandbox authority"):
        load_runtime_sandbox_profile_authority(conn, runtime_profile_ref="runtime.missing")


def test_load_runtime_sandbox_profile_authority_rejects_missing_sandbox_profile(monkeypatch) -> None:
    monkeypatch.setattr("registry.sandbox_profile_authority.is_native_runtime_profile_ref", lambda _: False)
    conn = _FakeConn(
        rows=[
            {
                "runtime_profile_ref": "praxis",
                "sandbox_profile_ref": "sandbox_profile.praxis.default",
                "sandbox_provider": None,
            }
        ]
    )

    with pytest.raises(SandboxProfileAuthorityError, match="references missing sandbox profile"):
        load_runtime_sandbox_profile_authority(conn, runtime_profile_ref="praxis")


def test_load_runtime_sandbox_profile_authority_returns_authoritative_record(monkeypatch) -> None:
    sync_calls: list[str] = []
    monkeypatch.setattr(
        "registry.sandbox_profile_authority.is_native_runtime_profile_ref",
        lambda runtime_profile_ref: runtime_profile_ref == "praxis",
    )
    monkeypatch.setattr(
        "registry.sandbox_profile_authority.sync_native_runtime_profile_authority",
        lambda conn, prune=False: sync_calls.append(f"sync:{prune}"),
    )
    conn = _FakeConn(
        rows=[
            {
                "runtime_profile_ref": "praxis",
                "sandbox_profile_ref": "sandbox_profile.praxis.default",
                "sandbox_provider": "docker_local",
                "docker_image": "registry/praxis@sha256:deadbeef",
                "docker_cpus": "2",
                "docker_memory": "4g",
                "network_policy": "provider_only",
                "workspace_materialization": "copy",
                "secret_allowlist": ["OPENAI_API_KEY", "ANTHROPIC_API_KEY"],
                "auth_mount_policy": "provider_scoped",
                "timeout_profile": "default",
            }
        ]
    )

    record = load_runtime_sandbox_profile_authority(conn, runtime_profile_ref="praxis")

    assert sync_calls == ["sync:False"]
    assert record == SandboxProfileAuthorityRecord(
        sandbox_profile_ref="sandbox_profile.praxis.default",
        sandbox_provider="docker_local",
        docker_image="registry/praxis@sha256:deadbeef",
        docker_cpus="2",
        docker_memory="4g",
        network_policy="provider_only",
        workspace_materialization="copy",
        secret_allowlist=("OPENAI_API_KEY", "ANTHROPIC_API_KEY"),
        auth_mount_policy="provider_scoped",
        timeout_profile="default",
    )


def test_sandbox_profile_execution_payload_preserves_explicit_contract() -> None:
    payload = sandbox_profile_execution_payload(
        SandboxProfileAuthorityRecord(
            sandbox_profile_ref="sandbox_profile.praxis.default",
            sandbox_provider="docker_local",
            docker_image="registry/praxis@sha256:deadbeef",
            docker_cpus="2",
            docker_memory="4g",
            network_policy="provider_only",
            workspace_materialization="copy",
            secret_allowlist=("OPENAI_API_KEY",),
            auth_mount_policy="provider_scoped",
            timeout_profile="default",
        )
    )

    assert payload == {
        "sandbox_profile_ref": "sandbox_profile.praxis.default",
        "sandbox_provider": "docker_local",
        "docker_image": "registry/praxis@sha256:deadbeef",
        "docker_cpus": "2",
        "docker_memory": "4g",
        "network_policy": "provider_only",
        "workspace_materialization": "copy",
        "secret_allowlist": ["OPENAI_API_KEY"],
        "auth_mount_policy": "provider_scoped",
        "timeout_profile": "default",
    }
