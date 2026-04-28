from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from storage.postgres import fresh_install_seed


class _FakeAsyncConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.calls.append((query, args))
        return "OK"


def _write_runtime_profiles_config(repo_root: Path) -> None:
    config_dir = repo_root / "config"
    config_dir.mkdir()
    (config_dir / "runtime_profiles.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "default_runtime_profile": "praxis",
                "sandbox_profiles": {
                    "sandbox_profile.praxis.default": {
                        "sandbox_provider": "docker_local",
                        "docker_image": None,
                        "docker_cpus": "2",
                        "docker_memory": "500m",
                        "network_policy": "provider_only",
                        "workspace_materialization": "none",
                        "secret_allowlist": ["OPENAI_API_KEY"],
                        "auth_mount_policy": "provider_scoped",
                        "timeout_profile": "default",
                    }
                },
                "runtime_profiles": {
                    "praxis": {
                        "instance_name": "praxis",
                        "workspace_ref": "praxis",
                        "sandbox_profile_ref": "sandbox_profile.praxis.default",
                        "model_profile_id": "model_profile.praxis.default",
                        "provider_policy_id": "provider_policy.praxis.default",
                        "provider_name": "openai",
                        "provider_names": ["openai"],
                        "allowed_models": ["gpt-5.4"],
                        "repo_root": ".",
                        "workdir": ".",
                        "receipts_dir": "artifacts/runtime_receipts",
                        "topology_dir": "artifacts/runtime_topology",
                    }
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_fresh_install_seed_materializes_runtime_and_public_policy_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_runtime_profiles_config(tmp_path)
    conn = _FakeAsyncConn()
    sync_calls: list[object] = []

    async def _sync(conn_arg: object):
        sync_calls.append(conn_arg)
        return ("praxis",)

    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.sync_native_runtime_profile_authority_async",
        _sync,
    )

    summary = asyncio.run(
        fresh_install_seed.seed_fresh_install_authority_async(
            conn,
            repo_root=tmp_path,
        )
    )

    assert summary.runtime_profiles == ("praxis",)
    assert summary.sandbox_profiles == ("sandbox_profile.praxis.default",)
    assert "functional_area.authority" in summary.functional_areas
    assert "functional_area.scheduler" in summary.functional_areas
    assert summary.workflow_definitions == (
        "workflow_definition.native_self_hosted_smoke.v1",
    )
    assert summary.synced_runtime_profiles == ("praxis",)
    assert sync_calls == [conn]
    rendered_queries = "\n".join(query for query, _args in conn.calls)
    rendered_args = "\n".join(str(args) for _query, args in conn.calls)
    assert "registry_workspace_authority" in rendered_queries
    assert "registry_sandbox_profile_authority" in rendered_queries
    assert "registry_runtime_profile_authority" in rendered_queries
    assert "registry_native_runtime_profile_authority" in rendered_queries
    assert "registry_native_runtime_defaults" in rendered_queries
    assert "operator_decisions" in rendered_queries
    assert "functional_areas" in rendered_queries
    assert "workflow_definitions" in rendered_queries
    assert "workflow_definition_nodes" in rendered_queries
    assert "workflow_definition_edges" in rendered_queries
    assert "allow_passthrough_echo" in rendered_args
    assert all("anthropic-cli-only" not in str(args) for _query, args in conn.calls)
    assert "architecture-policy::orient::mandatory-authority-envelope" in (
        summary.operator_decisions
    )


def test_fresh_install_seed_fails_closed_on_missing_config(tmp_path: Path) -> None:
    with pytest.raises(fresh_install_seed.FreshInstallSeedError) as exc_info:
        asyncio.run(
            fresh_install_seed.seed_fresh_install_authority_async(
                _FakeAsyncConn(),
                repo_root=tmp_path,
            )
        )

    assert exc_info.value.reason_code == "fresh_install_seed.config_missing"


def test_load_decisions_snapshot_returns_empty_tuple_when_missing(
    tmp_path: Path,
) -> None:
    rows = fresh_install_seed._load_decisions_snapshot(tmp_path)
    assert rows == ()


def test_load_decisions_snapshot_parses_committed_file(tmp_path: Path) -> None:
    policy_dir = tmp_path / "policy"
    policy_dir.mkdir()
    (policy_dir / "operator-decisions-snapshot.json").write_text(
        json.dumps(
            {
                "$schema_version": 1,
                "count": 2,
                "decisions": [
                    {
                        "decision_key": "architecture-policy::test::one",
                        "decision_kind": "architecture_policy",
                        "decision_status": "decided",
                        "decision_source": "test",
                        "decision_scope_kind": "authority_domain",
                        "decision_scope_ref": "test",
                        "title": "first",
                        "rationale": "because",
                        "decided_by": "praxis",
                        "scope_clamp": {
                            "applies_to": ["pending_review"],
                            "does_not_apply_to": [],
                        },
                    },
                    {
                        "decision_key": "architecture-policy::test::two",
                        "decision_kind": "architecture_policy",
                        "decision_status": "decided",
                        "decision_source": "test",
                        "decision_scope_kind": "authority_domain",
                        "decision_scope_ref": "test",
                        "title": "second",
                        "rationale": "and",
                        "decided_by": "praxis",
                        "scope_clamp": {
                            "applies_to": ["pending_review"],
                            "does_not_apply_to": [],
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    rows = fresh_install_seed._load_decisions_snapshot(tmp_path)
    assert len(rows) == 2
    assert rows[0]["decision_key"] == "architecture-policy::test::one"
    assert rows[1]["title"] == "second"


def test_seed_operator_decisions_from_snapshot_inserts_each_row(
    tmp_path: Path,
) -> None:
    policy_dir = tmp_path / "policy"
    policy_dir.mkdir()
    (policy_dir / "operator-decisions-snapshot.json").write_text(
        json.dumps(
            {
                "count": 1,
                "decisions": [
                    {
                        "decision_key": "architecture-policy::test::row",
                        "decision_kind": "architecture_policy",
                        "decision_status": "decided",
                        "decision_source": "operator_decisions_snapshot",
                        "decision_scope_kind": "authority_domain",
                        "decision_scope_ref": "test",
                        "title": "row",
                        "rationale": "snapshot row",
                        "decided_by": "praxis",
                        "scope_clamp": {
                            "applies_to": ["pending_review"],
                            "does_not_apply_to": [],
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    conn = _FakeAsyncConn()
    seeded = asyncio.run(
        fresh_install_seed._seed_operator_decisions_from_snapshot(conn, tmp_path)
    )
    assert seeded == ("architecture-policy::test::row",)
    assert len(conn.calls) == 1
    query, args = conn.calls[0]
    assert "INSERT INTO operator_decisions" in query
    assert "ON CONFLICT (decision_key) DO NOTHING" in query
    assert "architecture-policy::test::row" in args
    # scope_clamp serialized to JSON for the JSONB cast
    assert any('"applies_to"' in str(arg) for arg in args)


def test_seed_operator_decisions_from_snapshot_noop_when_missing(
    tmp_path: Path,
) -> None:
    conn = _FakeAsyncConn()
    seeded = asyncio.run(
        fresh_install_seed._seed_operator_decisions_from_snapshot(conn, tmp_path)
    )
    assert seeded == ()
    assert conn.calls == []
