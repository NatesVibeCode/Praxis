from __future__ import annotations

from pathlib import Path

import pytest

from runtime import setup_wizard
from runtime.service_lifecycle import RegisterRuntimeTargetCommand, normalize_substrate_kind


class _Authority:
    database_url = ""
    source = "test"


def test_runtime_setup_default_substrate_is_service_lifecycle_compatible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.delenv("PRAXIS_RUNTIME_SUBSTRATE_KIND", raising=False)
    monkeypatch.setattr(setup_wizard, "_env_value", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        setup_wizard,
        "_setup_authority_env",
        lambda *, repo_root=None: ({}, _Authority()),
    )
    monkeypatch.setattr(setup_wizard, "_docker_info", lambda: {"available": True})

    docker_report = setup_wizard.runtime_target_report(repo_root=repo_root)
    docker_command = RegisterRuntimeTargetCommand(
        runtime_target_ref=docker_report["runtime_target_ref"],
        substrate_kind=docker_report["substrate_kind"],
    )
    assert normalize_substrate_kind(docker_command.substrate_kind) == "container"

    monkeypatch.setattr(setup_wizard, "_docker_info", lambda: {"available": False})
    api_report = setup_wizard.runtime_target_report(repo_root=repo_root)
    api_command = RegisterRuntimeTargetCommand(
        runtime_target_ref=api_report["runtime_target_ref"],
        substrate_kind=api_report["substrate_kind"],
    )
    assert normalize_substrate_kind(api_command.substrate_kind) == "cloud_service"


def test_setup_apply_is_blocked_until_it_has_durable_write_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(setup_wizard, "_setup_authority_env", lambda *, repo_root=None: ({}, _Authority()))
    monkeypatch.setattr(
        setup_wizard,
        "runtime_target_report",
        lambda *, repo_root=None: {
            "runtime_target_ref": "runtime_target.praxis.default",
            "substrate_kind": "container",
            "api_authority": "http://127.0.0.1:8420",
            "db_authority": "",
            "db_authority_source": "test",
            "workspace_authority": str(repo_root),
            "host_traits": {},
        },
    )
    monkeypatch.setattr(
        setup_wizard,
        "sandbox_contract_report",
        lambda *, repo_root=None: {
            "empty_thin_sandbox_default": True,
            "checks": {},
            "blockers": [],
        },
    )
    monkeypatch.setattr(
        setup_wizard,
        "package_contract_report",
        lambda *, repo_root=None: {
            "complete_repo_package": True,
            "components": [],
            "checks": {},
            "blockers": [],
        },
    )
    monkeypatch.setattr(
        setup_wizard,
        "_native_instance_for_setup",
        lambda _env: {
            "repo_root": str(repo_root),
            "workdir": str(repo_root),
            "praxis_receipts_dir": str((repo_root / "artifacts" / "runtime_receipts").resolve()),
            "praxis_topology_dir": str((repo_root / "artifacts" / "runtime_topology").resolve()),
            "praxis_instance_name": "praxis",
            "praxis_runtime_profile": "praxis",
        },
    )
    monkeypatch.setattr(setup_wizard, "_orphan_container_count", lambda: 0)

    payload = setup_wizard.setup_payload(
        "apply",
        repo_root=repo_root,
        apply=True,
        authority_surface="api",
    )

    assert payload["ok"] is False
    assert payload["applied"] is False
    assert payload["mutation_performed"] is False
    assert payload["requires_authority_apply"] is False
    assert payload["error_code"] == "setup.apply_not_implemented"


def test_runtime_setup_apply_catalog_seed_is_preview_only() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    migration = (
        repo_root
        / "Code&DBs"
        / "Databases"
        / "migrations"
        / "workflow"
        / "209_empty_thin_sandbox_runtime_targets.sql"
    ).read_text(encoding="utf-8")
    apply_row = migration.split("'runtime-setup-apply'", 1)[1].split(")\nON CONFLICT", 1)[0]

    assert "'operation_query'" in apply_row
    assert "'query'" in apply_row
    assert "'runtime_setup_apply_requested'" not in apply_row
    assert "'observe'" in apply_row
    assert "'read_only'" in apply_row
