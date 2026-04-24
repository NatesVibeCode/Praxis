from __future__ import annotations

from pathlib import Path

import pytest

from adapters import task_profiles


def test_seed_profile_and_infer_task_type_work_without_database_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(task_profiles, "_task_profile_repo_root", lambda: tmp_path)
    task_profiles.reload_profiles_from_db()

    assert task_profiles.infer_task_type("Please debug and trace this failure") == "general"


def test_execution_task_type_inference_requires_database_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(task_profiles, "_task_profile_repo_root", lambda: tmp_path)
    task_profiles.reload_profiles_from_db()

    with pytest.raises(task_profiles.TaskProfileAuthorityError, match="explicit WORKFLOW_DATABASE_URL"):
        task_profiles.infer_task_type(
            "Please debug and trace this failure",
            require_authority=True,
        )


def test_try_resolve_profile_returns_none_without_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(task_profiles, "_task_profile_repo_root", lambda: tmp_path)
    task_profiles.reload_profiles_from_db()

    assert task_profiles.try_resolve_profile("code_generation") is None


def test_resolve_profile_requires_explicit_database_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(task_profiles, "_task_profile_repo_root", lambda: tmp_path)
    task_profiles.reload_profiles_from_db()

    with pytest.raises(task_profiles.TaskProfileAuthorityError, match="explicit WORKFLOW_DATABASE_URL"):
        task_profiles.resolve_profile("code_generation")
