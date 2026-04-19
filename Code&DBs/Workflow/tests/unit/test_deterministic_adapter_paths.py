from __future__ import annotations

from pathlib import Path

from adapters import deterministic


def test_translate_host_workspace_root_uses_workspace_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    host_root = tmp_path / "host"
    container_root = tmp_path / "container"
    container_root.mkdir()
    host_value = str(host_root / "artifacts" / "out.json")

    monkeypatch.setattr(deterministic, "authority_workspace_roots", lambda: (host_root,))
    monkeypatch.setattr(deterministic, "container_workspace_root", lambda: container_root)

    assert deterministic._translate_host_path_to_container(host_value) == str(
        container_root / "artifacts" / "out.json"
    )


def test_translate_host_workspace_root_is_quiet_when_host_root_visible(
    monkeypatch,
    tmp_path: Path,
) -> None:
    host_root = tmp_path / "host"
    host_root.mkdir()
    container_root = tmp_path / "container"
    container_root.mkdir()
    host_value = str(host_root / "artifacts" / "out.json")

    monkeypatch.setattr(deterministic, "authority_workspace_roots", lambda: (host_root,))
    monkeypatch.setattr(deterministic, "container_workspace_root", lambda: container_root)

    assert deterministic._translate_host_path_to_container(host_value) == host_value
