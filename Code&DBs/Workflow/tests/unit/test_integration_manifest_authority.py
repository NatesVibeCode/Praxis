from __future__ import annotations

from pathlib import Path

import pytest

from memory.data_dictionary_lineage_projector import DataDictionaryLineageProjector
from memory.data_dictionary_projector import DataDictionaryProjector
from registry import integration_registry_sync as integration_registry_sync_mod
from runtime.integration_manifest import (
    ManifestLoadReport,
    load_manifest_report,
)


class _RegistryConn:
    def __init__(self) -> None:
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, *args):
        if "information_schema.columns" in sql:
            return [
                {"column_name": "id"},
                {"column_name": "name"},
                {"column_name": "description"},
                {"column_name": "provider"},
                {"column_name": "capabilities"},
                {"column_name": "auth_status"},
            ]
        return []

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def test_manifest_load_report_keeps_valid_rows_and_surfaces_parse_failures(tmp_path: Path) -> None:
    (tmp_path / "good.toml").write_text(
        '[integration]\nid = "good"\nname = "Good"\nprovider = "http"\n',
        encoding="utf-8",
    )
    (tmp_path / "bad.toml").write_text("this is not [valid toml", encoding="utf-8")

    report = load_manifest_report(tmp_path)

    assert [manifest.id for manifest in report.manifests] == ["good"]
    assert len(report.errors) == 1
    assert "bad.toml" in report.errors[0]


def test_integration_registry_sync_aborts_when_manifest_report_has_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        integration_registry_sync_mod.integration_manifest,
        "load_manifest_report",
        lambda _manifest_dir=None: ManifestLoadReport(
            manifests=(),
            errors=("bad.toml: TOMLDecodeError: boom",),
        ),
    )
    monkeypatch.setattr(
        integration_registry_sync_mod,
        "projected_mcp_integrations",
        lambda: [],
    )

    conn = _RegistryConn()

    with pytest.raises(RuntimeError, match="bad.toml"):
        integration_registry_sync_mod.sync_integration_registry(conn)

    assert conn.batch_calls == []


def test_dictionary_projectors_fail_closed_when_manifest_report_has_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "runtime.integration_manifest.load_manifest_report",
        lambda: ManifestLoadReport(
            manifests=(),
            errors=("bad.toml: TOMLDecodeError: boom",),
        ),
    )

    with pytest.raises(RuntimeError, match="malformed manifest"):
        DataDictionaryProjector(object())._project_integration_manifests()

    with pytest.raises(RuntimeError, match="malformed manifest"):
        DataDictionaryLineageProjector(object())._project_integration_manifests(
            {"integration": set(), "tool": set()}
        )
