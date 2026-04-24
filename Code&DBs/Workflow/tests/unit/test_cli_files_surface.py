from __future__ import annotations

from io import StringIO
from pathlib import Path

import pytest

from surfaces.cli.commands import files as files_commands
from surfaces.cli.main import main as workflow_cli_main


def test_files_help_is_available() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["files", "--help"], stdout=stdout)

    assert rc == 0
    rendered = stdout.getvalue()
    assert "workflow files list [--scope SCOPE]" in rendered
    assert "workflow files content <file-id> [--output-file <path>] [--json]" in rendered
    assert "workflow files upload <path>" in rendered
    assert "workflow files delete <file-id> [--yes] [--json]" in rendered


def test_files_list_renders_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(files_commands, "_sync_conn", lambda: object())
    def _fake_list_files(conn, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return [
            {
                "id": "file-1",
                "filename": "report.txt",
                "scope": "instance",
                "size_bytes": 12,
                "created_at": "2026-04-16T00:00:00+00:00",
            }
        ]

    monkeypatch.setattr(files_commands, "list_files", _fake_list_files)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            ["files", "list", "--scope", "instance", "--query", "report", "--limit", "5", "--json"],
            stdout=stdout,
        )
        == 0
    )
    payload = stdout.getvalue()
    assert '"count": 1' in payload
    assert captured["kwargs"] == {
        "scope": "instance",
        "workflow_id": None,
        "step_id": None,
        "query": "report",
        "limit": 5,
    }


def test_files_upload_requires_confirmation() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["files", "upload", "artifact.txt"], stdout=stdout)

    assert rc == 2
    assert "confirmation required" in stdout.getvalue()


def test_files_upload_saves_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source = tmp_path / "artifact.txt"
    source.write_text("payload", encoding="utf-8")

    captured: dict[str, object] = {}

    monkeypatch.setattr(files_commands, "_sync_conn", lambda: object())
    def _fake_save_file(conn, repo_root, **kwargs):
        captured["kwargs"] = {"repo_root": repo_root, **kwargs}
        return {
            "id": "file_123",
            "filename": kwargs["filename"],
            "content_type": kwargs["content_type"],
            "size_bytes": len(kwargs["content"]),
            "scope": kwargs["scope"],
            "storage_path": "artifacts/uploads/file_123.txt",
        }

    monkeypatch.setattr(files_commands, "save_file", _fake_save_file)
    stdout = StringIO()

    rc = workflow_cli_main(
        [
            "files",
            "upload",
            str(source),
            "--scope",
            "workflow",
            "--workflow-id",
            "wf-1",
            "--description",
            "uploaded from cli",
            "--yes",
            "--json",
        ],
        stdout=stdout,
    )

    assert rc == 0
    payload = stdout.getvalue()
    assert '"id": "file_123"' in payload
    assert captured["kwargs"]["repo_root"].endswith("/Praxis")
    assert captured["kwargs"]["filename"] == "artifact.txt"
    assert captured["kwargs"]["scope"] == "workflow"
    assert captured["kwargs"]["workflow_id"] == "wf-1"
    assert captured["kwargs"]["description"] == "uploaded from cli"
    assert captured["kwargs"]["content"] == b"payload"


def test_files_content_writes_output_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = tmp_path / "downloaded.txt"

    monkeypatch.setattr(files_commands, "_sync_conn", lambda: object())
    monkeypatch.setattr(
        files_commands,
        "get_file_record",
        lambda conn, file_id: {
            "id": file_id,
            "filename": "report.txt",
            "content_type": "text/plain",
            "size_bytes": 7,
            "storage_path": "artifacts/uploads/file_123.txt",
            "scope": "instance",
            "created_at": "2026-04-16T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(
        files_commands,
        "get_file_content",
        lambda conn, repo_root, file_id: (b"payload", "text/plain", "report.txt"),
    )
    stdout = StringIO()

    rc = workflow_cli_main(
        ["files", "content", "file_123", "--output-file", str(output)],
        stdout=stdout,
    )

    assert rc == 0
    assert output.read_bytes() == b"payload"
    assert "written_to:" in stdout.getvalue()


def test_files_delete_requires_confirmation() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["files", "delete", "file_123"], stdout=stdout)

    assert rc == 2
    assert "confirmation required" in stdout.getvalue()
