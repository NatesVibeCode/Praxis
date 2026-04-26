from __future__ import annotations

import argparse
import json
import pytest
from pathlib import Path

from surfaces.cli import workflow_cli
from storage.postgres.validators import PostgresConfigurationError


class _Registry:
    def __init__(self, known_agents: set[str]) -> None:
        self._known_agents = known_agents

    def get(self, slug: str):
        alias_map = {
            "codex-5.3-spark": "gpt-5.3-codex-spark",
        }
        canonical = slug if slug in self._known_agents else alias_map.get(slug)
        if canonical in self._known_agents:
            return type("_ResolvedAgent", (), {"slug": canonical})()
        return None


def _agent_registry(known_agents: set[str]):
    class _AgentRegistry:
        @classmethod
        def load_from_postgres(cls, conn):
            return _Registry(known_agents)

    return _AgentRegistry


def _write_spec(
    tmp_path: Path,
    *,
    agent_slug: str,
    task_type: str | None = None,
    verify_refs: list[str] | None = None,
) -> str:
    job = {
        "label": "validate_job",
        "agent": agent_slug,
        "prompt": "Run identity check.",
    }
    if task_type is not None:
        job["task_type"] = task_type
    if verify_refs is not None:
        job["verify_refs"] = verify_refs
    payload = {
        "name": "cli validate smoke",
        "workflow_id": "cli_validate_smoke",
        "phase": "test",
        "jobs": [job],
    }
    path = tmp_path / "spec.queue.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def test_cmd_validate_returns_error_when_agent_not_in_postgres_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path, agent_slug="gpt-5.4-mini")

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        __import__("registry.agent_config", fromlist=["*"]),
        "AgentRegistry",
        _agent_registry({"gpt-4o"}),
    )

    result = workflow_cli.cmd_validate(argparse.Namespace(spec=spec_path))

    assert result == 1


def test_cmd_validate_passes_when_all_agents_are_known(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path, agent_slug="gpt-5.4-mini")

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        __import__("registry.agent_config", fromlist=["*"]),
        "AgentRegistry",
        _agent_registry({"gpt-5.4-mini", "gpt-4o"}),
    )

    result = workflow_cli.cmd_validate(argparse.Namespace(spec=spec_path))

    assert result == 0


def test_cmd_validate_rejects_mutating_job_without_verify_refs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = _write_spec(tmp_path, agent_slug="gpt-5.4-mini", task_type="build")

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        __import__("registry.agent_config", fromlist=["*"]),
        "AgentRegistry",
        _agent_registry({"gpt-5.4-mini", "gpt-4o"}),
    )

    result = workflow_cli.cmd_validate(argparse.Namespace(spec=spec_path))

    rendered = capsys.readouterr().out
    assert result == 1
    assert "requires verify_refs" in rendered


def test_cmd_validate_accepts_legacy_agent_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path, agent_slug="codex-5.3-spark")

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        __import__("registry.agent_config", fromlist=["*"]),
        "AgentRegistry",
        _agent_registry({"gpt-5.3-codex-spark"}),
    )

    result = workflow_cli.cmd_validate(argparse.Namespace(spec=spec_path))

    assert result == 0


def test_cmd_validate_reports_authority_error_when_pg_conn_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = _write_spec(tmp_path, agent_slug="gpt-5.4-mini")

    def _raise_authority_error():
        raise PostgresConfigurationError(
            "postgres.authority_unavailable",
            "WORKFLOW_DATABASE_URL authority unavailable: PermissionError: [Errno 1] Operation not permitted",
        )

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", _raise_authority_error)

    result = workflow_cli.cmd_validate(argparse.Namespace(spec=spec_path))

    rendered = capsys.readouterr().out
    assert result == 1
    assert "=== Spec Validation: FAILED ===" in rendered
    assert "AUTHORITY ERROR" in rendered
    assert "agent authority unavailable" in rendered
    assert "Remediation:" in rendered
    assert "Resolve WORKFLOW_DATABASE_URL" in rendered
    assert "Traceback" not in rendered
