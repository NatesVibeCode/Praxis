from __future__ import annotations

import argparse
import json
import pytest
from pathlib import Path

from surfaces.cli import workflow_cli


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


def _write_spec(tmp_path: Path, *, agent_slug: str) -> str:
    payload = {
        "name": "cli validate smoke",
        "workflow_id": "cli_validate_smoke",
        "phase": "test",
        "jobs": [
            {
                "label": "validate_job",
                "agent": agent_slug,
                "prompt": "Run identity check.",
            }
        ],
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
