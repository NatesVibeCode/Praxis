"""Gate and content tests for the operator console route.

Covers:
  - PRAXIS_OPERATOR_DEV_MODE=1 returns HTML
  - PRAXIS_OPERATOR_DEV_MODE unset or "0" returns 404
  - Served HTML references the agent_sessions API paths
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import rest  # noqa: E402


def test_operator_console_404_when_gate_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PRAXIS_OPERATOR_DEV_MODE", raising=False)
    with TestClient(rest.app) as client:
        response = client.get("/console")
    assert response.status_code == 404
    detail = response.json().get("detail") or {}
    assert detail.get("error_code") == "operator_console_disabled"


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "YES"])
def test_operator_console_serves_html_when_gate_on(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", value)
    with TestClient(rest.app) as client:
        response = client.get("/console")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"].startswith("no-store")
    body = response.text
    assert "Praxis Operator Console" in body


def test_operator_console_html_wires_to_agent_sessions_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "1")
    with TestClient(rest.app) as client:
        response = client.get("/console")
    body = response.text
    # The client must use the mounted agent_sessions base path.
    assert "/api/agent-sessions/agents" in body
    # Must name the normalized permission modes so the UI mirrors the matrix.
    for mode in ("read_only", "plan_only", "propose_edits", "auto_edits", "full_autonomy"):
        assert mode in body
    # Must name the CLI providers.
    for provider in ("claude", "codex", "gemini"):
        assert provider in body


def test_operator_console_trailing_slash_also_gated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PRAXIS_OPERATOR_DEV_MODE", raising=False)
    with TestClient(rest.app) as client:
        response = client.get("/console/")
    assert response.status_code == 404


def test_operator_console_zero_disables_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "0")
    with TestClient(rest.app) as client:
        response = client.get("/console")
    assert response.status_code == 404
