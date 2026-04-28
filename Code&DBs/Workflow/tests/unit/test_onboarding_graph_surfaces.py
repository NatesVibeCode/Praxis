"""Surface wiring tests for the onboarding gate-probe graph.

Packet 3 exposes the graph through three read surfaces (CLI, HTTP, MCP). Each
surface is a thin formatter over ``setup_graph_payload``. These tests verify
the payload shape and each surface's dispatch path.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.setup_wizard import _SETUP_MODES, setup_graph_payload


def test_setup_graph_payload_shape(tmp_path: Path) -> None:
    payload = setup_graph_payload(repo_root=tmp_path)
    assert payload["mode"] == "graph"
    assert "ok" in payload
    assert "authority_surface" in payload
    assert "platform" in payload
    assert "repo_root" in payload
    assert isinstance(payload["gates"], list)
    assert payload["gates"], "graph should contain at least one gate"

    # Summary counts align with the gates list.
    summary = payload["summary"]
    assert summary["total"] == len(payload["gates"])
    assert summary["total"] == (
        summary["ok"] + summary["missing"] + summary["blocked"] + summary["unknown"]
    )


def test_setup_graph_payload_gate_shape(tmp_path: Path) -> None:
    payload = setup_graph_payload(repo_root=tmp_path)
    for gate in payload["gates"]:
        assert gate["gate_ref"]
        assert gate["domain"] in {"platform", "runtime", "provider", "mcp"}
        assert gate["title"]
        assert gate["purpose"]
        assert gate["status"] in {"ok", "missing", "blocked", "unknown"}
        assert isinstance(gate["depends_on"], list)
        assert isinstance(gate["observed_state"], dict)
        assert "evaluated_at" in gate
        # Failed gates carry a copy-pasteable remediation.
        if gate["status"] in {"missing", "blocked"} and gate["domain"] != "runtime":
            # runtime.api_port_free can be blocked without actionable remediation
            # when upstream sniffing fails, but platform/provider/mcp gates must
            # give the user something to paste.
            assert (
                gate["remediation_hint"] or gate["remediation_doc_url"]
            ), f"gate {gate['gate_ref']} status={gate['status']} has no remediation"


def test_setup_graph_payload_ok_is_false_when_any_gate_failing() -> None:
    # A fresh tmp_path has no .venv/launcher/MCP config, so gates will fail.
    payload = setup_graph_payload(repo_root=Path("/tmp/does-not-exist-praxis"))
    if payload["summary"]["missing"] > 0 or payload["summary"]["blocked"] > 0:
        assert payload["ok"] is False


def test_setup_modes_includes_graph() -> None:
    assert "graph" in _SETUP_MODES
    assert {"doctor", "plan", "apply", "graph"} <= _SETUP_MODES


def test_cli_setup_graph_dispatches_to_payload() -> None:
    from surfaces.cli.commands import setup as setup_cmd

    fake_payload = {
        "mode": "graph",
        "ok": False,
        "gates": [],
        "summary": {"total": 0, "ok": 0, "missing": 0, "blocked": 0, "unknown": 0},
    }
    with patch.object(setup_cmd, "setup_payload_for_cli", return_value=fake_payload) as stub:
        stdout = io.StringIO()
        code = setup_cmd._setup_command(["graph"], stdout=stdout)
    assert code == 1  # ok=False -> exit 1
    stub.assert_called_once()
    # First positional arg is the mode.
    assert stub.call_args[0][0] == "graph"
    body = json.loads(stdout.getvalue())
    assert body["mode"] == "graph"


def test_cli_setup_rejects_unknown_mode() -> None:
    from surfaces.cli.commands import setup as setup_cmd

    stdout = io.StringIO()
    code = setup_cmd._setup_command(["bogus"], stdout=stdout)
    assert code == 2
    assert "doctor|plan|apply|graph" in stdout.getvalue()


def test_cli_setup_help_mentions_graph() -> None:
    from surfaces.cli.commands import setup as setup_cmd

    stdout = io.StringIO()
    code = setup_cmd._setup_command(["help"], stdout=stdout)
    assert code == 0
    assert "graph" in stdout.getvalue()


def test_cli_setup_apply_routes_gate_and_apply_ref_to_gate_handler() -> None:
    from surfaces.cli.commands import setup as setup_cmd

    with patch.object(
        setup_cmd,
        "setup_apply_gate_payload",
        return_value={"ok": True, "mode": "apply", "gate_ref": "mcp.claude_code"},
    ) as gate_stub:
        stdout = io.StringIO()
        code = setup_cmd._setup_command(
            ["apply", "--gate", "mcp.claude_code", "--yes"],
            stdout=stdout,
        )

    assert code == 0
    gate_stub.assert_called_once()
    assert json.loads(stdout.getvalue())["gate_ref"] == "mcp.claude_code"


def test_mcp_praxis_setup_graph_action_returns_graph_payload() -> None:
    from surfaces.mcp.tools import setup as mcp_setup

    fake_payload = {
        "mode": "graph",
        "ok": True,
        "gates": [],
        "summary": {"total": 0, "ok": 0, "missing": 0, "blocked": 0, "unknown": 0},
    }
    with patch.object(mcp_setup, "setup_graph_payload", return_value=fake_payload) as stub:
        result = mcp_setup.tool_praxis_setup({"action": "graph"})
    stub.assert_called_once()
    assert result["mode"] == "graph"


def test_mcp_praxis_setup_tool_advertises_graph_action() -> None:
    from surfaces.mcp.tools.setup import TOOLS

    schema = TOOLS["praxis_setup"][1]["inputSchema"]
    action_enum = schema["properties"]["action"]["enum"]
    assert "graph" in action_enum
    assert {"doctor", "plan", "apply", "graph"} == set(action_enum)


def test_mcp_praxis_setup_rejects_invalid_action() -> None:
    from surfaces.mcp.tools import setup as mcp_setup

    result = mcp_setup.tool_praxis_setup({"action": "bogus"})
    assert result["ok"] is False
    assert result["error_code"] == "setup.invalid_action"
    assert "graph" in result["message"]


def test_http_setup_graph_route_registered() -> None:
    from surfaces.api.handlers.workflow_admin import ADMIN_GET_ROUTES

    registered_paths: set[str] = set()
    for matcher, _handler in ADMIN_GET_ROUTES:
        # _exact returns a matcher that exposes the path via closure.
        # We can test by matching known paths against the route functions.
        for candidate in {"/api/setup/doctor", "/api/setup/plan", "/api/setup/graph"}:
            if matcher(candidate):
                registered_paths.add(candidate)
                break
    assert "/api/setup/graph" in registered_paths


def test_http_setup_graph_handler_returns_graph_payload() -> None:
    from surfaces.api.handlers import workflow_admin

    fake_payload = {
        "mode": "graph",
        "ok": True,
        "gates": [],
        "summary": {"total": 0, "ok": 0, "missing": 0, "blocked": 0, "unknown": 0},
    }

    class _FakeRequest:
        def __init__(self) -> None:
            self.sent: tuple[int, dict] | None = None

        def _send_json(self, code: int, body: dict) -> None:
            self.sent = (code, body)

    request = _FakeRequest()
    with patch.object(workflow_admin, "REPO_ROOT", _WORKFLOW_ROOT.parent):
        with patch(
            "runtime.setup_wizard.setup_graph_payload",
            return_value=fake_payload,
        ):
            workflow_admin._handle_setup_get(request, "/api/setup/graph")
    assert request.sent is not None
    status_code, body = request.sent
    assert status_code == 200
    assert body["mode"] == "graph"


def test_http_setup_apply_handler_routes_to_gate_payload() -> None:
    from surfaces.api.handlers import workflow_admin

    fake_payload = {"ok": True, "mode": "apply", "gate_ref": "mcp.claude_code"}

    class _FakeRequest:
        def __init__(self) -> None:
            self.sent: tuple[int, dict] | None = None
            self.headers = {"Content-Length": str(len(b'{"approved":true,"gate_ref":"mcp.claude_code"}'))}
            self.rfile = io.BytesIO(b'{"approved":true,"gate_ref":"mcp.claude_code"}')

        def _send_json(self, code: int, body: dict) -> None:
            self.sent = (code, body)

    request = _FakeRequest()
    with patch.object(workflow_admin, "REPO_ROOT", _WORKFLOW_ROOT.parent):
        with patch(
            "runtime.setup_wizard.setup_apply_gate_payload",
            return_value=fake_payload,
        ) as stub:
            workflow_admin._handle_setup_apply_post(request, "/api/setup/apply")
    assert request.sent is not None
    status_code, body = request.sent
    assert status_code == 200
    assert body["mode"] == "apply"
    stub.assert_called_once()
