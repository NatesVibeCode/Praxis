"""Unit tests for the onboarding gate-probe graph authority."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.onboarding import (
    GateGraph,
    GateGraphError,
    GateProbe,
    GateResult,
    ONBOARDING_GRAPH,
)
from runtime.onboarding import (
    probes_mcp,
    probes_platform,
    probes_provider,
    probes_runtime,
)


# --- Graph registry behavior --------------------------------------------------


def test_graph_refuses_duplicate_gate_ref() -> None:
    graph = GateGraph()
    probe = GateProbe(gate_ref="t.a", domain="t", title="A", purpose="p")
    graph.register(probe, lambda env, root: _ok(probe))
    with pytest.raises(GateGraphError, match="duplicate gate_ref"):
        graph.register(probe, lambda env, root: _ok(probe))


def test_graph_detects_cycle() -> None:
    graph = GateGraph()
    a = GateProbe(gate_ref="t.a", domain="t", title="A", purpose="p", depends_on=("t.b",))
    b = GateProbe(gate_ref="t.b", domain="t", title="B", purpose="p", depends_on=("t.a",))
    graph.register(a, lambda env, root: _ok(a))
    graph.register(b, lambda env, root: _ok(b))
    with pytest.raises(GateGraphError, match="cycle"):
        graph.evaluate({}, Path("/tmp"))


def test_graph_rejects_apply_targeting_unregistered_gate() -> None:
    graph = GateGraph()
    from runtime.onboarding import GateApply

    apply = GateApply(
        apply_ref="apply.x",
        gate_ref="t.missing",
        description="",
        handler=lambda: _ok(GateProbe(gate_ref="t.missing", domain="t", title="", purpose="")),
        mutates=(),
    )
    with pytest.raises(GateGraphError, match="unregistered gate"):
        graph.register_apply(apply)


def test_graph_marks_downstream_blocked_when_dependency_fails() -> None:
    graph = GateGraph()
    parent = GateProbe(gate_ref="t.parent", domain="t", title="P", purpose="p")
    child = GateProbe(
        gate_ref="t.child", domain="t", title="C", purpose="p", depends_on=("t.parent",)
    )
    graph.register(parent, lambda env, root: _result(parent, status="missing"))
    graph.register(child, lambda env, root: _ok(child))
    results = {r.gate_ref: r for r in graph.evaluate({}, Path("/tmp"))}
    assert results["t.parent"].status == "missing"
    assert results["t.child"].status == "blocked"
    assert "t.parent" in results["t.child"].observed_state["blocking_gates"]


def test_graph_skips_probes_for_non_matching_platform() -> None:
    graph = GateGraph()
    mac_only = GateProbe(
        gate_ref="t.mac_only",
        domain="t",
        title="M",
        purpose="p",
        platforms=("darwin",),
    )
    all_plat = GateProbe(gate_ref="t.all", domain="t", title="A", purpose="p")
    graph.register(mac_only, lambda env, root: _ok(mac_only))
    graph.register(all_plat, lambda env, root: _ok(all_plat))
    linux_results = {r.gate_ref for r in graph.evaluate({}, Path("/tmp"), platform="linux")}
    assert "t.mac_only" not in linux_results
    assert "t.all" in linux_results


def test_graph_captures_probe_exceptions_as_unknown() -> None:
    graph = GateGraph()
    p = GateProbe(gate_ref="t.fail", domain="t", title="F", purpose="p")

    def _raises(env, root):
        raise RuntimeError("probe exploded")

    graph.register(p, _raises)
    result = graph.evaluate({}, Path("/tmp"))[0]
    assert result.status == "unknown"
    assert result.observed_state["error_type"] == "RuntimeError"
    assert "exploded" in result.observed_state["error"]


# --- probes_platform ---------------------------------------------------------


def test_probe_homebrew_ok_when_brew_on_path(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_platform.shutil.which", return_value="/opt/homebrew/bin/brew"):
        result = probes_platform.probe_homebrew({}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["brew_path"] == "/opt/homebrew/bin/brew"


def test_probe_homebrew_missing_gives_install_command(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_platform.shutil.which", return_value=None):
        result = probes_platform.probe_homebrew({}, tmp_path)
    assert result.status == "missing"
    assert "brew.sh" in (result.remediation_doc_url or "")
    assert "install.sh" in (result.remediation_hint or "")


def test_probe_python_3_14_blocks_on_version_mismatch(tmp_path: Path) -> None:
    fake = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="3.13.1\n", stderr=""
    )
    with patch("runtime.onboarding.probes_platform.shutil.which", return_value="/usr/bin/python3.14"):
        with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
            result = probes_platform.probe_python_3_14({}, tmp_path)
    assert result.status == "blocked"
    assert "3.13.1" in result.observed_state["reported_version"]


def test_probe_python_3_14_missing_gives_install_commands(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_platform.shutil.which", return_value=None):
        result = probes_platform.probe_python_3_14({}, tmp_path)
    assert result.status == "missing"
    assert "brew install python@3.14" in (result.remediation_hint or "")
    assert "deadsnakes" in (result.remediation_hint or "")


def test_probe_postgres_role_requires_database_url(tmp_path: Path) -> None:
    result = probes_platform.probe_postgres_role({}, tmp_path)
    assert result.status == "unknown"
    assert "WORKFLOW_DATABASE_URL" in (result.remediation_hint or "")


def test_probe_postgres_role_missing_when_no_row(tmp_path: Path) -> None:
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_postgres_role(env, tmp_path)
    assert result.status == "missing"
    assert "CREATE USER" in (result.remediation_hint or "")


def test_probe_postgres_role_blocked_without_createdb(tmp_path: Path) -> None:
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="f|f\n", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_postgres_role(env, tmp_path)
    assert result.status == "blocked"
    assert "CREATEDB" in (result.remediation_hint or "")
    assert result.observed_state["rolcreatedb"] is False


def test_probe_postgres_role_ok_with_superuser(tmp_path: Path) -> None:
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="t|t\n", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_postgres_role(env, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["rolcreatedb"] is True
    assert result.observed_state["rolsuper"] is True


def test_probe_pgvector_missing_suggests_install(tmp_path: Path) -> None:
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_pgvector(env, tmp_path)
    assert result.status == "missing"
    assert "pgvector" in (result.remediation_hint or "")


# --- probes_runtime ----------------------------------------------------------


def test_probe_api_port_free_ok_when_free(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_runtime.socket.socket") as sock_cls:
        sock = sock_cls.return_value
        sock.bind.return_value = None
        result = probes_runtime.probe_api_port_free({}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["port_free"] is True


def test_probe_api_port_free_ok_when_held_by_praxis(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_runtime.socket.socket") as sock_cls:
        sock = sock_cls.return_value
        sock.bind.side_effect = OSError("address in use")
        with patch(
            "runtime.onboarding.probes_runtime._probe_api_identity",
            return_value={"api_version_header": None, "health_checks_observed": ["postgres", "worker"]},
        ):
            result = probes_runtime.probe_api_port_free({}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["holder"] == "praxis_api"


def test_probe_api_port_free_blocked_when_unknown_holder(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_runtime.socket.socket") as sock_cls:
        sock = sock_cls.return_value
        sock.bind.side_effect = OSError("address in use")
        with patch("runtime.onboarding.probes_runtime._probe_api_identity", return_value=None):
            result = probes_runtime.probe_api_port_free({}, tmp_path)
    assert result.status == "blocked"
    assert result.observed_state["holder"] == "unknown"
    assert "lsof" in (result.remediation_hint or "") or "PRAXIS_API_PORT" in (result.remediation_hint or "")


def test_probe_venv_missing_when_no_python(tmp_path: Path) -> None:
    result = probes_runtime.probe_venv({}, tmp_path)
    assert result.status == "missing"
    assert "./scripts/bootstrap" in (result.remediation_hint or "")


def test_probe_venv_blocked_on_version_mismatch(tmp_path: Path) -> None:
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.touch()
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="3.12\n", stderr="")
    with patch("runtime.onboarding.probes_runtime.subprocess.run", return_value=fake):
        result = probes_runtime.probe_venv({}, tmp_path)
    assert result.status == "blocked"
    assert "3.12" in (result.observed_state.get("version") or "")


def test_probe_launcher_missing_when_not_on_path(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_runtime.shutil.which", return_value=None):
        result = probes_runtime.probe_launcher_installed({}, tmp_path)
    assert result.status == "missing"
    assert "PRAXIS_LOCAL_BIN_DIR" in (result.remediation_hint or "")


def test_probe_launcher_blocked_when_not_praxis_binary(tmp_path: Path) -> None:
    impostor = tmp_path / "praxis"
    impostor.write_text("#!/usr/bin/env bash\necho not praxis\n")
    with patch("runtime.onboarding.probes_runtime.shutil.which", return_value=str(impostor)):
        result = probes_runtime.probe_launcher_installed({}, tmp_path)
    assert result.status == "blocked"
    assert "not a Praxis" in (result.remediation_hint or "")


def test_probe_launcher_ok_when_marker_present(tmp_path: Path) -> None:
    launcher = tmp_path / "praxis"
    launcher.write_text(
        "#!/usr/bin/env python3\n"
        "# Praxis runtime launcher. Managed by ./scripts/bootstrap.\n"
        "print('hi')\n"
    )
    with patch("runtime.onboarding.probes_runtime.shutil.which", return_value=str(launcher)):
        result = probes_runtime.probe_launcher_installed({}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["is_praxis_launcher"] is True


def test_probe_api_healthy_recognizes_praxis_check_shape(tmp_path: Path) -> None:
    class _FakeResponse:
        headers = {}

        def read(self, n):
            return json.dumps(
                {
                    "status": "healthy",
                    "checks": [
                        {"name": "postgres", "ok": True},
                        {"name": "worker", "ok": True},
                        {"name": "workflow", "ok": True},
                    ],
                }
            ).encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch("runtime.onboarding.probes_runtime.urlopen", return_value=_FakeResponse()):
        result = probes_runtime.probe_api_healthy({}, tmp_path)
    assert result.status == "ok"


def test_probe_api_healthy_missing_when_not_responding(tmp_path: Path) -> None:
    from urllib.error import URLError

    with patch("runtime.onboarding.probes_runtime.urlopen", side_effect=URLError("refused")):
        result = probes_runtime.probe_api_healthy({}, tmp_path)
    assert result.status == "missing"
    assert "artifacts/bootstrap/api.log" in (result.remediation_hint or "")


# --- probes_provider ---------------------------------------------------------


def test_probe_openai_ok_with_key(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_provider.resolve_secret", return_value="sk-test"):
        result = probes_provider.probe_openai({"OPENAI_API_KEY": "sk-test"}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["env_var"] == "OPENAI_API_KEY"


def test_probe_openai_missing_suggests_platform_specific_store(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_provider.resolve_secret", return_value=None):
        with patch("runtime.onboarding.probes_provider.sys") as fake_sys:
            fake_sys.platform = "darwin"
            result = probes_provider.probe_openai({}, tmp_path)
    assert result.status == "missing"
    assert "Keychain" in (result.remediation_hint or "")
    assert "security add-generic-password" in (result.remediation_hint or "")


def test_probe_openai_missing_on_linux_suggests_env_export(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_provider.resolve_secret", return_value=None):
        with patch("runtime.onboarding.probes_provider.sys") as fake_sys:
            fake_sys.platform = "linux"
            result = probes_provider.probe_openai({}, tmp_path)
    assert result.status == "missing"
    assert "export OPENAI_API_KEY" in (result.remediation_hint or "")
    assert "~/.bashrc" in (result.remediation_hint or "")


def test_probe_anthropic_cli_missing_when_claude_not_installed(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_provider.shutil.which", return_value=None):
        result = probes_provider.probe_anthropic_cli({}, tmp_path)
    assert result.status == "missing"
    assert "Claude Code CLI" in (result.remediation_hint or "")


def test_probe_anthropic_cli_ok_with_oauth_token(tmp_path: Path) -> None:
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="1.0.0", stderr="")
    with patch("runtime.onboarding.probes_provider.shutil.which", return_value="/usr/bin/claude"):
        with patch("runtime.onboarding.probes_provider.subprocess.run", return_value=fake):
            result = probes_provider.probe_anthropic_cli(
                {"CLAUDE_CODE_OAUTH_TOKEN": "token-here"}, tmp_path
            )
    assert result.status == "ok"
    assert result.observed_state["oauth_token_set"] is True


# --- probes_mcp --------------------------------------------------------------


def test_probe_mcp_missing_when_no_config(tmp_path: Path) -> None:
    result = probes_mcp.probe_claude_code_mcp({"CLAUDE_HOME": str(tmp_path / "empty")}, tmp_path)
    assert result.status == "missing"
    assert "mcpServers" in (result.remediation_hint or "") or "SETUP.md" in (result.remediation_hint or "")


def test_probe_mcp_blocked_on_invalid_json(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    claude_home.mkdir()
    (claude_home / ".mcp.json").write_text("{not valid json")
    result = probes_mcp.probe_claude_code_mcp({"CLAUDE_HOME": str(claude_home)}, tmp_path)
    assert result.status == "blocked"
    assert "JSON" in (result.remediation_hint or "")


def test_probe_mcp_blocked_when_cwd_points_at_wrong_repo(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    claude_home.mkdir()
    (claude_home / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "praxis": {
                        "command": "python",
                        "args": ["-m", "surfaces.mcp.server"],
                        "cwd": "/wrong/path",
                        "env": {"WORKFLOW_DATABASE_URL": "postgresql://x"},
                    }
                }
            }
        )
    )
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    result = probes_mcp.probe_claude_code_mcp({"CLAUDE_HOME": str(claude_home)}, repo_root)
    assert result.status == "blocked"
    assert "cwd" in " ".join(result.observed_state.get("issues", []))


def test_probe_mcp_ok_when_entry_points_at_repo(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    claude_home.mkdir()
    repo_root = tmp_path / "repo"
    workflow_root = repo_root / "Code&DBs" / "Workflow"
    workflow_root.mkdir(parents=True)
    (claude_home / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "praxis": {
                        "command": "python",
                        "args": ["-m", "surfaces.mcp.server"],
                        "cwd": str(workflow_root),
                        "env": {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p"},
                    }
                }
            }
        )
    )
    result = probes_mcp.probe_claude_code_mcp({"CLAUDE_HOME": str(claude_home)}, repo_root)
    assert result.status == "ok"
    assert result.observed_state["database_url_set"] is True


# --- Authority integration ---------------------------------------------------


def test_onboarding_graph_registers_all_expected_gates() -> None:
    refs = {p.gate_ref for p in ONBOARDING_GRAPH.probes()}
    expected = {
        "platform.homebrew",
        "platform.python3_14",
        "platform.psql",
        "platform.postgres_role",
        "platform.pgvector",
        "runtime.api_port_free",
        "runtime.venv",
        "runtime.launcher_installed",
        "runtime.api_healthy",
        "provider.openai",
        "provider.google",
        "provider.openrouter",
        "provider.deepseek",
        "provider.anthropic",
        "mcp.claude_code",
    }
    assert expected <= refs


def test_every_probe_declares_purpose_and_title() -> None:
    for probe in ONBOARDING_GRAPH.probes():
        assert probe.title.strip(), f"{probe.gate_ref} has empty title"
        assert probe.purpose.strip(), f"{probe.gate_ref} has empty purpose"
        assert probe.domain in {"platform", "runtime", "provider", "mcp"}


def test_every_declared_dependency_resolves_to_a_registered_gate() -> None:
    registered = {p.gate_ref for p in ONBOARDING_GRAPH.probes()}
    for probe in ONBOARDING_GRAPH.probes():
        for dep in probe.depends_on:
            assert dep in registered, f"{probe.gate_ref} depends on unregistered {dep}"


def test_graph_evaluation_produces_result_per_applicable_gate() -> None:
    results = ONBOARDING_GRAPH.evaluate({}, _WORKFLOW_ROOT.parent, platform="linux")
    # Homebrew is darwin-only and should be skipped on linux
    refs = {r.gate_ref for r in results}
    assert "platform.homebrew" not in refs
    # Every other gate should produce a result
    total_linux_gates = sum(
        1 for p in ONBOARDING_GRAPH.probes() if not p.platforms or "linux" in p.platforms
    )
    assert len(results) == total_linux_gates


# --- Helpers -----------------------------------------------------------------


def _ok(probe: GateProbe) -> GateResult:
    return _result(probe, status="ok")


def _result(probe, *, status):
    return GateResult(
        gate_ref=probe.gate_ref,
        status=status,
        observed_state={},
        remediation_hint=None,
        remediation_doc_url=None,
        apply_ref=None,
        evaluated_at=datetime.now(timezone.utc),
    )
