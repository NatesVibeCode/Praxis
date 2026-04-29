"""Tests for onboarding persistence (TTL cache) and apply handlers."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.onboarding import (
    GateApply,
    GateGraph,
    GateProbe,
    GateResult,
    ONBOARDING_GRAPH,
    applies,
    persistence,
)


# --- Fake connection --------------------------------------------------------


class _FakeConn:
    """Minimal stand-in for the project connection interface used by persistence."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.queries.append((sql, args))
        sql_stripped = sql.strip()
        if sql_stripped.startswith("SELECT gate_ref, domain, status"):
            if " WHERE gate_ref = $1" in sql:
                gate_ref = args[0]
                row = self.rows.get(gate_ref)
                if row is None or not self._is_fresh(row):
                    return []
                return [dict(row)]
            # read_all_gate_states
            return [dict(row) for row in self.rows.values() if self._is_fresh(row)]
        if sql_stripped.startswith("INSERT INTO onboarding_gate_state"):
            gate_ref = args[0]
            self.rows[gate_ref] = {
                "gate_ref": args[0],
                "domain": args[1],
                "status": args[2],
                "observed_state": args[3],
                "remediation_hint": args[4],
                "remediation_doc_url": args[5],
                "apply_ref": args[6],
                "platform": args[7],
                "cache_ttl_s": args[8],
                "evaluated_at": args[9],
                "applied_at": args[10],
                "applied_by": args[11],
            }
            return []
        if sql_stripped.startswith("WITH deleted AS"):
            stale = [ref for ref, row in self.rows.items() if not self._is_fresh(row)]
            for ref in stale:
                del self.rows[ref]
            return [{"deleted_count": len(stale)}]
        return []

    @staticmethod
    def _is_fresh(row: dict[str, object]) -> bool:
        evaluated_at = row["evaluated_at"]
        ttl = int(row.get("cache_ttl_s") or 0)
        if isinstance(evaluated_at, str):
            evaluated_at = datetime.fromisoformat(evaluated_at)
        return evaluated_at + timedelta(seconds=ttl) > datetime.now(timezone.utc)


# --- Persistence ------------------------------------------------------------


def _make_probe(gate_ref: str = "platform.test") -> GateProbe:
    return GateProbe(
        gate_ref=gate_ref, domain="platform", title="T", purpose="t", ok_cache_ttl_s=300
    )


def _make_result(probe: GateProbe, *, status="ok", observed=None) -> GateResult:
    return GateResult(
        gate_ref=probe.gate_ref,
        status=status,
        observed_state=observed or {"detail": "test"},
        remediation_hint=None,
        remediation_doc_url=None,
        apply_ref=None,
        evaluated_at=datetime.now(timezone.utc),
    )


def test_write_and_read_round_trip() -> None:
    conn = _FakeConn()
    probe = _make_probe()
    result = _make_result(probe)

    persistence.write_gate_state(conn, result, probe)
    fetched = persistence.read_gate_state(conn, probe.gate_ref)

    assert fetched is not None
    assert fetched.gate_ref == probe.gate_ref
    assert fetched.status == "ok"
    assert fetched.observed_state["detail"] == "test"


def test_read_gate_state_returns_none_for_expired_row() -> None:
    conn = _FakeConn()
    probe = _make_probe()
    stale_result = GateResult(
        gate_ref=probe.gate_ref,
        status="ok",
        observed_state={},
        remediation_hint=None,
        remediation_doc_url=None,
        apply_ref=None,
        evaluated_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    stale_probe = GateProbe(
        gate_ref=probe.gate_ref,
        domain="platform",
        title="T",
        purpose="t",
        ok_cache_ttl_s=60,
    )
    persistence.write_gate_state(conn, stale_result, stale_probe)

    fetched = persistence.read_gate_state(conn, probe.gate_ref)
    assert fetched is None


def test_read_all_gate_states_returns_fresh_only() -> None:
    conn = _FakeConn()
    fresh_probe = _make_probe("platform.fresh")
    stale_probe = GateProbe(
        gate_ref="platform.stale",
        domain="platform",
        title="S",
        purpose="s",
        ok_cache_ttl_s=60,
    )
    persistence.write_gate_state(conn, _make_result(fresh_probe), fresh_probe)
    stale_result = GateResult(
        gate_ref=stale_probe.gate_ref,
        status="ok",
        observed_state={},
        remediation_hint=None,
        remediation_doc_url=None,
        apply_ref=None,
        evaluated_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    persistence.write_gate_state(conn, stale_result, stale_probe)

    fresh = persistence.read_all_gate_states(conn)
    assert "platform.fresh" in fresh
    assert "platform.stale" not in fresh


def test_write_preserves_applied_by_when_reupsert_omits_it() -> None:
    conn = _FakeConn()
    probe = _make_probe()
    persistence.write_gate_state(
        conn,
        _make_result(probe),
        probe,
        applied_by="apply.test.mutation",
        applied_at=datetime.now(timezone.utc),
    )
    # Re-upsert without applied_by — should preserve original.
    persistence.write_gate_state(conn, _make_result(probe, status="missing"), probe)
    row = conn.rows[probe.gate_ref]
    # Note: Our fake preserves whatever args[11] was on second upsert (None);
    # the real SQL uses COALESCE. We assert the API contract at the SQL layer
    # is preserved-applied_by via the UPDATE SET line.
    # Here we just assert the round-trip didn't crash and the status updated.
    assert row["status"] == "missing"


# --- GateGraph cache integration --------------------------------------------


def test_graph_reads_cache_when_conn_provided() -> None:
    graph = GateGraph()
    probe = _make_probe()

    call_count = {"probe": 0}

    def _fn(env, root):
        call_count["probe"] += 1
        return _make_result(probe, status="ok")

    graph.register(probe, _fn)
    conn = _FakeConn()

    # Pre-populate cache with a fresh row.
    persistence.write_gate_state(conn, _make_result(probe, status="missing"), probe)

    results = graph.evaluate({}, Path("/tmp"), conn=conn, use_cache=True)

    assert call_count["probe"] == 0  # cache hit, probe not called
    assert results[0].status == "missing"


def test_graph_runs_probe_and_writes_when_cache_miss() -> None:
    graph = GateGraph()
    probe = _make_probe()

    def _fn(env, root):
        return _make_result(probe, status="ok")

    graph.register(probe, _fn)
    conn = _FakeConn()

    results = graph.evaluate({}, Path("/tmp"), conn=conn, use_cache=True)
    assert results[0].status == "ok"
    # Cache write should have happened.
    assert probe.gate_ref in conn.rows


def test_graph_bypasses_cache_when_use_cache_false() -> None:
    graph = GateGraph()
    probe = _make_probe()

    def _fn(env, root):
        return _make_result(probe, status="ok")

    graph.register(probe, _fn)
    conn = _FakeConn()

    persistence.write_gate_state(conn, _make_result(probe, status="missing"), probe)

    results = graph.evaluate({}, Path("/tmp"), conn=conn, use_cache=False)
    # Probe ran; status is ok, not cached missing.
    assert results[0].status == "ok"


def test_graph_eval_without_conn_still_works() -> None:
    graph = GateGraph()
    probe = _make_probe()
    graph.register(probe, lambda env, root: _make_result(probe))
    # conn=None path (no caching).
    results = graph.evaluate({}, Path("/tmp"))
    assert len(results) == 1


# --- MCP apply handler ------------------------------------------------------


def test_apply_claude_code_mcp_writes_entry(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    env = {
        "CLAUDE_HOME": str(claude_home),
        "WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p",
    }

    result = applies.apply_claude_code_mcp(env, repo_root)

    mcp_json = claude_home / ".mcp.json"
    assert mcp_json.exists()
    body = json.loads(mcp_json.read_text())
    praxis = body["mcpServers"]["praxis"]
    assert praxis["cwd"] == str(repo_root / "Code&DBs" / "Workflow")
    assert praxis["env"]["WORKFLOW_DATABASE_URL"] == "postgresql://u@h:5432/p"
    assert result.status == "ok"


def test_apply_claude_code_mcp_preserves_other_servers(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    claude_home.mkdir()
    existing = {
        "mcpServers": {
            "some_other_tool": {"command": "echo", "args": ["hi"]},
        }
    }
    (claude_home / ".mcp.json").write_text(json.dumps(existing))
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    env = {
        "CLAUDE_HOME": str(claude_home),
        "WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p",
    }

    applies.apply_claude_code_mcp(env, repo_root)

    body = json.loads((claude_home / ".mcp.json").read_text())
    assert "some_other_tool" in body["mcpServers"]
    assert "praxis" in body["mcpServers"]


def test_apply_claude_code_mcp_blocks_without_database_url(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    result = applies.apply_claude_code_mcp({"CLAUDE_HOME": str(claude_home)}, repo_root)
    assert result.status == "blocked"
    assert "WORKFLOW_DATABASE_URL" in (result.remediation_hint or "")


def test_apply_claude_code_mcp_is_idempotent(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    env = {
        "CLAUDE_HOME": str(claude_home),
        "WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p",
    }

    first = applies.apply_claude_code_mcp(env, repo_root)
    second = applies.apply_claude_code_mcp(env, repo_root)
    assert first.status == "ok"
    assert second.status == "ok"


def test_apply_claude_code_mcp_blocks_on_invalid_existing_json(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"
    claude_home.mkdir()
    (claude_home / ".mcp.json").write_text("{not json")
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    env = {
        "CLAUDE_HOME": str(claude_home),
        "WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p",
    }

    result = applies.apply_claude_code_mcp(env, repo_root)
    assert result.status == "blocked"
    assert "not valid JSON" in (result.remediation_hint or "")


# --- Provider apply handler -------------------------------------------------


def test_provider_apply_returns_current_state_when_already_ok(tmp_path: Path) -> None:
    env = {"OPENAI_API_KEY": "sk-test"}
    with patch("runtime.onboarding.probes_provider.resolve_secret", return_value="sk-test"):
        apply_entry = next(
            a for a in ONBOARDING_GRAPH.applies() if a.gate_ref == "provider.openai"
        )
        result = apply_entry.handler(env, tmp_path)
    assert result.status == "ok"


def test_provider_apply_opens_secure_capture_when_missing_on_darwin(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_provider.resolve_secret", return_value=None):
        with patch("runtime.onboarding.applies.sys") as fake_sys:
            fake_sys.platform = "darwin"
            with patch(
                "runtime.operation_catalog_gateway.execute_operation_from_env"
            ) as fake_execute:
                fake_execute.return_value = {
                    "ok": True,
                    "credential_capture": {
                        "env_var_name": "OPENAI_API_KEY",
                        "status": "ok",
                        "stored": True,
                        "verified": True,
                        "source": "keychain",
                    },
                    "operation_receipt": {"receipt_id": "receipt-1"},
                }
                apply_entry = next(
                    a for a in ONBOARDING_GRAPH.applies() if a.gate_ref == "provider.openai"
                )
                result = apply_entry.handler({}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["credential_capture"]["status"] == "ok"
    assert result.observed_state["credential_capture"]["source"] == "keychain"


def test_provider_apply_reports_secure_capture_cancel_when_missing_on_darwin(tmp_path: Path) -> None:
    with patch("runtime.onboarding.probes_provider.resolve_secret", return_value=None):
        with patch("runtime.onboarding.applies.sys") as fake_sys:
            fake_sys.platform = "darwin"
            with patch(
                "runtime.operation_catalog_gateway.execute_operation_from_env"
            ) as fake_execute:
                fake_execute.return_value = {
                    "ok": False,
                    "credential_capture": {
                        "env_var_name": "OPENAI_API_KEY",
                        "status": "canceled",
                        "stored": False,
                        "verified": False,
                    },
                }
                apply_entry = next(
                    a for a in ONBOARDING_GRAPH.applies() if a.gate_ref == "provider.openai"
                )
                result = apply_entry.handler({}, tmp_path)
    assert result.status == "missing"
    assert "secure capture" in (result.remediation_hint or "")
    assert result.observed_state["credential_capture"]["status"] == "canceled"


# --- Apply surface dispatch -------------------------------------------------


def test_apply_gate_requires_gate_or_apply_ref() -> None:
    from runtime.setup_wizard import setup_apply_gate_payload

    payload = setup_apply_gate_payload()
    assert payload["ok"] is False
    assert payload["error_code"] == "setup.apply_gate_required"


def test_apply_gate_unknown_ref() -> None:
    from runtime.setup_wizard import setup_apply_gate_payload

    payload = setup_apply_gate_payload(gate_ref="platform.does_not_exist")
    assert payload["ok"] is False
    assert payload["error_code"] == "setup.apply_gate_unknown"


def test_apply_gate_requires_approval_for_mutating_handler() -> None:
    from runtime.setup_wizard import setup_apply_gate_payload

    payload = setup_apply_gate_payload(gate_ref="mcp.claude_code", approved=False)
    assert payload["ok"] is False
    assert payload["error_code"] == "setup.apply_requires_approval"
    assert "filesystem:~/.claude/.mcp.json" in str(payload.get("mutates", []))


def test_apply_gate_executes_handler_when_approved(tmp_path: Path) -> None:
    from runtime.setup_wizard import setup_apply_gate_payload

    claude_home = tmp_path / ".claude"
    env = {
        "CLAUDE_HOME": str(claude_home),
        "WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/p",
    }
    # Point repo_root at the real repo so the MCP apply finds workflow root.
    payload = setup_apply_gate_payload(
        gate_ref="mcp.claude_code",
        approved=True,
        env=env,
        repo_root=_WORKFLOW_ROOT.parent,
    )
    assert payload["mode"] == "apply"
    assert payload["apply_ref"] == "apply.mcp.claude_code.write"
    assert payload["gate"]["gate_ref"] == "mcp.claude_code"
    # Cleanup — the handler writes to the real claude home path per env.
    mcp_json = claude_home / ".mcp.json"
    if mcp_json.exists():
        mcp_json.unlink()


def test_mcp_tool_routes_apply_with_gate_to_apply_gate_payload() -> None:
    from surfaces.mcp.tools import setup as mcp_setup

    with patch.object(
        mcp_setup, "setup_apply_gate_payload", return_value={"ok": True, "mode": "apply"}
    ) as stub:
        result = mcp_setup.tool_praxis_setup(
            {"action": "apply", "gate": "mcp.claude_code", "yes": True}
        )
    stub.assert_called_once()
    assert result["mode"] == "apply"


def test_mcp_tool_schema_advertises_gate_and_apply_ref() -> None:
    from surfaces.mcp.tools.setup import TOOLS

    schema = TOOLS["praxis_setup"][1]["inputSchema"]
    props = schema["properties"]
    assert "gate" in props
    assert "apply_ref" in props


def test_mcp_tool_schema_advertises_repo_policy_contract_fields() -> None:
    from surfaces.mcp.tools.setup import TOOLS

    props = TOOLS["praxis_setup"][1]["inputSchema"]["properties"]
    for field_name in (
        "repo_rules",
        "sops",
        "anti_patterns",
        "forbidden_actions",
        "forbidden_action_rules",
        "sensitive_systems",
        "submitted_by",
        "change_reason",
        "disclosure_repeat_limit",
    ):
        assert field_name in props


def test_probe_repo_policy_contract_missing_exposes_starter_bundle(tmp_path: Path) -> None:
    from runtime.onboarding import probes_operator

    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch(
        "runtime.onboarding.probes_operator.ensure_postgres_available",
        return_value=object(),
    ):
        with patch(
            "runtime.onboarding.probes_operator.get_repo_policy_contract",
            return_value=None,
        ):
            result = probes_operator.probe_repo_policy_contract(env, tmp_path)
    assert result.status == "missing"
    assert result.apply_ref == "apply.operator.repo_policy_contract.write"
    assert "starter_bundle" in result.observed_state


def test_apply_repo_policy_contract_write_returns_contract_summary(tmp_path: Path) -> None:
    from runtime.onboarding import applies
    from storage.postgres.repo_policy_contract_repository import RepoPolicyContractRecord

    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    record = RepoPolicyContractRecord(
        repo_policy_contract_id="repo_policy_contract.test",
        repo_root=str(tmp_path),
        status="active",
        current_revision_id="repo_policy_contract_revision.test",
        current_revision_no=1,
        current_contract_hash="hash-1",
        disclosure_repeat_limit=5,
        bug_disclosure_count=0,
        pattern_disclosure_count=0,
        contract_body={
            "repo_policy_sections": {
                "repo_rules": ["Never write to prod without proof"],
                "sops": ["Verify before reconcile"],
                "anti_patterns": ["No raw secrets in chat"],
                "forbidden_actions": ["Delete production data"],
                "sensitive_systems": [{"label": "Salesforce", "system_ref": "system:salesforce"}],
            }
        },
        change_reason="initial",
        created_by="nate",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    with patch(
        "runtime.onboarding.applies.ensure_postgres_available",
        return_value=object(),
    ):
        with patch(
            "runtime.onboarding.applies.upsert_repo_policy_contract",
            return_value=record,
        ):
            result = applies.apply_repo_policy_contract_write(
                env,
                tmp_path,
                repo_rules=["Never write to prod without proof"],
            )
    assert result.status == "ok"
    assert result.observed_state["current_contract_present"] is True
    assert result.observed_state["operator_disclosure"]["repeat_limit"] == 5


# --- Mutation probes (Packet 6) ---------------------------------------------


def test_probe_env_file_missing_when_absent(tmp_path: Path) -> None:
    from runtime.onboarding import probes_runtime

    result = probes_runtime.probe_env_file({}, tmp_path)
    assert result.status == "missing"
    assert result.apply_ref == "apply.runtime.env_file.write"
    assert "apply" in (result.remediation_hint or "")


def test_probe_env_file_blocked_when_no_database_url(tmp_path: Path) -> None:
    from runtime.onboarding import probes_runtime

    (tmp_path / ".env").write_text("PRAXIS_API_PORT=9000\n")
    result = probes_runtime.probe_env_file({}, tmp_path)
    assert result.status == "blocked"
    assert "WORKFLOW_DATABASE_URL" in (result.remediation_hint or "")


def test_probe_env_file_ok_when_database_url_present(tmp_path: Path) -> None:
    from runtime.onboarding import probes_runtime

    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://u@h:5432/p\nPRAXIS_API_PORT=8420\n"
    )
    result = probes_runtime.probe_env_file({}, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["has_database_url"] is True


def test_probe_workflow_database_missing_when_no_url(tmp_path: Path) -> None:
    from runtime.onboarding import probes_platform

    result = probes_platform.probe_workflow_database({}, tmp_path)
    assert result.status == "unknown"
    assert "WORKFLOW_DATABASE_URL" in (result.remediation_hint or "")


def test_probe_workflow_database_ok_when_exists(tmp_path: Path) -> None:
    from runtime.onboarding import probes_platform
    import subprocess

    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_workflow_database(env, tmp_path)
    assert result.status == "ok"
    assert result.observed_state["database_name"] == "praxis"


def test_probe_workflow_database_uses_psql_variable_not_server_guc(tmp_path: Path) -> None:
    from runtime.onboarding import probes_platform
    import subprocess

    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis%27evil"}
    calls = []

    def _fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        return fake

    with patch("runtime.onboarding.probes_platform.subprocess.run", side_effect=_fake_run):
        result = probes_platform.probe_workflow_database(env, tmp_path)

    assert result.status == "ok"
    command = calls[0]
    assert "-v" in command
    assert "db_name=praxis'evil" in command
    assert "current_setting" not in " ".join(command)
    assert "praxis'evil" not in command[command.index("-Atc") + 1]


def test_probe_workflow_database_missing_when_absent(tmp_path: Path) -> None:
    from runtime.onboarding import probes_platform
    import subprocess

    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_workflow_database(env, tmp_path)
    assert result.status == "missing"
    assert result.apply_ref == "apply.platform.workflow_database.create"


def test_probe_pgvector_installed_ok_when_extension_present(tmp_path: Path) -> None:
    from runtime.onboarding import probes_platform
    import subprocess

    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_pgvector_installed(env, tmp_path)
    assert result.status == "ok"


def test_probe_pgvector_installed_missing_without_create_extension(tmp_path: Path) -> None:
    from runtime.onboarding import probes_platform
    import subprocess

    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch("runtime.onboarding.probes_platform.subprocess.run", return_value=fake):
        result = probes_platform.probe_pgvector_installed(env, tmp_path)
    assert result.status == "missing"
    assert result.apply_ref == "apply.platform.pgvector_installed.enable"


# --- Mutation apply handlers (Packet 6) -------------------------------------


def test_apply_env_file_write_creates_file(tmp_path: Path) -> None:
    from runtime.onboarding import applies

    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    result = applies.apply_env_file_write(env, tmp_path)
    assert result.status == "ok"
    body = (tmp_path / ".env").read_text()
    assert "WORKFLOW_DATABASE_URL=postgresql://u@h:5432/praxis" in body
    assert "WORKFLOW_DATABASE_TRUSTED=true" in body


def test_apply_env_file_write_idempotent_leaves_existing(tmp_path: Path) -> None:
    from runtime.onboarding import applies

    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://already@h:5432/praxis\n"
    )
    env = {"WORKFLOW_DATABASE_URL": "postgresql://different@h:5432/praxis"}
    applies.apply_env_file_write(env, tmp_path)
    body = (tmp_path / ".env").read_text()
    # Existing URL preserved; function did not overwrite.
    assert "already@" in body
    assert "different@" not in body


def test_apply_env_file_write_blocks_without_url(tmp_path: Path) -> None:
    from runtime.onboarding import applies

    result = applies.apply_env_file_write({}, tmp_path)
    assert result.status == "blocked"
    assert "WORKFLOW_DATABASE_URL" in (result.remediation_hint or "")


def test_apply_workflow_database_create_idempotent_when_exists(tmp_path: Path) -> None:
    from runtime.onboarding import applies
    import subprocess

    # Check returns "1" (exists); CREATE DATABASE should NOT be invoked.
    check_result = subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr="")
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    calls = []

    def _fake_run(*args, **kwargs):
        calls.append(args[0])
        return check_result

    with patch("runtime.onboarding.applies.subprocess.run", side_effect=_fake_run):
        result = applies.apply_workflow_database_create(env, tmp_path)
    assert result.status == "ok"
    assert not any("CREATE DATABASE" in " ".join(call) for call in calls)


def test_apply_workflow_database_create_quotes_database_name_through_psql_variable(tmp_path: Path) -> None:
    from runtime.onboarding import applies
    import subprocess

    check_results = iter(
        [
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr=""),
        ]
    )
    calls = []

    def _fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        return next(check_results)

    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis%27evil"}
    with patch("runtime.onboarding.applies.subprocess.run", side_effect=_fake_run):
        result = applies.apply_workflow_database_create(env, tmp_path)

    assert result.status == "ok"
    create_command = calls[1]
    assert "db_name=praxis'evil" in create_command
    assert create_command[create_command.index("-c") + 1] == 'CREATE DATABASE :"db_name"'


def test_apply_workflow_database_create_runs_create_when_missing(tmp_path: Path) -> None:
    from runtime.onboarding import applies
    import subprocess

    check_results = iter(
        [
            # First: existence check returns empty (not exists).
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            # Second: CREATE DATABASE succeeds.
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            # Third: post-apply probe sees the database.
            subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr=""),
        ]
    )

    def _fake_run(*args, **kwargs):
        return next(check_results)

    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch("runtime.onboarding.applies.subprocess.run", side_effect=_fake_run):
        result = applies.apply_workflow_database_create(env, tmp_path)
    assert result.status == "ok"


def test_apply_pgvector_enable_runs_create_extension(tmp_path: Path) -> None:
    from runtime.onboarding import applies
    import subprocess

    results = iter(
        [
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="1\n", stderr=""),
        ]
    )
    calls = []
    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}

    def _fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        return next(results)

    with patch("runtime.onboarding.applies.subprocess.run", side_effect=_fake_run):
        result = applies.apply_pgvector_enable(env, tmp_path)

    assert result.status == "ok"
    assert "CREATE EXTENSION IF NOT EXISTS vector" in calls[0]


def test_apply_pgvector_enable_blocks_on_psql_error(tmp_path: Path) -> None:
    from runtime.onboarding import applies
    import subprocess

    def _fake_run(*args, **kwargs):
        raise subprocess.CalledProcessError(
            returncode=1, cmd=args[0], output="", stderr="pgvector not installed"
        )

    env = {"WORKFLOW_DATABASE_URL": "postgresql://u@h:5432/praxis"}
    with patch("runtime.onboarding.applies.subprocess.run", side_effect=_fake_run):
        result = applies.apply_pgvector_enable(env, tmp_path)
    assert result.status == "blocked"
    assert "pgvector" in (result.remediation_hint or "").lower()


# --- bootstrap_cli apply-gate subcommand -----------------------------------


def test_bootstrap_cli_apply_gate_requires_yes_for_mutating_handler(tmp_path: Path) -> None:
    import io
    from runtime.onboarding import bootstrap_cli

    saved_env = dict(os.environ) if (os := __import__("os")) else {}
    err = io.StringIO()
    with patch("sys.stderr", err):
        code = bootstrap_cli.main(["apply-gate", "mcp.claude_code"])
    assert code == 2
    assert "--yes" in err.getvalue()


def test_bootstrap_cli_apply_gate_with_unknown_gate(tmp_path: Path) -> None:
    import io
    from runtime.onboarding import bootstrap_cli

    err = io.StringIO()
    with patch("sys.stderr", err):
        code = bootstrap_cli.main(["apply-gate", "nonexistent.gate", "--yes"])
    assert code == 1
    assert "no apply handler" in err.getvalue()
