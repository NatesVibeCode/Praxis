from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from surfaces.api.handlers import workflow_admin
from surfaces.mcp.catalog import get_tool_catalog
from runtime.primitive_contracts import (
    bug_open_status_values,
    bug_resolved_status_values,
    redact_url,
    resolve_runtime_http_endpoints,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
LOCAL_ALPHA_PATH = REPO_ROOT / "scripts" / "praxis_ctl_local_alpha.py"


def _load_local_alpha():
    spec = importlib.util.spec_from_file_location("praxis_ctl_local_alpha_test", LOCAL_ALPHA_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeReceiptIngester:
    def load_recent(self, since_hours: int = 24):
        del since_hours
        return [{"run_id": "run-1"}]

    def compute_pass_rate(self, receipts):
        del receipts
        return 1.0

    def top_failure_codes(self, receipts):
        del receipts
        return []


class _FakeSubsystems:
    def get_receipt_ingester(self):
        return _FakeReceiptIngester()


def test_orient_includes_dependency_truth(monkeypatch) -> None:
    fake_dependency_truth = {
        "ok": True,
        "scope": "all",
        "manifest_path": "/tmp/requirements.runtime.txt",
        "required_count": 2,
        "available_count": 2,
        "missing_count": 0,
        "packages": [],
        "missing": [],
    }

    monkeypatch.setattr(
        workflow_admin,
        "dependency_truth_report",
        lambda scope="all": fake_dependency_truth,
    )
    monkeypatch.setattr(
        workflow_admin,
        "_handle_health",
        lambda subs, body: {
            "preflight": {"overall": "healthy"},
            "operator_snapshot": {},
            "proof_metrics": {},
            "schema_authority": {},
            "lane_recommendation": {},
        },
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_code_hotspots",
        lambda **kwargs: {"authority": "code_hotspots", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_bug_scoreboard",
        lambda **kwargs: {"authority": "bug_scoreboard", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_platform_observability",
        lambda **kwargs: {"authority": "platform_observability", "kwargs": kwargs},
    )

    result = workflow_admin._handle_orient(_FakeSubsystems(), {})

    assert result["dependency_truth"] == fake_dependency_truth
    assert result["recent_activity"] == {
        "total_workflows_24h": 1,
        "pass_rate": 1.0,
        "top_failure_codes": [],
    }
    assert result["engineering_observability"]["code_hotspots"]["authority"] == "code_hotspots"
    assert result["engineering_observability"]["bug_scoreboard"]["authority"] == "bug_scoreboard"
    assert result["engineering_observability"]["platform_observability"]["authority"] == "platform_observability"


def test_orient_advertises_catalog_backed_cli(monkeypatch) -> None:
    monkeypatch.setattr(workflow_admin, "dependency_truth_report", lambda scope="all": {"ok": True})
    monkeypatch.setattr(
        workflow_admin,
        "_handle_health",
        lambda subs, body: {
            "preflight": {"overall": "healthy"},
            "operator_snapshot": {},
            "proof_metrics": {},
            "schema_authority": {},
            "lane_recommendation": {},
        },
    )

    result = workflow_admin._handle_orient(_FakeSubsystems(), {})

    architecture_scan = result["search_surfaces"]["architecture_scan"]
    assert "workflow architecture scan" in architecture_scan
    assert "raw SQL literals" in architecture_scan

    code_discovery = result["search_surfaces"]["code_discovery"]
    assert "workflow discover" in code_discovery
    assert "workflow tools describe praxis_discover" in code_discovery
    assert "praxis_discover" in code_discovery
    assert "hybrid retrieval" in code_discovery

    knowledge_graph = result["search_surfaces"]["knowledge_graph"]
    assert "workflow recall" in knowledge_graph
    assert "workflow tools describe praxis_recall" in knowledge_graph
    assert "graph traversal" in knowledge_graph

    cli_surface = result["cli_surface"]
    tool_count = len(get_tool_catalog())
    assert cli_surface["preferred"] is True
    assert cli_surface["tool_count"] == tool_count
    discovery_commands = {item["command"]: item for item in cli_surface["discovery_commands"]}
    assert "workflow tools list" in discovery_commands
    assert "failure" in discovery_commands["workflow tools search <text>"]["examples"][0]
    assert "workflow architecture scan" in discovery_commands
    assert "--scope surfaces --json" in discovery_commands["workflow architecture scan"]["examples"][1]
    recommended_reads = {item["command"]: item for item in cli_surface["recommended_reads"]}
    assert "workflow query" in recommended_reads
    assert "what is failing right now?" in recommended_reads["workflow query"]["examples"][0]
    assert "workflow health" in recommended_reads
    assert "retry logic with exponential backoff" in recommended_reads["workflow discover"]["examples"][0]

    instructions = result["instructions"]
    assert "Prefer the catalog-backed `workflow` CLI" in instructions
    assert f"There are currently {tool_count} catalog-backed tools" in instructions
    assert "workflow tools list" in instructions
    assert "workflow health" in instructions
    assert "workflow tools call <tool|alias|entrypoint>" in instructions
    assert "write/dispatch flows require `--yes`" in instructions
    assert "workflow query" in instructions
    assert "workflow architecture scan" in instructions
    assert "kickoff first" in instructions


def test_orient_projects_mandatory_authority_envelope(monkeypatch) -> None:
    fake_dependency_truth = {"ok": True, "missing_count": 0}
    fake_native_instance = {
        "praxis_instance_name": "praxis",
        "praxis_runtime_profile": "praxis",
        "repo_root": "/repo",
        "workdir": "/repo",
    }
    fake_standing_orders = [
        {
            "authority_domain": "orient",
            "policy_slug": "architecture-policy::orient::mandatory-authority-envelope",
            "title": "Orient is the mandatory runtime authority envelope",
        }
    ]

    monkeypatch.setattr(
        workflow_admin,
        "dependency_truth_report",
        lambda scope="all": fake_dependency_truth,
    )
    monkeypatch.setattr(
        workflow_admin,
        "_handle_health",
        lambda subs, body: {
            "preflight": {"overall": "healthy"},
            "operator_snapshot": {},
            "proof_metrics": {},
            "schema_authority": {},
            "lane_recommendation": {"recommended_posture": "build"},
        },
    )
    monkeypatch.setattr(
        workflow_admin,
        "_build_standing_orders",
        lambda subs: fake_standing_orders,
    )
    monkeypatch.setattr(
        workflow_admin,
        "_workflow_env",
        lambda subs: {
            "WORKFLOW_DATABASE_URL": "postgresql://nate:secret@repo.test:5432/praxis",
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "repo_env:/repo/.env",
            "PRAXIS_API_BASE_URL": "http://praxis.test:8420",
        },
    )
    monkeypatch.setattr(
        workflow_admin,
        "native_instance_contract",
        lambda env=None: fake_native_instance,
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_code_hotspots",
        lambda **kwargs: {"authority": "code_hotspots", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_bug_scoreboard",
        lambda **kwargs: {"authority": "bug_scoreboard", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_platform_observability",
        lambda **kwargs: {"authority": "platform_observability", "kwargs": kwargs},
    )

    result = workflow_admin._handle_orient(_FakeSubsystems(), {})

    envelope = result["authority_envelope"]
    assert result["native_instance"] == fake_native_instance
    assert result["instruction_authority"]["packet_read_order"][:4] == [
        "standing_orders",
        "authority_envelope",
        "tool_guidance",
        "primitive_contracts",
    ]
    assert result["instruction_authority"]["downstream_truth_surfaces"]["primitive_contracts"] == (
        "/orient#primitive_contracts"
    )
    assert envelope["kind"] == "orient_authority_envelope"
    assert envelope["mandatory"] is True
    assert envelope["policy_decision_ref"] == (
        "operator_decision.architecture_policy.orient.mandatory_authority_envelope"
    )
    assert envelope["native_instance"] == fake_native_instance
    assert envelope["standing_orders_count"] == 1
    assert envelope["health_overall"] == "healthy"
    assert envelope["lane_recommendation"] == {"recommended_posture": "build"}
    assert envelope["dependency_truth"] == {"ok": True, "missing_count": 0}
    assert envelope["scope_source"]["default"] == "/orient#authority_envelope.native_instance"
    assert envelope["tool_guidance"] == result["tool_guidance"]
    assert envelope["primitive_contracts"] == result["primitive_contracts"]
    assert envelope["primitive_contracts_ref"] == "/orient#primitive_contracts"

    tool_guidance = result["tool_guidance"]
    assert tool_guidance["kind"] == "orient_tool_guidance"
    assert tool_guidance["policy_decision_ref"] == (
        "operator_decision.architecture_policy.orient.authority_envelope_tool_guidance"
    )
    assert tool_guidance["preferred_operator_surface"]["command_prefix"] == "workflow"
    assert tool_guidance["catalog"]["schema_command"] == "workflow tools describe <tool|alias|entrypoint>"
    assert tool_guidance["catalog"]["directive"].startswith("Inspect the live catalog")
    primary_read_commands = {item["command"] for item in tool_guidance["primary_reads"]}
    assert {
        "workflow query",
        "workflow health",
        "workflow discover",
        "workflow recall",
        "workflow bugs",
    }.issubset(primary_read_commands)
    assert tool_guidance["dispatch"]["command"] == "workflow tools call praxis_workflow"
    assert tool_guidance["guardrails"] == {
        "write_dispatch_requires_yes": True,
        "session_tools_require_workflow_token": True,
        "search_before_build": True,
    }

    primitive_contracts = result["primitive_contracts"]
    assert primitive_contracts["kind"] == "orient_primitive_contracts"
    assert primitive_contracts["policy_decision_ref"] == (
        "operator_decision.architecture_policy.primitive_contracts."
        "orient_projects_operation_runtime_state_contracts"
    )

    operation_posture = primitive_contracts["operation_posture"]
    assert operation_posture["catalog_postures"] == ["build", "observe", "operate"]
    assert operation_posture["posture_rules"]["observe"]["forbids"] == ["mutate"]
    assert operation_posture["semantic_operations"]["repair"]["requires"] == [
        "proof_ref",
        "before_state_ref",
        "after_state_ref",
    ]

    runtime_binding = primitive_contracts["runtime_binding"]
    assert runtime_binding["database"]["env_ref"] == "WORKFLOW_DATABASE_URL"
    assert runtime_binding["database"]["authority_source"] == "repo_env:/repo/.env"
    assert runtime_binding["database"]["redacted_url"] == (
        "postgresql://nate:***@repo.test:5432/praxis"
    )
    assert runtime_binding["database"]["secret_policy"].startswith("never emit raw DSN")
    assert runtime_binding["http_endpoints"]["authority_source"] == "env:PRAXIS_API_BASE_URL"
    assert runtime_binding["http_endpoints"]["launch_url"] == "http://praxis.test:8420/app"
    assert runtime_binding["workspace"]["repo_root"] == "/repo"

    state_semantics = primitive_contracts["state_semantics"]["bug"]
    assert state_semantics["open_statuses"] == ["OPEN", "IN_PROGRESS"]
    assert state_semantics["resolved_statuses"] == ["FIXED", "WONT_FIX", "DEFERRED"]
    assert state_semantics["status_predicates"]["IN_PROGRESS"]["is_open"] is True
    assert state_semantics["status_predicates"]["WONT_FIX"]["is_resolved"] is True

    proof_ref = primitive_contracts["proof_ref"]
    assert "decision" in proof_ref["allowed_ref_kinds"]
    assert proof_ref["replay_ref"]["blocked_reason_field"] == "replay_reason_code"

    failure_identity = primitive_contracts["failure_identity"]
    assert failure_identity["authority"] == "runtime.bug_evidence.build_failure_signature"
    assert failure_identity["fingerprint_field"] == "fingerprint"


def test_runtime_http_endpoints_resolve_from_binding_authority() -> None:
    endpoints = resolve_runtime_http_endpoints(
        workflow_env={"PRAXIS_API_BASE_URL": "http://praxis.test:9444"},
        native_instance={"repo_root": "/repo", "workdir": "/repo"},
    )

    assert endpoints == {
        "api_base_url": "http://praxis.test:9444",
        "launch_url": "http://praxis.test:9444/app",
        "dashboard_url": "http://praxis.test:9444/app",
        "api_docs_url": "http://praxis.test:9444/docs",
        "authority_source": "env:PRAXIS_API_BASE_URL",
    }


def test_primitive_contract_helpers_are_secret_safe_and_predicate_backed() -> None:
    assert redact_url("postgresql://user:pass@db.local:5432/praxis?sslmode=require") == (
        "postgresql://user:***@db.local:5432/praxis"
    )
    assert bug_open_status_values() == ("OPEN", "IN_PROGRESS")
    assert bug_resolved_status_values() == ("FIXED", "WONT_FIX", "DEFERRED")


def test_praxis_ctl_frontdoor_urls_come_from_runtime_binding(monkeypatch) -> None:
    local_alpha = _load_local_alpha()
    calls: list[str] = []

    def _fake_http_request(url: str, **kwargs):
        del kwargs
        calls.append(url)
        if url.endswith("/api/health"):
            return 200, b'{"status": "healthy"}'
        if url.endswith("/orient"):
            return 200, b'{"platform": "praxis-workflow"}'
        if url.endswith("/mcp"):
            return 200, b'{"result": {"serverInfo": {"name": "praxis-mcp"}}}'
        if url.endswith("/app"):
            return 200, b'<title>Praxis</title><div id="root"></div>'
        return 404, b"{}"

    monkeypatch.setenv("PRAXIS_API_BASE_URL", "https://praxis.example:9443")
    monkeypatch.delenv("PRAXIS_WORKFLOW_API_BASE_URL", raising=False)
    monkeypatch.setattr(local_alpha, "_http_request", _fake_http_request)

    payload = local_alpha._probe_frontdoor_semantics()

    assert not hasattr(local_alpha, "DEFAULT_API_BASE_URL")
    assert payload["api_server_ready"] is True
    assert payload["workflow_api_ready"] is True
    assert payload["mcp_bridge_ready"] is True
    assert payload["ui_ready"] is True
    assert payload["launch_url"] == "https://praxis.example:9443/app"
    assert payload["dashboard_url"] == "https://praxis.example:9443/app"
    assert payload["api_docs_url"] == "https://praxis.example:9443/docs"
    assert calls == [
        "https://praxis.example:9443/api/health",
        "https://praxis.example:9443/orient",
        "https://praxis.example:9443/mcp",
        "https://praxis.example:9443/app",
    ]


def test_praxis_ctl_doctor_includes_dependency_truth(monkeypatch, tmp_path: Path, capsys) -> None:
    local_alpha = _load_local_alpha()
    fake_dependency_truth = {
        "ok": False,
        "scope": "all",
        "manifest_path": "/tmp/requirements.runtime.txt",
        "required_count": 4,
        "available_count": 3,
        "missing_count": 1,
        "packages": [],
        "missing": [],
    }

    class _FakeSyncStatus:
        sync_status = "skipped"
        sync_cycle_id = None
        sync_error_count = 0

    monkeypatch.setattr(
        local_alpha,
        "workflow_database_status_payload",
        lambda env=None: {
            "database_reachable": True,
            "schema_bootstrapped": True,
            "workflow_operational": True,
            "missing_schema_objects": [],
        },
    )
    monkeypatch.setattr(
        local_alpha,
        "_env_for_authority",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/praxis"},
    )
    monkeypatch.setattr(local_alpha, "dependency_truth_report", lambda scope="all": fake_dependency_truth)
    monkeypatch.setattr(local_alpha, "latest_workflow_run_sync_status", lambda: _FakeSyncStatus())
    monkeypatch.setattr(local_alpha, "get_workflow_run_sync_status", lambda run_id: _FakeSyncStatus())
    monkeypatch.setattr(
        local_alpha,
        "_probe_frontdoor_semantics",
        lambda: {
            "api_server_ready": True,
            "workflow_api_ready": True,
            "mcp_bridge_ready": True,
            "ui_ready": True,
            "launch_url": "http://127.0.0.1:8420/app",
            "dashboard_url": "http://127.0.0.1:8420/app",
            "api_docs_url": "http://127.0.0.1:8420/docs",
        },
    )

    exit_code = local_alpha.cmd_doctor(services_ready="true", state_file=str(tmp_path / "state.json"))
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["dependency_truth"] == fake_dependency_truth
    assert payload["database_reachable"] is True
    assert payload["schema_bootstrapped"] is True
    assert payload["workflow_operational"] is True
    assert payload["api_server_ready"] is True
    assert payload["workflow_api_ready"] is True
    assert payload["mcp_bridge_ready"] is True
    assert payload["ui_ready"] is True
    assert payload["launch_url"] == "http://127.0.0.1:8420/app"


def test_praxis_ctl_doctor_reports_operational_authority_with_schema_drift(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    local_alpha = _load_local_alpha()

    class _FakeSyncStatus:
        sync_status = "skipped"
        sync_cycle_id = None
        sync_error_count = 0

    monkeypatch.setattr(
        local_alpha,
        "workflow_database_status_payload",
        lambda env=None: {
            "database_reachable": True,
            "schema_bootstrapped": False,
            "missing_schema_objects": ["data_dictionary_effective"],
            "compile_artifact_authority_ready": True,
            "compile_index_authority_ready": True,
            "execution_packet_authority_ready": True,
            "repo_snapshot_authority_ready": True,
            "verification_registry_ready": True,
            "verifier_authority_ready": True,
            "healer_authority_ready": True,
        },
    )
    monkeypatch.setattr(
        local_alpha,
        "_env_for_authority",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/praxis"},
    )
    monkeypatch.setattr(local_alpha, "dependency_truth_report", lambda scope="all": {"ok": True})
    monkeypatch.setattr(local_alpha, "latest_workflow_run_sync_status", lambda: _FakeSyncStatus())
    monkeypatch.setattr(local_alpha, "get_workflow_run_sync_status", lambda run_id: _FakeSyncStatus())
    monkeypatch.setattr(
        local_alpha,
        "_probe_frontdoor_semantics",
        lambda: {
            "api_server_ready": True,
            "workflow_api_ready": True,
            "mcp_bridge_ready": True,
            "ui_ready": True,
            "launch_url": "http://127.0.0.1:8420/app",
            "dashboard_url": "http://127.0.0.1:8420/app",
            "api_docs_url": "http://127.0.0.1:8420/docs",
        },
    )

    exit_code = local_alpha.cmd_doctor(services_ready="true", state_file=str(tmp_path / "state.json"))
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["database_reachable"] is True
    assert payload["schema_bootstrapped"] is False
    assert payload["workflow_operational"] is True
    assert payload["missing_schema_objects"] == ["data_dictionary_effective"]


def test_praxis_ctl_env_for_authority_uses_shared_repo_resolver_when_process_env_missing(monkeypatch) -> None:
    local_alpha = _load_local_alpha()
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        local_alpha,
        "workflow_database_env_for_repo",
        lambda repo_root, env=None: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/praxis"},
    )

    assert local_alpha._env_for_authority() == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/praxis",
    }


def test_praxis_ctl_runtime_endpoints_use_runtime_binding_contract(monkeypatch) -> None:
    local_alpha = _load_local_alpha()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        local_alpha,
        "_env_for_authority",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/praxis"},
    )
    monkeypatch.setattr(
        local_alpha,
        "native_instance_contract",
        lambda env=None: {
            "repo_root": "/repo",
            "workdir": "/repo",
            "praxis_runtime_profile": "praxis",
        },
    )

    def _fake_runtime_http_endpoints(*, workflow_env, native_instance, workflow_env_error=None):
        captured["workflow_env"] = dict(workflow_env)
        captured["native_instance"] = dict(native_instance)
        captured["workflow_env_error"] = workflow_env_error
        return {
            "api_base_url": "https://runtime.example",
            "launch_url": "https://runtime.example/app",
            "dashboard_url": "https://runtime.example/app",
            "api_docs_url": "https://runtime.example/docs",
        }

    monkeypatch.setattr(
        local_alpha,
        "resolve_runtime_http_endpoints",
        _fake_runtime_http_endpoints,
    )

    endpoints = local_alpha._runtime_binding_http_endpoints({})

    assert endpoints["api_base_url"] == "https://runtime.example"
    assert endpoints["launch_url"] == "https://runtime.example/app"
    assert captured["workflow_env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/praxis",
    }
    assert captured["native_instance"] == {
        "repo_root": "/repo",
        "workdir": "/repo",
        "praxis_runtime_profile": "praxis",
    }
    assert captured["workflow_env_error"] is None


def test_probe_frontdoor_semantics_uses_ui_header_for_workflow_probes(monkeypatch) -> None:
    local_alpha = _load_local_alpha()
    calls: list[tuple[str, str, dict[str, str] | None]] = []
    monkeypatch.setenv("PRAXIS_API_BASE_URL", "http://praxis.test:9555")

    def _fake_http_request(
        url: str,
        *,
        method: str = "GET",
        payload=None,
        headers=None,
        timeout_s: float = 4.0,
    ):
        del payload, timeout_s
        header_map = dict(headers or {})
        calls.append((url, method, header_map or None))
        if url.endswith("/api/health"):
            return 200, json.dumps({"status": "healthy"}).encode("utf-8")
        if url.endswith("/orient"):
            return 200, json.dumps({"platform": "dag-workflow"}).encode("utf-8")
        if url.endswith("/mcp"):
            return 200, json.dumps({"result": {"serverInfo": {"name": "praxis-mcp"}}}).encode(
                "utf-8"
            )
        if url.endswith("/app"):
            return 200, b"<html><head><title>Praxis</title></head><body><div id=\"root\"></div></body></html>"
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(local_alpha, "_http_request", _fake_http_request)

    payload = local_alpha._probe_frontdoor_semantics()

    assert payload["launch_url"] == "http://praxis.test:9555/app"
    assert payload["dashboard_url"] == "http://praxis.test:9555/app"
    assert payload["api_docs_url"] == "http://praxis.test:9555/docs"
    assert payload["workflow_api_ready"] is True
    assert payload["mcp_bridge_ready"] is True
    assert any(
        url.endswith("/orient") and method == "POST" and headers == {"X-Praxis-UI": "1"}
        for url, method, headers in calls
    )
    assert any(
        url.endswith("/mcp") and method == "POST" and headers == {"X-Praxis-UI": "1"}
        for url, method, headers in calls
    )
