from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from surfaces.api.handlers import workflow_admin
from surfaces.mcp.catalog import get_tool_catalog


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

    monkeypatch.setattr(workflow_admin, "dependency_truth_report", lambda scope="all": fake_dependency_truth)
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
    assert "workflow tools call <tool|alias>" in instructions
    assert "write/dispatch flows require `--yes`" in instructions
    assert "workflow query" in instructions
    assert "workflow architecture scan" in instructions
    assert "kickoff first" in instructions


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

    class _FakeDatabaseStatus:
        def to_json(self):
            return {"database_reachable": True, "schema_bootstrapped": True}

    class _FakeSyncStatus:
        sync_status = "skipped"
        sync_cycle_id = None
        sync_error_count = 0

    monkeypatch.setattr(local_alpha, "local_postgres_health", lambda env=None: _FakeDatabaseStatus())
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
    assert payload["api_server_ready"] is True
    assert payload["workflow_api_ready"] is True
    assert payload["mcp_bridge_ready"] is True
    assert payload["ui_ready"] is True
    assert payload["launch_url"] == "http://127.0.0.1:8420/app"


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


def test_probe_frontdoor_semantics_uses_ui_header_for_workflow_probes(monkeypatch) -> None:
    local_alpha = _load_local_alpha()
    calls: list[tuple[str, str, dict[str, str] | None]] = []

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
