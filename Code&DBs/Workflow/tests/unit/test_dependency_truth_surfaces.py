from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from surfaces.api.handlers import workflow_admin


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

    result = workflow_admin._handle_orient(_FakeSubsystems(), {})

    assert result["dependency_truth"] == fake_dependency_truth
    assert result["recent_activity"] == {
        "total_workflows_24h": 1,
        "pass_rate": 1.0,
        "top_failure_codes": [],
    }


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
            "helm_url": "http://127.0.0.1:8420/app/helm",
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
    assert payload["helm_url"] == "http://127.0.0.1:8420/app/helm"


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
    assert ("http://127.0.0.1:8421/orient", "POST", {"X-Praxis-UI": "1"}) in calls
    assert ("http://127.0.0.1:8421/mcp", "POST", {"X-Praxis-UI": "1"}) in calls
