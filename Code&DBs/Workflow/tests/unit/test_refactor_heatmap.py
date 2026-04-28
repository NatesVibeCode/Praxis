from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.bug_tracker import BugCategory, BugSeverity, BugStatus
from runtime.operations.queries.operator_observability import (
    QueryRefactorHeatmap,
    handle_query_refactor_heatmap,
)
from runtime.refactor_heatmap import build_refactor_heatmap


def _bug(
    *,
    bug_id: str,
    title: str,
    severity: BugSeverity = BugSeverity.P1,
    description: str = "",
):
    now = datetime(2026, 4, 28, tzinfo=timezone.utc)
    return SimpleNamespace(
        bug_id=bug_id,
        title=title,
        status=BugStatus.OPEN,
        severity=severity,
        category=BugCategory.ARCHITECTURE,
        description=description,
        summary=description[:200],
        filed_at=now,
        updated_at=now,
        resolved_at=None,
        tags=(),
        resume_context={},
        filed_by="test",
        assigned_to=None,
        owner_ref=None,
        source_issue_id=None,
        decision_ref=None,
        resolution_summary=None,
        discovered_in_run_id=None,
        discovered_in_receipt_id=None,
        source_kind="manual_review",
    )


class _FakeBugTracker:
    def __init__(self, bugs):
        self.calls = []
        self._bugs = bugs

    def list_bugs(self, **kwargs):
        self.calls.append(kwargs)
        return self._bugs[: kwargs.get("limit", len(self._bugs))]


class _FakeSubsystems:
    def __init__(self, repo_root, bugs):
        self._repo_root = repo_root
        self.tracker = _FakeBugTracker(bugs)

    def get_bug_tracker(self):
        return self.tracker


def test_refactor_heatmap_ranks_authority_spread_from_bugs_and_source_topology(tmp_path):
    workflow_root = tmp_path / "Code&DBs" / "Workflow"
    api_root = workflow_root / "surfaces" / "api"
    runtime_root = workflow_root / "runtime"
    api_root.mkdir(parents=True)
    runtime_root.mkdir(parents=True)
    (api_root / "provider_surface.py").write_text(
        "\n".join(
            [
                "from runtime.task_type_router import TaskTypeRouter",
                "from registry.route_catalog_repository import Repo",
                "def dispatch_provider_route():",
                "    return 'provider model route transport access_control'",
            ]
        )
    )
    (runtime_root / "task_type_router.py").write_text(
        "class TaskTypeRouter:\n"
        "    def route(self):\n"
        + "\n".join("        provider = 'model route transport'" for _ in range(130))
        + "\n        return provider\n"
    )
    (runtime_root / "workflow_lifecycle.py").write_text(
        "workflow_runs = 'claim lease worker receipt admission'\n"
    )

    subsystems = _FakeSubsystems(
        tmp_path,
        [
            _bug(
                bug_id="BUG-AAAA1111",
                title="Provider routing and admission authority is duplicated",
                description="provider model route transport access_control task_type_router",
            ),
            _bug(
                bug_id="BUG-BBBB2222",
                title="Workflow execution lifecycle is spread",
                severity=BugSeverity.P2,
                description="workflow_runs claim lease worker receipt admission",
            ),
        ],
    )

    payload = build_refactor_heatmap(subsystems, limit=3, bug_limit=10)

    assert payload["view"] == "refactor_heatmap"
    assert payload["summary"]["source_files_scanned"] == 3
    assert payload["summary"]["architecture_bugs_considered"] == 2
    assert payload["heatmap"][0]["domain"] == "provider_routing_admission"
    assert payload["heatmap"][0]["priority"] == "P1"
    assert payload["heatmap"][0]["metrics"]["surface_coupling_files"] == 1
    assert payload["heatmap"][0]["metrics"]["large_symbols"] >= 1
    assert "BUG-AAAA1111" in payload["heatmap"][0]["evidence"]["bug_ids"]
    assert subsystems.tracker.calls[0]["category"] is BugCategory.ARCHITECTURE
    assert subsystems.tracker.calls[0]["open_only"] is True


def test_refactor_heatmap_handler_delegates_to_read_model(tmp_path):
    (tmp_path / "Code&DBs" / "Workflow").mkdir(parents=True)
    subsystems = _FakeSubsystems(tmp_path, [])

    payload = handle_query_refactor_heatmap(
        QueryRefactorHeatmap(limit=1, include_domains=["provider_routing_admission"]),
        subsystems,
    )

    assert payload["view"] == "refactor_heatmap"
    assert payload["summary"]["returned_count"] == 1
    assert payload["heatmap"][0]["domain"] == "provider_routing_admission"
