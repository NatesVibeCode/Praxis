from pathlib import Path
from types import SimpleNamespace

from surfaces.api.handlers import workflow_admin

from runtime.primitive_contracts import bug_status_sql_in_literal


def test_platform_overview_counts_in_progress_bugs_as_open() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "surfaces"
        / "api"
        / "handlers"
        / "workflow_admin.py"
    ).read_text()

    # Workflow admin must route through the state-semantics contract helper,
    # not hand-roll the SQL list of open statuses.
    assert "bug_status_sql_in_literal" in source
    assert "COUNT(*) FROM bugs WHERE status = 'OPEN'" not in source
    # The helper must continue to emit every non-terminal status as open.
    assert bug_status_sql_in_literal("open") == (
        "UPPER(status) IN ('OPEN', 'IN_PROGRESS', 'FIX_PENDING_VERIFICATION')"
    )


def test_platform_overview_degrades_missing_registry_without_hiding_ticket_counts(monkeypatch) -> None:
    class _Pg:
        def fetchval(self, query: str):
            if "platform_registry" in query:
                raise RuntimeError('relation "platform_registry" does not exist')
            if "COUNT(*) FROM bugs WHERE" in query:
                return 7
            if "COUNT(*) FROM bugs" in query:
                return 12
            if "workflow_runs" in query:
                return 3
            if "pg_tables" in query:
                return 22
            raise AssertionError(f"unexpected query: {query}")

        def execute(self, query: str):
            if "provider_model_candidates" in query:
                return []
            if "GROUP BY severity" in query:
                return [{"code": "P1", "count": 2}]
            raise AssertionError(f"unexpected query: {query}")

    class _Request:
        def __init__(self) -> None:
            self.subsystems = SimpleNamespace(get_pg_conn=lambda: _Pg())
            self.sent = None

        def _send_json(self, status: int, payload: dict) -> None:
            self.sent = (status, payload)

    monkeypatch.setattr("runtime.receipt_store.list_receipts", lambda limit=20: [])
    monkeypatch.setattr(
        "surfaces.api.handlers.workflow_run._handle_status",
        lambda _subs, _body: {"pass_rate": 0.5, "total_workflows": 4},
    )

    request = _Request()

    workflow_admin._handle_platform_overview_get(request, "/api/platform-overview")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["total_bugs"] == 12
    assert payload["open_bugs"] == 7
    assert payload["total_registry_items"] == 0
    assert payload["observability_state"] == "degraded"
    assert "platform_registry" in payload["degraded_sources"]
