from pathlib import Path

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
    # The helper must continue to emit both OPEN and IN_PROGRESS as open.
    assert bug_status_sql_in_literal("open") == "UPPER(status) IN ('OPEN', 'IN_PROGRESS')"
