from pathlib import Path


def test_platform_overview_counts_in_progress_bugs_as_open() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "surfaces"
        / "api"
        / "handlers"
        / "workflow_admin.py"
    ).read_text()

    assert "UPPER(status) IN ('OPEN', 'IN_PROGRESS')" in source
    assert "COUNT(*) FROM bugs WHERE status = 'OPEN'" not in source
