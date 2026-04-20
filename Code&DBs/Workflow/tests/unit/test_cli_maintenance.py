from __future__ import annotations

import json
from io import StringIO

from surfaces.cli.commands import maintenance as maintenance_commands
from surfaces.cli.main import main as workflow_cli_main


def test_maintenance_backfill_requires_confirmation() -> None:
    stdout = StringIO()

    exit_code = workflow_cli_main(["maintenance", "backfill-failure-categories"], stdout=stdout)

    assert exit_code == 2
    assert "confirmation required" in stdout.getvalue()


def test_maintenance_backfill_invokes_runtime_backfill(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(maintenance_commands, "cli_sync_conn", lambda: sentinel)

    def _fake_backfill(conn: object) -> dict[str, object]:
        captured["conn"] = conn
        return {
            "receipts_scanned": 2,
            "receipts_updated": 1,
            "workflow_jobs_updated": 1,
            "workflow_jobs_remaining": 0,
            "receipt_meta_updated": 1,
            "receipt_meta_remaining": 0,
            "final_breakdown": [{"failure_category": "rate_limit", "count": 1}],
            "zone_breakdown": [{"failure_zone": "external", "count": 1}],
            "remaining_unclassified": 0,
        }

    monkeypatch.setattr(
        maintenance_commands,
        "backfill_failure_categories",
        _fake_backfill,
    )

    stdout = StringIO()
    exit_code = workflow_cli_main(
        ["maintenance", "backfill-failure-categories", "--yes", "--json"],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured["conn"] is sentinel
    payload = json.loads(stdout.getvalue())
    assert payload["receipts_updated"] == 1
