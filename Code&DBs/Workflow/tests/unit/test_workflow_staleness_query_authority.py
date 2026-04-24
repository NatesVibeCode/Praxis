from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from surfaces.api.handlers import workflow_query_core


class _BoomConn:
    def execute(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        raise RuntimeError("database unavailable")


def test_staleness_query_degrades_when_database_candidate_discovery_fails() -> None:
    detector = SimpleNamespace(
        scan=lambda _items: pytest.fail("staleness scan should not run without candidates"),
        alert_summary=lambda _items: {},
    )
    subs = SimpleNamespace(
        _pg_conn=_BoomConn(),
        get_staleness_detector=lambda: detector,
    )

    result = workflow_query_core.handle_query(subs, {"question": "staleness"})

    assert result["routed_to"] == "staleness_detector"
    assert result["status"] == "degraded"
    assert result["reason_code"] == "workflow_query.staleness_candidate_authority_failed"
    assert result["candidate_authority_ready"] is False
    assert result["sources"] == []
    assert len(result["errors"]) == 4
