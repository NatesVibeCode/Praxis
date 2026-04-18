"""Unit tests for the tiered claim-selection ORDER BY in :func:`claim_one`.

The ordering itself is enforced by the database.  These tests pin:
  1. The query shape (CASE-based tier expression and the interval arg).
  2. ``claim_one``'s behaviour when the DB returns candidates in tiered
     order — the function must claim the first row that passes admission.
"""

from __future__ import annotations

from runtime.workflow import _claiming as _claiming_mod


class _QueryRecorder:
    def __init__(self, candidates: list[dict]) -> None:
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self._candidates = list(candidates)
        self._claim_called = False

    def execute(self, query: str, *args):
        self.queries.append((query, args))
        if "FROM workflow_jobs j" in query and "LIMIT 50" in query:
            return list(self._candidates)
        if query.strip().startswith("UPDATE workflow_jobs"):
            if self._claim_called:
                return []
            self._claim_called = True
            job_id = args[0]
            for candidate in self._candidates:
                if candidate.get("id") == job_id:
                    claimed = dict(candidate)
                    claimed["status"] = "claimed"
                    claimed["resolved_agent"] = args[2]
                    return [claimed]
            return []
        if query.strip().startswith("SELECT run_id FROM workflow_jobs"):
            return [{"run_id": "run-x"}]
        return []


def _install_passthroughs(monkeypatch) -> None:
    monkeypatch.setattr(_claiming_mod, "_job_has_touch_conflict", lambda _conn, _job: False)
    monkeypatch.setattr(_claiming_mod, "_select_claim_route", lambda _conn, job: str(job.get("agent_slug") or ""))
    monkeypatch.setattr(_claiming_mod, "_recompute_workflow_run_state", lambda _conn, _run_id: None)


def test_claim_one_query_has_tiered_case_and_interval_arg(monkeypatch) -> None:
    _install_passthroughs(monkeypatch)
    conn = _QueryRecorder([])
    assert _claiming_mod.claim_one(conn, "worker-a") is None

    assert conn.queries, "claim_one must issue at least one query"
    select_query, select_args = conn.queries[0]
    assert "FROM workflow_jobs j" in select_query
    assert "CASE" in select_query
    assert "j.attempt > 0" in select_query
    assert "COALESCE(j.ready_at, j.created_at)" in select_query
    assert "r.requested_at DESC" in select_query
    assert select_args == (str(_claiming_mod._STALE_READY_PROMOTION_INTERVAL_SECONDS),)


def test_claim_one_claims_first_row_returned_by_the_ordered_query(monkeypatch) -> None:
    """DB-ordered first row wins, regardless of its tier label."""
    _install_passthroughs(monkeypatch)
    candidates = [
        {"id": 42, "run_id": "run-x", "label": "promoted_retry", "status": "ready",
         "agent_slug": "anthropic/claude", "attempt": 2},
        {"id": 99, "run_id": "run-x", "label": "fresh", "status": "ready",
         "agent_slug": "anthropic/claude", "attempt": 0},
    ]
    conn = _QueryRecorder(candidates)

    claimed = _claiming_mod.claim_one(conn, "worker-a")

    assert claimed is not None
    assert claimed["id"] == 42
    assert claimed["status"] == "claimed"
    assert claimed["resolved_agent"] == "anthropic/claude"


def test_claim_one_skips_first_when_admission_fails_and_falls_through(monkeypatch) -> None:
    _install_passthroughs(monkeypatch)
    candidates = [
        {"id": 1, "run_id": "run-x", "label": "touch_conflict", "status": "ready",
         "agent_slug": "anthropic/claude", "attempt": 1},
        {"id": 2, "run_id": "run-x", "label": "next_up", "status": "ready",
         "agent_slug": "anthropic/claude", "attempt": 0},
    ]
    conn = _QueryRecorder(candidates)

    monkeypatch.setattr(
        _claiming_mod,
        "_job_has_touch_conflict",
        lambda _conn, job: job.get("id") == 1,
    )

    claimed = _claiming_mod.claim_one(conn, "worker-a")

    assert claimed is not None
    assert claimed["id"] == 2
