"""Unit tests for the data_dictionary_governance_admin HTTP handlers."""
from __future__ import annotations

import io
from types import SimpleNamespace
from typing import Any

from surfaces.api.handlers import data_dictionary_governance_admin as handler


class _RequestStub:
    def __init__(self, path: str) -> None:
        self.rfile = io.BytesIO(b"")
        self.headers = {"Content-Length": "0"}
        self.path = path
        self._conn = object()
        self.subsystems = SimpleNamespace(get_pg_conn=lambda: self._conn)
        self.sent: tuple[int, Any] | None = None

    def _send_json(self, status: int, payload: Any) -> None:
        self.sent = (status, payload)


def test_scan_is_dry_run(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, tracker=None, *, dry_run=False, triggered_by="heartbeat", record_scan=True):
        captured["dry_run"] = dry_run
        captured["tracker_is_none"] = tracker is None
        return {"total_violations": 0, "by_policy": {}, "violations": []}

    monkeypatch.setattr(handler, "run_governance_scan", fake)
    stub = _RequestStub("/api/data-dictionary/governance")
    handler._handle_scan(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured["dry_run"] is True
    assert captured["tracker_is_none"] is True


def test_enforce_passes_real_tracker(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, tracker=None, *, dry_run=False, triggered_by="heartbeat", record_scan=True):
        captured["dry_run"] = dry_run
        captured["has_tracker"] = tracker is not None
        return {"filed_bugs": [], "skipped_existing": []}

    monkeypatch.setattr(handler, "run_governance_scan", fake)
    stub = _RequestStub("/api/data-dictionary/governance/enforce")
    handler._handle_enforce(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured["dry_run"] is False
    assert captured["has_tracker"] is True


def test_scan_500_on_runtime_exception(monkeypatch) -> None:
    def boom(conn, tracker=None, *, dry_run=False, triggered_by="heartbeat", record_scan=True):
        raise RuntimeError("db dead")

    monkeypatch.setattr(handler, "run_governance_scan", boom)
    stub = _RequestStub("/api/data-dictionary/governance")
    handler._handle_scan(stub, stub.path)
    assert stub.sent[0] == 500
    assert "db dead" in stub.sent[1]["error"]


def test_enforce_500_on_runtime_exception(monkeypatch) -> None:
    def boom(conn, tracker=None, *, dry_run=False, triggered_by="heartbeat", record_scan=True):
        raise RuntimeError("bug writer down")

    monkeypatch.setattr(handler, "run_governance_scan", boom)
    stub = _RequestStub("/api/data-dictionary/governance/enforce")
    handler._handle_enforce(stub, stub.path)
    assert stub.sent[0] == 500
    assert "bug writer down" in stub.sent[1]["error"]


def test_scorecard_returns_metrics(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn):
        captured["called"] = True
        return {"compliance_score": 0.82, "grade": "B", "metrics": {}}

    monkeypatch.setattr(handler, "compute_scorecard", fake)
    stub = _RequestStub("/api/data-dictionary/governance/scorecard")
    handler._handle_scorecard(stub, stub.path)
    assert stub.sent[0] == 200
    assert stub.sent[1]["compliance_score"] == 0.82
    assert stub.sent[1]["grade"] == "B"
    assert captured["called"] is True


def test_scorecard_500_on_exception(monkeypatch) -> None:
    def boom(conn):
        raise RuntimeError("stats unavailable")

    monkeypatch.setattr(handler, "compute_scorecard", boom)
    stub = _RequestStub("/api/data-dictionary/governance/scorecard")
    handler._handle_scorecard(stub, stub.path)
    assert stub.sent[0] == 500
    assert "stats unavailable" in stub.sent[1]["error"]


def test_remediate_handler_returns_plans(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, discover=None):
        captured["called"] = True
        captured["discover_is_callable"] = callable(discover)
        return {"total_violations": 0, "plans": []}

    monkeypatch.setattr(handler, "suggest_all_remediations", fake)
    stub = _RequestStub("/api/data-dictionary/governance/remediate")
    # Need an indexer-returning stub to avoid attribute errors.
    from types import SimpleNamespace
    stub.subsystems = SimpleNamespace(
        get_pg_conn=lambda: object(),
        get_module_indexer=lambda: SimpleNamespace(search=lambda **kw: []),
    )
    handler._handle_remediate(stub, stub.path)
    assert stub.sent[0] == 200
    assert stub.sent[1] == {"total_violations": 0, "plans": []}
    assert captured["called"] is True
    assert captured["discover_is_callable"] is True


def test_remediate_handler_500_on_exception(monkeypatch) -> None:
    def boom(conn, *, discover=None):
        raise RuntimeError("scan dead")

    monkeypatch.setattr(handler, "suggest_all_remediations", boom)
    from types import SimpleNamespace
    stub = _RequestStub("/api/data-dictionary/governance/remediate")
    stub.subsystems = SimpleNamespace(
        get_pg_conn=lambda: object(),
        get_module_indexer=lambda: SimpleNamespace(search=lambda **kw: []),
    )
    handler._handle_remediate(stub, stub.path)
    assert stub.sent[0] == 500
    assert "scan dead" in stub.sent[1]["error"]


def test_cluster_handler_returns_clusters(monkeypatch) -> None:
    def fake(conn):
        return {
            "total_violations": 11,
            "cluster_count": 3,
            "bulk_fixes_available": 1,
            "clusters": [],
        }

    monkeypatch.setattr(handler, "suggest_cluster_fixes", fake)
    stub = _RequestStub("/api/data-dictionary/governance/clusters")
    handler._handle_cluster(stub, stub.path)
    assert stub.sent[0] == 200
    assert stub.sent[1]["cluster_count"] == 3
    assert stub.sent[1]["bulk_fixes_available"] == 1


def test_cluster_handler_500_on_exception(monkeypatch) -> None:
    def boom(conn):
        raise RuntimeError("cluster dead")

    monkeypatch.setattr(handler, "suggest_cluster_fixes", boom)
    stub = _RequestStub("/api/data-dictionary/governance/clusters")
    handler._handle_cluster(stub, stub.path)
    assert stub.sent[0] == 500


def test_route_matchers_match_only_exact_paths() -> None:
    get_matchers = handler.DATA_DICTIONARY_GOVERNANCE_GET_ROUTES
    post_matchers = handler.DATA_DICTIONARY_GOVERNANCE_POST_ROUTES

    # Exact-match sibling paths should not cross-leak (scan detail uses a
    # prefix matcher and is tested separately).
    exact_paths = [
        "/api/data-dictionary/governance/scorecard",
        "/api/data-dictionary/governance/remediate",
        "/api/data-dictionary/governance/clusters",
        "/api/data-dictionary/governance/scans",
        "/api/data-dictionary/governance/pending",
        "/api/data-dictionary/governance",
    ]
    exact_matchers = [
        (fn, h) for (fn, h) in get_matchers
        # Include every GET matcher that returns True for at least one of
        # our exact paths — the prefix matcher for /scans/<id> returns True
        # for `/scans/<id>` shapes, not the bare `/scans` ones.
    ]
    assert len(exact_matchers) == len(get_matchers)

    # Every path should match exactly one GET matcher.
    for p in exact_paths:
        hits = [fn for (fn, _h) in get_matchers if fn(p)]
        assert len(hits) == 1, f"{p} matched {len(hits)} matchers, expected 1"

    # Scan-detail prefix matcher handles the /scans/<id> shape.
    scan_detail_path = "/api/data-dictionary/governance/scans/abc-123"
    hits = [fn for (fn, _h) in get_matchers if fn(scan_detail_path)]
    assert len(hits) >= 1  # prefix matcher fires on this shape

    # POST side: enforce + drain.
    assert len(post_matchers) == 2
    post_paths = {
        "/api/data-dictionary/governance/enforce",
        "/api/data-dictionary/governance/drain",
    }
    for p in post_paths:
        hits = [fn for (fn, _h) in post_matchers if fn(p)]
        assert len(hits) == 1, f"POST {p} matched {len(hits)} matchers"
    # POST enforce must not match bare governance path.
    for fn, _h in post_matchers:
        assert fn("/api/data-dictionary/governance") is False
