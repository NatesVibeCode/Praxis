from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime import intent_recognition
from runtime.intent_binding import BoundIntent, SuggestedPillCandidate
from runtime.intent_recognition import recognize_intent


class _StubConn:
    pass


def _install_authority(monkeypatch) -> None:
    objects = [
        {
            "object_kind": "tool:praxis_connector",
            "label": "praxis_connector",
            "category": "tool",
            "summary": (
                "Build API connectors for third-party applications. One call stamps "
                "a workflow spec and launches a 4-job pipeline (discover API -> map "
                "objects -> build client -> review)."
            ),
        },
        {
            "object_kind": "tool:praxis_research_workflow",
            "label": "praxis_research_workflow",
            "category": "tool",
            "summary": (
                "Run a parallel multi-angle research workflow on any topic. One call "
                "generates seed decomposition, N parallel research workers, and synthesis."
            ),
        },
    ]
    descriptions = {
        "tool:praxis_connector": {
            "object": objects[0],
            "fields": [
                {
                    "field_path": "action",
                    "field_kind": "enum",
                    "description": "Operation: build, list, get, register, verify.",
                    "source": "auto",
                },
                {
                    "field_path": "app_name",
                    "field_kind": "text",
                    "description": "Display name of the application. Required for 'build'.",
                    "source": "auto",
                },
                {
                    "field_path": "auth_docs_url",
                    "field_kind": "text",
                    "description": "Public API documentation URL used by action='build'.",
                    "source": "auto",
                },
            ],
        },
        "tool:praxis_research_workflow": {
            "object": objects[1],
            "fields": [
                {
                    "field_path": "topic",
                    "field_kind": "text",
                    "description": "The research topic or question to investigate. Required for 'run'.",
                    "source": "auto",
                },
                {
                    "field_path": "workers",
                    "field_kind": "number",
                    "description": "Number of parallel loop workers.",
                    "source": "auto",
                },
            ],
        },
    }

    def _fake_list_object_kinds(conn, **_kwargs):
        return [dict(row) for row in objects]

    def _fake_describe_object(conn, *, object_kind, **_kwargs):
        return descriptions[object_kind]

    def _fake_bind_data_pills(intent, *, conn, object_kinds=None, suggestion_limit=20):
        return BoundIntent(
            intent=intent,
            suggested=[
                SuggestedPillCandidate(
                    object_kind="tool:praxis_connector",
                    field_path="app_name",
                    field_kind="text",
                    label=None,
                    description="Display name of the application.",
                    source="auto",
                    display_order=20,
                    score=42.0,
                    confidence="high",
                    matched_terms=["app", "application", "name", "integration"],
                    reason="matched loose prose against data dictionary",
                ),
                SuggestedPillCandidate(
                    object_kind="tool:praxis_connector",
                    field_path="auth_docs_url",
                    field_kind="text",
                    label=None,
                    description="Public API documentation URL.",
                    source="auto",
                    display_order=40,
                    score=30.0,
                    confidence="high",
                    matched_terms=["api", "docs", "integration", "search"],
                    reason="matched loose prose against data dictionary",
                ),
            ],
        )

    monkeypatch.setattr(intent_recognition, "list_object_kinds", _fake_list_object_kinds)
    monkeypatch.setattr(intent_recognition, "describe_object", _fake_describe_object)
    monkeypatch.setattr(intent_recognition, "bind_data_pills", _fake_bind_data_pills)


def test_recognize_intent_extracts_and_matches_user_stated_spans(monkeypatch) -> None:
    _install_authority(monkeypatch)

    result = recognize_intent(
        (
            "A repeatable workflow where we feed in an app name or app domain "
            "and plan search retrieve evaluate then build a custom integration."
        ),
        conn=_StubConn(),
    )

    payload = result.to_dict()
    extracted = [span["text"].lower() for span in payload["extracted"]]
    assert "app name" in extracted
    assert "app domain" in extracted
    assert "search" in extracted
    assert "retrieve" in extracted
    assert "evaluate" in extracted
    assert "custom integration" in extracted
    refs = {match["authority_ref"] for match in payload["matched"]}
    assert "tool:praxis_connector.app_name" in refs
    assert "tool:praxis_connector.auth_docs_url" in refs
    assert "tool:praxis_connector" in refs
    # Gaps are honest: the recognizer does not invent authority for every
    # operation word just because it appeared in source order.
    gap_spans = {gap["span_text"].lower() for gap in payload["gaps"]}
    assert "app name" not in gap_spans
    assert "custom integration" not in gap_spans


def test_recognize_intent_extracts_review_verbs(monkeypatch) -> None:
    _install_authority(monkeypatch)

    result = recognize_intent(
        "Please audit the module, then inspect and confirm changes and validate results before verify.",
        conn=_StubConn(),
    )

    extracted = [span["text"].lower() for span in result.to_dict()["extracted"]]
    assert "audit" in extracted
    assert "inspect" in extracted
    assert "confirm" in extracted
    assert "validate" in extracted
    assert "verify" in extracted


def test_recognize_intent_suggests_only_from_matched_authority(monkeypatch) -> None:
    _install_authority(monkeypatch)

    result = recognize_intent(
        "Do research on the market and build a custom integration.",
        conn=_StubConn(),
    )

    suggestions = result.to_dict()["suggested"]
    titles = {entry["title"] for entry in suggestions}
    assert "discover API" in titles
    assert "map objects" in titles
    assert any("fan out research" in entry["title"].lower() for entry in suggestions)
    assert all(entry["status"] == "suggested" for entry in suggestions)
    assert all(entry["source_authority_ref"].startswith("tool:") for entry in suggestions)


def test_recognize_intent_reports_gap_for_unmatched_span(monkeypatch) -> None:
    def _fake_list_object_kinds(conn, **_kwargs):
        return []

    def _fake_bind_data_pills(intent, *, conn, object_kinds=None, suggestion_limit=20):
        return BoundIntent(intent=intent)

    monkeypatch.setattr(intent_recognition, "list_object_kinds", _fake_list_object_kinds)
    monkeypatch.setattr(intent_recognition, "bind_data_pills", _fake_bind_data_pills)

    result = recognize_intent("Search for a strange capability.", conn=_StubConn())

    gaps = result.to_dict()["gaps"]
    assert gaps
    assert gaps[0]["span_text"].lower() == "search"
