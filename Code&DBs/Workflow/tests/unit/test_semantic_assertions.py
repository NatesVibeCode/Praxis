from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from runtime.operations.commands.semantic_assertions import (
    RecordSemanticAssertionCommand,
    handle_record_semantic_assertion,
)
from runtime.operations.queries.semantic_assertions import (
    QuerySemanticAssertions,
    handle_query_semantic_assertions,
)
from runtime.semantic_assertions import (
    SemanticAssertionError,
    SemanticAssertionRecord,
    normalize_semantic_assertion_record,
    semantic_assertion_id,
)


def test_hidden_authority_keys_are_rejected_in_qualifiers() -> None:
    with pytest.raises(SemanticAssertionError) as exc_info:
        normalize_semantic_assertion_record(
            SemanticAssertionRecord(
                semantic_assertion_id="",
                predicate_slug="grouped_in",
                assertion_status="active",
                subject_kind="bug",
                subject_ref="bug.checkout.1",
                object_kind="functional_area",
                object_ref="functional_area.checkout",
                qualifiers_json={"subject_kind": "hidden_override"},
                source_kind="operator",
                source_ref="nate",
                evidence_ref=None,
                bound_decision_id=None,
                valid_from=datetime(2026, 4, 16, 19, 0, tzinfo=timezone.utc),
                valid_to=None,
                created_at=datetime(2026, 4, 16, 19, 0, tzinfo=timezone.utc),
                updated_at=datetime(2026, 4, 16, 19, 0, tzinfo=timezone.utc),
            )
        )

    assert exc_info.value.reason_code == "semantic_assertion.hidden_authority"
    assert exc_info.value.details["conflicting_keys"] == ["subject_kind"]


def test_semantic_assertion_id_is_stable_for_same_edge_and_source() -> None:
    left = semantic_assertion_id(
        predicate_slug="grouped_in",
        subject_kind="bug",
        subject_ref="bug.checkout.1",
        object_kind="functional_area",
        object_ref="functional_area.checkout",
        source_kind="operator",
        source_ref="nate",
    )
    right = semantic_assertion_id(
        predicate_slug="grouped_in",
        subject_kind="bug",
        subject_ref="bug.checkout.1",
        object_kind="functional_area",
        object_ref="functional_area.checkout",
        source_kind="operator",
        source_ref="nate",
    )

    assert left == right
    assert left.startswith("semantic_assertion.grouped_in.")


def test_record_semantic_assertion_handler_uses_frontdoor(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeFrontdoor:
        async def record_assertion_async(self, **kwargs):
            captured.update(kwargs)
            return {"semantic_assertion": {"semantic_assertion_id": "semantic_assertion.grouped_in.abc"}}

    from surfaces.api import semantic_assertions as semantic_surface

    monkeypatch.setattr(semantic_surface, "SemanticAssertionFrontdoor", _FakeFrontdoor)

    result = asyncio.run(
        handle_record_semantic_assertion(
            RecordSemanticAssertionCommand(
                predicate_slug="grouped_in",
                subject_kind="bug",
                subject_ref="bug.checkout.1",
                object_kind="functional_area",
                object_ref="functional_area.checkout",
                source_kind="operator",
                source_ref="nate",
            ),
            subsystems=object(),
        )
    )

    assert result["semantic_assertion"]["semantic_assertion_id"].startswith(
        "semantic_assertion.grouped_in."
    )
    assert captured["predicate_slug"] == "grouped_in"
    assert captured["subject_ref"] == "bug.checkout.1"


def test_query_semantic_assertions_handler_uses_frontdoor(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeSubsystems:
        def _postgres_env(self):
            return {"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"}

    class _FakeFrontdoor:
        async def list_assertions_async(self, **kwargs):
            captured.update(kwargs)
            return {"semantic_assertions": []}

    from surfaces.api import semantic_assertions as semantic_surface

    monkeypatch.setattr(semantic_surface, "SemanticAssertionFrontdoor", _FakeFrontdoor)

    result = asyncio.run(
        handle_query_semantic_assertions(
            QuerySemanticAssertions(
                predicate_slug="grouped_in",
                source_kind="operator",
                active_only=True,
                limit=25,
            ),
            _FakeSubsystems(),
        )
    )

    assert result == {"semantic_assertions": []}
    assert captured["predicate_slug"] == "grouped_in"
    assert captured["source_kind"] == "operator"
    assert captured["limit"] == 25
