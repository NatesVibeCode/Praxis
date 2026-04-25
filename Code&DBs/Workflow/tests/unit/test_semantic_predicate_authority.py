"""Unit tests for runtime.semantic_predicate_authority."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from runtime.semantic_predicate_authority import (
    PREDICATE_KINDS,
    SemanticPredicateAuthorityError,
    get_predicate,
    list_predicates,
    record_predicate,
)


class _FakeConn:
    """Minimal connection fake that mirrors the semantic_predicate_catalog table shape."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "INSERT INTO semantic_predicate_catalog" in normalized:
            (
                _predicate_id,
                slug,
                kind,
                applies_kind,
                applies_ref,
                summary,
                rationale,
                validator_ref,
                policy_json,
                decision_ref,
                enabled,
                metadata_json,
            ) = args
            now = datetime.now(timezone.utc)
            existing = self.rows.get(slug)
            row = {
                "predicate_id": existing["predicate_id"] if existing else _predicate_id,
                "predicate_slug": slug,
                "predicate_kind": kind,
                "applies_to_kind": applies_kind,
                "applies_to_ref": applies_ref,
                "summary": summary,
                "rationale": rationale,
                "validator_ref": validator_ref,
                "propagation_policy": json.loads(policy_json),
                "decision_ref": decision_ref,
                "enabled": enabled,
                "metadata": json.loads(metadata_json),
                "created_at": existing["created_at"] if existing else now,
                "updated_at": now,
            }
            self.rows[slug] = row
            return dict(row)
        if "FROM semantic_predicate_catalog" in normalized and "WHERE predicate_slug" in normalized:
            slug = args[0]
            row = self.rows.get(slug)
            return dict(row) if row else None
        return None

    def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "FROM semantic_predicate_catalog" not in normalized:
            return []
        results = list(self.rows.values())
        if "WHERE enabled = TRUE" in normalized:
            results = [r for r in results if r["enabled"]]
        # The list query uses param-substitution in the WHERE clause and a
        # final LIMIT param.  Simulate what the real query does: filter by
        # any positional args matched on (kind, applies_to_kind, applies_to_ref).
        # The test that exercises filters supplies them explicitly.
        positional = list(args[:-1])  # drop limit
        for value in positional:
            if value in PREDICATE_KINDS:
                results = [r for r in results if r["predicate_kind"] == value]
            else:
                results = [
                    r
                    for r in results
                    if r["applies_to_kind"] == value or r["applies_to_ref"] == value
                ]
        limit = int(args[-1])
        results.sort(key=lambda r: (r["predicate_kind"], r["predicate_slug"]))
        return [dict(r) for r in results[:limit]]


def test_record_predicate_inserts_row_and_returns_normalized_payload() -> None:
    conn = _FakeConn()
    out = record_predicate(
        conn,
        predicate_slug="dataset_promotion.invalidates_curated_projection_cache",
        predicate_kind="causal",
        applies_to_kind="object_kind",
        applies_to_ref="dataset_promotion",
        summary="Every promotion fires curated-projection cache invalidation",
        rationale="Manual and auto promotions must invalidate the same cache.",
        propagation_policy={
            "on_event": "dataset_promotion_recorded",
            "fires": [{"action": "cache_invalidate"}],
        },
        decision_ref="architecture-policy::semantics::predicate-catalog-propagation",
    )
    assert out["status"] == "recorded"
    record = out["predicate"]
    assert record["predicate_slug"] == "dataset_promotion.invalidates_curated_projection_cache"
    assert record["predicate_kind"] == "causal"
    assert record["propagation_policy"]["fires"][0]["action"] == "cache_invalidate"
    assert record["enabled"] is True


def test_record_predicate_rejects_unknown_kind() -> None:
    conn = _FakeConn()
    with pytest.raises(SemanticPredicateAuthorityError) as exc:
        record_predicate(
            conn,
            predicate_slug="weird.thing",
            predicate_kind="not_a_real_kind",
            applies_to_kind="object_kind",
            summary="x",
            rationale="x",
            decision_ref="x",
        )
    assert exc.value.reason_code == "semantic_predicate.invalid_kind"


def test_record_predicate_rejects_blank_required_fields() -> None:
    conn = _FakeConn()
    with pytest.raises(SemanticPredicateAuthorityError) as exc:
        record_predicate(
            conn,
            predicate_slug="",
            predicate_kind="invariant",
            applies_to_kind="object_kind",
            summary="x",
            rationale="x",
            decision_ref="x",
        )
    assert exc.value.reason_code == "semantic_predicate.invalid_submission"
    assert exc.value.details["field"] == "predicate_slug"


def test_record_predicate_upserts_on_conflict() -> None:
    conn = _FakeConn()
    record_predicate(
        conn,
        predicate_slug="invariant.demo",
        predicate_kind="invariant",
        applies_to_kind="surface",
        summary="initial",
        rationale="x",
        decision_ref="x",
    )
    out = record_predicate(
        conn,
        predicate_slug="invariant.demo",
        predicate_kind="invariant",
        applies_to_kind="surface",
        summary="updated summary",
        rationale="updated rationale",
        decision_ref="x",
    )
    assert out["predicate"]["summary"] == "updated summary"
    assert out["predicate"]["rationale"] == "updated rationale"


def test_get_predicate_returns_existing_row() -> None:
    conn = _FakeConn()
    record_predicate(
        conn,
        predicate_slug="bugs.duplicate_via_failure_signature",
        predicate_kind="equivalence",
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        summary="bugs equivalence",
        rationale="x",
        decision_ref="x",
    )
    out = get_predicate(conn, predicate_slug="bugs.duplicate_via_failure_signature")
    assert out["predicate"]["predicate_kind"] == "equivalence"


def test_get_predicate_raises_not_found() -> None:
    conn = _FakeConn()
    with pytest.raises(SemanticPredicateAuthorityError) as exc:
        get_predicate(conn, predicate_slug="missing.slug")
    assert exc.value.reason_code == "semantic_predicate.not_found"
    assert exc.value.status_code == 404


def test_list_predicates_filters_by_kind_and_returns_count() -> None:
    conn = _FakeConn()
    for slug, kind in [
        ("a.invariant", "invariant"),
        ("b.causal", "causal"),
        ("c.invariant", "invariant"),
    ]:
        record_predicate(
            conn,
            predicate_slug=slug,
            predicate_kind=kind,
            applies_to_kind="object_kind",
            summary="x",
            rationale="x",
            decision_ref="x",
        )
    out = list_predicates(conn, predicate_kind="invariant")
    assert out["count"] == 2
    assert {p["predicate_slug"] for p in out["predicates"]} == {"a.invariant", "c.invariant"}


def test_list_predicates_excludes_disabled_by_default() -> None:
    conn = _FakeConn()
    record_predicate(
        conn,
        predicate_slug="enabled.one",
        predicate_kind="invariant",
        applies_to_kind="object_kind",
        summary="x",
        rationale="x",
        decision_ref="x",
        enabled=True,
    )
    record_predicate(
        conn,
        predicate_slug="disabled.one",
        predicate_kind="invariant",
        applies_to_kind="object_kind",
        summary="x",
        rationale="x",
        decision_ref="x",
        enabled=False,
    )
    out = list_predicates(conn)
    assert {p["predicate_slug"] for p in out["predicates"]} == {"enabled.one"}


def test_predicate_kinds_constant_matches_schema_check() -> None:
    """If this set drifts from the migration's CHECK constraint the migration
    will reject inserts the authority happily made.  Pin the contract."""
    assert PREDICATE_KINDS == frozenset(
        {
            "invariant",
            "equivalence",
            "causal",
            "retraction",
            "temporal_validity",
            "trust_weight",
        }
    )
