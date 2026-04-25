"""Unit tests for runtime.primitive_authority."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from runtime.primitive_authority import (
    PRIMITIVE_KINDS,
    PrimitiveAuthorityError,
    get_primitive,
    list_primitives,
    record_primitive,
)


class _FakeConn:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "INSERT INTO primitive_catalog" in normalized:
            (
                _id,
                slug,
                kind,
                summary,
                rationale,
                spec_json,
                deps_json,
                decision,
                enabled,
                meta_json,
            ) = args
            now = datetime.now(timezone.utc)
            existing = self.rows.get(slug)
            row = {
                "primitive_id": existing["primitive_id"] if existing else _id,
                "primitive_slug": slug,
                "primitive_kind": kind,
                "summary": summary,
                "rationale": rationale,
                "spec": json.loads(spec_json),
                "depends_on": json.loads(deps_json),
                "decision_ref": decision,
                "enabled": enabled,
                "metadata": json.loads(meta_json),
                "created_at": existing["created_at"] if existing else now,
                "updated_at": now,
            }
            self.rows[slug] = row
            return dict(row)
        if "FROM primitive_catalog" in normalized and "WHERE primitive_slug" in normalized:
            row = self.rows.get(args[0])
            return dict(row) if row else None
        return None

    def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        rows = list(self.rows.values())
        if "WHERE enabled = TRUE" in sql or "AND enabled = TRUE" in sql:
            rows = [r for r in rows if r["enabled"]]
        positional = list(args[:-1])  # drop limit
        for value in positional:
            if value in PRIMITIVE_KINDS:
                rows = [r for r in rows if r["primitive_kind"] == value]
        limit = int(args[-1])
        rows.sort(key=lambda r: (r["primitive_kind"], r["primitive_slug"]))
        return [dict(r) for r in rows[:limit]]


def test_record_primitive_inserts_and_normalizes() -> None:
    conn = _FakeConn()
    out = record_primitive(
        conn,
        primitive_slug="semantic_predicate_catalog",
        primitive_kind="domain_authority",
        summary="catalog of semantic predicates",
        rationale="meta-layer for the platform",
        spec={"authority_module": "runtime.semantic_predicate_authority"},
        depends_on=[],
        decision_ref="architecture-policy::primitives::catalog-managed-blueprints",
    )
    assert out["status"] == "recorded"
    rec = out["primitive"]
    assert rec["primitive_kind"] == "domain_authority"
    assert rec["spec"]["authority_module"] == "runtime.semantic_predicate_authority"
    assert rec["depends_on"] == []


def test_record_primitive_rejects_unknown_kind() -> None:
    conn = _FakeConn()
    with pytest.raises(PrimitiveAuthorityError) as exc:
        record_primitive(
            conn,
            primitive_slug="x",
            primitive_kind="bogus_kind",
            summary="x",
            rationale="x",
            decision_ref="x",
        )
    assert exc.value.reason_code == "primitive.invalid_kind"


def test_record_primitive_rejects_non_object_spec() -> None:
    conn = _FakeConn()
    with pytest.raises(PrimitiveAuthorityError) as exc:
        record_primitive(
            conn,
            primitive_slug="x",
            primitive_kind="read_engine",
            summary="x",
            rationale="x",
            decision_ref="x",
            spec="not a dict",  # type: ignore[arg-type]
        )
    assert exc.value.reason_code == "primitive.invalid_submission"
    assert exc.value.details["field"] == "spec"


def test_record_primitive_upserts_on_conflict() -> None:
    conn = _FakeConn()
    record_primitive(
        conn,
        primitive_slug="dup",
        primitive_kind="read_engine",
        summary="initial",
        rationale="initial",
        decision_ref="d",
    )
    out = record_primitive(
        conn,
        primitive_slug="dup",
        primitive_kind="read_engine",
        summary="updated summary",
        rationale="updated rationale",
        decision_ref="d",
    )
    assert out["primitive"]["summary"] == "updated summary"


def test_get_primitive_returns_existing_row() -> None:
    conn = _FakeConn()
    record_primitive(
        conn,
        primitive_slug="x",
        primitive_kind="gateway_handler",
        summary="x",
        rationale="x",
        decision_ref="d",
    )
    out = get_primitive(conn, primitive_slug="x")
    assert out["primitive"]["primitive_kind"] == "gateway_handler"


def test_get_primitive_raises_not_found() -> None:
    conn = _FakeConn()
    with pytest.raises(PrimitiveAuthorityError) as exc:
        get_primitive(conn, primitive_slug="missing")
    assert exc.value.reason_code == "primitive.not_found"
    assert exc.value.status_code == 404


def test_list_primitives_filters_by_kind() -> None:
    conn = _FakeConn()
    for slug, kind in [
        ("a", "domain_authority"),
        ("b", "read_engine"),
        ("c", "domain_authority"),
    ]:
        record_primitive(
            conn,
            primitive_slug=slug,
            primitive_kind=kind,
            summary="x",
            rationale="x",
            decision_ref="d",
        )
    out = list_primitives(conn, primitive_kind="domain_authority")
    assert out["count"] == 2
    assert {p["primitive_slug"] for p in out["primitives"]} == {"a", "c"}


def test_primitive_kinds_constant_locks_schema_check() -> None:
    """If this set drifts from the migration's CHECK constraint the migration
    would reject inserts the authority happily made."""
    assert PRIMITIVE_KINDS == frozenset(
        {
            "domain_authority",
            "read_engine",
            "write_engine",
            "gateway_handler",
            "repository",
        }
    )
