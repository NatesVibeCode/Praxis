"""Unit tests for runtime.semantic_equivalence_engine."""

from __future__ import annotations

import json
from typing import Any

import pytest

from runtime.semantic_equivalence_engine import (
    SemanticEquivalenceError,
    _signature_dict,
    _signature_hash,
    compute_equivalence_signatures,
    load_equivalence_predicates,
    rank_candidate_against_existing,
)


class _SyncConn:
    """Tiny sync-conn fake that returns predicate rows."""

    def __init__(self, predicates: list[dict[str, Any]]) -> None:
        self._predicates = predicates
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "FROM semantic_predicate_catalog" not in normalized:
            return []
        rows = [
            row
            for row in self._predicates
            if row.get("predicate_kind") == "equivalence"
            and row.get("applies_to_kind") == args[0]
        ]
        if "applies_to_ref = $2" in normalized and len(args) > 1:
            rows = [row for row in rows if row.get("applies_to_ref") == args[1]]
        return [dict(row) for row in rows]


def _bug_predicate() -> dict[str, Any]:
    return {
        "predicate_slug": "bugs.duplicate_via_failure_signature",
        "predicate_kind": "equivalence",
        "applies_to_kind": "object_kind",
        "applies_to_ref": "bug",
        "propagation_policy": {
            "compare_fields": ["failure_signature"],
            "fallback_fields": ["title_anchor"],
            "merge_policy": "increment_recurrence_count",
        },
    }


def test_signature_dict_omits_missing_and_empty_fields() -> None:
    payload = {"failure_signature": "abc", "title_anchor": "", "extra": "ignored"}
    sig = _signature_dict(payload, ["failure_signature", "title_anchor", "missing"])
    assert sig == {"failure_signature": "abc"}


def test_signature_dict_supports_dotted_paths() -> None:
    payload = {"resume_context": {"failure_signature": "deep"}}
    sig = _signature_dict(payload, ["resume_context.failure_signature"])
    assert sig == {"resume_context.failure_signature": "deep"}


def test_signature_hash_is_stable_and_deterministic() -> None:
    h1 = _signature_hash({"a": "1", "b": "2"})
    h2 = _signature_hash({"b": "2", "a": "1"})
    assert h1 == h2
    assert _signature_hash({}) is None


def test_compute_equivalence_signatures_returns_compare_and_fallback() -> None:
    conn = _SyncConn([_bug_predicate()])
    out = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={
            "failure_signature": "sha256:abc",
            "title_anchor": "trigger:double_fire",
        },
    )
    assert len(out) == 1
    sig = out[0]
    assert sig["predicate_slug"] == "bugs.duplicate_via_failure_signature"
    assert sig["compare_signature"] == {"failure_signature": "sha256:abc"}
    assert sig["fallback_signature"] == {"title_anchor": "trigger:double_fire"}
    assert sig["compare_signature_hash"] is not None
    assert sig["fallback_signature_hash"] is not None
    assert sig["merge_policy"] == "increment_recurrence_count"


def test_compute_equivalence_signatures_handles_jsonb_string_propagation_policy() -> None:
    pred = _bug_predicate()
    pred["propagation_policy"] = json.dumps(pred["propagation_policy"])
    conn = _SyncConn([pred])
    out = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={"failure_signature": "x"},
    )
    assert out and out[0]["compare_signature"] == {"failure_signature": "x"}


def test_compute_equivalence_signatures_filters_to_matching_scope() -> None:
    other = dict(_bug_predicate())
    other["predicate_slug"] = "datasets.candidate_dedupe"
    other["applies_to_ref"] = "dataset_candidate"
    conn = _SyncConn([_bug_predicate(), other])
    out = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={"failure_signature": "x"},
    )
    assert {sig["predicate_slug"] for sig in out} == {"bugs.duplicate_via_failure_signature"}


def test_load_equivalence_predicates_skips_non_equivalence_kinds() -> None:
    causal = {
        "predicate_slug": "x",
        "predicate_kind": "causal",
        "applies_to_kind": "object_kind",
        "applies_to_ref": "bug",
        "propagation_policy": {"on_event": "anything"},
    }
    conn = _SyncConn([causal, _bug_predicate()])
    predicates = load_equivalence_predicates(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
    )
    assert {p["predicate_slug"] for p in predicates} == {"bugs.duplicate_via_failure_signature"}


def test_rank_candidate_against_existing_scores_compare_above_fallback() -> None:
    conn = _SyncConn([_bug_predicate()])
    sigs = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={
            "failure_signature": "sha256:abc",
            "title_anchor": "trigger:double_fire",
        },
    )
    existing = [
        {
            "bug_id": "BUG-EXACT",
            "failure_signature": "sha256:abc",
            "title_anchor": "different",
        },
        {
            "bug_id": "BUG-FUZZY",
            "failure_signature": "sha256:other",
            "title_anchor": "trigger:double_fire",
        },
        {
            "bug_id": "BUG-NONE",
            "failure_signature": "sha256:other",
            "title_anchor": "different",
        },
    ]
    matches = rank_candidate_against_existing(
        candidate_signatures=sigs,
        existing_payloads=existing,
        existing_id_field="bug_id",
    )
    assert [m["id"] for m in matches] == ["BUG-EXACT", "BUG-FUZZY"]
    assert matches[0]["match_kind"] == "compare"
    assert matches[0]["score"] == 1.0
    assert matches[1]["match_kind"] == "fallback"
    assert matches[1]["score"] == 0.5
    assert matches[0]["merge_policy"] == "increment_recurrence_count"


def test_rank_candidate_returns_empty_when_no_matches() -> None:
    conn = _SyncConn([_bug_predicate()])
    sigs = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={"failure_signature": "sha256:abc"},
    )
    matches = rank_candidate_against_existing(
        candidate_signatures=sigs,
        existing_payloads=[
            {"bug_id": "BUG-A", "failure_signature": "sha256:other"},
        ],
        existing_id_field="bug_id",
    )
    assert matches == []


def test_compute_equivalence_signatures_returns_empty_when_no_predicate_in_scope() -> None:
    conn = _SyncConn([])
    out = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={"failure_signature": "x"},
    )
    assert out == []
