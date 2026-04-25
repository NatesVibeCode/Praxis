"""Equivalence engine for the semantic_predicate_catalog.

When a domain authority needs to ask "is this candidate equivalent to one
already on file?" — bug deduplication, dataset candidate dedupe, decision
collision detection — it should not invent a per-domain signature scheme.
This engine reads enabled equivalence predicates that target the same
``applies_to_kind`` / ``applies_to_ref`` and returns structured signatures
the caller can use to query their domain table.

Predicate shape::

    {
      "predicate_kind": "equivalence",
      "applies_to_kind": "object_kind",
      "applies_to_ref":  "bug",
      "propagation_policy": {
        "compare_fields":  ["failure_signature"],
        "fallback_fields": ["title_anchor"],
        "merge_policy":    "increment_recurrence_count"
      }
    }

Engine API::

    signatures = compute_equivalence_signatures(
        conn,
        applies_to_kind="object_kind",
        applies_to_ref="bug",
        candidate_payload={"failure_signature": "...", "title_anchor": "..."},
    )
    # -> [
    #   {
    #     "predicate_slug": "bugs.duplicate_via_failure_signature",
    #     "compare_signature":  {"failure_signature": "..."},
    #     "fallback_signature": {"title_anchor": "..."},
    #     "merge_policy": "increment_recurrence_count",
    #   },
    # ]

The engine is intentionally read-only and side-effect-free.  Callers decide
how to query their domain table for matches and what merge policy means in
their domain.  That keeps the engine a pure semantic layer and avoids the
"engine writes my table" coupling.

Today this primarily codifies the bugs duplicate-check rule that
``runtime.bug_tracker.build_failure_signature`` produces.  Future predicates
plug in without changing engine code.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any


class SemanticEquivalenceError(RuntimeError):
    """Raised when an equivalence predicate cannot be evaluated."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


# ---------------------------------------------------------------------------
# Predicate lookup
# ---------------------------------------------------------------------------


def _decode_policy(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def load_equivalence_predicates(
    conn: Any,
    *,
    applies_to_kind: str,
    applies_to_ref: str | None = None,
) -> list[dict[str, Any]]:
    """Return enabled equivalence predicates for the given scope.

    Sync — equivalence is read-side and stateless.  Callers are sync (e.g.
    ``runtime.bug_tracker.duplicate_check``); the engine matches that lane.
    """

    if applies_to_ref:
        rows = conn.execute(
            """
            SELECT predicate_slug, predicate_kind,
                   applies_to_kind, applies_to_ref,
                   propagation_policy, decision_ref
              FROM semantic_predicate_catalog
             WHERE enabled = TRUE
               AND predicate_kind = 'equivalence'
               AND applies_to_kind = $1
               AND applies_to_ref = $2
             ORDER BY predicate_slug
            """,
            applies_to_kind,
            applies_to_ref,
        )
    else:
        rows = conn.execute(
            """
            SELECT predicate_slug, predicate_kind,
                   applies_to_kind, applies_to_ref,
                   propagation_policy, decision_ref
              FROM semantic_predicate_catalog
             WHERE enabled = TRUE
               AND predicate_kind = 'equivalence'
               AND applies_to_kind = $1
             ORDER BY predicate_slug
            """,
            applies_to_kind,
        )
    predicates: list[dict[str, Any]] = []
    for row in rows or []:
        record = dict(row)
        record["propagation_policy"] = _decode_policy(record.get("propagation_policy"))
        predicates.append(record)
    return predicates


# ---------------------------------------------------------------------------
# Signature computation
# ---------------------------------------------------------------------------


def _payload_value(payload: Mapping[str, Any], field_name: str) -> Any:
    """Resolve ``field_name`` against payload, supporting dotted nested paths."""

    if "." not in field_name:
        return payload.get(field_name)
    cursor: Any = payload
    for part in field_name.split("."):
        if isinstance(cursor, Mapping):
            cursor = cursor.get(part)
        else:
            return None
        if cursor is None:
            return None
    return cursor


def _signature_dict(payload: Mapping[str, Any], fields: Iterable[str]) -> dict[str, Any]:
    """Project the named fields out of payload into a signature dict.

    A field whose value is ``None`` or empty string is omitted so that
    "field present but empty" and "field missing" signal the same thing.
    Order is preserved by ``fields`` so signature_hash is stable.
    """

    sig: dict[str, Any] = {}
    for field_name in fields:
        if not field_name:
            continue
        value = _payload_value(payload, field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        sig[field_name] = value
    return sig


def _signature_hash(signature: Mapping[str, Any]) -> str | None:
    """Return a deterministic hash for a signature, or None when empty."""

    if not signature:
        return None
    serialized = json.dumps(signature, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def compute_equivalence_signatures(
    conn: Any,
    *,
    applies_to_kind: str,
    applies_to_ref: str | None,
    candidate_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Compute compare/fallback signatures for every matching equivalence predicate.

    Returns one entry per predicate::

        {
          "predicate_slug": "...",
          "applies_to_kind": "...",
          "applies_to_ref":  "...",
          "compare_fields":  [...],
          "compare_signature": {field: value, ...},
          "compare_signature_hash": "sha256:...",
          "fallback_fields":  [...],
          "fallback_signature": {field: value, ...},
          "fallback_signature_hash": "sha256:...",
          "merge_policy": "...",
        }
    """

    predicates = load_equivalence_predicates(
        conn,
        applies_to_kind=applies_to_kind,
        applies_to_ref=applies_to_ref,
    )
    return _compute_signatures_from_predicates(predicates, candidate_payload)


def _compute_signatures_from_predicates(
    predicates: Iterable[Mapping[str, Any]],
    candidate_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for predicate in predicates:
        if predicate.get("predicate_kind") != "equivalence":
            continue
        policy = predicate.get("propagation_policy") or {}
        if not isinstance(policy, Mapping):
            continue
        compare_fields = list(policy.get("compare_fields") or [])
        fallback_fields = list(policy.get("fallback_fields") or [])
        compare_sig = _signature_dict(candidate_payload, compare_fields)
        fallback_sig = _signature_dict(candidate_payload, fallback_fields)
        results.append(
            {
                "predicate_slug": predicate.get("predicate_slug"),
                "applies_to_kind": predicate.get("applies_to_kind"),
                "applies_to_ref": predicate.get("applies_to_ref"),
                "compare_fields": compare_fields,
                "compare_signature": compare_sig,
                "compare_signature_hash": _signature_hash(compare_sig),
                "fallback_fields": fallback_fields,
                "fallback_signature": fallback_sig,
                "fallback_signature_hash": _signature_hash(fallback_sig),
                "merge_policy": policy.get("merge_policy"),
            }
        )
    return results


# ---------------------------------------------------------------------------
# Match scoring (caller-driven)
# ---------------------------------------------------------------------------


_MATCH_KIND_COMPARE = "compare"
_MATCH_KIND_FALLBACK = "fallback"


def rank_candidate_against_existing(
    *,
    candidate_signatures: Iterable[Mapping[str, Any]],
    existing_payloads: Iterable[Mapping[str, Any]],
    existing_id_field: str,
) -> list[dict[str, Any]]:
    """For each existing payload, return its strongest match against any
    candidate signature.

    Match score:
      * 1.0  — every compare_field of some predicate matches exactly
      * 0.5  — every fallback_field of some predicate matches exactly
      * 0.0  — no match (omitted from result)
    """

    candidate_signatures_list = list(candidate_signatures)
    matches: list[dict[str, Any]] = []
    for existing in existing_payloads:
        best: dict[str, Any] | None = None
        for sig in candidate_signatures_list:
            compare_fields = sig.get("compare_fields") or []
            fallback_fields = sig.get("fallback_fields") or []
            existing_compare = _signature_dict(existing, compare_fields)
            existing_fallback = _signature_dict(existing, fallback_fields)
            candidate_compare = sig.get("compare_signature") or {}
            candidate_fallback = sig.get("fallback_signature") or {}
            score = 0.0
            match_kind: str | None = None
            if (
                candidate_compare
                and existing_compare
                and candidate_compare == existing_compare
            ):
                score = 1.0
                match_kind = _MATCH_KIND_COMPARE
            elif (
                candidate_fallback
                and existing_fallback
                and candidate_fallback == existing_fallback
            ):
                score = 0.5
                match_kind = _MATCH_KIND_FALLBACK
            if score > 0 and (best is None or score > best["score"]):
                best = {
                    "id": _payload_value(existing, existing_id_field),
                    "predicate_slug": sig.get("predicate_slug"),
                    "score": score,
                    "match_kind": match_kind,
                    "merge_policy": sig.get("merge_policy"),
                }
        if best is not None:
            matches.append(best)
    matches.sort(key=lambda match: match["score"], reverse=True)
    return matches


__all__ = [
    "SemanticEquivalenceError",
    "compute_equivalence_signatures",
    "load_equivalence_predicates",
    "rank_candidate_against_existing",
]
