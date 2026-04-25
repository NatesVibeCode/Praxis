"""CQRS authority for the semantic_predicate_catalog.

The catalog declares typed semantic facts that govern behavior across the
system: invariants, equivalence rules, causal propagations (cache
invalidation, event cascades), retraction cascades, temporal validity, and
trust weighting.  Each row is a single declaration that downstream surfaces
read instead of hand-coding the rule.

This module owns the durable read/write contract.  Gateway-friendly handlers
in runtime/operations/{commands,queries}/semantic_predicate_catalog.py wrap these
helpers so the catalog auto-mounts at REST/MCP/CLI through the operation
catalog, the same way every other catalog-mounted operation reaches its
surfaces.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any
from uuid import uuid4


PREDICATE_KINDS = frozenset(
    {
        "invariant",
        "equivalence",
        "causal",
        "retraction",
        "temporal_validity",
        "trust_weight",
    }
)


class SemanticPredicateAuthorityError(RuntimeError):
    """Raised when semantic predicate authority rejects an operation."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.status_code = status_code
        self.details = dict(details or {})


# ---------------------------------------------------------------------------
# Validators (kept in this module so callers don't need a pydantic dependency
# to use the authority — the gateway wrappers add Pydantic separately).
# ---------------------------------------------------------------------------


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_kind(value: object) -> str:
    text = _require_text(value, field_name="predicate_kind")
    if text not in PREDICATE_KINDS:
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.invalid_kind",
            f"predicate_kind must be one of {sorted(PREDICATE_KINDS)}",
            details={"predicate_kind": text},
        )
    return text


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise SemanticPredicateAuthorityError(
                "semantic_predicate.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, dict):
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.invalid_submission",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _bounded_limit(value: object) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError) as exc:
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": value},
        ) from exc
    if limit < 1 or limit > 1000:
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": limit},
        )
    return limit


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _decode_jsonb(value: object) -> dict[str, Any]:
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


# ---------------------------------------------------------------------------
# Core write: upsert a predicate row.
# ---------------------------------------------------------------------------


def record_predicate(
    conn: Any,
    *,
    predicate_slug: str,
    predicate_kind: str,
    applies_to_kind: str,
    summary: str,
    rationale: str,
    decision_ref: str,
    applies_to_ref: str | None = None,
    validator_ref: str | None = None,
    propagation_policy: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    """Upsert one semantic_predicate_catalog row by predicate_slug."""

    slug = _require_text(predicate_slug, field_name="predicate_slug")
    kind = _require_kind(predicate_kind)
    applies_kind = _require_text(applies_to_kind, field_name="applies_to_kind")
    summary_text = _require_text(summary, field_name="summary")
    rationale_text = _require_text(rationale, field_name="rationale")
    decision = _require_text(decision_ref, field_name="decision_ref")
    applies_ref = _optional_text(applies_to_ref, field_name="applies_to_ref")
    validator = _optional_text(validator_ref, field_name="validator_ref")
    policy = _json_object(propagation_policy, field_name="propagation_policy")
    meta = _json_object(metadata, field_name="metadata")

    row = conn.fetchrow(
        """
        INSERT INTO semantic_predicate_catalog (
            predicate_id, predicate_slug, predicate_kind,
            applies_to_kind, applies_to_ref,
            summary, rationale,
            validator_ref, propagation_policy,
            decision_ref, enabled, metadata
        ) VALUES (
            $1::uuid, $2, $3, $4, $5,
            $6, $7, $8, $9::jsonb, $10, $11, $12::jsonb
        )
        ON CONFLICT (predicate_slug) DO UPDATE SET
            predicate_kind     = EXCLUDED.predicate_kind,
            applies_to_kind    = EXCLUDED.applies_to_kind,
            applies_to_ref     = EXCLUDED.applies_to_ref,
            summary            = EXCLUDED.summary,
            rationale          = EXCLUDED.rationale,
            validator_ref      = EXCLUDED.validator_ref,
            propagation_policy = EXCLUDED.propagation_policy,
            decision_ref       = EXCLUDED.decision_ref,
            enabled            = EXCLUDED.enabled,
            metadata           = EXCLUDED.metadata,
            updated_at         = now()
        RETURNING predicate_id, predicate_slug, predicate_kind,
                  applies_to_kind, applies_to_ref,
                  summary, rationale,
                  validator_ref, propagation_policy,
                  decision_ref, enabled, metadata,
                  created_at, updated_at
        """,
        str(uuid4()),
        slug,
        kind,
        applies_kind,
        applies_ref,
        summary_text,
        rationale_text,
        validator,
        json.dumps(policy, sort_keys=True, default=str),
        decision,
        bool(enabled),
        json.dumps(meta, sort_keys=True, default=str),
    )
    record = _row(row)
    record["propagation_policy"] = _decode_jsonb(record.get("propagation_policy"))
    record["metadata"] = _decode_jsonb(record.get("metadata"))
    return {"status": "recorded", "predicate": record}


# ---------------------------------------------------------------------------
# Reads: list (filterable) and get (by slug).
# ---------------------------------------------------------------------------


def list_predicates(
    conn: Any,
    *,
    predicate_kind: str | None = None,
    applies_to_kind: str | None = None,
    applies_to_ref: str | None = None,
    enabled_only: bool = True,
    limit: int = 100,
) -> dict[str, Any]:
    """List semantic predicates with optional filters."""

    kind = _optional_text(predicate_kind, field_name="predicate_kind")
    if kind is not None and kind not in PREDICATE_KINDS:
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.invalid_kind",
            f"predicate_kind must be one of {sorted(PREDICATE_KINDS)}",
            details={"predicate_kind": kind},
        )
    applies_kind = _optional_text(applies_to_kind, field_name="applies_to_kind")
    applies_ref = _optional_text(applies_to_ref, field_name="applies_to_ref")
    bounded = _bounded_limit(limit)

    clauses: list[str] = []
    args: list[Any] = []
    if enabled_only:
        clauses.append("enabled = TRUE")
    if kind is not None:
        args.append(kind)
        clauses.append(f"predicate_kind = ${len(args)}")
    if applies_kind is not None:
        args.append(applies_kind)
        clauses.append(f"applies_to_kind = ${len(args)}")
    if applies_ref is not None:
        args.append(applies_ref)
        clauses.append(f"applies_to_ref = ${len(args)}")
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    args.append(bounded)
    rows = (
        conn.fetch(
            f"""
            SELECT predicate_id, predicate_slug, predicate_kind,
                   applies_to_kind, applies_to_ref,
                   summary, rationale,
                   validator_ref, propagation_policy,
                   decision_ref, enabled, metadata,
                   created_at, updated_at
              FROM semantic_predicate_catalog
            {where}
             ORDER BY predicate_kind, predicate_slug
             LIMIT ${len(args)}
            """,
            *args,
        )
        if hasattr(conn, "fetch") and callable(conn.fetch)
        else conn.execute(
            f"""
            SELECT predicate_id, predicate_slug, predicate_kind,
                   applies_to_kind, applies_to_ref,
                   summary, rationale,
                   validator_ref, propagation_policy,
                   decision_ref, enabled, metadata,
                   created_at, updated_at
              FROM semantic_predicate_catalog
            {where}
             ORDER BY predicate_kind, predicate_slug
             LIMIT ${len(args)}
            """,
            *args,
        )
    )
    predicates: list[dict[str, Any]] = []
    for row in rows or []:
        record = _row(row)
        record["propagation_policy"] = _decode_jsonb(record.get("propagation_policy"))
        record["metadata"] = _decode_jsonb(record.get("metadata"))
        predicates.append(record)
    return {
        "predicates": predicates,
        "count": len(predicates),
        "filters": {
            "predicate_kind": kind,
            "applies_to_kind": applies_kind,
            "applies_to_ref": applies_ref,
            "enabled_only": enabled_only,
            "limit": bounded,
        },
    }


def get_predicate(conn: Any, *, predicate_slug: str) -> dict[str, Any]:
    """Fetch one semantic predicate by predicate_slug."""

    slug = _require_text(predicate_slug, field_name="predicate_slug")
    row = conn.fetchrow(
        """
        SELECT predicate_id, predicate_slug, predicate_kind,
               applies_to_kind, applies_to_ref,
               summary, rationale,
               validator_ref, propagation_policy,
               decision_ref, enabled, metadata,
               created_at, updated_at
          FROM semantic_predicate_catalog
         WHERE predicate_slug = $1
        """,
        slug,
    )
    if row is None:
        raise SemanticPredicateAuthorityError(
            "semantic_predicate.not_found",
            "no semantic predicate matches the requested slug",
            status_code=404,
            details={"predicate_slug": slug},
        )
    record = _row(row)
    record["propagation_policy"] = _decode_jsonb(record.get("propagation_policy"))
    record["metadata"] = _decode_jsonb(record.get("metadata"))
    return {"predicate": record}


__all__ = [
    "PREDICATE_KINDS",
    "SemanticPredicateAuthorityError",
    "record_predicate",
    "list_predicates",
    "get_predicate",
]
