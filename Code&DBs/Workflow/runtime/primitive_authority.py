"""CQRS authority for the primitive_catalog.

Each row declares one platform primitive (authority, engine, gateway
wrapper, repository) with the modules / catalog rows / tests it must
have to exist consistently.  Downstream engines (consistency scanner,
future scaffolder) read this catalog to detect drift between blueprint
and reality.

The primitive_catalog primitive is itself in the catalog — recursive
proof that the contract holds.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any
from uuid import uuid4


PRIMITIVE_KINDS = frozenset(
    {
        "domain_authority",
        "read_engine",
        "write_engine",
        "gateway_handler",
        "repository",
    }
)


class PrimitiveAuthorityError(RuntimeError):
    """Raised when primitive authority rejects an operation."""

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
# Validators
# ---------------------------------------------------------------------------


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PrimitiveAuthorityError(
            "primitive.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_kind(value: object) -> str:
    text = _require_text(value, field_name="primitive_kind")
    if text not in PRIMITIVE_KINDS:
        raise PrimitiveAuthorityError(
            "primitive.invalid_kind",
            f"primitive_kind must be one of {sorted(PRIMITIVE_KINDS)}",
            details={"primitive_kind": text},
        )
    return text


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise PrimitiveAuthorityError(
                "primitive.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, dict):
        raise PrimitiveAuthorityError(
            "primitive.invalid_submission",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _json_array(value: object, *, field_name: str) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise PrimitiveAuthorityError(
                "primitive.invalid_submission",
                f"{field_name} must be a JSON array",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, list):
        raise PrimitiveAuthorityError(
            "primitive.invalid_submission",
            f"{field_name} must be a JSON array",
            details={"field": field_name},
        )
    return list(value)


def _bounded_limit(value: object) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError) as exc:
        raise PrimitiveAuthorityError(
            "primitive.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": value},
        ) from exc
    if limit < 1 or limit > 1000:
        raise PrimitiveAuthorityError(
            "primitive.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": limit},
        )
    return limit


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _decode_jsonb(value: object) -> Any:
    if value is None:
        return value
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _normalize_record(row: Any) -> dict[str, Any]:
    record = _row(row)
    record["spec"] = _decode_jsonb(record.get("spec")) or {}
    record["depends_on"] = _decode_jsonb(record.get("depends_on")) or []
    record["metadata"] = _decode_jsonb(record.get("metadata")) or {}
    return record


# ---------------------------------------------------------------------------
# Core writes / reads
# ---------------------------------------------------------------------------


def record_primitive(
    conn: Any,
    *,
    primitive_slug: str,
    primitive_kind: str,
    summary: str,
    rationale: str,
    decision_ref: str,
    spec: Mapping[str, Any] | None = None,
    depends_on: list[Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    """Upsert one primitive_catalog row by primitive_slug."""

    slug = _require_text(primitive_slug, field_name="primitive_slug")
    kind = _require_kind(primitive_kind)
    summary_text = _require_text(summary, field_name="summary")
    rationale_text = _require_text(rationale, field_name="rationale")
    decision = _require_text(decision_ref, field_name="decision_ref")
    spec_obj = _json_object(spec, field_name="spec")
    deps = _json_array(depends_on, field_name="depends_on")
    meta = _json_object(metadata, field_name="metadata")

    row = conn.fetchrow(
        """
        INSERT INTO primitive_catalog (
            primitive_id, primitive_slug, primitive_kind,
            summary, rationale, spec, depends_on,
            decision_ref, enabled, metadata
        ) VALUES (
            $1::uuid, $2, $3, $4, $5, $6::jsonb, $7::jsonb,
            $8, $9, $10::jsonb
        )
        ON CONFLICT (primitive_slug) DO UPDATE SET
            primitive_kind = EXCLUDED.primitive_kind,
            summary        = EXCLUDED.summary,
            rationale      = EXCLUDED.rationale,
            spec           = EXCLUDED.spec,
            depends_on     = EXCLUDED.depends_on,
            decision_ref   = EXCLUDED.decision_ref,
            enabled        = EXCLUDED.enabled,
            metadata       = EXCLUDED.metadata,
            updated_at     = now()
        RETURNING primitive_id, primitive_slug, primitive_kind,
                  summary, rationale, spec, depends_on,
                  decision_ref, enabled, metadata,
                  created_at, updated_at
        """,
        str(uuid4()),
        slug,
        kind,
        summary_text,
        rationale_text,
        json.dumps(spec_obj, sort_keys=True, default=str),
        json.dumps(deps, sort_keys=True, default=str),
        decision,
        bool(enabled),
        json.dumps(meta, sort_keys=True, default=str),
    )
    return {"status": "recorded", "primitive": _normalize_record(row)}


def list_primitives(
    conn: Any,
    *,
    primitive_kind: str | None = None,
    enabled_only: bool = True,
    limit: int = 100,
) -> dict[str, Any]:
    """List primitives with optional filter by primitive_kind."""

    kind = _optional_text(primitive_kind, field_name="primitive_kind")
    if kind is not None and kind not in PRIMITIVE_KINDS:
        raise PrimitiveAuthorityError(
            "primitive.invalid_kind",
            f"primitive_kind must be one of {sorted(PRIMITIVE_KINDS)}",
            details={"primitive_kind": kind},
        )
    bounded = _bounded_limit(limit)
    clauses: list[str] = []
    args: list[Any] = []
    if enabled_only:
        clauses.append("enabled = TRUE")
    if kind is not None:
        args.append(kind)
        clauses.append(f"primitive_kind = ${len(args)}")
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    args.append(bounded)
    fetch = (
        conn.fetch
        if hasattr(conn, "fetch") and callable(conn.fetch)
        else conn.execute
    )
    rows = fetch(
        f"""
        SELECT primitive_id, primitive_slug, primitive_kind,
               summary, rationale, spec, depends_on,
               decision_ref, enabled, metadata,
               created_at, updated_at
          FROM primitive_catalog
        {where}
         ORDER BY primitive_kind, primitive_slug
         LIMIT ${len(args)}
        """,
        *args,
    )
    primitives = [_normalize_record(row) for row in rows or []]
    return {
        "primitives": primitives,
        "count": len(primitives),
        "filters": {
            "primitive_kind": kind,
            "enabled_only": enabled_only,
            "limit": bounded,
        },
    }


def get_primitive(conn: Any, *, primitive_slug: str) -> dict[str, Any]:
    """Fetch one primitive by primitive_slug."""

    slug = _require_text(primitive_slug, field_name="primitive_slug")
    row = conn.fetchrow(
        """
        SELECT primitive_id, primitive_slug, primitive_kind,
               summary, rationale, spec, depends_on,
               decision_ref, enabled, metadata,
               created_at, updated_at
          FROM primitive_catalog
         WHERE primitive_slug = $1
        """,
        slug,
    )
    if row is None:
        raise PrimitiveAuthorityError(
            "primitive.not_found",
            "no primitive matches the requested slug",
            status_code=404,
            details={"primitive_slug": slug},
        )
    return {"primitive": _normalize_record(row)}


__all__ = [
    "PRIMITIVE_KINDS",
    "PrimitiveAuthorityError",
    "record_primitive",
    "list_primitives",
    "get_primitive",
]
