"""Runtime authority for data dictionary stewardship.

Three layers coexist in `data_dictionary_stewardship`:
- `auto`     — projector-derived (e.g. `created_by` column → publisher)
- `inferred` — behavioural signal (who actually writes to this table?)
- `operator` — hand-authored, highest precedence

Projectors call `apply_projected_stewards()` to replace their own rows
idempotently (keyed on origin_ref.projector). Operators mutate through
`set_operator_steward()` / `clear_operator_steward()`. Reads go through
`describe_stewards()` (effective merged view plus raw layers) and
`find_by_steward()` (what does alice@ own?).
"""

from __future__ import annotations

from typing import Any, Iterable

from storage.postgres.data_dictionary_repository import get_object
from storage.postgres.data_dictionary_stewardship_repository import (
    count_stewards_by_kind,
    count_stewards_by_source,
    delete_steward,
    list_assets_owned_by,
    list_steward_layers,
    list_stewards_for,
    replace_projected_stewards,
    upsert_steward,
)
from storage.postgres.validators import PostgresWriteError


class DataDictionaryStewardshipError(RuntimeError):
    """Raised when a stewardship authority call is rejected."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _raise_storage(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise DataDictionaryStewardshipError(str(exc), status_code=status_code) from exc


def _ensure_object_known(conn: Any, object_kind: str) -> None:
    row = get_object(conn, object_kind=object_kind)
    if row is None:
        raise DataDictionaryStewardshipError(
            f"object_kind {object_kind!r} is not registered in the data dictionary",
            status_code=404,
        )


# --- projector-facing API -------------------------------------------------


def apply_projected_stewards(
    conn: Any,
    *,
    projector_tag: str,
    entries: Iterable[dict[str, Any]],
    source: str = "auto",
) -> dict[str, Any]:
    """Idempotently write stewardship rows for one projector.

    Each entry is a dict with:
        object_kind, steward_kind, steward_id       (required)
        field_path, steward_type, confidence, origin_ref, metadata  (optional)
    """
    tag = _text(projector_tag)
    if not tag:
        raise DataDictionaryStewardshipError("projector_tag is required")
    if source not in ("auto", "inferred"):
        raise DataDictionaryStewardshipError(
            "apply_projected_stewards only writes auto/inferred layers"
        )

    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(entries or []):
        if not isinstance(raw, dict):
            raise DataDictionaryStewardshipError(
                f"entries[{index}] must be an object"
            )
        kind = _text(raw.get("object_kind"))
        sk = _text(raw.get("steward_kind"))
        sid = _text(raw.get("steward_id"))
        if not kind or not sk or not sid:
            raise DataDictionaryStewardshipError(
                f"entries[{index}] requires object_kind, steward_kind, steward_id"
            )
        origin_ref = dict(raw.get("origin_ref") or {})
        origin_ref.setdefault("projector", tag)
        normalized.append({
            "object_kind": kind,
            "field_path": _text(raw.get("field_path")),
            "steward_kind": sk,
            "steward_id": sid,
            "steward_type": _text(raw.get("steward_type")) or "person",
            "confidence": float(raw.get("confidence", 1.0)),
            "origin_ref": origin_ref,
            "metadata": raw.get("metadata") or {},
        })

    try:
        written = replace_projected_stewards(
            conn,
            source=source,
            projector_tag=tag,
            entries=normalized,
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {
        "projector": tag,
        "source": source,
        "stewards_written": written,
    }


# --- operator-facing API --------------------------------------------------


def set_operator_steward(
    conn: Any,
    *,
    object_kind: str,
    steward_kind: str,
    steward_id: str,
    steward_type: str = "person",
    field_path: str = "",
    confidence: float = 1.0,
    metadata: Any = None,
) -> dict[str, Any]:
    """Write an operator-layer steward that outranks auto/inferred."""
    kind = _text(object_kind)
    sk = _text(steward_kind)
    sid = _text(steward_id)
    if not kind or not sk or not sid:
        raise DataDictionaryStewardshipError(
            "object_kind, steward_kind, steward_id are required"
        )
    stype = _text(steward_type) or "person"
    _ensure_object_known(conn, kind)
    try:
        row = upsert_steward(
            conn,
            object_kind=kind,
            field_path=_text(field_path),
            steward_kind=sk,
            steward_id=sid,
            steward_type=stype,
            source="operator",
            confidence=confidence,
            origin_ref={"source": "operator"},
            metadata=metadata or {},
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"steward": dict(row)}


def clear_operator_steward(
    conn: Any,
    *,
    object_kind: str,
    steward_kind: str,
    steward_id: str,
    field_path: str = "",
) -> dict[str, Any]:
    kind = _text(object_kind)
    sk = _text(steward_kind)
    sid = _text(steward_id)
    if not kind or not sk or not sid:
        raise DataDictionaryStewardshipError(
            "object_kind, steward_kind, steward_id are required"
        )
    try:
        removed = delete_steward(
            conn,
            object_kind=kind,
            field_path=_text(field_path),
            steward_kind=sk,
            steward_id=sid,
            source="operator",
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {
        "object_kind": kind,
        "field_path": _text(field_path),
        "steward_kind": sk,
        "steward_id": sid,
        "removed": removed,
    }


# --- read API -------------------------------------------------------------


def describe_stewards(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
    include_layers: bool = False,
) -> dict[str, Any]:
    """Return effective stewards on an object (or one of its fields)."""
    kind = _text(object_kind)
    if not kind:
        raise DataDictionaryStewardshipError("object_kind is required")
    fp = field_path if field_path is None else _text(field_path)

    effective = list_stewards_for(conn, object_kind=kind, field_path=fp)
    response: dict[str, Any] = {
        "object_kind": kind,
        "field_path": fp,
        "effective": effective,
    }
    if include_layers:
        response["layers"] = list_steward_layers(
            conn, object_kind=kind, field_path=fp,
        )
    return response


def find_by_steward(
    conn: Any,
    *,
    steward_id: str,
    steward_kind: str | None = None,
) -> dict[str, Any]:
    """Reverse lookup: what assets does this principal steward?"""
    sid = _text(steward_id)
    if not sid:
        raise DataDictionaryStewardshipError("steward_id is required")
    kind = steward_kind if steward_kind is None else _text(steward_kind)
    rows = list_assets_owned_by(conn, steward_id=sid, steward_kind=kind)
    return {
        "steward_id": sid,
        "steward_kind": kind,
        "matches": rows,
    }


def stewardship_summary(conn: Any) -> dict[str, Any]:
    """Counts useful for health dashboards."""
    return {
        "stewards_by_source": count_stewards_by_source(conn),
        "stewards_by_kind": count_stewards_by_kind(conn),
    }


__all__ = [
    "DataDictionaryStewardshipError",
    "apply_projected_stewards",
    "clear_operator_steward",
    "describe_stewards",
    "find_by_steward",
    "set_operator_steward",
    "stewardship_summary",
]
