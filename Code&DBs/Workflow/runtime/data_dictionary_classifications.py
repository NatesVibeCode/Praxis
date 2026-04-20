"""Runtime authority for data dictionary classifications / tags.

Three layers coexist in `data_dictionary_classifications`:
- `auto`     — projector-derived (name heuristics, type hints)
- `inferred` — sampler-derived (regex match on observed values)
- `operator` — hand-authored, highest precedence

Projectors call `apply_projected_classifications()` to replace their own
rows idempotently (keyed on origin_ref.projector). Operators mutate through
`set_operator_classification()` / `clear_operator_classification()`. Reads
go through `describe_classifications()` (effective merged view plus raw
layers) and `find_by_tag()` (compliance reports).
"""

from __future__ import annotations

from typing import Any, Iterable

from storage.postgres.data_dictionary_classification_repository import (
    count_classifications_by_source,
    delete_classification,
    list_by_tag,
    list_classification_layers,
    list_classifications_for,
    replace_projected_classifications,
    upsert_classification,
)
from storage.postgres.data_dictionary_repository import get_object
from storage.postgres.validators import PostgresWriteError


class DataDictionaryClassificationError(RuntimeError):
    """Raised when a classification authority call is rejected."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _raise_storage(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise DataDictionaryClassificationError(str(exc), status_code=status_code) from exc


def _ensure_object_known(conn: Any, object_kind: str) -> None:
    row = get_object(conn, object_kind=object_kind)
    if row is None:
        raise DataDictionaryClassificationError(
            f"object_kind {object_kind!r} is not registered in the data dictionary",
            status_code=404,
        )


# --- projector-facing API -------------------------------------------------


def apply_projected_classifications(
    conn: Any,
    *,
    projector_tag: str,
    entries: Iterable[dict[str, Any]],
    source: str = "auto",
) -> dict[str, Any]:
    """Idempotently write classifications for one projector.

    Each entry is a dict with:
        object_kind, tag_key               (required)
        field_path                         (optional, "" = object-level)
        tag_value, confidence, origin_ref, metadata  (optional)
    """
    tag = _text(projector_tag)
    if not tag:
        raise DataDictionaryClassificationError("projector_tag is required")
    if source not in ("auto", "inferred"):
        raise DataDictionaryClassificationError(
            "apply_projected_classifications only writes auto/inferred layers"
        )

    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(entries or []):
        if not isinstance(raw, dict):
            raise DataDictionaryClassificationError(
                f"entries[{index}] must be an object"
            )
        kind = _text(raw.get("object_kind"))
        key = _text(raw.get("tag_key"))
        if not kind or not key:
            raise DataDictionaryClassificationError(
                f"entries[{index}] requires object_kind and tag_key"
            )
        origin_ref = dict(raw.get("origin_ref") or {})
        origin_ref.setdefault("projector", tag)
        normalized.append({
            "object_kind": kind,
            "field_path": _text(raw.get("field_path")),
            "tag_key": key,
            "tag_value": _text(raw.get("tag_value")),
            "confidence": float(raw.get("confidence", 1.0)),
            "origin_ref": origin_ref,
            "metadata": raw.get("metadata") or {},
        })

    try:
        written = replace_projected_classifications(
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
        "classifications_written": written,
    }


# --- operator-facing API --------------------------------------------------


def set_operator_classification(
    conn: Any,
    *,
    object_kind: str,
    tag_key: str,
    tag_value: str = "",
    field_path: str = "",
    confidence: float = 1.0,
    metadata: Any = None,
) -> dict[str, Any]:
    """Write an operator-layer tag that outranks auto/inferred."""
    kind = _text(object_kind)
    key = _text(tag_key)
    if not kind or not key:
        raise DataDictionaryClassificationError(
            "object_kind and tag_key are required"
        )
    _ensure_object_known(conn, kind)
    try:
        row = upsert_classification(
            conn,
            object_kind=kind,
            field_path=_text(field_path),
            tag_key=key,
            tag_value=_text(tag_value),
            source="operator",
            confidence=confidence,
            origin_ref={"source": "operator"},
            metadata=metadata or {},
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"classification": dict(row)}


def clear_operator_classification(
    conn: Any,
    *,
    object_kind: str,
    tag_key: str,
    field_path: str = "",
) -> dict[str, Any]:
    kind = _text(object_kind)
    key = _text(tag_key)
    if not kind or not key:
        raise DataDictionaryClassificationError(
            "object_kind and tag_key are required"
        )
    try:
        removed = delete_classification(
            conn,
            object_kind=kind,
            field_path=_text(field_path),
            tag_key=key,
            source="operator",
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {
        "object_kind": kind,
        "field_path": _text(field_path),
        "tag_key": key,
        "removed": removed,
    }


# --- read API -------------------------------------------------------------


def describe_classifications(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
    include_layers: bool = False,
) -> dict[str, Any]:
    """Return effective tags on an object (or one of its fields)."""
    kind = _text(object_kind)
    if not kind:
        raise DataDictionaryClassificationError("object_kind is required")
    fp = field_path if field_path is None else _text(field_path)

    effective = list_classifications_for(
        conn, object_kind=kind, field_path=fp,
    )
    response: dict[str, Any] = {
        "object_kind": kind,
        "field_path": fp,
        "effective": effective,
    }
    if include_layers:
        response["layers"] = list_classification_layers(
            conn, object_kind=kind, field_path=fp,
        )
    return response


def find_by_tag(
    conn: Any,
    *,
    tag_key: str,
    tag_value: str | None = None,
) -> dict[str, Any]:
    """Compliance-report helper: list every field with a given tag."""
    key = _text(tag_key)
    if not key:
        raise DataDictionaryClassificationError("tag_key is required")
    value = tag_value if tag_value is None else _text(tag_value)
    rows = list_by_tag(conn, tag_key=key, tag_value=value)
    return {
        "tag_key": key,
        "tag_value": value,
        "matches": rows,
    }


def classification_summary(conn: Any) -> dict[str, Any]:
    """Counts useful for health dashboards."""
    return {"classifications_by_source": count_classifications_by_source(conn)}


__all__ = [
    "DataDictionaryClassificationError",
    "apply_projected_classifications",
    "classification_summary",
    "clear_operator_classification",
    "describe_classifications",
    "find_by_tag",
    "set_operator_classification",
]
