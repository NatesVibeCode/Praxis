"""Explicit sync Postgres repository for surface catalog authority writes."""

from __future__ import annotations

from typing import Any

from .validators import PostgresWriteError, _optional_text, _require_text

_ALLOWED_FAMILIES = frozenset({"trigger", "gather", "think", "act", "control"})
_ALLOWED_STATUSES = frozenset({"ready", "coming_soon"})
_ALLOWED_DROP_KINDS = frozenset({"node", "edge"})
_ALLOWED_TRUTH_CATEGORIES = frozenset({"runtime", "persisted", "alias", "partial", "coming_soon"})
_ALLOWED_SURFACE_TIERS = frozenset({"primary", "advanced", "hidden"})


def _row_dict(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "surface_catalog.write_failed",
            f"{operation} returned no row",
        )
    return dict(row)


def _require_enum(value: object, *, field_name: str, allowed: frozenset[str]) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized not in allowed:
        raise PostgresWriteError(
            "surface_catalog.invalid_submission",
            f"{field_name} must be one of: {', '.join(sorted(allowed))}",
            details={"field": field_name, "allowed": sorted(allowed), "value": value},
        )
    return normalized


def _optional_enum(value: object | None, *, field_name: str, allowed: frozenset[str]) -> str | None:
    normalized = _optional_text(value, field_name=field_name)
    if normalized is None:
        return None
    if normalized not in allowed:
        raise PostgresWriteError(
            "surface_catalog.invalid_submission",
            f"{field_name} must be one of: {', '.join(sorted(allowed))}",
            details={"field": field_name, "allowed": sorted(allowed), "value": value},
        )
    return normalized


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PostgresWriteError(
            "surface_catalog.invalid_submission",
            f"{field_name} must be a boolean",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int):
        raise PostgresWriteError(
            "surface_catalog.invalid_submission",
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    catalog_item_id = _require_text(item.get("catalog_item_id"), field_name="catalog_item_id")
    surface_name = _require_text(item.get("surface_name") or "canvas", field_name="surface_name")
    label = _require_text(item.get("label"), field_name="label")
    icon = _require_text(item.get("icon"), field_name="icon")
    family = _require_enum(item.get("family"), field_name="family", allowed=_ALLOWED_FAMILIES)
    status = _require_enum(item.get("status"), field_name="status", allowed=_ALLOWED_STATUSES)
    drop_kind = _require_enum(item.get("drop_kind"), field_name="drop_kind", allowed=_ALLOWED_DROP_KINDS)
    action_value = _optional_text(item.get("action_value"), field_name="action_value")
    gate_family = _optional_text(item.get("gate_family"), field_name="gate_family")
    description = str(item.get("description") or "").strip()
    truth_category = _require_enum(
        item.get("truth_category"),
        field_name="truth_category",
        allowed=_ALLOWED_TRUTH_CATEGORIES,
    )
    truth_badge = _require_text(item.get("truth_badge"), field_name="truth_badge")
    truth_detail = _require_text(item.get("truth_detail"), field_name="truth_detail")
    surface_tier = _require_enum(
        item.get("surface_tier"),
        field_name="surface_tier",
        allowed=_ALLOWED_SURFACE_TIERS,
    )
    surface_badge = _require_text(item.get("surface_badge"), field_name="surface_badge")
    surface_detail = _require_text(item.get("surface_detail"), field_name="surface_detail")
    hard_choice = _optional_text(item.get("hard_choice"), field_name="hard_choice")
    enabled = _require_bool(item.get("enabled", True), field_name="enabled")
    display_order = _require_int(item.get("display_order", 0), field_name="display_order")
    binding_revision = _require_text(item.get("binding_revision"), field_name="binding_revision")
    decision_ref = _require_text(item.get("decision_ref"), field_name="decision_ref")

    if drop_kind == "node":
        if not action_value or gate_family is not None:
            raise PostgresWriteError(
                "surface_catalog.invalid_submission",
                "node catalog rows require action_value and must not set gate_family",
                details={"field": "drop_kind", "drop_kind": drop_kind},
            )
    if drop_kind == "edge":
        if not gate_family or action_value is not None:
            raise PostgresWriteError(
                "surface_catalog.invalid_submission",
                "edge catalog rows require gate_family and must not set action_value",
                details={"field": "drop_kind", "drop_kind": drop_kind},
            )

    return {
        "catalog_item_id": catalog_item_id,
        "surface_name": surface_name,
        "label": label,
        "icon": icon,
        "family": family,
        "status": status,
        "drop_kind": drop_kind,
        "action_value": action_value,
        "gate_family": gate_family,
        "description": description,
        "truth_category": truth_category,
        "truth_badge": truth_badge,
        "truth_detail": truth_detail,
        "surface_tier": surface_tier,
        "surface_badge": surface_badge,
        "surface_detail": surface_detail,
        "hard_choice": hard_choice,
        "enabled": enabled,
        "display_order": display_order,
        "binding_revision": binding_revision,
        "decision_ref": decision_ref,
    }


def upsert_surface_catalog_record(conn: Any, *, item: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_item(item)
    row = conn.fetchrow(
        """
        INSERT INTO surface_catalog_registry (
            catalog_item_id,
            surface_name,
            label,
            icon,
            family,
            status,
            drop_kind,
            action_value,
            gate_family,
            description,
            truth_category,
            truth_badge,
            truth_detail,
            surface_tier,
            surface_badge,
            surface_detail,
            hard_choice,
            enabled,
            display_order,
            binding_revision,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
        )
        ON CONFLICT (catalog_item_id) DO UPDATE SET
            surface_name = EXCLUDED.surface_name,
            label = EXCLUDED.label,
            icon = EXCLUDED.icon,
            family = EXCLUDED.family,
            status = EXCLUDED.status,
            drop_kind = EXCLUDED.drop_kind,
            action_value = EXCLUDED.action_value,
            gate_family = EXCLUDED.gate_family,
            description = EXCLUDED.description,
            truth_category = EXCLUDED.truth_category,
            truth_badge = EXCLUDED.truth_badge,
            truth_detail = EXCLUDED.truth_detail,
            surface_tier = EXCLUDED.surface_tier,
            surface_badge = EXCLUDED.surface_badge,
            surface_detail = EXCLUDED.surface_detail,
            hard_choice = EXCLUDED.hard_choice,
            enabled = EXCLUDED.enabled,
            display_order = EXCLUDED.display_order,
            binding_revision = EXCLUDED.binding_revision,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        RETURNING *
        """,
        normalized["catalog_item_id"],
        normalized["surface_name"],
        normalized["label"],
        normalized["icon"],
        normalized["family"],
        normalized["status"],
        normalized["drop_kind"],
        normalized["action_value"],
        normalized["gate_family"],
        normalized["description"],
        normalized["truth_category"],
        normalized["truth_badge"],
        normalized["truth_detail"],
        normalized["surface_tier"],
        normalized["surface_badge"],
        normalized["surface_detail"],
        normalized["hard_choice"],
        normalized["enabled"],
        normalized["display_order"],
        normalized["binding_revision"],
        normalized["decision_ref"],
    )
    return _row_dict(row, operation="upserting surface catalog row")


def load_surface_catalog_record(conn: Any, *, catalog_item_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT * FROM surface_catalog_registry WHERE catalog_item_id = $1",
        _require_text(catalog_item_id, field_name="catalog_item_id"),
    )
    return None if row is None else dict(row)


def list_surface_catalog_records(
    conn: Any,
    *,
    surface_name: str = "canvas",
    include_disabled: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if not isinstance(limit, int) or limit <= 0:
        raise PostgresWriteError(
            "surface_catalog.invalid_submission",
            "limit must be a positive integer",
            details={"field": "limit"},
        )
    params: list[Any] = [_require_text(surface_name, field_name="surface_name")]
    sql = "SELECT * FROM surface_catalog_registry WHERE surface_name = $1"
    if not include_disabled:
        sql += " AND enabled = TRUE"
    params.append(limit)
    sql += f" ORDER BY display_order, catalog_item_id LIMIT ${len(params)}"
    rows = conn.execute(sql, *params)
    return [dict(row) for row in rows]


def retire_surface_catalog_record(conn: Any, *, catalog_item_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "UPDATE surface_catalog_registry SET enabled = FALSE, updated_at = now() "
        "WHERE catalog_item_id = $1 RETURNING *",
        _require_text(catalog_item_id, field_name="catalog_item_id"),
    )
    return None if row is None else dict(row)


__all__ = [
    "list_surface_catalog_records",
    "load_surface_catalog_record",
    "retire_surface_catalog_record",
    "upsert_surface_catalog_record",
]
