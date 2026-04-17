"""Explicit Postgres repository for roadmap authoring authority."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

import asyncpg

from storage.postgres.validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_text,
)


def _commit_timestamp(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "roadmap_authoring.invalid_submission",
                f"{field_name} must be an ISO-8601 datetime string",
                details={"field": field_name, "value": value},
            ) from exc
    elif not isinstance(value, datetime):
        raise PostgresWriteError(
            "roadmap_authoring.invalid_submission",
            f"{field_name} must be a datetime or ISO-8601 string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PostgresWriteError(
            "roadmap_authoring.invalid_submission",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return parsed.astimezone(timezone.utc)


class PostgresRoadmapAuthoringRepository:
    """Write roadmap packages through explicit Postgres authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def record_roadmap_package(
        self,
        *,
        roadmap_items: Sequence[Mapping[str, Any]],
        roadmap_item_dependencies: Sequence[Mapping[str, Any]],
    ) -> dict[str, list[str]]:
        normalized_items: list[Mapping[str, Any]] = []
        for index, item in enumerate(roadmap_items):
            normalized_items.append(
                {
                    "roadmap_item_id": _require_text(
                        item.get("roadmap_item_id"),
                        field_name=f"roadmap_items[{index}].roadmap_item_id",
                    ),
                    "roadmap_key": _require_text(
                        item.get("roadmap_key"),
                        field_name=f"roadmap_items[{index}].roadmap_key",
                    ),
                    "title": _require_text(
                        item.get("title"),
                        field_name=f"roadmap_items[{index}].title",
                    ),
                    "item_kind": _require_text(
                        item.get("item_kind"),
                        field_name=f"roadmap_items[{index}].item_kind",
                    ),
                    "status": _require_text(
                        item.get("status"),
                        field_name=f"roadmap_items[{index}].status",
                    ),
                    "lifecycle": _require_text(
                        item.get("lifecycle"),
                        field_name=f"roadmap_items[{index}].lifecycle",
                    ),
                    "priority": _require_text(
                        item.get("priority"),
                        field_name=f"roadmap_items[{index}].priority",
                    ),
                    "parent_roadmap_item_id": _optional_text(
                        item.get("parent_roadmap_item_id"),
                        field_name=f"roadmap_items[{index}].parent_roadmap_item_id",
                    ),
                    "source_bug_id": _optional_text(
                        item.get("source_bug_id"),
                        field_name=f"roadmap_items[{index}].source_bug_id",
                    ),
                    "registry_paths": item.get("registry_paths"),
                    "summary": _require_text(
                        item.get("summary"),
                        field_name=f"roadmap_items[{index}].summary",
                    ),
                    "acceptance_criteria": item.get("acceptance_criteria"),
                    "decision_ref": _require_text(
                        item.get("decision_ref"),
                        field_name=f"roadmap_items[{index}].decision_ref",
                    ),
                    "created_at": _commit_timestamp(
                        item.get("created_at"),
                        field_name=f"roadmap_items[{index}].created_at",
                    ),
                    "updated_at": _commit_timestamp(
                        item.get("updated_at"),
                        field_name=f"roadmap_items[{index}].updated_at",
                    ),
                },
            )

        normalized_dependencies: list[Mapping[str, Any]] = []
        for index, dependency in enumerate(roadmap_item_dependencies):
            normalized_dependencies.append(
                {
                    "roadmap_item_dependency_id": _require_text(
                        dependency.get("roadmap_item_dependency_id"),
                        field_name=f"roadmap_item_dependencies[{index}].roadmap_item_dependency_id",
                    ),
                    "roadmap_item_id": _require_text(
                        dependency.get("roadmap_item_id"),
                        field_name=f"roadmap_item_dependencies[{index}].roadmap_item_id",
                    ),
                    "depends_on_roadmap_item_id": _require_text(
                        dependency.get("depends_on_roadmap_item_id"),
                        field_name=f"roadmap_item_dependencies[{index}].depends_on_roadmap_item_id",
                    ),
                    "dependency_kind": _require_text(
                        dependency.get("dependency_kind"),
                        field_name=f"roadmap_item_dependencies[{index}].dependency_kind",
                    ),
                    "decision_ref": _require_text(
                        dependency.get("decision_ref"),
                        field_name=f"roadmap_item_dependencies[{index}].decision_ref",
                    ),
                    "created_at": _commit_timestamp(
                        dependency.get("created_at"),
                        field_name=f"roadmap_item_dependencies[{index}].created_at",
                    ),
                },
            )

        try:
            async with self._conn.transaction():
                for item in normalized_items:
                    await self._conn.execute(
                        """
                        INSERT INTO roadmap_items (
                            roadmap_item_id,
                            roadmap_key,
                            title,
                            item_kind,
                            status,
                            lifecycle,
                            priority,
                            parent_roadmap_item_id,
                            source_bug_id,
                            registry_paths,
                            summary,
                            acceptance_criteria,
                            decision_ref,
                            target_start_at,
                            target_end_at,
                            completed_at,
                            created_at,
                            updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12::jsonb, $13, NULL, NULL, NULL, $14, $15
                        )
                        ON CONFLICT (roadmap_item_id) DO UPDATE SET
                            roadmap_key = EXCLUDED.roadmap_key,
                            title = EXCLUDED.title,
                            item_kind = EXCLUDED.item_kind,
                            status = EXCLUDED.status,
                            lifecycle = EXCLUDED.lifecycle,
                            priority = EXCLUDED.priority,
                            parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
                            source_bug_id = EXCLUDED.source_bug_id,
                            registry_paths = EXCLUDED.registry_paths,
                            summary = EXCLUDED.summary,
                            acceptance_criteria = EXCLUDED.acceptance_criteria,
                            decision_ref = EXCLUDED.decision_ref,
                            updated_at = EXCLUDED.updated_at
                        """,
                        item["roadmap_item_id"],
                        item["roadmap_key"],
                        item["title"],
                        item["item_kind"],
                        item["status"],
                        item["lifecycle"],
                        item["priority"],
                        item["parent_roadmap_item_id"],
                        item["source_bug_id"],
                        _encode_jsonb(
                            item["registry_paths"],
                            field_name="roadmap_items.registry_paths",
                        ),
                        item["summary"],
                        _encode_jsonb(
                            item["acceptance_criteria"],
                            field_name="roadmap_items.acceptance_criteria",
                        ),
                        item["decision_ref"],
                        item["created_at"],
                        item["updated_at"],
                    )

                for dependency in normalized_dependencies:
                    await self._conn.execute(
                        """
                        INSERT INTO roadmap_item_dependencies (
                            roadmap_item_dependency_id,
                            roadmap_item_id,
                            depends_on_roadmap_item_id,
                            dependency_kind,
                            decision_ref,
                            created_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6
                        )
                        ON CONFLICT (roadmap_item_dependency_id) DO UPDATE SET
                            roadmap_item_id = EXCLUDED.roadmap_item_id,
                            depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
                            dependency_kind = EXCLUDED.dependency_kind,
                            decision_ref = EXCLUDED.decision_ref
                        """,
                        dependency["roadmap_item_dependency_id"],
                        dependency["roadmap_item_id"],
                        dependency["depends_on_roadmap_item_id"],
                        dependency["dependency_kind"],
                        dependency["decision_ref"],
                        dependency["created_at"],
                    )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "roadmap_authoring.write_failed",
                "failed to write roadmap package",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        return {
            "roadmap_item_ids": [
                str(item["roadmap_item_id"])
                for item in normalized_items
            ],
            "roadmap_item_dependency_ids": [
                str(dependency["roadmap_item_dependency_id"])
                for dependency in normalized_dependencies
            ],
        }


__all__ = ["PostgresRoadmapAuthoringRepository"]
