"""Explicit sync Postgres repository for uploaded-file metadata."""

from __future__ import annotations

from typing import Any

from .validators import _require_text


def _row_dict(row: Any) -> dict[str, Any] | None:
    return None if row is None else dict(row)


class PostgresUploadedFileRepository:
    """Owns canonical uploaded_files metadata reads and writes."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def _fetchrow_compat(self, query: str, *args: Any) -> Any:
        if hasattr(self._conn, "fetchrow"):
            return self._conn.fetchrow(query, *args)
        rows = self._conn.execute(query, *args)
        return rows[0] if rows else None

    def insert_uploaded_file(
        self,
        *,
        file_id: str,
        filename: str,
        content_type: str,
        size_bytes: int,
        storage_path: str,
        scope: str,
        workflow_id: str | None = None,
        step_id: str | None = None,
        description: str = "",
    ) -> None:
        self._conn.execute(
            """INSERT INTO uploaded_files (
                   id, filename, content_type, size_bytes, storage_path,
                   scope, workflow_id, step_id, description
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
            _require_text(file_id, field_name="file_id"),
            _require_text(filename, field_name="filename"),
            _require_text(content_type, field_name="content_type"),
            int(size_bytes),
            _require_text(storage_path, field_name="storage_path"),
            _require_text(scope, field_name="scope"),
            workflow_id,
            step_id,
            str(description or ""),
        )

    def list_uploaded_files(
        self,
        *,
        scope: str | None = None,
        workflow_id: str | None = None,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        index = 1

        if scope:
            conditions.append(f"scope = ${index}")
            params.append(_require_text(scope, field_name="scope"))
            index += 1
        if workflow_id:
            conditions.append(f"workflow_id = ${index}")
            params.append(_require_text(workflow_id, field_name="workflow_id"))
            index += 1
        if step_id:
            conditions.append(f"step_id = ${index}")
            params.append(_require_text(step_id, field_name="step_id"))
            index += 1

        where = " AND ".join(conditions) if conditions else "1=1"
        rows = self._conn.execute(
            f"""SELECT id, filename, content_type, size_bytes, scope, workflow_id,
                      step_id, description, created_at
               FROM uploaded_files
               WHERE {where}
               ORDER BY created_at DESC
               LIMIT 100""",
            *params,
        )
        result: list[dict[str, Any]] = []
        for row in rows or []:
            item = dict(row)
            if item.get("created_at") is not None:
                item["created_at"] = item["created_at"].isoformat()
            result.append(item)
        return result

    def load_uploaded_file(self, *, file_id: str) -> dict[str, Any] | None:
        row = self._fetchrow_compat(
            "SELECT storage_path, content_type, filename FROM uploaded_files WHERE id = $1",
            _require_text(file_id, field_name="file_id"),
        )
        return _row_dict(row)

    def delete_uploaded_file(self, *, file_id: str) -> dict[str, Any] | None:
        row = self._fetchrow_compat(
            "DELETE FROM uploaded_files WHERE id = $1 RETURNING storage_path",
            _require_text(file_id, field_name="file_id"),
        )
        return _row_dict(row)


__all__ = ["PostgresUploadedFileRepository"]
