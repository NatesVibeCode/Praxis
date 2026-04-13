"""File storage helpers for uploaded file persistence."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

UPLOAD_DIR = "artifacts/uploads"


def ensure_upload_dir(repo_root: str) -> Path:
    """Create uploads directory if needed."""
    directory = Path(repo_root) / UPLOAD_DIR
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def save_file(
    pg: Any,
    repo_root: str,
    filename: str,
    content: bytes,
    content_type: str = "application/octet-stream",
    scope: str = "instance",
    workflow_id: str | None = None,
    step_id: str | None = None,
    description: str = "",
) -> dict[str, Any]:
    """Save a file to disk and record it in the database."""
    file_id = f"file_{uuid.uuid4().hex[:12]}"
    extension = Path(filename).suffix or ""
    storage_name = f"{file_id}{extension}"

    upload_dir = ensure_upload_dir(repo_root)
    storage_path = str(Path(UPLOAD_DIR) / storage_name)
    full_path = upload_dir / storage_name
    full_path.write_bytes(content)

    try:
        pg.execute(
            """INSERT INTO uploaded_files (
                   id, filename, content_type, size_bytes, storage_path,
                   scope, workflow_id, step_id, description
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
            file_id,
            filename,
            content_type,
            len(content),
            storage_path,
            scope,
            workflow_id,
            step_id,
            description,
        )
    except Exception:
        # Keep disk and DB state aligned when the metadata write fails.
        if full_path.exists():
            full_path.unlink()
        raise

    return {
        "id": file_id,
        "filename": filename,
        "content_type": content_type,
        "size_bytes": len(content),
        "scope": scope,
        "storage_path": storage_path,
    }


def list_files(
    pg: Any,
    scope: str | None = None,
    workflow_id: str | None = None,
    step_id: str | None = None,
) -> list[dict[str, Any]]:
    """List files filtered by scope."""
    conditions: list[str] = []
    params: list[Any] = []
    index = 1

    if scope:
        conditions.append(f"scope = ${index}")
        params.append(scope)
        index += 1
    if workflow_id:
        conditions.append(f"workflow_id = ${index}")
        params.append(workflow_id)
        index += 1
    if step_id:
        conditions.append(f"step_id = ${index}")
        params.append(step_id)
        index += 1

    where = " AND ".join(conditions) if conditions else "1=1"
    rows = pg.execute(
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


def delete_file(pg: Any, repo_root: str, file_id: str) -> bool:
    """Delete a file from disk and database."""
    rows = pg.execute("SELECT storage_path FROM uploaded_files WHERE id = $1", file_id)
    if not rows:
        return False

    storage_path = rows[0]["storage_path"]
    pg.execute("DELETE FROM uploaded_files WHERE id = $1", file_id)

    full_path = Path(repo_root) / storage_path
    if full_path.is_file():
        full_path.unlink()
    else:
        logger.warning("uploaded file missing on disk: %s", full_path)
    return True


def get_file_content(
    pg: Any,
    repo_root: str,
    file_id: str,
) -> tuple[bytes, str, str] | None:
    """Read file content and metadata."""
    rows = pg.execute(
        "SELECT storage_path, content_type, filename FROM uploaded_files WHERE id = $1",
        file_id,
    )
    if not rows:
        return None

    row = rows[0]
    full_path = Path(repo_root) / row["storage_path"]
    if not full_path.is_file():
        logger.warning("uploaded file metadata exists but disk file is missing: %s", full_path)
        return None

    return full_path.read_bytes(), row["content_type"], row["filename"]
