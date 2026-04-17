"""File storage helpers for uploaded file persistence."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

from storage.postgres.uploaded_file_repository import PostgresUploadedFileRepository

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
        PostgresUploadedFileRepository(pg).insert_uploaded_file(
            file_id=file_id,
            filename=filename,
            content_type=content_type,
            size_bytes=len(content),
            storage_path=storage_path,
            scope=scope,
            workflow_id=workflow_id,
            step_id=step_id,
            description=description,
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
    query: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List files filtered by scope."""
    return PostgresUploadedFileRepository(pg).list_uploaded_files(
        scope=scope,
        workflow_id=workflow_id,
        step_id=step_id,
        query=query,
        limit=limit,
    )


def get_file_record(pg: Any, file_id: str) -> dict[str, Any] | None:
    """Read one uploaded file metadata row."""
    row = PostgresUploadedFileRepository(pg).load_uploaded_file(file_id=file_id)
    if row is None:
        return None
    return dict(row)


def delete_file(pg: Any, repo_root: str, file_id: str) -> bool:
    """Delete a file from disk and database."""
    repository = PostgresUploadedFileRepository(pg)
    row = repository.delete_uploaded_file(file_id=file_id)
    if row is None:
        return False

    storage_path = row["storage_path"]

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
    row = get_file_record(pg, file_id)
    if row is None:
        return None

    full_path = Path(repo_root) / row["storage_path"]
    if not full_path.is_file():
        logger.warning("uploaded file metadata exists but disk file is missing: %s", full_path)
        return None

    return full_path.read_bytes(), row["content_type"], row["filename"]
