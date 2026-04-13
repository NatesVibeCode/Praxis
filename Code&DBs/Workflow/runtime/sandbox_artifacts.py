"""Sandbox artifact capture and storage.

Postgres-backed store for file artifacts produced within sandboxes,
supporting SHA256 content-addressing, search, and diffing.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection


@dataclass(frozen=True)
class ArtifactRecord:
    artifact_id: str
    file_path: str
    sha256: str
    byte_count: int
    line_count: int
    captured_at: datetime
    sandbox_id: str
    diff_stats: Optional[dict]


class ArtifactStore:
    """Postgres-backed artifact store."""

    def __init__(self, conn: "SyncPostgresConnection") -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def capture(
        self, file_path: str, content: str, sandbox_id: str
    ) -> ArtifactRecord:
        """Compute SHA256, persist content, return record."""
        artifact_id = uuid.uuid4().hex[:16]
        sha = hashlib.sha256(content.encode()).hexdigest()
        byte_count = len(content.encode())
        line_count = content.count("\n") + (1 if content and not content.endswith("\n") else 0)
        now = datetime.now(timezone.utc)

        self._conn.execute(
            "INSERT INTO sandbox_artifacts "
            "(artifact_id, file_path, sha256, byte_count, line_count, captured_at, sandbox_id, content) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            artifact_id, file_path, sha, byte_count, line_count, now, sandbox_id, content,
        )

        return ArtifactRecord(
            artifact_id=artifact_id,
            file_path=file_path,
            sha256=sha,
            byte_count=byte_count,
            line_count=line_count,
            captured_at=now,
            sandbox_id=sandbox_id,
            diff_stats=None,
        )

    def get(self, artifact_id: str) -> Optional[ArtifactRecord]:
        row = self._conn.fetchrow(
            "SELECT * FROM sandbox_artifacts WHERE artifact_id = $1", artifact_id,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    def get_content(self, artifact_id: str) -> Optional[str]:
        row = self._conn.fetchrow(
            "SELECT content FROM sandbox_artifacts WHERE artifact_id = $1",
            artifact_id,
        )
        if row is None:
            return None
        return row["content"]

    def list_by_sandbox(self, sandbox_id: str) -> list[ArtifactRecord]:
        rows = self._conn.execute(
            "SELECT * FROM sandbox_artifacts WHERE sandbox_id = $1 ORDER BY captured_at",
            sandbox_id,
        )
        return [self._row_to_record(r) for r in rows]

    def search(self, query: str, limit: int = 20) -> list[ArtifactRecord]:
        """Search by file_path pattern using SQL LIKE."""
        rows = self._conn.execute(
            "SELECT * FROM sandbox_artifacts WHERE file_path LIKE $1 ORDER BY captured_at DESC LIMIT $2",
            f"%{query}%", limit,
        )
        return [self._row_to_record(r) for r in rows]

    def diff(self, artifact_id_a: str, artifact_id_b: str) -> dict:
        """Compare two artifacts: same_hash and size_delta."""
        a = self.get(artifact_id_a)
        b = self.get(artifact_id_b)
        if a is None or b is None:
            missing = []
            if a is None:
                missing.append(artifact_id_a)
            if b is None:
                missing.append(artifact_id_b)
            return {"error": "not_found", "missing": missing}
        return {
            "same_hash": a.sha256 == b.sha256,
            "size_delta": b.byte_count - a.byte_count,
        }

    def stats(self) -> dict:
        """Total artifacts, total bytes, unique sandboxes."""
        row = self._conn.fetchrow(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(byte_count), 0) as total_bytes, "
            "COUNT(DISTINCT sandbox_id) as sandboxes FROM sandbox_artifacts"
        )
        return {
            "total_artifacts": row["cnt"],
            "total_bytes": row["total_bytes"],
            "unique_sandboxes": row["sandboxes"],
        }

    # ------------------------------------------------------------------
    # internal
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row) -> ArtifactRecord:
        captured = row["captured_at"]
        if isinstance(captured, str):
            captured = datetime.fromisoformat(captured)
        return ArtifactRecord(
            artifact_id=row["artifact_id"],
            file_path=row["file_path"],
            sha256=row["sha256"],
            byte_count=row["byte_count"],
            line_count=row["line_count"],
            captured_at=captured,
            sandbox_id=row["sandbox_id"],
            diff_stats=None,
        )
