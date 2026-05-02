"""Canonical repo snapshot authority for receipt and proof provenance."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime.materialize_index import current_repo_fingerprint


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def repo_snapshot_ref(repo_root: str | Path, repo_fingerprint: str) -> str:
    resolved_root = str(Path(repo_root).resolve())
    digest = hashlib.sha1(
        f"{resolved_root}|{repo_fingerprint}".encode("utf-8")
    ).hexdigest()[:16]
    return f"repo_snapshot:{digest}"


def record_repo_snapshot(
    *,
    conn,
    workspace_root: str | Path,
    workspace_ref: str | None = None,
    runtime_profile_ref: str | None = None,
    packet_provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Upsert one canonical repo snapshot row and return a compact receipt payload."""

    repo_info = current_repo_fingerprint(workspace_root)
    resolved_root = str(Path(repo_info["repo_root"]).resolve())
    snapshot_ref = repo_snapshot_ref(resolved_root, str(repo_info["repo_fingerprint"]))
    now = _utc_now()
    safe_packet = _json_safe(packet_provenance or {})
    row = conn.fetchrow(
        """
        INSERT INTO repo_snapshots (
            repo_snapshot_ref,
            repo_root,
            repo_fingerprint,
            git_head,
            git_branch,
            git_dirty,
            git_status_hash,
            workspace_ref,
            runtime_profile_ref,
            packet_provenance,
            created_at,
            last_seen_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12
        )
        ON CONFLICT (repo_snapshot_ref) DO UPDATE
        SET workspace_ref = COALESCE(EXCLUDED.workspace_ref, repo_snapshots.workspace_ref),
            runtime_profile_ref = COALESCE(EXCLUDED.runtime_profile_ref, repo_snapshots.runtime_profile_ref),
            packet_provenance = CASE
                WHEN EXCLUDED.packet_provenance = '{}'::jsonb THEN repo_snapshots.packet_provenance
                ELSE EXCLUDED.packet_provenance
            END,
            last_seen_at = EXCLUDED.last_seen_at
        RETURNING repo_snapshot_ref, repo_root, repo_fingerprint, git_dirty, last_seen_at
        """,
        snapshot_ref,
        resolved_root,
        str(repo_info["repo_fingerprint"]),
        str(repo_info["git_head"]),
        str(repo_info["git_branch"]),
        bool(repo_info["git_dirty"]),
        str(repo_info["git_status_hash"]),
        str(workspace_ref or "") or None,
        str(runtime_profile_ref or "") or None,
        json.dumps(safe_packet, sort_keys=True, default=str),
        now,
        now,
    )
    compact = {
        "available": True,
        "repo_snapshot_ref": snapshot_ref,
        "repo_fingerprint": str(repo_info["repo_fingerprint"]),
        "git_dirty": bool(repo_info["git_dirty"]),
        "captured_at": (
            row["last_seen_at"].isoformat()
            if row and row.get("last_seen_at") is not None
            else now.isoformat()
        ),
    }
    return compact


__all__ = ["record_repo_snapshot", "repo_snapshot_ref"]
