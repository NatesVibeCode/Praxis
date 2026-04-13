from __future__ import annotations

from datetime import datetime, timezone

import runtime.repo_snapshot_store as repo_snapshot_store


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        return {
            "repo_snapshot_ref": args[0],
            "repo_root": args[1],
            "repo_fingerprint": args[2],
            "git_dirty": args[5],
            "last_seen_at": datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
        }


def test_record_repo_snapshot_returns_compact_receipt_payload(monkeypatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(
        repo_snapshot_store,
        "current_repo_fingerprint",
        lambda workspace_root: {
            "repo_root": "/repo",
            "repo_fingerprint": "fp-123",
            "git_head": "head-123",
            "git_branch": "main",
            "git_dirty": True,
            "git_status_hash": "status-123",
        },
    )

    payload = repo_snapshot_store.record_repo_snapshot(
        conn=conn,
        workspace_root="/repo",
        workspace_ref="praxis",
        runtime_profile_ref="praxis",
        packet_provenance={"source_kind": "test"},
    )

    assert payload["available"] is True
    assert payload["repo_snapshot_ref"].startswith("repo_snapshot:")
    assert payload["repo_fingerprint"] == "fp-123"
    assert payload["git_dirty"] is True
    assert payload["captured_at"] == "2026-04-09T12:00:00+00:00"
    assert "workspace_ref" not in payload
    assert "runtime_profile_ref" not in payload
    assert len(conn.calls) == 1
