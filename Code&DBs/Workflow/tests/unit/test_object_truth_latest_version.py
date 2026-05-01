from __future__ import annotations

from datetime import datetime, timedelta, timezone

from runtime.operations.queries.object_truth_latest import (
    QueryObjectTruthLatestVersionRead,
    handle_object_truth_latest_version_read,
)
from storage.postgres.object_truth_repository import load_latest_object_truth_version


class _LatestVersionConn:
    def __init__(self, rows):
        self.rows = rows
        self.args = None

    def fetch(self, query: str, *args):
        assert "FROM object_truth_object_versions" in query
        self.args = args
        return self.rows


class _Subsystems:
    def __init__(self, conn) -> None:
        self.conn = conn

    def get_pg_conn(self):
        return self.conn


def _row(
    *,
    digest: str,
    payload_digest: str,
    updated_at: datetime,
    evidence_tier: str = "verified",
):
    return {
        "object_version_digest": digest,
        "object_version_ref": f"object_truth_object_version:{digest}",
        "system_ref": "Salesforce",
        "object_ref": "Account",
        "identity_digest": "identity:1",
        "payload_digest": payload_digest,
        "schema_snapshot_digest": "schema:1",
        "source_metadata_json": {"evidence_tier": evidence_tier, "client_ref": "client:1"},
        "hierarchy_signals_json": {},
        "object_version_json": {
            "object_version_digest": digest,
            "system_ref": "Salesforce",
            "object_ref": "Account",
            "identity": {"identity_digest": "identity:1"},
        },
        "observed_by_ref": "test",
        "source_ref": "unit",
        "created_at": updated_at,
        "updated_at": updated_at,
    }


def test_latest_object_truth_version_returns_fresh_latest_version() -> None:
    now = datetime.now(timezone.utc)
    result = load_latest_object_truth_version(
        _LatestVersionConn([
            _row(digest="v2", payload_digest="payload:2", updated_at=now),
        ]),
        system_ref="Salesforce",
        object_ref="Account",
        identity_digest="identity:1",
        max_age_seconds=3600,
    )

    assert result["state"] == "ready"
    assert result["version"]["object_version_digest"] == "v2"
    assert result["freshness"]["state"] == "fresh"
    assert result["no_go_states"] == []


def test_latest_object_truth_version_reports_stale_and_conflict() -> None:
    old = datetime.now(timezone.utc) - timedelta(days=2)
    result = load_latest_object_truth_version(
        _LatestVersionConn([
            _row(digest="v2", payload_digest="payload:2", updated_at=old),
            _row(digest="v1", payload_digest="payload:1", updated_at=old),
        ]),
        system_ref="Salesforce",
        object_ref="Account",
        identity_digest="identity:1",
        max_age_seconds=60,
    )

    assert result["state"] == "blocked"
    assert result["freshness"]["state"] == "stale"
    assert set(result["no_go_states"]) == {"stale", "conflict"}
    assert result["conflicts"][0]["conflict_type"] == "multiple_payload_digests"


def test_latest_object_truth_version_query_handler() -> None:
    conn = _LatestVersionConn([
        _row(
            digest="v2",
            payload_digest="payload:2",
            updated_at=datetime.now(timezone.utc),
        )
    ])

    result = handle_object_truth_latest_version_read(
        QueryObjectTruthLatestVersionRead(system_ref="Salesforce", object_ref="Account"),
        _Subsystems(conn),
    )

    assert result["ok"] is True
    assert result["operation"] == "object_truth_latest_version_read"
    assert result["version"]["object_version_digest"] == "v2"
