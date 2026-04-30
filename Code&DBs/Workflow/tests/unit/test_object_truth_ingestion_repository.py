from __future__ import annotations

from storage.postgres import object_truth_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "object_truth_system_snapshots" in sql:
            return {
                "system_snapshot_id": args[0],
                "system_snapshot_digest": args[1],
                "client_ref": args[2],
                "system_ref": args[3],
                "metadata_json": args[12],
            }
        if "object_truth_sample_captures" in sql:
            return {
                "sample_id": args[0],
                "system_snapshot_id": args[1],
                "sample_capture_digest": args[2],
                "metadata_json": args[16],
                "replay_fixture_json": args[18],
                "object_version_refs_json": args[19],
            }
        return None

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))
        return []

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def test_persist_ingestion_sample_writes_snapshot_sample_and_payload_refs() -> None:
    conn = _RecordingConn()

    result = repo.persist_ingestion_sample(
        conn,
        system_snapshot={
            "system_snapshot_id": "snapshot.1",
            "system_snapshot_digest": "snapshot.digest",
            "client_ref": "client.acme",
            "system_ref": "salesforce",
            "integration_id": "integration.salesforce.prod",
            "connector_ref": "connector.salesforce",
            "environment_ref": "sandbox",
            "auth_context_hash": "auth.digest",
            "captured_at": "2026-04-30T16:00:00Z",
            "capture_receipt_id": "receipt.capture.1",
            "schema_snapshot_count": 1,
            "sample_count": 1,
            "metadata_json": {"region": "us"},
        },
        sample_capture={
            "sample_id": "sample.1",
            "sample_capture_digest": "sample.digest",
            "schema_snapshot_digest": "schema.digest",
            "system_ref": "salesforce",
            "object_ref": "account",
            "sample_strategy": "fixture",
            "source_query_json": {"query": {"fields": ["id"]}},
            "sample_size_requested": 1,
            "sample_size_returned": 1,
            "sample_hash": "sample.hash",
            "status": "captured",
            "source_window_json": {},
            "metadata_json": {},
        },
        payload_references=[
            {
                "payload_index": 0,
                "external_record_id": "001",
                "source_metadata_digest": "metadata.digest",
                "raw_payload_ref": "vault://raw/001",
                "raw_payload_hash": "raw.hash",
                "normalized_payload_hash": "normalized.hash",
                "privacy_classification": "confidential",
                "retention_policy_ref": "retention.object_truth.redacted_hashes",
                "reference_digest": "reference.digest",
                "redacted_preview_digest": "preview.digest",
                "source_metadata_json": {"external_record_id": "001"},
                "redacted_preview_json": {"preview_digest": "preview.digest"},
                "raw_payload_reference_json": {"reference_digest": "reference.digest"},
            }
        ],
        object_version_refs=[{"object_version_digest": "version.digest"}],
        replay_fixture={"fixture_digest": "fixture.digest"},
    )

    assert "INSERT INTO object_truth_system_snapshots" in conn.fetchrow_calls[0][0]
    assert "INSERT INTO object_truth_sample_captures" in conn.fetchrow_calls[1][0]
    assert "DELETE FROM object_truth_raw_payload_references" in conn.execute_calls[0][0]
    assert "INSERT INTO object_truth_raw_payload_references" in conn.batch_calls[0][0]
    assert result["payload_reference_count"] == 1
    assert result["sample_capture"]["object_version_refs_json"] == [
        {"object_version_digest": "version.digest"}
    ]
