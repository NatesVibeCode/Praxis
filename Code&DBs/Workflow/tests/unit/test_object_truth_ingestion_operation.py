from __future__ import annotations

from types import SimpleNamespace

from runtime.operations.commands import object_truth_ingestion as commands
from runtime.operations.queries import object_truth_ingestion as queries


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _command() -> commands.RecordObjectTruthIngestionSampleCommand:
    return commands.RecordObjectTruthIngestionSampleCommand(
        client_ref="client.acme",
        system_ref="salesforce",
        integration_id="integration.salesforce.prod",
        connector_ref="connector.salesforce",
        environment_ref="sandbox",
        object_ref="account",
        schema_snapshot_digest="schema.digest.account",
        captured_at="2026-04-30T16:00:00Z",
        capture_receipt_id="receipt.capture.1",
        auth_context={"tenant": "acme"},
        identity_fields=["id"],
        sample_strategy="fixture",
        source_query={"fields": ["id", "name", "email"]},
        sample_payloads=[
            {"id": "001", "name": "Acme", "email": "owner@example.com"},
        ],
        privacy_classification="confidential",
        retention_policy_ref="retention.object_truth.redacted_hashes",
        observed_by_ref="operator:nate",
    )


def test_ingestion_sample_record_persists_payload_refs_and_event(monkeypatch) -> None:
    object_version_calls: list[dict[str, object]] = []
    sample_calls: list[dict[str, object]] = []

    def _persist_object_version(conn, *, object_version, observed_by_ref=None, source_ref=None):
        object_version_calls.append(
            {
                "object_version": object_version,
                "observed_by_ref": observed_by_ref,
                "source_ref": source_ref,
            }
        )
        return {
            "object_version_ref": f"object_truth_object_version:{object_version['object_version_digest']}",
            "field_observation_count": len(object_version["field_observations"]),
        }

    def _persist_ingestion_sample(conn, **kwargs):
        sample_calls.append(kwargs)
        return {
            "sample_capture": {"sample_id": kwargs["sample_capture"]["sample_id"]},
            "payload_reference_count": len(kwargs["payload_references"]),
            "object_version_count": len(kwargs["object_version_refs"]),
        }

    monkeypatch.setattr(commands, "persist_object_version", _persist_object_version)
    monkeypatch.setattr(commands, "persist_ingestion_sample", _persist_ingestion_sample)

    result = commands.handle_object_truth_ingestion_sample_record(_command(), _subsystems())

    assert result["ok"] is True
    assert result["operation"] == "object_truth_ingestion_sample_record"
    assert result["payload_reference_count"] == 1
    assert len(object_version_calls) == 1
    assert len(sample_calls) == 1
    assert sample_calls[0]["payload_references"][0]["inline_payload_stored"] is False
    assert "raw_payload_json" not in sample_calls[0]["payload_references"][0]["raw_payload_reference_json"]
    assert result["event_payload"]["object_version_count"] == 1
    assert result["event_payload"]["payload_reference_count"] == 1
    assert result["replay_fixture"]["fixture_digest"]


def test_ingestion_sample_record_requires_identity_fields_for_payloads() -> None:
    try:
        commands.RecordObjectTruthIngestionSampleCommand(
            client_ref="client.acme",
            system_ref="salesforce",
            integration_id="integration.salesforce.prod",
            connector_ref="connector.salesforce",
            environment_ref="sandbox",
            object_ref="account",
            schema_snapshot_digest="schema.digest.account",
            captured_at="2026-04-30T16:00:00Z",
            capture_receipt_id="receipt.capture.1",
            auth_context={"tenant": "acme"},
            sample_payloads=[{"id": "001"}],
        )
    except ValueError as exc:
        assert "identity_fields are required" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("payload ingestion should require identity fields")


def test_ingestion_sample_read_lists_and_describes(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_ingestion_samples",
        lambda conn, client_ref=None, system_ref=None, object_ref=None, limit=50: [
            {"sample_id": "sample.1", "client_ref": client_ref, "system_ref": system_ref}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_ingestion_sample",
        lambda conn, sample_id, include_payload_references=True: {
            "sample_id": sample_id,
            "payload_reference_count": 1 if include_payload_references else 0,
        },
    )

    listed = queries.handle_object_truth_ingestion_sample_read(
        queries.QueryObjectTruthIngestionSampleRead(
            action="list",
            client_ref="client.acme",
            system_ref="salesforce",
        ),
        _subsystems(),
    )
    described = queries.handle_object_truth_ingestion_sample_read(
        queries.QueryObjectTruthIngestionSampleRead(
            action="describe",
            sample_id="sample.1",
        ),
        _subsystems(),
    )

    assert listed["count"] == 1
    assert listed["items"][0]["client_ref"] == "client.acme"
    assert described["ok"] is True
    assert described["sample"]["payload_reference_count"] == 1
