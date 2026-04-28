from __future__ import annotations

from runtime.operations.commands import object_truth
from runtime.operations.commands.object_truth import (
    StoreObservedRecordCommand,
    handle_store_observed_record,
)


class _Subsystems:
    def get_pg_conn(self) -> object:
        return object()


def test_store_observed_record_command_persists_object_version(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _persist(conn, *, object_version, observed_by_ref, source_ref):
        captured["conn"] = conn
        captured["object_version"] = object_version
        captured["observed_by_ref"] = observed_by_ref
        captured["source_ref"] = source_ref
        return {
            "object_version_ref": f"object_truth_object_version:{object_version['object_version_digest']}",
            "field_observation_count": len(object_version["field_observations"]),
        }

    monkeypatch.setattr(object_truth, "persist_object_version", _persist)

    command = StoreObservedRecordCommand(
        system_ref=" salesforce ",
        object_ref="account",
        record={"id": "001", "name": "Acme", "billing": {"city": "Denver"}},
        identity_fields=["id"],
        source_metadata={"updated_at": "2026-04-28T10:00:00Z"},
        observed_by_ref="operator:nate",
        source_ref="sample:accounts:001",
    )

    result = handle_store_observed_record(command, _Subsystems())

    assert result["ok"] is True
    assert result["operation"] == "object_truth_store_observed_record"
    assert result["field_observation_count"] == 4
    assert result["event_payload"]["system_ref"] == "salesforce"
    assert result["event_payload"]["observed_by_ref"] == "operator:nate"
    assert captured["observed_by_ref"] == "operator:nate"
    assert captured["source_ref"] == "sample:accounts:001"
    assert captured["object_version"]["identity"]["identity_values"] == {"id": "001"}
