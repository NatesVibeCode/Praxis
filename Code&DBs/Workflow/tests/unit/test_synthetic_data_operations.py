from __future__ import annotations

from runtime.operations.commands import synthetic_data as commands
from runtime.operations.queries import synthetic_data as queries


class _Subsystems:
    def get_pg_conn(self):
        return object()


def test_synthetic_data_generate_persists_dataset_and_event_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _persist(conn, *, dataset, observed_by_ref=None, source_ref=None):
        captured["conn"] = conn
        captured["dataset"] = dataset
        captured["observed_by_ref"] = observed_by_ref
        captured["source_ref"] = source_ref
        return {**dataset, "records": dataset["records"][:5]}

    monkeypatch.setattr(commands, "persist_synthetic_dataset", _persist)

    result = commands.handle_synthetic_data_generate(
        commands.GenerateSyntheticDataCommand(
            intent="Renewal risk data",
            namespace="ops",
            scenario_pack_refs=["renewal_risk"],
            object_counts={"Account": 10, "Ticket": 10},
            seed="operation-seed",
            observed_by_ref="test",
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "synthetic_data_generate"
    assert result["dataset_ref"].startswith("synthetic_dataset:ops:")
    assert result["event_payload"]["quality_state"] == "accepted"
    assert result["event_payload"]["record_count"] == 20
    assert captured["observed_by_ref"] == "test"


def test_synthetic_data_read_lists_describes_and_lists_records(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_synthetic_datasets",
        lambda conn, namespace=None, source_context_ref=None, quality_state=None, limit=50: [
            {"dataset_ref": "synthetic_dataset.demo", "namespace": namespace, "quality_state": quality_state}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_synthetic_dataset",
        lambda conn, dataset_ref, include_records=True, limit=500: {
            "dataset_ref": dataset_ref,
            "records": [{"record_ref": "synthetic_record.demo"}] if include_records else [],
        },
    )
    monkeypatch.setattr(
        queries,
        "list_synthetic_records",
        lambda conn, dataset_ref, object_kind=None, limit=500: [
            {"dataset_ref": dataset_ref, "object_kind": object_kind or "Account"}
        ],
    )

    listed = queries.handle_synthetic_data_read(
        queries.QuerySyntheticDataRead(namespace="demo", quality_state="accepted"),
        _Subsystems(),
    )
    described = queries.handle_synthetic_data_read(
        queries.QuerySyntheticDataRead(action="describe_dataset", dataset_ref="synthetic_dataset.demo"),
        _Subsystems(),
    )
    records = queries.handle_synthetic_data_read(
        queries.QuerySyntheticDataRead(action="list_records", dataset_ref="synthetic_dataset.demo", object_kind="Account"),
        _Subsystems(),
    )

    assert listed["count"] == 1
    assert described["dataset"]["dataset_ref"] == "synthetic_dataset.demo"
    assert records["records"] == [{"dataset_ref": "synthetic_dataset.demo", "object_kind": "Account"}]
