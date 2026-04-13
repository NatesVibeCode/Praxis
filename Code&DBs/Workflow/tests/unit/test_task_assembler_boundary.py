from __future__ import annotations

from pathlib import Path

from runtime import task_assembler


class _CloneConn:
    def execute(self, query: str, *args):
        if "SELECT manifest, description FROM app_manifests WHERE id = $1" in query:
            return [
                {
                    "manifest": {"version": 2, "grid": "4x4", "quadrants": {"A1": {"module": "metric"}}},
                    "description": "Template",
                }
            ]
        raise AssertionError(f"unexpected execute query: {query}")


class _NoSqlConn:
    def execute(self, query: str, *args):
        raise AssertionError(f"task assembler should not write SQL directly: {query}")


def test_clone_template_delegates_manifest_write_to_storage_owner(monkeypatch) -> None:
    conn = _CloneConn()
    assembler = task_assembler.TaskAssembler(conn)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        task_assembler,
        "create_app_manifest",
        lambda _conn, **kwargs: captured.update(kwargs) or {"id": kwargs["manifest_id"]},
    )

    manifest_id = assembler._clone_template("template-123", "Support workspace")

    assert manifest_id is not None
    assert captured["manifest_id"] == manifest_id
    assert captured["created_by"] == "task_assembler"
    assert captured["description"] == "Cloned from template-123 for: Support workspace"


def test_execute_plan_delegates_object_and_manifest_writes(monkeypatch) -> None:
    conn = _NoSqlConn()
    assembler = task_assembler.TaskAssembler(conn)
    ensure_calls: list[dict[str, object]] = []
    object_calls: list[dict[str, object]] = []
    manifest_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        task_assembler,
        "ensure_object_type_record",
        lambda _conn, **kwargs: ensure_calls.append(kwargs),
    )
    monkeypatch.setattr(
        task_assembler,
        "create_object_record",
        lambda _conn, **kwargs: object_calls.append(kwargs) or {"object_id": kwargs["object_id"]},
    )
    monkeypatch.setattr(
        task_assembler,
        "create_app_manifest",
        lambda _conn, **kwargs: manifest_calls.append(kwargs) or {"id": kwargs["manifest_id"]},
    )

    manifest_id = assembler._execute_plan(
        task_assembler.AssemblyPlan(
            task="Track incidents",
            object_type={
                "name": "Incident",
                "description": "Incident queue",
                "properties": [{"name": "title", "type": "text"}],
            },
            modules=[
                {
                    "module_id": "data-table",
                    "quadrant": "A1",
                    "span": "2x2",
                    "config": {"objectType": "incident"},
                }
            ],
            seed_records=[{"title": "P1 outage"}],
            explanation="Built workspace for incidents",
        )
    )

    assert manifest_id.startswith("task-")
    assert ensure_calls == [
        {
            "type_id": "incident",
            "name": "Incident",
            "description": "Incident queue",
            "property_definitions": [{"name": "title", "type": "text"}],
        }
    ]
    assert len(object_calls) == 1
    assert object_calls[0]["type_id"] == "incident"
    assert object_calls[0]["properties"] == {"title": "P1 outage"}
    assert manifest_calls == [
        {
            "manifest_id": manifest_id,
            "name": "Track incidents",
            "description": "Built workspace for incidents",
            "manifest": {
                "version": 2,
                "grid": "4x4",
                "quadrants": {
                    "A1": {
                        "module": "data-table",
                        "span": "2x2",
                        "config": {"objectType": "incident"},
                    }
                },
            },
            "created_by": "task_assembler",
            "version": 2,
        }
    ]


def test_task_assembler_no_longer_owns_manifest_or_object_write_sql() -> None:
    source = Path(task_assembler.__file__).read_text(encoding="utf-8")

    forbidden_sql_snippets = (
        "INSERT INTO app_manifests",
        "UPDATE app_manifests",
        "INSERT INTO object_types",
        "INSERT INTO objects",
    )
    leaked = [snippet for snippet in forbidden_sql_snippets if snippet in source]
    assert leaked == [], f"task_assembler.py still owns canonical write SQL: {leaked}"
